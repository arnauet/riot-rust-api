use anyhow::Result;
use polars::lazy::dsl::count;
use polars::prelude::*;
use std::fs::{self, File};
use std::path::Path;

pub struct PlayerProfileArgs<'a> {
    pub player_parquet: &'a Path,
    pub out_parquet: &'a Path,
    pub history_size: usize,
    pub min_matches: usize,
}

pub fn build_player_profiles(args: PlayerProfileArgs) -> Result<()> {
    let mut df = LazyFrame::scan_parquet(
        args.player_parquet.to_string_lossy().to_string(),
        ScanArgsParquet::default(),
    )?
    .collect()?;

    ensure_column(
        &mut df,
        "earlyLaningPhaseGoldExpAdvantage",
        DataType::Float64,
    )?;
    ensure_column(&mut df, "laningPhaseGoldExpAdvantage", DataType::Float64)?;
    ensure_column(&mut df, "maxCsAdvantageOnLaneOpponent", DataType::Float64)?;
    ensure_column(
        &mut df,
        "visionScoreAdvantageLaneOpponent",
        DataType::Float64,
    )?;

    let allowed_roles = Series::new(
        "roles_filter",
        vec!["TOP", "JUNGLE", "MIDDLE", "BOTTOM", "UTILITY"],
    );

    let base = df
        .lazy()
        .filter(col("queue_id").eq(lit(420)))
        .filter(col("role").is_in(lit(allowed_roles)))
        .with_column(
            when(col("team_id").eq(lit(100)))
                .then(lit(200))
                .otherwise(lit(100))
                .alias("opp_team_id"),
        );

    let opponents = base.clone().select([
        col("match_id"),
        col("role"),
        col("team_id"),
        col("gold_earned").alias("opp_gold_earned"),
        col("total_cs").alias("opp_total_cs"),
        col("vision_score").alias("opp_vision_score"),
    ]);

    let with_opponent = base
        .join(
            opponents,
            [col("match_id"), col("role"), col("opp_team_id")],
            [col("match_id"), col("role"), col("team_id")],
            JoinArgs::new(JoinType::Left),
        )
        .with_columns([
            (col("gold_earned") - col("opp_gold_earned")).alias("gold_diff_vs_lane"),
            (col("total_cs") - col("opp_total_cs")).alias("cs_diff_vs_lane"),
            (col("vision_score") - col("opp_vision_score")).alias("vision_diff_vs_lane"),
            col("earlyLaningPhaseGoldExpAdvantage").alias("early_gold_xp_adv"),
            col("laningPhaseGoldExpAdvantage").alias("laning_gold_xp_adv"),
            col("maxCsAdvantageOnLaneOpponent").alias("max_cs_adv_lane"),
            col("visionScoreAdvantageLaneOpponent").alias("vision_score_adv_lane"),
        ])
        .with_columns([
            col("game_creation")
                .rank(RankOptions {
                    method: RankMethod::Dense,
                    descending: true,
                    ..Default::default()
                })
                .over([col("puuid"), col("role")])
                .alias("recent_rank"),
            count()
                .over([col("puuid"), col("role")])
                .alias("games_available"),
        ])
        .filter(col("recent_rank").le(lit(args.history_size as u32)))
        .with_column(
            count()
                .over([col("puuid"), col("role")])
                .alias("games_used"),
        );

    let aggregated = with_opponent
        .group_by([col("puuid"), col("role")])
        .agg([
            col("games_available")
                .max()
                .cast(DataType::Int32)
                .alias("games_available"),
            col("games_used")
                .max()
                .cast(DataType::Int32)
                .alias("games_used"),
            col("champion_name").first().alias("main_champion_name"),
            col("win").cast(DataType::Float64).mean().alias("win_rate"),
            col("kills").mean().alias("avg_kills"),
            col("deaths").mean().alias("avg_deaths"),
            col("assists").mean().alias("avg_assists"),
            ((col("kills") + col("assists"))
                / when(col("deaths").eq(lit(0)))
                    .then(lit(1))
                    .otherwise(col("deaths")))
            .mean()
            .alias("avg_kda"),
            col("gold_earned").mean().alias("avg_gold_earned"),
            col("gold_per_min").mean().alias("avg_gold_per_min"),
            col("damage_to_champions")
                .mean()
                .alias("avg_damage_to_champions"),
            col("damage_per_min").mean().alias("avg_damage_per_min"),
            col("total_cs").mean().alias("avg_total_cs"),
            col("lane_minions_first10").mean().alias("avg_cs10"),
            col("vision_score").mean().alias("avg_vision_score"),
            col("vision_score_per_min")
                .mean()
                .alias("avg_vision_score_per_min"),
            col("turret_takedowns").mean().alias("avg_turret_takedowns"),
            col("inhibitor_takedowns")
                .mean()
                .alias("avg_inhibitor_takedowns"),
            col("gold_diff_vs_lane")
                .mean()
                .alias("avg_gold_diff_vs_lane"),
            col("cs_diff_vs_lane").mean().alias("avg_cs_diff_vs_lane"),
            col("vision_diff_vs_lane")
                .mean()
                .alias("avg_vision_diff_vs_lane"),
            col("early_gold_xp_adv")
                .mean()
                .alias("avg_early_gold_xp_adv"),
            col("laning_gold_xp_adv")
                .mean()
                .alias("avg_laning_gold_xp_adv"),
            col("max_cs_adv_lane").mean().alias("avg_max_cs_adv_lane"),
            col("vision_score_adv_lane")
                .mean()
                .alias("avg_vision_score_adv_lane"),
        ])
        .filter(col("games_used").ge(lit(args.min_matches as i32)));

    let mut result = aggregated.collect()?;

    if let Some(parent) = args.out_parquet.parent() {
        if !parent.as_os_str().is_empty() {
            fs::create_dir_all(parent)?;
        }
    }

    let mut file = File::create(args.out_parquet)?;
    ParquetWriter::new(&mut file).finish(&mut result)?;

    println!(
        "Built {} player profiles (history_size={}, min_matches={})",
        result.height(),
        args.history_size,
        args.min_matches
    );

    Ok(())
}

fn ensure_column(df: &mut DataFrame, name: &str, dtype: DataType) -> Result<()> {
    if !df.get_column_names().iter().any(|c| *c == name) {
        let series = Series::full_null(name, df.height(), &dtype);
        df.with_column(series)?;
    }
    Ok(())
}
