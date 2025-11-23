use std::fs;
use std::path::{Path, PathBuf};

use anyhow::{Result, anyhow};
use polars::prelude::*;

pub fn kraken_prepare_ml_dispatch(
    variant: &str,
    player_parquet: Option<PathBuf>,
    team_parquet: Option<PathBuf>,
    out_dir: &Path,
    history_size: usize,
    min_matches: usize,
) -> Result<()> {
    fs::create_dir_all(out_dir)?;

    match variant {
        "team-outcome" => {
            let Some(team_path) = team_parquet else {
                return Err(anyhow!("--team-parquet is required for team-outcome"));
            };
            kraken_build_ml_team_outcome(&team_path, out_dir)
        }
        "player-profile-only" => {
            let Some(player_path) = player_parquet else {
                return Err(anyhow!(
                    "--player-parquet is required for player-profile-only"
                ));
            };
            kraken_build_player_profile(&player_path, out_dir, history_size, min_matches)
        }
        "lobby-outcome" => {
            let Some(player_path) = player_parquet else {
                return Err(anyhow!("--player-parquet is required for lobby-outcome"));
            };
            let Some(team_path) = team_parquet else {
                return Err(anyhow!("--team-parquet is required for lobby-outcome"));
            };
            let profile_path = out_dir.join("player_profile.parquet");
            let profile_opt = if profile_path.exists() {
                Some(profile_path)
            } else {
                None
            };
            kraken_build_ml_lobby_outcome(&player_path, &team_path, profile_opt.as_deref(), out_dir)
        }
        _ => Err(anyhow!("Unknown variant: {}", variant)),
    }
}

pub fn kraken_build_player_profile(
    player_parquet: &Path,
    out_dir: &Path,
    history_size: usize,
    min_matches: usize,
) -> Result<()> {
    let lf = LazyFrame::scan_parquet(player_parquet, Default::default())?;

    let duration_minutes = col("game_duration").cast(DataType::Float64) / lit(60.0);

    let with_features = lf
        .filter(col("queue_id").eq(lit(420i32)))
        .with_columns([
            duration_minutes.clone().alias("game_duration_minutes"),
            (col("total_cs").cast(DataType::Float64) / duration_minutes.clone())
                .alias("cs_per_min"),
            col("game_creation")
                .rank(
                    RankOptions {
                        method: RankMethod::Dense,
                        descending: true,
                        ..Default::default()
                    },
                    None,
                )
                .over([col("puuid"), col("role")])
                .alias("recent_rank"),
        ])
        .filter(col("recent_rank").le(lit(history_size as u32)));

    let aggregated = with_features
        .group_by([col("puuid"), col("role")])
        .agg([
            len().alias("games_used"),
            col("win")
                .cast(DataType::Float64)
                .mean()
                .alias("recent_winrate"),
            col("kills")
                .cast(DataType::Float64)
                .mean()
                .alias("recent_avg_kills"),
            col("deaths")
                .cast(DataType::Float64)
                .mean()
                .alias("recent_avg_deaths"),
            col("assists")
                .cast(DataType::Float64)
                .mean()
                .alias("recent_avg_assists"),
            col("gold_per_min")
                .cast(DataType::Float64)
                .mean()
                .alias("recent_avg_gold_per_min"),
            col("damage_per_min")
                .cast(DataType::Float64)
                .mean()
                .alias("recent_avg_damage_per_min"),
            col("vision_score_per_min")
                .cast(DataType::Float64)
                .mean()
                .alias("recent_avg_vision_score_per_min"),
            col("cs_per_min")
                .cast(DataType::Float64)
                .mean()
                .alias("recent_avg_cs_per_min"),
            col("game_duration_minutes")
                .cast(DataType::Float64)
                .mean()
                .alias("recent_avg_game_duration"),
        ])
        .filter(col("games_used").ge(lit(min_matches as u32)));

    let mut df = aggregated.collect()?;
    let out_path = out_dir.join("player_profile.parquet");
    let mut file = std::fs::File::create(out_path)?;
    ParquetWriter::new(&mut file).finish(&mut df)?;
    Ok(())
}

pub fn kraken_build_ml_team_outcome(team_parquet: &Path, out_dir: &Path) -> Result<()> {
    let lf = LazyFrame::scan_parquet(team_parquet, Default::default())?
        .filter(col("queue_id").eq(lit(420i32)))
        .select([
            col("match_id"),
            col("queue_id"),
            col("team_id"),
            col("team_side"),
            col("team_win"),
            col("top_champion_id"),
            col("jungle_champion_id"),
            col("middle_champion_id"),
            col("bottom_champion_id"),
            col("utility_champion_id"),
            col("game_duration"),
            col("team_kills"),
            col("team_deaths"),
            col("team_assists"),
            col("team_gold_earned"),
            col("team_gold_per_min"),
            col("team_damage_to_champions"),
            col("team_damage_per_min"),
            col("team_vision_score"),
            col("team_vision_score_per_min"),
            col("team_cs_total"),
            col("team_cs_per_min"),
            col("team_towers_destroyed"),
            col("team_inhibitors_destroyed"),
            col("team_dragons"),
            col("team_barons"),
            col("team_heralds"),
            col("team_plates"),
        ]);

    let mut df = lf.collect()?;
    let out_path = out_dir.join("ml_team_outcome.parquet");
    let mut file = std::fs::File::create(out_path)?;
    ParquetWriter::new(&mut file).finish(&mut df)?;
    Ok(())
}

pub fn kraken_build_ml_lobby_outcome(
    player_parquet: &Path,
    team_parquet: &Path,
    player_profile_parquet: Option<&Path>,
    out_dir: &Path,
) -> Result<()> {
    let players = LazyFrame::scan_parquet(player_parquet, Default::default())?
        .filter(col("queue_id").eq(lit(420i32)));

    let roles = ["TOP", "JUNGLE", "MIDDLE", "BOTTOM", "UTILITY"];

    let mut aggs: Vec<Expr> = Vec::new();
    for role in roles.iter() {
        let lower = role.to_lowercase();
        let champ_alias = format!("ally_{}_champion_id", lower);
        let puuid_alias = format!("ally_{}_puuid", lower);
        aggs.push(
            col("champion_id")
                .filter(col("role").eq(lit(*role)))
                .first()
                .alias(&champ_alias),
        );
        aggs.push(
            col("puuid")
                .filter(col("role").eq(lit(*role)))
                .first()
                .alias(&puuid_alias),
        );
    }

    let grouped = players
        .group_by([col("match_id"), col("team_id")])
        .agg(aggs)
        .with_columns([when(col("team_id").eq(lit(100i32)))
            .then(lit(200i32))
            .otherwise(lit(100i32))
            .alias("enemy_team_id")]);

    let mut enemy_select: Vec<Expr> = vec![col("match_id"), col("team_id").alias("enemy_team_id")];
    for role in roles.iter() {
        let lower = role.to_lowercase();
        let ally_champ = format!("ally_{}_champion_id", lower);
        let ally_puuid = format!("ally_{}_puuid", lower);
        let enemy_champ = format!("enemy_{}_champion_id", lower);
        let enemy_puuid = format!("enemy_{}_puuid", lower);
        enemy_select.push(col(ally_champ).alias(&enemy_champ));
        enemy_select.push(col(ally_puuid).alias(&enemy_puuid));
    }

    let enemy = grouped.clone().select(enemy_select);

    let ally_enemy = grouped
        .join(
            enemy,
            [col("match_id"), col("enemy_team_id")],
            [col("match_id"), col("enemy_team_id")],
            JoinArgs::new(JoinType::Left),
        )
        .drop([col("enemy_team_id")]);

    let teams = LazyFrame::scan_parquet(team_parquet, Default::default())?
        .filter(col("queue_id").eq(lit(420i32)))
        .select([
            col("match_id"),
            col("queue_id"),
            col("team_id"),
            col("team_side"),
            col("team_win"),
        ]);

    let mut lobby = teams.join(
        ally_enemy,
        [col("match_id"), col("team_id")],
        [col("match_id"), col("team_id")],
        JoinArgs::new(JoinType::Inner),
    );

    if let Some(profile_path) = player_profile_parquet {
        let profile = LazyFrame::scan_parquet(profile_path, Default::default())?;
        for role in roles.iter() {
            let lower = role.to_lowercase();
            let role_profile = profile.clone().filter(col("role").eq(lit(*role))).select([
                col("puuid"),
                col("games_used").alias("recent_games"),
                col("recent_winrate"),
                col("recent_avg_gold_per_min"),
                col("recent_avg_damage_per_min"),
                col("recent_avg_vision_score_per_min"),
            ]);

            let ally_puuid_col = format!("ally_{}_puuid", lower);
            let ally_cols: [String; 5] = [
                format!("ally_{}_recent_games", lower),
                format!("ally_{}_recent_winrate", lower),
                format!("ally_{}_recent_gold_per_min", lower),
                format!("ally_{}_recent_damage_per_min", lower),
                format!("ally_{}_recent_vision_per_min", lower),
            ];

            lobby = lobby
                .join(
                    role_profile.clone(),
                    [col(&ally_puuid_col)],
                    [col("puuid")],
                    JoinArgs::new(JoinType::Left),
                )
                .rename(
                    &[
                        "recent_games",
                        "recent_winrate",
                        "recent_avg_gold_per_min",
                        "recent_avg_damage_per_min",
                        "recent_avg_vision_score_per_min",
                    ],
                    &[
                        &ally_cols[0],
                        &ally_cols[1],
                        &ally_cols[2],
                        &ally_cols[3],
                        &ally_cols[4],
                    ],
                )
                .drop([col("puuid")]);

            let enemy_puuid_col = format!("enemy_{}_puuid", lower);
            let enemy_cols: [String; 5] = [
                format!("enemy_{}_recent_games", lower),
                format!("enemy_{}_recent_winrate", lower),
                format!("enemy_{}_recent_gold_per_min", lower),
                format!("enemy_{}_recent_damage_per_min", lower),
                format!("enemy_{}_recent_vision_per_min", lower),
            ];

            lobby = lobby
                .join(
                    role_profile,
                    [col(&enemy_puuid_col)],
                    [col("puuid")],
                    JoinArgs::new(JoinType::Left),
                )
                .rename(
                    &[
                        "recent_games",
                        "recent_winrate",
                        "recent_avg_gold_per_min",
                        "recent_avg_damage_per_min",
                        "recent_avg_vision_score_per_min",
                    ],
                    &[
                        &enemy_cols[0],
                        &enemy_cols[1],
                        &enemy_cols[2],
                        &enemy_cols[3],
                        &enemy_cols[4],
                    ],
                )
                .drop([col("puuid")]);
        }
    }

    let mut df = lobby.collect()?;
    let out_path = out_dir.join("ml_lobby_outcome.parquet");
    let mut file = std::fs::File::create(out_path)?;
    ParquetWriter::new(&mut file).finish(&mut df)?;
    Ok(())
}
