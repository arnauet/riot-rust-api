use anyhow::Result;
use chrono::{DateTime, Utc};
use polars::prelude::*;
use serde_json::Value;
use std::collections::HashMap;
use std::fs;
use std::path::Path;

fn format_ts_millis(ts: i64) -> String {
    DateTime::<Utc>::from_timestamp_millis(ts)
        .map(|dt| dt.to_rfc3339())
        .unwrap_or_else(|| ts.to_string())
}

pub fn kraken_summary_raw(matches_dir: &Path, max_files: Option<usize>) -> Result<()> {
    println!("== Kraken Summary (raw JSON) ==");

    let mut to_visit = vec![matches_dir.to_path_buf()];
    let mut processed = 0usize;
    let mut queue_counts: HashMap<i64, usize> = HashMap::new();
    let mut champion_counts: HashMap<String, usize> = HashMap::new();
    let mut min_game_creation: Option<i64> = None;
    let mut max_game_creation: Option<i64> = None;
    let mut participants_total: usize = 0;

    while let Some(path) = to_visit.pop() {
        if let Some(limit) = max_files {
            if processed >= limit {
                break;
            }
        }

        if path.is_dir() {
            if let Ok(entries) = fs::read_dir(&path) {
                for entry in entries.flatten() {
                    let p = entry.path();
                    if p.is_dir() {
                        to_visit.push(p);
                    } else if p.extension().and_then(|e| e.to_str()) == Some("json") {
                        let contents = match fs::read_to_string(&p) {
                            Ok(data) => data,
                            Err(_) => continue,
                        };

                        let parsed: Value = match serde_json::from_str(&contents) {
                            Ok(v) => v,
                            Err(_) => continue,
                        };

                        let Some(info) = parsed.get("info") else {
                            continue;
                        };

                        let queue_id = info
                            .get("queueId")
                            .and_then(|v| v.as_i64())
                            .unwrap_or_default();
                        *queue_counts.entry(queue_id).or_insert(0) += 1;

                        if let Some(gc) = info.get("gameCreation").and_then(|v| v.as_i64()) {
                            min_game_creation = Some(match min_game_creation {
                                Some(current) => current.min(gc),
                                None => gc,
                            });
                            max_game_creation = Some(match max_game_creation {
                                Some(current) => current.max(gc),
                                None => gc,
                            });
                        }

                        if let Some(participants) =
                            info.get("participants").and_then(|p| p.as_array())
                        {
                            participants_total += participants.len();
                            for participant in participants {
                                if let Some(champ) =
                                    participant.get("championName").and_then(|c| c.as_str())
                                {
                                    *champion_counts.entry(champ.to_string()).or_insert(0) += 1;
                                }
                            }
                        }

                        processed += 1;

                        if let Some(limit) = max_files {
                            if processed >= limit {
                                break;
                            }
                        }
                    }
                }
            }
        }
    }

    println!("Matches scanned: {}", processed);
    let soloq = queue_counts.get(&420).cloned().unwrap_or_default();
    let other: usize = queue_counts
        .iter()
        .filter(|(k, _)| **k != 420)
        .map(|(_, v)| *v)
        .sum();
    println!(
        "Queue distribution: SoloQ={} Other={} ({} queues tracked)",
        soloq,
        other,
        queue_counts.len()
    );

    if let (Some(min_gc), Some(max_gc)) = (min_game_creation, max_game_creation) {
        println!(
            "Time range: {} -> {}",
            format_ts_millis(min_gc),
            format_ts_millis(max_gc)
        );
    }

    println!("Participants counted: {}", participants_total);

    if !champion_counts.is_empty() {
        let mut champs: Vec<_> = champion_counts.into_iter().collect();
        champs.sort_by(|a, b| b.1.cmp(&a.1));
        let top = champs.into_iter().take(10);
        println!("Top champions:");
        for (champ, count) in top {
            println!("  {:<20} {}", champ, count);
        }
    }

    Ok(())
}

pub fn kraken_summary_player(
    parquet_path: &Path,
    max_rows: Option<usize>,
    by_role: bool,
    by_champion_top_k: Option<usize>,
) -> Result<()> {
    println!("== Kraken Summary (player parquet) ==");

    let mut lf = LazyFrame::scan_parquet(
        parquet_path.to_string_lossy().as_ref(),
        ScanArgsParquet::default(),
    )?;
    if let Some(limit) = max_rows {
        lf = lf.limit(limit.try_into().unwrap_or(u32::MAX));
    }

    let basic = lf
        .clone()
        .select([
            len().alias("rows"),
            col("match_id").n_unique().alias("matches"),
            col("puuid").n_unique().alias("players"),
        ])
        .collect()?;
    let rows = basic.column("rows").ok().and_then(|c| c.get(0).ok());
    let matches = basic.column("matches").ok().and_then(|c| c.get(0).ok());
    let players = basic.column("players").ok().and_then(|c| c.get(0).ok());
    println!(
        "Rows / matches / players: rows={:?} matches={:?} players={:?}",
        rows, matches, players
    );

    let queue_dist = lf
        .clone()
        .group_by([col("queue_id")])
        .agg([len().alias("games")])
        .sort(
            "games",
            SortOptions {
                descending: true,
                nulls_last: true,
                ..Default::default()
            },
        )
        .collect()?;
    println!("Queue distribution:\n{}", queue_dist);

    let side_win = lf
        .clone()
        .filter(col("queue_id").eq(lit(420)))
        .group_by([col("team_id")])
        .agg([col("team_win").cast(DataType::Float64).mean().alias("win_rate")])
        .sort("team_id", SortOptions::default())
        .collect()?;
    println!("SoloQ side winrate:\n{}", side_win);

    let role_dist = lf
        .clone()
        .group_by([col("role")])
        .agg([len().alias("games")])
        .sort(
            "games",
            SortOptions {
                descending: true,
                nulls_last: true,
                ..Default::default()
            },
        )
        .collect()?;
    println!("Role distribution:\n{}", role_dist);

    if by_role {
        let role_stats = lf
            .clone()
            .group_by([col("role")])
            .agg([
                col("kills")
                    .cast(DataType::Float64)
                    .mean()
                    .alias("avg_kills"),
                col("gold_per_min")
                    .cast(DataType::Float64)
                    .mean()
                    .alias("avg_gpm"),
                col("damage_per_min")
                    .cast(DataType::Float64)
                    .mean()
                    .alias("avg_dpm"),
                col("vision_score_per_min")
                    .cast(DataType::Float64)
                    .mean()
                    .alias("avg_vspm"),
                col("win").cast(DataType::Float64).mean().alias("win_rate"),
            ])
            .sort("role", SortOptions::default())
            .collect()?;
        println!("Per-role stats:\n{}", role_stats);
    }

    if let Some(k) = by_champion_top_k {
        let champ_stats = lf
            .clone()
            .group_by([col("champion_name")])
            .agg([
                len().alias("games"),
                col("win").cast(DataType::Float64).mean().alias("win_rate"),
            ])
            .sort(
                "games",
                SortOptions {
                    descending: true,
                    nulls_last: true,
                    ..Default::default()
                },
            )
            .limit(k.try_into().unwrap_or(u32::MAX))
            .collect()?;
        println!("Top champions:\n{}", champ_stats);
    }

    Ok(())
}

pub fn kraken_summary_team(parquet_path: &Path, max_rows: Option<usize>) -> Result<()> {
    println!("== Kraken Summary (team parquet) ==");

    let mut lf = LazyFrame::scan_parquet(
        parquet_path.to_string_lossy().as_ref(),
        ScanArgsParquet::default(),
    )?;
    if let Some(limit) = max_rows {
        lf = lf.limit(limit.try_into().unwrap_or(u32::MAX));
    }

    let basic = lf
        .clone()
        .select([
            len().alias("rows"),
            col("match_id").n_unique().alias("matches"),
        ])
        .collect()?;
    println!("Rows / matches:\n{}", basic);

    let side_win = lf
        .clone()
        .filter(col("queue_id").eq(lit(420)))
        .group_by([col("team_id")])
        .agg([col("team_win").cast(DataType::Float64).mean().alias("win_rate")])
        .sort("team_id", SortOptions::default())
        .collect()?;
    println!("SoloQ team winrate:\n{}", side_win);

    Ok(())
}
