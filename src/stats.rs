use csv::Writer;
use serde::Serialize;
use serde_json::Value;
use std::error::Error;
use std::fs;
use std::path::Path;

#[derive(Serialize)]
struct BasicStatsRow {
    match_id: String,
    game_creation: i64,
    queue_id: i64,
    champion_name: String,
    role: String,
    win: u8,
    kills: i64,
    deaths: i64,
    assists: i64,
    cs_total: i64,
    gold_earned: i64,
    game_duration: i64,
}

pub fn extract_basic_stats_for_puuid(
    puuid: &str,
    matches_dir: &Path,
    out_file: &Path,
) -> Result<(), Box<dyn Error>> {
    if let Some(parent) = out_file.parent() {
        if !parent.as_os_str().is_empty() {
            fs::create_dir_all(parent)?;
        }
    }

    let mut writer = Writer::from_path(out_file)?;

    for entry in fs::read_dir(matches_dir)? {
        let entry = match entry {
            Ok(entry) => entry,
            Err(_) => continue,
        };

        let path = entry.path();
        if path.extension().and_then(|ext| ext.to_str()) != Some("json") {
            continue;
        }

        let contents = match fs::read_to_string(&path) {
            Ok(data) => data,
            Err(_) => continue,
        };

        let parsed: Value = match serde_json::from_str(&contents) {
            Ok(value) => value,
            Err(_) => continue,
        };

        let info = match parsed.get("info") {
            Some(value) => value,
            None => continue,
        };

        let participants = match info.get("participants").and_then(|p| p.as_array()) {
            Some(list) => list,
            None => continue,
        };

        let participant = match participants.iter().find(|p| {
            p.get("puuid")
                .and_then(|value| value.as_str())
                .map(|value| value == puuid)
                .unwrap_or(false)
        }) {
            Some(p) => p,
            None => continue,
        };

        let match_id = parsed
            .get("metadata")
            .and_then(|metadata| metadata.get("matchId"))
            .and_then(|value| value.as_str())
            .map(|value| value.to_string())
            .or_else(|| {
                path.file_stem()
                    .and_then(|name| name.to_str())
                    .map(|value| value.to_string())
            });

        let Some(match_id) = match_id else {
            continue;
        };

        let row = BasicStatsRow {
            match_id,
            game_creation: info
                .get("gameCreation")
                .and_then(|value| value.as_i64())
                .unwrap_or(0),
            queue_id: info
                .get("queueId")
                .and_then(|value| value.as_i64())
                .unwrap_or(0),
            champion_name: participant
                .get("championName")
                .and_then(|value| value.as_str())
                .unwrap_or("")
                .to_string(),
            role: participant
                .get("teamPosition")
                .and_then(|value| value.as_str())
                .unwrap_or("")
                .to_string(),
            win: participant
                .get("win")
                .and_then(|value| value.as_bool())
                .map(|won| if won { 1 } else { 0 })
                .unwrap_or(0),
            kills: participant
                .get("kills")
                .and_then(|value| value.as_i64())
                .unwrap_or(0),
            deaths: participant
                .get("deaths")
                .and_then(|value| value.as_i64())
                .unwrap_or(0),
            assists: participant
                .get("assists")
                .and_then(|value| value.as_i64())
                .unwrap_or(0),
            cs_total: participant
                .get("totalMinionsKilled")
                .and_then(|value| value.as_i64())
                .unwrap_or(0)
                + participant
                    .get("neutralMinionsKilled")
                    .and_then(|value| value.as_i64())
                    .unwrap_or(0),
            gold_earned: participant
                .get("goldEarned")
                .and_then(|value| value.as_i64())
                .unwrap_or(0),
            game_duration: info
                .get("gameDuration")
                .and_then(|value| value.as_i64())
                .unwrap_or(0),
        };

        if writer.serialize(row).is_err() {
            continue;
        }
    }

    writer.flush()?;
    Ok(())
}
