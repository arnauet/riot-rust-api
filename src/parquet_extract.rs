use polars::prelude::ParquetWriter;
use polars::prelude::*;
use serde_json::Value;
use std::error::Error;
use std::fs::{self, File};
use std::path::{Path, PathBuf};

struct PlayerRow {
    match_id: String,
    game_creation: i64,
    game_duration: i32,
    queue_id: i32,
    game_version: String,
    team_id: i32,
    puuid: String,
    champion_id: i32,
    champion_name: String,
    role: String,
    win: bool,
    kills: i32,
    deaths: i32,
    assists: i32,
    champ_level: i32,
    gold_earned: i32,
    gold_spent: i32,
    total_minions_killed: i32,
    neutral_minions_killed: i32,
    total_cs: i32,
    damage_to_champions: i32,
    damage_to_objectives: i32,
    damage_to_turrets: i32,
    turret_takedowns: i32,
    inhibitor_takedowns: i32,
    vision_score: i32,
    wards_placed: i32,
    wards_killed: i32,
    control_wards_placed: i32,
    damage_per_min: Option<f64>,
    gold_per_min: Option<f64>,
    team_damage_percentage: Option<f64>,
    kill_participation: Option<f64>,
    kda: Option<f64>,
    vision_score_per_min: Option<f64>,
    lane_minions_first10: Option<f64>,
    jungle_cs_before10: Option<f64>,
}

pub fn extract_parquet(
    matches_dir: &Path,
    out_parquet: &Path,
    level: &str,
) -> Result<(), Box<dyn Error>> {
    if let Some(parent) = out_parquet.parent() {
        if !parent.as_os_str().is_empty() {
            fs::create_dir_all(parent)?;
        }
    }
    match level {
        "player" => extract_player_parquet(matches_dir, out_parquet),
        "team" => extract_team_parquet(matches_dir, out_parquet),
        other => Err(format!(
            "Unsupported level '{}'. Supported levels: player, team.",
            other
        )
        .into()),
    }
}

fn extract_player_parquet(matches_dir: &Path, out_parquet: &Path) -> Result<(), Box<dyn Error>> {
    let mut rows: Vec<PlayerRow> = Vec::new();

    for path in collect_json_files(matches_dir) {
        let contents = match fs::read_to_string(&path) {
            Ok(data) => data,
            Err(err) => {
                eprintln!("Skipping unreadable file {}: {}", path.display(), err);
                continue;
            }
        };

        let parsed: Value = match serde_json::from_str(&contents) {
            Ok(value) => value,
            Err(err) => {
                eprintln!("Skipping invalid JSON {}: {}", path.display(), err);
                continue;
            }
        };

        let Some(metadata) = parsed.get("metadata") else {
            eprintln!("Missing metadata in {}", path.display());
            continue;
        };

        let Some(info) = parsed.get("info") else {
            eprintln!("Missing info section in {}", path.display());
            continue;
        };

        let Some(participants) = info.get("participants").and_then(|p| p.as_array()) else {
            eprintln!("Missing participants array in {}", path.display());
            continue;
        };

        let Some(match_id) = metadata
            .get("matchId")
            .and_then(|v| v.as_str())
            .map(|s| s.to_string())
            .or_else(|| {
                path.file_stem()
                    .and_then(|s| s.to_str())
                    .map(|s| s.to_string())
            })
        else {
            continue;
        };

        let game_creation = info
            .get("gameCreation")
            .and_then(|v| v.as_i64())
            .unwrap_or_default();
        let game_duration = info
            .get("gameDuration")
            .and_then(|v| v.as_i64())
            .unwrap_or_default() as i32;
        let queue_id = info
            .get("queueId")
            .and_then(|v| v.as_i64())
            .unwrap_or_default() as i32;
        let game_version = info
            .get("gameVersion")
            .and_then(|v| v.as_str())
            .unwrap_or("")
            .to_string();

        for participant in participants {
            let team_id = participant
                .get("teamId")
                .and_then(|v| v.as_i64())
                .unwrap_or_default() as i32;
            let puuid = participant
                .get("puuid")
                .and_then(|v| v.as_str())
                .unwrap_or("")
                .to_string();
            let champion_id = participant
                .get("championId")
                .and_then(|v| v.as_i64())
                .unwrap_or_default() as i32;
            let champion_name = participant
                .get("championName")
                .and_then(|v| v.as_str())
                .unwrap_or("")
                .to_string();
            let role = participant
                .get("teamPosition")
                .and_then(|v| v.as_str())
                .filter(|s| !s.is_empty())
                .map(|s| s.to_string())
                .or_else(|| {
                    participant
                        .get("individualPosition")
                        .and_then(|v| v.as_str())
                        .map(|s| s.to_string())
                })
                .unwrap_or_default();
            let win = participant
                .get("win")
                .and_then(|v| v.as_bool())
                .unwrap_or(false);
            let kills = as_i32(participant.get("kills"));
            let deaths = as_i32(participant.get("deaths"));
            let assists = as_i32(participant.get("assists"));
            let champ_level = as_i32(participant.get("champLevel"));
            let gold_earned = as_i32(participant.get("goldEarned"));
            let gold_spent = as_i32(participant.get("goldSpent"));
            let total_minions_killed = as_i32(participant.get("totalMinionsKilled"));
            let neutral_minions_killed = as_i32(participant.get("neutralMinionsKilled"));
            let total_cs = total_minions_killed + neutral_minions_killed;
            let damage_to_champions = as_i32(participant.get("totalDamageDealtToChampions"));
            let damage_to_objectives = as_i32(participant.get("damageDealtToObjectives"));
            let damage_to_turrets = as_i32(participant.get("damageDealtToTurrets"));
            let turret_takedowns = as_i32(participant.get("turretTakedowns"));
            let inhibitor_takedowns = as_i32(participant.get("inhibitorTakedowns"));
            let vision_score = as_i32(participant.get("visionScore"));
            let wards_placed = as_i32(participant.get("wardsPlaced"));
            let wards_killed = as_i32(participant.get("wardsKilled"));
            let control_wards_placed = as_i32(participant.get("visionWardsBoughtInGame"));

            let challenges = participant.get("challenges");

            let row = PlayerRow {
                match_id: match_id.clone(),
                game_creation,
                game_duration,
                queue_id,
                game_version: game_version.clone(),
                team_id,
                puuid,
                champion_id,
                champion_name,
                role,
                win,
                kills,
                deaths,
                assists,
                champ_level,
                gold_earned,
                gold_spent,
                total_minions_killed,
                neutral_minions_killed,
                total_cs,
                damage_to_champions,
                damage_to_objectives,
                damage_to_turrets,
                turret_takedowns,
                inhibitor_takedowns,
                vision_score,
                wards_placed,
                wards_killed,
                control_wards_placed,
                damage_per_min: as_f64(challenges, "damagePerMinute"),
                gold_per_min: as_f64(challenges, "goldPerMinute"),
                team_damage_percentage: as_f64(challenges, "teamDamagePercentage"),
                kill_participation: as_f64(challenges, "killParticipation"),
                kda: as_f64(challenges, "kda"),
                vision_score_per_min: as_f64(challenges, "visionScorePerMinute"),
                lane_minions_first10: as_f64(challenges, "laneMinionsFirst10Minutes"),
                jungle_cs_before10: as_f64(challenges, "jungleCsBefore10Minutes"),
            };

            rows.push(row);
        }
    }

    let mut df = build_dataframe(rows)?;
    let mut file = File::create(out_parquet)?;
    ParquetWriter::new(&mut file).finish(&mut df)?;

    Ok(())
}

#[derive(Default)]
struct TeamRow {
    match_id: String,
    platform_id: Option<String>,
    queue_id: i32,
    game_version: String,
    game_creation: i64,
    game_duration: i32,
    team_id: i16,
    team_side: String,
    team_win: i8,
    top_champion_id: Option<i32>,
    jungle_champion_id: Option<i32>,
    middle_champion_id: Option<i32>,
    bottom_champion_id: Option<i32>,
    utility_champion_id: Option<i32>,
    team_kills: i32,
    team_deaths: i32,
    team_assists: i32,
    team_gold_earned: i64,
    team_damage_to_champions: i64,
    team_vision_score: i64,
    team_cs_total: i32,
    team_gold_per_min: Option<f64>,
    team_damage_per_min: Option<f64>,
    team_vision_score_per_min: Option<f64>,
    team_cs_per_min: Option<f64>,
    team_towers_destroyed: i32,
    team_inhibitors_destroyed: i32,
    team_dragons: i32,
    team_barons: i32,
    team_heralds: i32,
    team_plates: Option<i32>,
    first_blood: Option<bool>,
    first_tower: Option<bool>,
    first_inhibitor: Option<bool>,
    first_baron: Option<bool>,
    first_dragon: Option<bool>,
    first_herald: Option<bool>,
}

fn extract_team_parquet(matches_dir: &Path, out_parquet: &Path) -> Result<(), Box<dyn Error>> {
    let mut rows: Vec<TeamRow> = Vec::new();

    for path in collect_json_files(matches_dir) {
        let contents = match fs::read_to_string(&path) {
            Ok(data) => data,
            Err(err) => {
                eprintln!("Skipping unreadable file {}: {}", path.display(), err);
                continue;
            }
        };

        let parsed: Value = match serde_json::from_str(&contents) {
            Ok(value) => value,
            Err(err) => {
                eprintln!("Skipping invalid JSON {}: {}", path.display(), err);
                continue;
            }
        };

        let Some(metadata) = parsed.get("metadata") else {
            eprintln!("Missing metadata in {}", path.display());
            continue;
        };

        let Some(info) = parsed.get("info") else {
            eprintln!("Missing info section in {}", path.display());
            continue;
        };

        let Some(participants) = info.get("participants").and_then(|p| p.as_array()) else {
            eprintln!("Missing participants array in {}", path.display());
            continue;
        };

        let Some(teams) = info.get("teams").and_then(|t| t.as_array()) else {
            eprintln!("Missing teams array in {}", path.display());
            continue;
        };

        let Some(match_id) = metadata
            .get("matchId")
            .and_then(|v| v.as_str())
            .map(|s| s.to_string())
            .or_else(|| {
                path.file_stem()
                    .and_then(|s| s.to_str())
                    .map(|s| s.to_string())
            })
        else {
            continue;
        };

        let platform_id = metadata
            .get("platformId")
            .and_then(|v| v.as_str())
            .map(|s| s.to_string())
            .or_else(|| {
                info.get("platformId")
                    .and_then(|v| v.as_str())
                    .map(|s| s.to_string())
            });

        let game_creation = info
            .get("gameCreation")
            .and_then(|v| v.as_i64())
            .unwrap_or_default();
        let game_duration = info
            .get("gameDuration")
            .and_then(|v| v.as_i64())
            .unwrap_or_default() as i32;
        let queue_id = info
            .get("queueId")
            .and_then(|v| v.as_i64())
            .unwrap_or_default() as i32;
        let game_version = info
            .get("gameVersion")
            .and_then(|v| v.as_str())
            .unwrap_or("")
            .to_string();

        for team in teams {
            let Some(team_id) = team.get("teamId").and_then(|v| v.as_i64()) else {
                continue;
            };

            let team_participants: Vec<&Value> = participants
                .iter()
                .filter(|p| p.get("teamId").and_then(|v| v.as_i64()) == Some(team_id))
                .collect();

            let team_kills: i32 = team_participants
                .iter()
                .map(|p| as_i32(p.get("kills")))
                .sum();
            let team_deaths: i32 = team_participants
                .iter()
                .map(|p| as_i32(p.get("deaths")))
                .sum();
            let team_assists: i32 = team_participants
                .iter()
                .map(|p| as_i32(p.get("assists")))
                .sum();
            let team_gold_earned: i64 = team_participants
                .iter()
                .map(|p| as_i32(p.get("goldEarned")) as i64)
                .sum();
            let team_damage_to_champions: i64 = team_participants
                .iter()
                .map(|p| as_i32(p.get("totalDamageDealtToChampions")) as i64)
                .sum();
            let team_vision_score: i64 = team_participants
                .iter()
                .map(|p| as_i32(p.get("visionScore")) as i64)
                .sum();
            let team_cs_total: i32 = team_participants
                .iter()
                .map(|p| {
                    as_i32(p.get("totalMinionsKilled")) + as_i32(p.get("neutralMinionsKilled"))
                })
                .sum();

            let team_win = team.get("win").and_then(|v| v.as_bool()).unwrap_or(false);

            let (
                team_towers_destroyed,
                team_inhibitors_destroyed,
                team_dragons,
                team_barons,
                team_heralds,
                team_plates,
                first_blood,
                first_tower,
                first_inhibitor,
                first_baron,
                first_dragon,
                first_herald,
            ) = team_objectives(team);

            let row = TeamRow {
                match_id: match_id.clone(),
                platform_id: platform_id.clone(),
                queue_id,
                game_version: game_version.clone(),
                game_creation,
                game_duration,
                team_id: team_id as i16,
                team_side: if team_id == 100 { "blue" } else { "red" }.to_string(),
                team_win: if team_win { 1 } else { 0 },
                top_champion_id: find_role_champion(&team_participants, "TOP"),
                jungle_champion_id: find_role_champion(&team_participants, "JUNGLE"),
                middle_champion_id: find_role_champion(&team_participants, "MIDDLE"),
                bottom_champion_id: find_role_champion(&team_participants, "BOTTOM"),
                utility_champion_id: find_role_champion(&team_participants, "UTILITY"),
                team_kills,
                team_deaths,
                team_assists,
                team_gold_earned,
                team_damage_to_champions,
                team_vision_score,
                team_cs_total,
                team_gold_per_min: per_min(team_gold_earned, game_duration),
                team_damage_per_min: per_min(team_damage_to_champions, game_duration),
                team_vision_score_per_min: per_min(team_vision_score, game_duration),
                team_cs_per_min: per_min(team_cs_total as i64, game_duration),
                team_towers_destroyed,
                team_inhibitors_destroyed,
                team_dragons,
                team_barons,
                team_heralds,
                team_plates,
                first_blood,
                first_tower,
                first_inhibitor,
                first_baron,
                first_dragon,
                first_herald,
            };

            rows.push(row);
        }
    }

    let mut df = build_team_dataframe(rows)?;
    let mut file = File::create(out_parquet)?;
    ParquetWriter::new(&mut file).finish(&mut df)?;

    Ok(())
}

fn collect_json_files(root: &Path) -> Vec<PathBuf> {
    let mut files = Vec::new();
    let mut stack = vec![root.to_path_buf()];

    while let Some(path) = stack.pop() {
        let Ok(entries) = fs::read_dir(&path) else {
            continue;
        };

        for entry in entries.flatten() {
            let path = entry.path();
            if path.is_dir() {
                stack.push(path);
            } else if path
                .extension()
                .and_then(|ext| ext.to_str())
                .map(|ext| ext.eq_ignore_ascii_case("json"))
                .unwrap_or(false)
            {
                files.push(path);
            }
        }
    }

    files
}

fn build_dataframe(rows: Vec<PlayerRow>) -> Result<DataFrame, PolarsError> {
    let mut match_id: Vec<String> = Vec::new();
    let mut game_creation: Vec<i64> = Vec::new();
    let mut game_duration: Vec<i32> = Vec::new();
    let mut queue_id: Vec<i32> = Vec::new();
    let mut game_version: Vec<String> = Vec::new();
    let mut team_id: Vec<i32> = Vec::new();
    let mut puuid: Vec<String> = Vec::new();
    let mut champion_id: Vec<i32> = Vec::new();
    let mut champion_name: Vec<String> = Vec::new();
    let mut role: Vec<String> = Vec::new();
    let mut win: Vec<bool> = Vec::new();
    let mut kills: Vec<i32> = Vec::new();
    let mut deaths: Vec<i32> = Vec::new();
    let mut assists: Vec<i32> = Vec::new();
    let mut champ_level: Vec<i32> = Vec::new();
    let mut gold_earned: Vec<i32> = Vec::new();
    let mut gold_spent: Vec<i32> = Vec::new();
    let mut total_minions_killed: Vec<i32> = Vec::new();
    let mut neutral_minions_killed: Vec<i32> = Vec::new();
    let mut total_cs: Vec<i32> = Vec::new();
    let mut damage_to_champions: Vec<i32> = Vec::new();
    let mut damage_to_objectives: Vec<i32> = Vec::new();
    let mut damage_to_turrets: Vec<i32> = Vec::new();
    let mut turret_takedowns: Vec<i32> = Vec::new();
    let mut inhibitor_takedowns: Vec<i32> = Vec::new();
    let mut vision_score: Vec<i32> = Vec::new();
    let mut wards_placed: Vec<i32> = Vec::new();
    let mut wards_killed: Vec<i32> = Vec::new();
    let mut control_wards_placed: Vec<i32> = Vec::new();
    let mut damage_per_min: Vec<Option<f64>> = Vec::new();
    let mut gold_per_min: Vec<Option<f64>> = Vec::new();
    let mut team_damage_percentage: Vec<Option<f64>> = Vec::new();
    let mut kill_participation: Vec<Option<f64>> = Vec::new();
    let mut kda: Vec<Option<f64>> = Vec::new();
    let mut vision_score_per_min: Vec<Option<f64>> = Vec::new();
    let mut lane_minions_first10: Vec<Option<f64>> = Vec::new();
    let mut jungle_cs_before10: Vec<Option<f64>> = Vec::new();

    for row in rows {
        match_id.push(row.match_id);
        game_creation.push(row.game_creation);
        game_duration.push(row.game_duration);
        queue_id.push(row.queue_id);
        game_version.push(row.game_version);
        team_id.push(row.team_id);
        puuid.push(row.puuid);
        champion_id.push(row.champion_id);
        champion_name.push(row.champion_name);
        role.push(row.role);
        win.push(row.win);
        kills.push(row.kills);
        deaths.push(row.deaths);
        assists.push(row.assists);
        champ_level.push(row.champ_level);
        gold_earned.push(row.gold_earned);
        gold_spent.push(row.gold_spent);
        total_minions_killed.push(row.total_minions_killed);
        neutral_minions_killed.push(row.neutral_minions_killed);
        total_cs.push(row.total_cs);
        damage_to_champions.push(row.damage_to_champions);
        damage_to_objectives.push(row.damage_to_objectives);
        damage_to_turrets.push(row.damage_to_turrets);
        turret_takedowns.push(row.turret_takedowns);
        inhibitor_takedowns.push(row.inhibitor_takedowns);
        vision_score.push(row.vision_score);
        wards_placed.push(row.wards_placed);
        wards_killed.push(row.wards_killed);
        control_wards_placed.push(row.control_wards_placed);
        damage_per_min.push(row.damage_per_min);
        gold_per_min.push(row.gold_per_min);
        team_damage_percentage.push(row.team_damage_percentage);
        kill_participation.push(row.kill_participation);
        kda.push(row.kda);
        vision_score_per_min.push(row.vision_score_per_min);
        lane_minions_first10.push(row.lane_minions_first10);
        jungle_cs_before10.push(row.jungle_cs_before10);
    }

    DataFrame::new(vec![
        Series::new("match_id", match_id),
        Series::new("game_creation", game_creation),
        Series::new("game_duration", game_duration),
        Series::new("queue_id", queue_id),
        Series::new("game_version", game_version),
        Series::new("team_id", team_id),
        Series::new("puuid", puuid),
        Series::new("champion_id", champion_id),
        Series::new("champion_name", champion_name),
        Series::new("role", role),
        Series::new("win", win),
        Series::new("kills", kills),
        Series::new("deaths", deaths),
        Series::new("assists", assists),
        Series::new("champ_level", champ_level),
        Series::new("gold_earned", gold_earned),
        Series::new("gold_spent", gold_spent),
        Series::new("total_minions_killed", total_minions_killed),
        Series::new("neutral_minions_killed", neutral_minions_killed),
        Series::new("total_cs", total_cs),
        Series::new("damage_to_champions", damage_to_champions),
        Series::new("damage_to_objectives", damage_to_objectives),
        Series::new("damage_to_turrets", damage_to_turrets),
        Series::new("turret_takedowns", turret_takedowns),
        Series::new("inhibitor_takedowns", inhibitor_takedowns),
        Series::new("vision_score", vision_score),
        Series::new("wards_placed", wards_placed),
        Series::new("wards_killed", wards_killed),
        Series::new("control_wards_placed", control_wards_placed),
        Series::new("damage_per_min", damage_per_min),
        Series::new("gold_per_min", gold_per_min),
        Series::new("team_damage_percentage", team_damage_percentage),
        Series::new("kill_participation", kill_participation),
        Series::new("kda", kda),
        Series::new("vision_score_per_min", vision_score_per_min),
        Series::new("lane_minions_first10", lane_minions_first10),
        Series::new("jungle_cs_before10", jungle_cs_before10),
    ])
}

fn build_team_dataframe(rows: Vec<TeamRow>) -> Result<DataFrame, PolarsError> {
    let mut match_id: Vec<String> = Vec::new();
    let mut platform_id: Vec<Option<String>> = Vec::new();
    let mut queue_id: Vec<i32> = Vec::new();
    let mut game_version: Vec<String> = Vec::new();
    let mut game_creation: Vec<i64> = Vec::new();
    let mut game_duration: Vec<i32> = Vec::new();
    let mut team_id: Vec<i16> = Vec::new();
    let mut team_side: Vec<String> = Vec::new();
    let mut team_win: Vec<i8> = Vec::new();
    let mut top_champion_id: Vec<Option<i32>> = Vec::new();
    let mut jungle_champion_id: Vec<Option<i32>> = Vec::new();
    let mut middle_champion_id: Vec<Option<i32>> = Vec::new();
    let mut bottom_champion_id: Vec<Option<i32>> = Vec::new();
    let mut utility_champion_id: Vec<Option<i32>> = Vec::new();
    let mut team_kills: Vec<i32> = Vec::new();
    let mut team_deaths: Vec<i32> = Vec::new();
    let mut team_assists: Vec<i32> = Vec::new();
    let mut team_gold_earned: Vec<i64> = Vec::new();
    let mut team_damage_to_champions: Vec<i64> = Vec::new();
    let mut team_vision_score: Vec<i64> = Vec::new();
    let mut team_cs_total: Vec<i32> = Vec::new();
    let mut team_gold_per_min: Vec<Option<f64>> = Vec::new();
    let mut team_damage_per_min: Vec<Option<f64>> = Vec::new();
    let mut team_vision_score_per_min: Vec<Option<f64>> = Vec::new();
    let mut team_cs_per_min: Vec<Option<f64>> = Vec::new();
    let mut team_towers_destroyed: Vec<i32> = Vec::new();
    let mut team_inhibitors_destroyed: Vec<i32> = Vec::new();
    let mut team_dragons: Vec<i32> = Vec::new();
    let mut team_barons: Vec<i32> = Vec::new();
    let mut team_heralds: Vec<i32> = Vec::new();
    let mut team_plates: Vec<Option<i32>> = Vec::new();
    let mut first_blood: Vec<Option<bool>> = Vec::new();
    let mut first_tower: Vec<Option<bool>> = Vec::new();
    let mut first_inhibitor: Vec<Option<bool>> = Vec::new();
    let mut first_baron: Vec<Option<bool>> = Vec::new();
    let mut first_dragon: Vec<Option<bool>> = Vec::new();
    let mut first_herald: Vec<Option<bool>> = Vec::new();

    for row in rows {
        match_id.push(row.match_id);
        platform_id.push(row.platform_id);
        queue_id.push(row.queue_id);
        game_version.push(row.game_version);
        game_creation.push(row.game_creation);
        game_duration.push(row.game_duration);
        team_id.push(row.team_id);
        team_side.push(row.team_side);
        team_win.push(row.team_win);
        top_champion_id.push(row.top_champion_id);
        jungle_champion_id.push(row.jungle_champion_id);
        middle_champion_id.push(row.middle_champion_id);
        bottom_champion_id.push(row.bottom_champion_id);
        utility_champion_id.push(row.utility_champion_id);
        team_kills.push(row.team_kills);
        team_deaths.push(row.team_deaths);
        team_assists.push(row.team_assists);
        team_gold_earned.push(row.team_gold_earned);
        team_damage_to_champions.push(row.team_damage_to_champions);
        team_vision_score.push(row.team_vision_score);
        team_cs_total.push(row.team_cs_total);
        team_gold_per_min.push(row.team_gold_per_min);
        team_damage_per_min.push(row.team_damage_per_min);
        team_vision_score_per_min.push(row.team_vision_score_per_min);
        team_cs_per_min.push(row.team_cs_per_min);
        team_towers_destroyed.push(row.team_towers_destroyed);
        team_inhibitors_destroyed.push(row.team_inhibitors_destroyed);
        team_dragons.push(row.team_dragons);
        team_barons.push(row.team_barons);
        team_heralds.push(row.team_heralds);
        team_plates.push(row.team_plates);
        first_blood.push(row.first_blood);
        first_tower.push(row.first_tower);
        first_inhibitor.push(row.first_inhibitor);
        first_baron.push(row.first_baron);
        first_dragon.push(row.first_dragon);
        first_herald.push(row.first_herald);
    }

    DataFrame::new(vec![
        Series::new("match_id", match_id),
        Series::new("platform_id", platform_id),
        Series::new("queue_id", queue_id),
        Series::new("game_version", game_version),
        Series::new("game_creation", game_creation),
        Series::new("game_duration", game_duration),
        Series::new("team_id", team_id),
        Series::new("team_side", team_side),
        Series::new("team_win", team_win),
        Series::new("top_champion_id", top_champion_id),
        Series::new("jungle_champion_id", jungle_champion_id),
        Series::new("middle_champion_id", middle_champion_id),
        Series::new("bottom_champion_id", bottom_champion_id),
        Series::new("utility_champion_id", utility_champion_id),
        Series::new("team_kills", team_kills),
        Series::new("team_deaths", team_deaths),
        Series::new("team_assists", team_assists),
        Series::new("team_gold_earned", team_gold_earned),
        Series::new("team_damage_to_champions", team_damage_to_champions),
        Series::new("team_vision_score", team_vision_score),
        Series::new("team_cs_total", team_cs_total),
        Series::new("team_gold_per_min", team_gold_per_min),
        Series::new("team_damage_per_min", team_damage_per_min),
        Series::new("team_vision_score_per_min", team_vision_score_per_min),
        Series::new("team_cs_per_min", team_cs_per_min),
        Series::new("team_towers_destroyed", team_towers_destroyed),
        Series::new("team_inhibitors_destroyed", team_inhibitors_destroyed),
        Series::new("team_dragons", team_dragons),
        Series::new("team_barons", team_barons),
        Series::new("team_heralds", team_heralds),
        Series::new("team_plates", team_plates),
        Series::new("first_blood", first_blood),
        Series::new("first_tower", first_tower),
        Series::new("first_inhibitor", first_inhibitor),
        Series::new("first_baron", first_baron),
        Series::new("first_dragon", first_dragon),
        Series::new("first_herald", first_herald),
    ])
}

fn as_i32(value: Option<&Value>) -> i32 {
    value
        .and_then(|v| v.as_i64())
        .unwrap_or_default()
        .try_into()
        .unwrap_or_default()
}

fn as_f64(container: Option<&Value>, key: &str) -> Option<f64> {
    container.and_then(|c| c.get(key)).and_then(|v| v.as_f64())
}

fn find_role_champion(participants: &[&Value], role: &str) -> Option<i32> {
    participants
        .iter()
        .find(|p| {
            p.get("teamPosition")
                .and_then(|v| v.as_str())
                .map(|s| s.eq_ignore_ascii_case(role))
                .unwrap_or(false)
        })
        .and_then(|p| p.get("championId"))
        .and_then(|v| v.as_i64())
        .map(|id| id as i32)
}

fn team_objectives(
    team: &Value,
) -> (
    i32,
    i32,
    i32,
    i32,
    i32,
    Option<i32>,
    Option<bool>,
    Option<bool>,
    Option<bool>,
    Option<bool>,
    Option<bool>,
    Option<bool>,
) {
    let objectives = team.get("objectives");

    let tower = objective_kills(objectives, "tower");
    let inhibitor = objective_kills(objectives, "inhibitor");
    let dragon = objective_kills(objectives, "dragon");
    let baron = objective_kills(objectives, "baron");
    let herald = objective_kills(objectives, "riftHerald");
    let plates = objectives
        .and_then(|o| o.get("tower"))
        .and_then(|t| t.get("plates"))
        .and_then(|v| v.as_i64())
        .map(|v| v as i32);

    let first_blood = objective_first(objectives, "champion");
    let first_tower = objective_first(objectives, "tower");
    let first_inhibitor = objective_first(objectives, "inhibitor");
    let first_baron = objective_first(objectives, "baron");
    let first_dragon = objective_first(objectives, "dragon");
    let first_herald = objective_first(objectives, "riftHerald");

    (
        tower,
        inhibitor,
        dragon,
        baron,
        herald,
        plates,
        first_blood,
        first_tower,
        first_inhibitor,
        first_baron,
        first_dragon,
        first_herald,
    )
}

fn objective_kills(objectives: Option<&Value>, key: &str) -> i32 {
    objectives
        .and_then(|o| o.get(key))
        .and_then(|obj| obj.get("kills"))
        .and_then(|v| v.as_i64())
        .unwrap_or_default() as i32
}

fn objective_first(objectives: Option<&Value>, key: &str) -> Option<bool> {
    objectives
        .and_then(|o| o.get(key))
        .and_then(|obj| obj.get("first"))
        .and_then(|v| v.as_bool())
}

fn per_min(total: i64, duration_secs: i32) -> Option<f64> {
    if duration_secs <= 0 {
        return None;
    }

    Some(total as f64 / (duration_secs as f64 / 60.0))
}
