use crate::riot_api::RiotClient;
use serde_json::Value;
use std::collections::{HashMap, HashSet, VecDeque};
use std::error::Error;
use std::fs;
use std::io::BufRead;
use std::path::PathBuf;
use std::time::{Duration, Instant};

#[derive(Debug, Clone, PartialEq, Eq)]
enum KrakenMode {
    Explore,
    Focus,
    SeedOnly,
}

#[derive(Debug, Clone)]
pub struct KrakenAbsorbArgs {
    pub seed_puuid: Option<String>,
    pub seed_file: Option<PathBuf>,
    pub duration_mins: u64,
    pub out_dir: PathBuf,
    pub max_req_per_2min: usize,
    pub max_matches_per_player: usize,
    pub max_matches_total: Option<usize>,
    pub idle_exit_after_mins: Option<u64>,
    pub mode: String,
    pub role_focus: Option<String>,
    pub allow_ranks: Option<String>,
    pub log_interval_secs: u64,
}

#[derive(Debug, Clone)]
pub struct KrakenEatArgs {
    pub seed_puuid: String,
    pub out_dir: PathBuf,
    pub duration_mins: Option<u64>,
}

pub fn kraken_eat_run(args: &KrakenEatArgs, client: &RiotClient) -> Result<(), Box<dyn Error>> {
    let absorb_args = KrakenAbsorbArgs {
        seed_puuid: Some(args.seed_puuid.clone()),
        seed_file: None,
        duration_mins: args.duration_mins.unwrap_or(10),
        out_dir: args.out_dir.clone(),
        max_req_per_2min: 60,
        max_matches_per_player: 20,
        max_matches_total: Some(1000),
        idle_exit_after_mins: Some(10),
        mode: "explore".to_string(),
        role_focus: None,
        allow_ranks: None,
        log_interval_secs: 45,
    };

    kraken_absorb_run(&absorb_args, client)
}

pub fn kraken_absorb_run(
    args: &KrakenAbsorbArgs,
    client: &RiotClient,
) -> Result<(), Box<dyn Error>> {
    let mut seeds: Vec<String> = Vec::new();

    if let Some(seed) = &args.seed_puuid {
        if !seed.trim().is_empty() {
            seeds.push(seed.trim().to_string());
        }
    }

    if let Some(path) = &args.seed_file {
        let file = fs::File::open(path)?;
        let reader = std::io::BufReader::new(file);
        for line in reader.lines() {
            if let Ok(value) = line {
                let trimmed = value.trim();
                if !trimmed.is_empty() {
                    seeds.push(trimmed.to_string());
                }
            }
        }
    }

    if seeds.is_empty() {
        return Err("You must provide at least one seed via --seed-puuid or --seed-file".into());
    }

    fs::create_dir_all(&args.out_dir)?;

    let mode = match args.mode.to_lowercase().as_str() {
        "explore" => KrakenMode::Explore,
        "focus" => KrakenMode::Focus,
        "seed-only" => KrakenMode::SeedOnly,
        _ => KrakenMode::Explore,
    };

    let role_focus: Option<HashSet<String>> = args.role_focus.as_ref().map(|raw| {
        raw.split(',')
            .map(|r| r.trim().to_uppercase())
            .filter(|r| !r.is_empty())
            .collect()
    });

    let allowed_ranks: Option<HashSet<String>> = args.allow_ranks.as_ref().map(|raw| {
        raw.split(',')
            .map(|r| r.trim().to_uppercase())
            .filter(|r| !r.is_empty())
            .collect()
    });

    let mut queue: VecDeque<String> = VecDeque::new();
    let mut seen_puuids: HashSet<String> = HashSet::new();
    let mut rank_cache: HashMap<String, Option<String>> = HashMap::new();

    for seed in seeds {
        if kraken_maybe_enqueue_player(
            &seed,
            &mut seen_puuids,
            &mut queue,
            &allowed_ranks,
            &mut rank_cache,
            client,
            &mode,
            None,
        )? {
            continue;
        }
    }

    if queue.is_empty() {
        return Err("No seeds enqueued after applying filters".into());
    }

    let mut seen_match_ids: HashSet<String> = HashSet::new();
    let mut matches_per_player: HashMap<String, usize> = HashMap::new();
    let mut downloaded_matches: usize = 0;
    let mut written_matches: usize = 0;
    let start = Instant::now();
    let mut last_written_at = Instant::now();
    let max_duration = Duration::from_secs(args.duration_mins * 60);
    let idle_limit = args
        .idle_exit_after_mins
        .map(|mins| Duration::from_secs(mins * 60));
    let mut last_log = Instant::now();

    let max_new_focus = 2usize;

    while !queue.is_empty() && start.elapsed() < max_duration {
        if let Some(max_total) = args.max_matches_total {
            if written_matches >= max_total {
                break;
            }
        }

        if let Some(limit) = idle_limit {
            if written_matches > 0 && last_written_at.elapsed() >= limit {
                break;
            }
        }

        if last_log.elapsed() >= Duration::from_secs(args.log_interval_secs) {
            eprintln!(
                "[kraken-absorb] elapsed={}s fetched={} written={} queue={} seen_players={} max_req_per_2min={}",
                start.elapsed().as_secs(),
                downloaded_matches,
                written_matches,
                queue.len(),
                seen_puuids.len(),
                args.max_req_per_2min
            );
            last_log = Instant::now();
        }

        let puuid = match queue.pop_front() {
            Some(p) => p,
            None => break,
        };

        let mut downloaded_for_puuid = *matches_per_player.get(&puuid).unwrap_or(&0);
        if downloaded_for_puuid >= args.max_matches_per_player {
            continue;
        }

        let match_ids = match client.get_match_ids_by_puuid(&puuid, 100) {
            Ok(ids) => ids,
            Err(err) => {
                eprintln!("Failed to fetch match IDs for {}: {}", puuid, err);
                continue;
            }
        };

        for match_id in match_ids {
            if let Some(max_total) = args.max_matches_total {
                if written_matches >= max_total {
                    break;
                }
            }

            if downloaded_for_puuid >= args.max_matches_per_player {
                break;
            }

            if !seen_match_ids.insert(match_id.clone()) {
                continue;
            }

            downloaded_matches += 1;
            let match_json: Value = match client.get_match_json(&match_id) {
                Ok(json) => json,
                Err(err) => {
                    eprintln!("Failed to fetch match {}: {}", match_id, err);
                    continue;
                }
            };

            let write_allowed = kraken_match_passes_roles(&match_json, role_focus.as_ref());

            let mut new_added_this_match = 0usize;
            if let Some(participants) = match_json
                .get("metadata")
                .and_then(|metadata| metadata.get("participants"))
                .and_then(|list| list.as_array())
            {
                for participant in participants {
                    if let Some(participant_puuid) = participant.as_str() {
                        if mode == KrakenMode::SeedOnly {
                            if !seen_puuids.contains(participant_puuid) {
                                seen_puuids.insert(participant_puuid.to_string());
                            }
                            continue;
                        }

                        if mode == KrakenMode::Focus && new_added_this_match >= max_new_focus {
                            seen_puuids.insert(participant_puuid.to_string());
                            continue;
                        }

                        let enqueued = kraken_maybe_enqueue_player(
                            participant_puuid,
                            &mut seen_puuids,
                            &mut queue,
                            &allowed_ranks,
                            &mut rank_cache,
                            client,
                            &mode,
                            Some(max_new_focus.saturating_sub(new_added_this_match)),
                        )?;
                        if enqueued {
                            new_added_this_match += 1;
                        }
                    }
                }
            }

            if write_allowed {
                if let Err(err) = save_match(&args.out_dir, &match_id, &match_json) {
                    eprintln!("Failed to save match {}: {}", match_id, err);
                    continue;
                }
                written_matches += 1;
                last_written_at = Instant::now();
            }

            downloaded_for_puuid += 1;
        }

        matches_per_player.insert(puuid.clone(), downloaded_for_puuid);
    }

    Ok(())
}

fn kraken_match_passes_roles(match_json: &Value, role_focus: Option<&HashSet<String>>) -> bool {
    let Some(role_focus) = role_focus else {
        return true;
    };

    if let Some(participants) = match_json
        .get("info")
        .and_then(|info| info.get("participants"))
        .and_then(|list| list.as_array())
    {
        for participant in participants {
            if let Some(role) = participant
                .get("teamPosition")
                .and_then(|r| r.as_str())
                .or_else(|| {
                    participant
                        .get("individualPosition")
                        .and_then(|r| r.as_str())
                })
            {
                if role_focus.contains(&role.to_uppercase()) {
                    return true;
                }
            }
        }
    }

    false
}

fn kraken_maybe_enqueue_player(
    puuid: &str,
    seen_puuids: &mut HashSet<String>,
    queue: &mut VecDeque<String>,
    allowed_ranks: &Option<HashSet<String>>,
    rank_cache: &mut HashMap<String, Option<String>>,
    client: &RiotClient,
    mode: &KrakenMode,
    remaining_focus_slots: Option<usize>,
) -> Result<bool, Box<dyn Error>> {
    if seen_puuids.contains(puuid) {
        return Ok(false);
    }

    if let Some(allowed) = allowed_ranks {
        let tier = if let Some(cached) = rank_cache.get(puuid) {
            cached.clone()
        } else {
            let tier = client
                .get_ranked_tier_by_puuid(puuid)?
                .map(|t| t.to_uppercase());
            rank_cache.insert(puuid.to_string(), tier.clone());
            tier
        };

        if let Some(tier_value) = tier {
            if !allowed.contains(&tier_value) {
                seen_puuids.insert(puuid.to_string());
                return Ok(false);
            }
        }
    }

    if let Some(limit) = remaining_focus_slots {
        if *mode == KrakenMode::Focus && limit == 0 {
            seen_puuids.insert(puuid.to_string());
            return Ok(false);
        }
    }

    seen_puuids.insert(puuid.to_string());
    queue.push_back(puuid.to_string());
    Ok(true)
}

fn save_match(out_dir: &PathBuf, match_id: &str, match_json: &Value) -> Result<(), Box<dyn Error>> {
    let serialized = serde_json::to_vec_pretty(match_json)?;
    let file_path = out_dir.join(format!("{}.json", match_id));
    fs::write(file_path, serialized)?;
    Ok(())
}
