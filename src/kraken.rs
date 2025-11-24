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
    let mut matches_per_player: HashMap<String, usize> = HashMap::new();

    for seed in seeds {
        let current_count = *matches_per_player.get(&seed).unwrap_or(&0);
        if kraken_maybe_enqueue_player(
            &seed,
            &mut seen_puuids,
            &mut queue,
            &allowed_ranks,
            &mut rank_cache,
            client,
            &mode,
            None,
            current_count,
        )? {
            continue;
        }
    }

    if queue.is_empty() {
        return Err("No seeds enqueued after applying filters".into());
    }

    let mut seen_match_ids: HashSet<String> = HashSet::new();
    let mut downloaded_matches: usize = 0;
    let mut written_matches: usize = 0;
    let start = Instant::now();
    let mut last_written_at = Instant::now();
    let max_duration = Duration::from_secs(args.duration_mins * 60);
    let idle_limit = args
        .idle_exit_after_mins
        .map(|mins| Duration::from_secs(mins * 60));
    let mut last_log = Instant::now();

    // AJUSTE: aumentar max_new_focus según el modo
    let max_new_focus = match mode {
        KrakenMode::Explore => 10,  // Agregar todos los jugadores
        KrakenMode::Focus => 5,     // Balance entre diversidad y profundidad
        KrakenMode::SeedOnly => 0,
    };

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
            // MEJORADO: logging con métricas de cobertura
            let avg_matches_per_player = if !matches_per_player.is_empty() {
                matches_per_player.values().sum::<usize>() as f64 
                    / matches_per_player.len() as f64
            } else {
                0.0
            };
            
            let profiles_with_10plus = matches_per_player
                .values()
                .filter(|&&count| count >= 10)
                .count();
            
            eprintln!(
                "[kraken-absorb] elapsed={}s fetched={} written={} queue={} seen_players={} profiles_10+={} avg_matches/player={:.1} max_req_per_2min={}",
                start.elapsed().as_secs(),
                downloaded_matches,
                written_matches,
                queue.len(),
                seen_puuids.len(),
                profiles_with_10plus,
                avg_matches_per_player,
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

            // NUEVO: Filtro temporal - solo partidas de últimos 90 días
            if !is_recent_match(&match_json, 90) {
                continue;
            }

            // NUEVO: Solo partidas ranked (queue_id 420)
            if !is_ranked_match(&match_json) {
                continue;
            }

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

                        // NUEVO: Priorizar jugadores con pocas partidas
                        let current_count = *matches_per_player
                            .get(participant_puuid)
                            .unwrap_or(&0);
                        
                        let enqueued = kraken_maybe_enqueue_player(
                            participant_puuid,
                            &mut seen_puuids,
                            &mut queue,
                            &allowed_ranks,
                            &mut rank_cache,
                            client,
                            &mode,
                            Some(max_new_focus.saturating_sub(new_added_this_match)),
                            current_count,
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

    // NUEVO: Estadísticas finales de cobertura
    print_coverage_stats(&matches_per_player, written_matches);

    Ok(())
}

// NUEVO: Verificar si la partida es reciente
fn is_recent_match(match_json: &Value, max_age_days: i64) -> bool {
    if let Some(game_creation) = match_json
        .get("info")
        .and_then(|info| info.get("gameCreation"))
        .and_then(|gc| gc.as_i64())
    {
        let now_millis = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_millis() as i64;
        
        let cutoff = now_millis - (max_age_days * 24 * 60 * 60 * 1000);
        return game_creation >= cutoff;
    }
    true  // Si no hay timestamp, incluir por seguridad
}

// NUEVO: Verificar si es partida ranked
fn is_ranked_match(match_json: &Value) -> bool {
    if let Some(queue_id) = match_json
        .get("info")
        .and_then(|info| info.get("queueId"))
        .and_then(|qid| qid.as_i64())
    {
        return queue_id == 420;  // Solo Ranked Solo/Duo
    }
    false
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

// MODIFICADO: Agregar priorización por count de partidas
fn kraken_maybe_enqueue_player(
    puuid: &str,
    seen_puuids: &mut HashSet<String>,
    queue: &mut VecDeque<String>,
    allowed_ranks: &Option<HashSet<String>>,
    rank_cache: &mut HashMap<String, Option<String>>,
    client: &RiotClient,
    mode: &KrakenMode,
    remaining_focus_slots: Option<usize>,
    current_match_count: usize,
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
    
    // NUEVO: Priorizar jugadores con pocas partidas (< 10)
    // Los agregamos al frente para procesarlos antes
    if current_match_count < 10 {
        queue.push_front(puuid.to_string());
    } else {
        queue.push_back(puuid.to_string());
    }
    
    Ok(true)
}

// NUEVO: Imprimir estadísticas de cobertura
fn print_coverage_stats(
    matches_per_player: &HashMap<String, usize>,
    total_matches: usize,
) {
    eprintln!("\n=== Coverage Statistics ===");
    
    let total_players = matches_per_player.len();
    let profiles_5plus = matches_per_player.values().filter(|&&c| c >= 5).count();
    let profiles_10plus = matches_per_player.values().filter(|&&c| c >= 10).count();
    let profiles_20plus = matches_per_player.values().filter(|&&c| c >= 20).count();
    
    eprintln!("Total unique players: {}", total_players);
    eprintln!("Profiles with 5+ matches: {} ({:.1}%)", 
              profiles_5plus, profiles_5plus as f64 / total_players as f64 * 100.0);
    eprintln!("Profiles with 10+ matches: {} ({:.1}%)", 
              profiles_10plus, profiles_10plus as f64 / total_players as f64 * 100.0);
    eprintln!("Profiles with 20+ matches: {} ({:.1}%)", 
              profiles_20plus, profiles_20plus as f64 / total_players as f64 * 100.0);
    
    if total_players > 0 {
        let sum: usize = matches_per_player.values().sum();
        let avg = sum as f64 / total_players as f64;
        eprintln!("Average matches per player: {:.1}", avg);
    }
    
    eprintln!("Total matches written: {}", total_matches);
    eprintln!("===========================\n");
}

fn save_match(out_dir: &PathBuf, match_id: &str, match_json: &Value) -> Result<(), Box<dyn Error>> {
    let serialized = serde_json::to_vec_pretty(match_json)?;
    let file_path = out_dir.join(format!("{}.json", match_id));
    fs::write(file_path, serialized)?;
    Ok(())
}
