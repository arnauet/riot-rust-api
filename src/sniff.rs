use crate::riot_api::RiotClient;
use serde_json::Value;
use std::collections::{HashMap, HashSet, VecDeque};
use std::error::Error;
use std::fs;
use std::path::PathBuf;
use std::time::{Duration, Instant};

#[derive(Debug)]
pub struct SniffArgs {
    pub seed_puuids: Vec<String>,
    pub duration_mins: u64,
    pub out_dir: PathBuf,
    pub max_req_per_2min: usize,
    pub max_matches_per_player: usize,
}

pub async fn run_sniff(args: SniffArgs, client: RiotClient) -> Result<(), Box<dyn Error>> {
    fs::create_dir_all(&args.out_dir)?;

    eprintln!(
        "Starting sniff with max {} requests per 2 minutes",
        args.max_req_per_2min
    );

    let mut queue: VecDeque<String> = args.seed_puuids.iter().cloned().collect();
    let mut seen_puuids: HashSet<String> = args.seed_puuids.iter().cloned().collect();
    let mut seen_match_ids: HashSet<String> = HashSet::new();
    let mut matches_per_player: HashMap<String, usize> = HashMap::new();
    let mut downloaded_matches: usize = 0;
    let start = Instant::now();
    let max_duration = Duration::from_secs(args.duration_mins * 60);

    while !queue.is_empty() && start.elapsed() < max_duration {
        let puuid = match queue.pop_front() {
            Some(p) => p,
            None => break,
        };

        let mut downloaded_for_puuid = *matches_per_player.get(&puuid).unwrap_or(&0);

        // Get up to 100 match IDs for this player using the shared rate limiter.
        let match_ids = match client.get_match_ids_by_puuid(&puuid, 100).await {
            Ok(ids) => ids,
            Err(err) => {
                eprintln!("Failed to fetch match IDs for {}: {}", puuid, err);
                continue;
            }
        };

        for match_id in match_ids {
            if downloaded_for_puuid >= args.max_matches_per_player {
                break;
            }

            if !seen_match_ids.insert(match_id.clone()) {
                continue;
            }

            let match_json: Value = match client.get_match_json(&match_id).await {
                Ok(json) => json,
                Err(err) => {
                    eprintln!("Failed to fetch match {}: {}", match_id, err);
                    continue;
                }
            };

            if let Err(err) = save_match(&args.out_dir, &match_id, &match_json) {
                eprintln!("Failed to save match {}: {}", match_id, err);
                continue;
            }

            // Enqueue new participants for crawling.
            if let Some(participants) = match_json
                .get("metadata")
                .and_then(|metadata| metadata.get("participants"))
                .and_then(|list| list.as_array())
            {
                for participant in participants {
                    if let Some(participant_puuid) = participant.as_str() {
                        if seen_puuids.insert(participant_puuid.to_string()) {
                            queue.push_back(participant_puuid.to_string());
                        }
                    }
                }
            }

            downloaded_for_puuid += 1;
            downloaded_matches += 1;
        }

        matches_per_player.insert(puuid.clone(), downloaded_for_puuid);

        eprintln!(
            "Sniff progress: downloaded {} matches, queue size {}, elapsed {}s",
            downloaded_matches,
            queue.len(),
            start.elapsed().as_secs()
        );
    }

    Ok(())
}

fn save_match(out_dir: &PathBuf, match_id: &str, match_json: &Value) -> Result<(), Box<dyn Error>> {
    let serialized = serde_json::to_vec_pretty(match_json)?;
    let file_path = out_dir.join(format!("{}.json", match_id));
    fs::write(file_path, serialized)?;
    Ok(())
}
