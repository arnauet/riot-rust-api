use clap::{Parser, Subcommand};
use std::env;
use std::path::PathBuf;

mod riot_api;
mod sniff;
mod stats;

// Example usage:
// cargo run -- --game-name "DeadlyBubble" --tag-line "EUW"
// RIOT_PUUID="..." cargo run -- matches --count 10

#[derive(Parser, Debug)]
#[command(
    name = "riot-rust-api",
    about = "CLI client for Riot Games API",
    version
)]
struct Cli {
    /// Optional subcommand for additional actions
    #[command(subcommand)]
    command: Option<Commands>,

    /// Riot game name (e.g., Summoner name)
    #[arg(long = "game-name")]
    game_name: Option<String>,

    /// Riot tag line (e.g., region tag)
    #[arg(long = "tag-line")]
    tag_line: Option<String>,
}

#[derive(Subcommand, Debug)]
enum Commands {
    /// List match IDs for a given PUUID
    Matches {
        /// Player Universal Unique Identifier (can also come from RIOT_PUUID env var)
        #[arg(long = "puuid")]
        puuid: Option<String>,

        /// Number of matches to retrieve (default 20)
        #[arg(long = "count", default_value_t = 20)]
        count: usize,
    },

    /// Download match JSON payloads for a given PUUID and save to disk
    DownloadMatches {
        /// Player Universal Unique Identifier (can also come from RIOT_PUUID env var)
        #[arg(long = "puuid")]
        puuid: Option<String>,

        /// Number of matches to download
        #[arg(long = "count", default_value_t = 20)]
        count: usize,

        /// Output directory for saved match JSON files
        #[arg(long = "out-dir", default_value = "data/raw/matches")]
        out_dir: String,
    },

    /// Extract basic stats for downloaded matches and save them to CSV
    ExtractStats {
        /// Player Universal Unique Identifier (can also come from RIOT_PUUID env var)
        #[arg(long = "puuid")]
        puuid: Option<String>,

        /// Directory containing downloaded match JSON files
        #[arg(long = "matches-dir", default_value = "data/raw/matches")]
        matches_dir: String,

        /// Output CSV file path
        #[arg(
            long = "out-file",
            default_value = "data/processed/deadlybubble_basic.csv"
        )]
        out_file: String,
    },

    /// Crawl matches starting from seed PUUIDs, discovering new players along the way
    Sniff {
        /// Seed PUUIDs to start crawling from (at least one required)
        #[arg(long = "seed-puuid")]
        seed_puuid: Vec<String>,

        /// Duration in minutes for how long the crawler should run
        #[arg(long = "duration-mins")]
        duration_mins: u64,

        /// Output directory where downloaded match JSON files will be written
        #[arg(long = "out-dir")]
        out_dir: String,

        /// Maximum requests allowed in any 2-minute window (default 80 for safety)
        #[arg(long = "max-req-per-2min", default_value_t = 80)]
        max_req_per_2min: usize,

        /// Maximum unique matches to download per player
        #[arg(long = "max-matches-per-player", default_value_t = 100)]
        max_matches_per_player: usize,
    },
}

fn main() {
    let args = Cli::parse();

    match &args.command {
        Some(Commands::Matches { puuid, count }) => {
            let puuid_str = resolve_puuid(puuid);

            match riot_api::get_match_ids_by_puuid(&puuid_str, *count) {
                Ok(match_ids) => {
                    eprintln!("Fetched {} match IDs", match_ids.len());
                    for id in match_ids {
                        println!("{}", id);
                    }
                }
                Err(err) => {
                    eprintln!("Error fetching match IDs: {}", err);
                    std::process::exit(1);
                }
            }
        }
        Some(Commands::DownloadMatches {
            puuid,
            count,
            out_dir,
        }) => {
            let puuid_str = resolve_puuid(puuid);

            let out_path = PathBuf::from(out_dir);

            match riot_api::download_and_save_matches(&puuid_str, *count, &out_path) {
                Ok(()) => {
                    eprintln!("Saved {} matches to {}", count, out_dir);
                }
                Err(err) => {
                    eprintln!("Error downloading matches: {}", err);
                    std::process::exit(1);
                }
            }
        }
        Some(Commands::ExtractStats {
            puuid,
            matches_dir,
            out_file,
        }) => {
            let puuid_str = resolve_puuid(puuid);

            let matches_path = PathBuf::from(matches_dir);
            let out_path = PathBuf::from(out_file);

            if let Err(err) =
                stats::extract_basic_stats_for_puuid(&puuid_str, &matches_path, &out_path)
            {
                eprintln!("Error extracting stats: {}", err);
                std::process::exit(1);
            }
        }
        Some(Commands::Sniff {
            seed_puuid,
            duration_mins,
            out_dir,
            max_req_per_2min,
            max_matches_per_player,
        }) => {
            if seed_puuid.is_empty() {
                eprintln!("You must provide at least one --seed-puuid for sniffing");
                std::process::exit(1);
            }

            let client = match riot_api::RiotClient::new_with_max(*max_req_per_2min) {
                Ok(client) => client,
                Err(err) => {
                    eprintln!("Failed to create Riot API client: {}", err);
                    std::process::exit(1);
                }
            };

            let args = sniff::SniffArgs {
                seed_puuids: seed_puuid.clone(),
                duration_mins: *duration_mins,
                out_dir: PathBuf::from(out_dir),
                max_req_per_2min: *max_req_per_2min,
                max_matches_per_player: *max_matches_per_player,
            };

            if let Err(err) = sniff::run_sniff(args, client) {
                eprintln!("Error running sniff crawler: {}", err);
                std::process::exit(1);
            }
        }
        None => {
            let game_name = args.game_name.as_deref().unwrap_or("");
            let tag_line = args.tag_line.as_deref().unwrap_or("");

            if game_name.is_empty() || tag_line.is_empty() {
                eprintln!(
                    "Both --game-name and --tag-line must be provided when not using a subcommand"
                );
                std::process::exit(1);
            }

            match riot_api::get_puuid(game_name, tag_line) {
                Ok(puuid) => println!("{}", puuid),
                Err(err) => {
                    eprintln!("Error fetching PUUID: {}", err);
                    std::process::exit(1);
                }
            }
        }
    }
}

fn resolve_puuid(puuid_arg: &Option<String>) -> String {
    match puuid_arg {
        Some(value) if !value.trim().is_empty() => value.clone(),
        _ => match env::var("RIOT_PUUID") {
            Ok(env_value) if !env_value.trim().is_empty() => env_value,
            _ => {
                eprintln!("You must provide --puuid or define RIOT_PUUID in the environment");
                std::process::exit(1);
            }
        },
    }
}
