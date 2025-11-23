use clap::{Parser, Subcommand};
use std::env;
use std::path::PathBuf;

mod kraken;
mod parquet_extract;
mod riot_api;
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

    /// Long-running kraken harvester for crawling matches
    KrakenAbsorb {
        /// Optional single seed PUUID to start crawling from
        #[arg(long = "seed-puuid")]
        seed_puuid: Option<String>,

        /// Optional file containing one PUUID per line
        #[arg(long = "seed-file")]
        seed_file: Option<String>,

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

        /// Stop after writing this many matches in total (optional)
        #[arg(long = "max-matches-total")]
        max_matches_total: Option<usize>,

        /// Exit if no matches are written for this many minutes (optional)
        #[arg(long = "idle-exit-after-mins")]
        idle_exit_after_mins: Option<u64>,

        /// Crawl strategy: explore, focus, or seed-only
        #[arg(long = "mode", default_value = "explore")]
        mode: String,

        /// Comma-separated list of roles to keep when writing matches
        #[arg(long = "role-focus")]
        role_focus: Option<String>,

        /// Comma-separated list of allowed tiers for rank filtering
        #[arg(long = "allow-ranks")]
        allow_ranks: Option<String>,

        /// Progress log interval in seconds
        #[arg(long = "log-interval-secs", default_value_t = 60)]
        log_interval_secs: u64,
    },

    /// Quick kraken crawl with opinionated defaults
    KrakenEat {
        /// Seed PUUID to start crawling from
        #[arg(long = "seed-puuid")]
        seed_puuid: String,

        /// Output directory for downloaded matches
        #[arg(long = "out-dir")]
        out_dir: String,

        /// Optional duration in minutes (default 10)
        #[arg(long = "duration-mins")]
        duration_mins: Option<u64>,
    },

    /// Extract player- or team-level features into Parquet for ML workflows
    ExtractParquet {
        /// Directory containing downloaded match JSON files
        #[arg(long = "matches-dir")]
        matches_dir: String,

        /// Output Parquet file path
        #[arg(long = "out-parquet")]
        out_parquet: String,

        /// Aggregation level (currently only 'player' is supported)
        #[arg(long = "level")]
        level: String,
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
        Some(Commands::KrakenAbsorb {
            seed_puuid,
            seed_file,
            duration_mins,
            out_dir,
            max_req_per_2min,
            max_matches_per_player,
            max_matches_total,
            idle_exit_after_mins,
            mode,
            role_focus,
            allow_ranks,
            log_interval_secs,
        }) => {
            let client = match riot_api::RiotClient::new_with_max(*max_req_per_2min) {
                Ok(client) => client,
                Err(err) => {
                    eprintln!("Failed to create Riot API client: {}", err);
                    std::process::exit(1);
                }
            };

            let args = kraken::KrakenAbsorbArgs {
                seed_puuid: seed_puuid.clone(),
                seed_file: seed_file.as_ref().map(PathBuf::from),
                duration_mins: *duration_mins,
                out_dir: PathBuf::from(out_dir),
                max_req_per_2min: *max_req_per_2min,
                max_matches_per_player: *max_matches_per_player,
                max_matches_total: *max_matches_total,
                idle_exit_after_mins: *idle_exit_after_mins,
                mode: mode.clone(),
                role_focus: role_focus.clone(),
                allow_ranks: allow_ranks.clone(),
                log_interval_secs: *log_interval_secs,
            };

            if let Err(err) = kraken::kraken_absorb_run(&args, &client) {
                eprintln!("Error running kraken-absorb crawler: {}", err);
                std::process::exit(1);
            }
        }
        Some(Commands::KrakenEat {
            seed_puuid,
            out_dir,
            duration_mins,
        }) => {
            let client = match riot_api::RiotClient::new_with_max(60) {
                Ok(client) => client,
                Err(err) => {
                    eprintln!("Failed to create Riot API client: {}", err);
                    std::process::exit(1);
                }
            };

            let args = kraken::KrakenEatArgs {
                seed_puuid: seed_puuid.clone(),
                out_dir: PathBuf::from(out_dir),
                duration_mins: *duration_mins,
            };

            if let Err(err) = kraken::kraken_eat_run(&args, &client) {
                eprintln!("Error running kraken-eat crawler: {}", err);
                std::process::exit(1);
            }
        }
        Some(Commands::ExtractParquet {
            matches_dir,
            out_parquet,
            level,
        }) => {
            let matches_path = PathBuf::from(matches_dir);
            let out_path = PathBuf::from(out_parquet);

            if let Err(err) =
                parquet_extract::extract_parquet(&matches_path, &out_path, level.as_str())
            {
                eprintln!("Error extracting Parquet dataset: {}", err);
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
