use clap::{Parser, Subcommand};
use std::env;
use std::path::PathBuf;

mod riot_api;

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
}

#[tokio::main]
async fn main() {
    let args = Cli::parse();

    match &args.command {
        Some(Commands::Matches { puuid, count }) => {
            let puuid_str = match puuid {
                Some(value) if !value.trim().is_empty() => value.clone(),
                _ => match env::var("RIOT_PUUID") {
                    Ok(env_value) if !env_value.trim().is_empty() => env_value,
                    _ => {
                        eprintln!(
                            "You must provide --puuid or define RIOT_PUUID in the environment"
                        );
                        std::process::exit(1);
                    }
                },
            };

            match riot_api::get_match_ids_by_puuid(&puuid_str, *count).await {
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
            let puuid_str = match puuid {
                Some(value) if !value.trim().is_empty() => value.clone(),
                _ => match env::var("RIOT_PUUID") {
                    Ok(env_value) if !env_value.trim().is_empty() => env_value,
                    _ => {
                        eprintln!(
                            "You must provide --puuid or define RIOT_PUUID in the environment"
                        );
                        std::process::exit(1);
                    }
                },
            };

            let out_path = PathBuf::from(out_dir);

            match riot_api::download_and_save_matches(&puuid_str, *count, &out_path).await {
                Ok(()) => {
                    eprintln!("Saved {} matches to {}", count, out_dir);
                }
                Err(err) => {
                    eprintln!("Error downloading matches: {}", err);
                    std::process::exit(1);
                }
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

            match riot_api::get_puuid(game_name, tag_line).await {
                Ok(puuid) => println!("{}", puuid),
                Err(err) => {
                    eprintln!("Error fetching PUUID: {}", err);
                    std::process::exit(1);
                }
            }
        }
    }
}
