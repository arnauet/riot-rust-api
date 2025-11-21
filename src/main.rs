use clap::{Parser, Subcommand};

mod riot_api;

// Example usage:
// cargo run -- --game-name "DeadlyBubble" --tag-line "EUW"
// RIOT_PUUID="..." cargo run -- matches --count 10

#[derive(Parser, Debug)]
#[command(name = "riot-rust-api", about = "CLI client for Riot Games API", version)]
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
        #[arg(long = "puuid", env = "RIOT_PUUID")]
        puuid: String,

        /// Number of matches to retrieve (default 20)
        #[arg(long = "count", default_value_t = 20)]
        count: usize,
    },
}

#[tokio::main]
async fn main() {
    let args = Cli::parse();

    match &args.command {
        Some(Commands::Matches { puuid, count }) => {
            match riot_api::get_match_ids_by_puuid(puuid, *count).await {
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
