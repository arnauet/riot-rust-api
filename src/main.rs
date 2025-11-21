use clap::{Parser, Subcommand};

mod riot_api;

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
        /// Player Universal Unique Identifier
        #[arg(long = "puuid")]
        puuid: String,

        /// Number of matches to retrieve (default 20)
        #[arg(long = "count", default_value_t = 20)]
        count: usize,
    },
}

#[tokio::main]
async fn main() {
    let args = Cli::parse();

    // Si hay subcomando, lo usamos
    if let Some(command) = &args.command {
        match command {
            Commands::Matches { puuid, count } => {
                match riot_api::get_match_ids_by_puuid(puuid, *count).await {
                    Ok(match_ids) => {
                        for id in match_ids {
                            println!("{id}");
                        }
                    }
                    Err(err) => {
                        eprintln!("Error fetching match IDs: {err}");
                        std::process::exit(1);
                    }
                }
            }
        }
    } else {
        // Modo “por defecto”: obtener PUUID a partir de game name + tag line
        let game_name = match &args.game_name {
            Some(s) if !s.is_empty() => s,
            _ => {
                eprintln!("--game-name es obligatorio si no usas subcomando");
                std::process::exit(1);
            }
        };

        let tag_line = match &args.tag_line {
            Some(s) if !s.is_empty() => s,
            _ => {
                eprintln!("--tag-line es obligatorio si no usas subcomando");
                std::process::exit(1);
            }
        };

        match riot_api::get_puuid(game_name, tag_line).await {
            Ok(puuid) => println!("{puuid}"),
            Err(err) => {
                eprintln!("Error fetching PUUID: {err}");
                std::process::exit(1);
            }
        }
    }
}

