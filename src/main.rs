use clap::Parser;

mod riot_api;

#[derive(Parser, Debug)]
#[command(name = "riot-rust-api", about = "CLI client for Riot Games API", version)]
struct Cli {
    /// Riot game name (e.g., Summoner name)
    #[arg(long = "game-name")]
    game_name: String,

    /// Riot tag line (e.g., region tag)
    #[arg(long = "tag-line")]
    tag_line: String,
}

#[tokio::main]
async fn main() {
    let args = Cli::parse();

    match riot_api::get_puuid(&args.game_name, &args.tag_line).await {
        Ok(puuid) => println!("{}", puuid),
        Err(err) => {
            eprintln!("Error fetching PUUID: {}", err);
            std::process::exit(1);
        }
    }
}
