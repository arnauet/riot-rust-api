# riot-rust-api

Rust CLI to interact with the Riot API and work with downloaded matches.

## Quick requirements
- Environment variable `RIOT_API_KEY` with your Riot API key.
- Player PUUID provided via `--puuid` or the `RIOT_PUUID` environment variable.

## Core features
- Resolve a PUUID from a Riot game name and tag line.
- List match IDs for a given PUUID.
- Download matches and save them as JSON files.
- Extract basic statistics from downloaded matches into a CSV file.

## Usage examples

### Get a PUUID from game name and tag
```bash
cargo run -- --game-name "DeadlyBubble" --tag-line "EUW"
```

### List match IDs
```bash
RIOT_PUUID="..." cargo run -- matches --count 10
```

### Download matches to disk
```bash
RIOT_PUUID="..." cargo run -- download-matches \
  --count 20 \
  --out-dir data/raw/matches
```

### Extract basic stats to CSV
```bash
RIOT_PUUID="..." cargo run -- extract-stats \
  --matches-dir data/raw/matches \
  --out-file data/processed/deadlybubble_basic.csv
```

### Fields parsed into the CSV
- `match_id`
- `game_creation` (timestamp)
- `queue_id`
- `champion_name`
- `role`
- `win` (1/0)
- `kills`, `deaths`, `assists`
- `cs_total` (total + neutral minions)
- `gold_earned`
- `game_duration` (seconds)
