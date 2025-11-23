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
- Crawl from seed PUUIDs to discover and download matches with rate-limited kraken harvesters.
- Extract player-level features from downloaded matches into a Parquet dataset for ML.

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

### Build a Parquet dataset for ML features
```bash
cargo run -- extract-parquet \
  --matches-dir data/raw/kraken_test \
  --out-parquet data/processed/player_match.parquet \
  --level player
```

### Kraken harvesters

Full crawl with flexible controls:
```bash
cargo run -- kraken-absorb \
  --seed-puuid PUUID_ONE \
  --seed-file seeds.txt \
  --duration-mins 60 \
  --out-dir data/raw/kraken \
  --max-req-per-2min 80 \
  --max-matches-per-player 100 \
  --max-matches-total 2000 \
  --idle-exit-after-mins 15 \
  --mode explore \
  --role-focus "JUNGLE,TOP" \
  --allow-ranks "EMERALD,DIAMOND" \
  --log-interval-secs 60
```

Quick snack crawl with safe defaults:
```bash
cargo run -- kraken-eat \
  --seed-puuid SOME_PUUID \
  --out-dir data/raw/kraken_snack \
  --duration-mins 10
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

### Columns written to Parquet (--level player)
- `match_id`, `game_creation`, `game_duration`, `queue_id`, `game_version`
- `team_id`, `puuid`, `champion_id`, `champion_name`, `role`, `win`
- `kills`, `deaths`, `assists`, `champ_level`, `gold_earned`, `gold_spent`
- `total_minions_killed`, `neutral_minions_killed`, `total_cs`
- `damage_to_champions`, `damage_to_objectives`, `damage_to_turrets`
- `turret_takedowns`, `inhibitor_takedowns`, `vision_score`, `wards_placed`, `wards_killed`, `control_wards_placed`
- Challenge-derived metrics (nullable): `damage_per_min`, `gold_per_min`, `team_damage_percentage`, `kill_participation`, `kda`, `vision_score_per_min`, `lane_minions_first10`, `jungle_cs_before10`

