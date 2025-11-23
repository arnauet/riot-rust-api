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

Team-level (two rows per match, one per side):
```bash
cargo run -- extract-parquet \
  --matches-dir data/raw/kraken_test \
  --out-parquet data/processed/team_match.parquet \
  --level team
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

### Summaries for harvested data

Lightweight checks straight from raw JSON:
```bash
cargo run -- kraken-summary \
  --matches-dir data/raw/kraken_absorb_test \
  --max-rows 500
```

Parquet-based summary with role and champion breakdowns:
```bash
cargo run -- kraken-summary \
  --player-parquet data/processed/player_match.parquet \
  --by-role \
  --by-champion-top-k 20
```

### Build ML-ready datasets

Player profiles (recent history per player-role):
```bash
cargo run -- kraken-prepare-ml \
  --variant player-profile-only \
  --player-parquet data/processed/player_match.parquet \
  --out-dir data/ml \
  --history-size 10 \
  --min-matches 5
```

Team outcome dataset (per team per match, post-game stats as features):
```bash
cargo run -- kraken-prepare-ml \
  --variant team-outcome \
  --team-parquet data/processed/team_match.parquet \
  --out-dir data/ml
```

Lobby outcome dataset (draft + optional profiles, no post-game leakage):
```bash
cargo run -- kraken-prepare-ml \
  --variant lobby-outcome \
  --player-parquet data/processed/player_match.parquet \
  --team-parquet data/processed/team_match.parquet \
  --out-dir data/ml
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

### Columns written to Parquet (--level team)
- `match_id`, `platform_id`, `queue_id`, `game_version`, `game_creation`, `game_duration`
- `team_id`, `team_side`, `team_win`
- `top_champion_id`, `jungle_champion_id`, `middle_champion_id`, `bottom_champion_id`, `utility_champion_id`
- Aggregates: `team_kills`, `team_deaths`, `team_assists`, `team_gold_earned`, `team_damage_to_champions`, `team_vision_score`, `team_cs_total`
- Per-minute metrics: `team_gold_per_min`, `team_damage_per_min`, `team_vision_score_per_min`, `team_cs_per_min`
- Objectives: `team_towers_destroyed`, `team_inhibitors_destroyed`, `team_dragons`, `team_barons`, `team_heralds`, `team_plates`
- First objectives (nullable): `first_blood`, `first_tower`, `first_inhibitor`, `first_baron`, `first_dragon`, `first_herald`

