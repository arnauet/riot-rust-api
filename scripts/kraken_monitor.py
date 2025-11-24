#!/usr/bin/env python3

"""

Helps monitoring kraken data absortion and the DataSet quality in real time


"""

import polars as pl
from pathlib import Path
import sys
import time
from datetime import datetime

def analyze_coverage(
    player_parquet: Path,
    match_parquet: Path,
    profile_parquet: Path,
    lobby_parquet: Path,
):
    print("\n" + "=" * 80)
    print(f"📊 Dataset Coverage Analysis - {datetime.now()}")
    print("=" * 80)
    
    # 1. Partidas totales
    try:
        matches = pl.read_parquet(match_parquet)
        total_matches = matches.height
        print(f"\n✓ Total matches: {total_matches:,}")
        
        # Solo ranked
        ranked_matches = matches.filter(pl.col("queue_id") == 420)
        print(f"  └─ Ranked matches: {ranked_matches.height:,} ({ranked_matches.height/total_matches:.1%})")
    except Exception as e:
        print(f"✗ Could not read matches: {e}")
        total_matches = 0
    
    # 2. Perfiles de jugadores
    try:
        profiles = pl.read_parquet(profile_parquet)
        total_profiles = profiles.height
        print(f"\n✓ Player profiles: {total_profiles:,}")
        
        # Distribución de games_used
        if "games_used" in profiles.columns:
            games_dist = profiles.group_by("games_used").agg(
                pl.count().alias("count")
            ).sort("games_used")
            
            profiles_5plus = profiles.filter(pl.col("games_used") >= 5).height
            profiles_10plus = profiles.filter(pl.col("games_used") >= 10).height
            profiles_20plus = profiles.filter(pl.col("games_used") >= 20).height
            
            print(f"  ├─ With 5+ matches: {profiles_5plus:,} ({profiles_5plus/total_profiles:.1%})")
            print(f"  ├─ With 10+ matches: {profiles_10plus:,} ({profiles_10plus/total_profiles:.1%})")
            print(f"  └─ With 20+ matches: {profiles_20plus:,} ({profiles_20plus/total_profiles:.1%})")
    except Exception as e:
        print(f"✗ Could not read profiles: {e}")
        total_profiles = 0
    
    # 3. Cobertura en lobby
    try:
        lobby = pl.read_parquet(lobby_parquet)
        total_lobby_rows = lobby.height
        print(f"\n✓ Lobby outcome rows: {total_lobby_rows:,}")
        
        # Detectar columnas de histórico
        recent_cols = [c for c in lobby.columns if "recent_" in c]
        print(f"  └─ History features: {len(recent_cols)}")
        
        if recent_cols:
            # Cobertura completa: todas las features sin null
            complete_mask = pl.all_horizontal([
                pl.col(c).is_not_null() for c in recent_cols
            ])
            complete = lobby.filter(complete_mask)
            complete_rows = complete.height
            
            print(f"\n📈 Coverage Analysis:")
            print(f"  ├─ Rows with complete coverage: {complete_rows:,}")
            print(f"  ├─ Coverage ratio: {complete_rows/total_lobby_rows:.1%}")
            print(f"  └─ Matches with complete coverage: {complete_rows//2:,}")
            
            # Objetivo para ML
            min_viable = 100_000  # 100K partidas
            min_good = 200_000    # 200K partidas
            
            if complete_rows >= min_good:
                print(f"\n  ✅ EXCELLENT: Dataset ready for production ML!")
            elif complete_rows >= min_viable:
                print(f"\n  ✓ VIABLE: Dataset sufficient for initial training")
                print(f"     Need {min_good - complete_rows:,} more rows for production")
            else:
                print(f"\n  ⚠️  INSUFFICIENT: Need {min_viable - complete_rows:,} more rows")
                print(f"     Continue data collection...")
            
            # Estimación de partidas necesarias
            if total_matches > 0 and total_profiles > 0:
                coverage_rate = complete_rows / (total_matches * 2)  # *2 porque 2 rows por match
                print(f"\n📊 Projections:")
                print(f"  Current coverage rate: {coverage_rate:.1%}")
                
                if coverage_rate > 0:
                    matches_needed_viable = int(min_viable / coverage_rate / 2)
                    matches_needed_good = int(min_good / coverage_rate / 2)
                    
                    print(f"  Matches needed for viable dataset: ~{matches_needed_viable:,}")
                    print(f"  Matches needed for good dataset: ~{matches_needed_good:,}")
                    
                    # Estimación con tasa actual
                    matches_per_profile = total_matches / total_profiles if total_profiles > 0 else 0
                    profiles_needed = int(matches_needed_good * 10 / matches_per_profile) if matches_per_profile > 0 else 0
                    print(f"  Estimated profiles needed: ~{profiles_needed:,}")
        
    except Exception as e:
        print(f"✗ Could not analyze lobby: {e}")
    
    print("\n" + "=" * 80 + "\n")


def main():
    player_parquet = Path("data/parquet/player.parquet")
    match_parquet = Path("data/parquet/match.parquet")
    profile_parquet = Path("data/ml/player_profile.parquet")
    lobby_parquet = Path("data/ml/ml_lobby_outcome.parquet")
    
    # Verificar que existan
    for p in [player_parquet, match_parquet, profile_parquet, lobby_parquet]:
        if not p.exists():
            print(f"⚠️  File not found: {p}")
            print(f"   Run the pipeline first to generate parquet files")
    
    if "--watch" in sys.argv:
        print("👀 Monitoring mode: updating every 5 minutes...")
        print("   Press Ctrl+C to stop")
        while True:
            try:
                analyze_coverage(player_parquet, match_parquet, profile_parquet, lobby_parquet)
                time.sleep(300)  # 5 minutos
            except KeyboardInterrupt:
                print("\n\nStopping monitor...")
                break
    else:
        analyze_coverage(player_parquet, match_parquet, profile_parquet, lobby_parquet)


if __name__ == "__main__":
    main()
