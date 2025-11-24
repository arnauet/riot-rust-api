#!/usr/bin/env python3
"""
kraken_check.py

sanity checks for generated datasets by kraken:

- data/ml/player_profile.parquet
- data/ml/ml_team_outcome.parquet
- data/ml/ml_lobby_outcome.parquet

Ejemplos:

  # check routes
  python kraken_check.py --all

  # size of history-size and min_matches respected
  python kraken_check.py --check-player-profile \
    --history-size 10 --min-matches 5

  # form sanity check
  python kraken_check.py --all --form
"""

import argparse
import sys
from pathlib import Path

import polars as pl

def p(msg: str) -> None:
    """Pretty print simple."""
    print(msg, flush=True)


def header(title: str) -> None:
    p("\n" + "=" * 80)
    p(title)
    p("=" * 80)

def check_player_profile(
    path: Path,
    form_only: bool,
    history_size: int | None,
    min_matches: int | None,
) -> None:
    header(f"PLAYER PROFILE :: {path}")

    if not path.exists():
        p(f"[ERROR] File not found: {path}")
        return

    df = pl.read_parquet(path)

    p(f"Shape: {df.shape}")
    p(f"Columns ({len(df.columns)}): {df.columns}")

    if form_only:
        return

    # 1) unicidad (puuid, role)
    # Check if (puuid, role) combinations are unique
    n_total = df.height
    n_unique = df.select(["puuid", "role"]).unique().height
    has_dupes = n_total != n_unique
    
    if has_dupes:
        n_dupes = n_total - n_unique
        p(f"[WARN] (puuid, role) NO es único. Filas duplicadas: {n_dupes}")
    else:
        p("[OK] (puuid, role) es clave única.")

    # 2) distribución de games_used
    if "games_used" in df.columns:
        p("\nDistribución de games_used:")
        desc = df.select("games_used").describe()
        p(desc)

        gmin = int(df["games_used"].min())
        gmax = int(df["games_used"].max())
        p(f"games_used min={gmin}, max={gmax}")

        if history_size is not None:
            if gmax > history_size:
                p(f"[WARN] games_used max={gmax} > history_size={history_size}")
            else:
                p(f"[OK] games_used ≤ history_size={history_size}")

        if min_matches is not None:
            if gmin < min_matches:
                p(f"[WARN] games_used min={gmin} < min_matches={min_matches}")
            else:
                p(f"[OK] games_used ≥ min_matches={min_matches}")
    else:
        p("[WARN] 'games_used' no está en player_profile; no se puede validar historia.")

    # 3) chequeo rápido de NaNs / nulls en features numéricas clave
    num_cols = [
        c for c in df.columns
        if df.schema[c] in (pl.Float64, pl.Float32, pl.Int64, pl.Int32, pl.UInt32)
        and c not in ("games_used",)
    ]
    if num_cols:
        p("\nNulls por columna numérica (top 10):")
        null_counts = (
            df.select([pl.col(c).null_count().alias(c) for c in num_cols])
              .transpose(include_header=True, header_name="column", column_names=["nulls"])
              .sort("nulls", descending=True)
        )
        # solo mostrar las 10 peores
        p(null_counts.head(10))


# -------------------- checks para ml_team_outcome -------------------- #

def check_team_outcome(path: Path, form_only: bool) -> None:
    header(f"TEAM OUTCOME :: {path}")

    if not path.exists():
        p(f"[ERROR] File not found: {path}")
        return

    df = pl.read_parquet(path)
    p(f"Shape: {df.shape}")
    p(f"Columns ({len(df.columns)}): {df.columns}")

    if form_only:
        return

    # 1) dos filas por partida, una por team_id
    per_match = (
        df.group_by("match_id")
          .agg([
              pl.len().alias("rows"),
              pl.n_unique("team_id").alias("teams"),
              pl.col("team_win").sum().alias("wins_sum"),
          ])
    )
    p("\nDistribución de filas por match_id:")
    p(per_match["rows"].value_counts().sort("rows"))

    bad_rows = per_match.filter(pl.col("rows") != 2)
    if bad_rows.height > 0:
        p(f"[WARN] Algunos match_id no tienen 2 filas (ejemplo):")
        p(bad_rows.head(5))
    else:
        p("[OK] Todos los match_id tienen 2 filas.")

    bad_teams = per_match.filter(pl.col("teams") != 2)
    if bad_teams.height > 0:
        p("[WARN] Algunos match_id no tienen 2 team_id distintos (ejemplo):")
        p(bad_teams.head(5))
    else:
        p("[OK] Todos los match_id tienen team_id {100, 200} (en práctica 2 equipos).")

    # 2) consistencia de team_win (exactamente 1 victoria por partida ranked)
    soloq = df.filter(pl.col("queue_id") == 420)
    if soloq.height > 0:
        per_match_soloq = (
            soloq.group_by("match_id")
                 .agg(pl.col("team_win").sum().alias("wins_sum"))
        )
        bad_wins = per_match_soloq.filter(pl.col("wins_sum") != 1)
        if bad_wins.height > 0:
            p("[WARN] En SoloQ hay partidas donde wins_sum != 1 (ejemplo):")
            p(bad_wins.head(5))
        else:
            p("[OK] En SoloQ siempre hay exactamente un team_win=1 por match_id.")

        # Side winrate
        p("\nSoloQ side winrate:")
        side_wr = (
            soloq.group_by("team_side")
                 .agg(pl.col("team_win").cast(pl.Float64).mean().alias("win_rate"))
        )
        p(side_wr)
    else:
        p("[WARN] No hay filas con queue_id=420 en ml_team_outcome.")


# -------------------- checks para ml_lobby_outcome -------------------- #

def check_lobby_outcome(path: Path, form_only: bool) -> None:
    header(f"LOBBY OUTCOME :: {path}")

    if not path.exists():
        p(f"[ERROR] File not found: {path}")
        return

    df = pl.read_parquet(path)
    p(f"Shape: {df.shape}")
    p(f"Columns ({len(df.columns)}): {df.columns}")

    if form_only:
        return

    # 1) dos filas por partida
    per_match = (
        df.group_by("match_id")
          .agg(pl.len().alias("rows"))
    )
    p("\nDistribución de filas por match_id:")
    p(per_match["rows"].value_counts().sort("rows"))

    bad_rows = per_match.filter(pl.col("rows") != 2)
    if bad_rows.height > 0:
        p("[WARN] Algunos match_id en lobby_outcome no tienen 2 filas (ejemplo):")
        p(bad_rows.head(5))
    else:
        p("[OK] Todos los match_id tienen 2 filas en lobby_outcome.")

    # 2) campeones aliados/enemigos no nulos (mínimo, que la mayoría tengan datos)
    champ_cols = [c for c in df.columns if c.endswith("_champion_id")]
    if champ_cols:
        p("\nNulls en columnas *_champion_id (top):")
        nulls = (
            df.select([pl.col(c).null_count().alias(c) for c in champ_cols])
              .transpose(include_header=True, header_name="column", column_names=["nulls"])
              .sort("nulls", descending=True)
        )
        p(nulls.head(10))

    # 3) cobertura de player_profile: cuántas filas tienen historiales completos
    recent_cols = [c for c in df.columns if "recent_" in c]
    if recent_cols:
        any_null_recent = (
            df.select(
                pl.any_horizontal([pl.col(c).is_null() for c in recent_cols])
                  .alias("any_null_recent")
            )
        )
        n_with_nulls = int(any_null_recent["any_null_recent"].sum())
        p(
            f"\nFilas con algún null en columnas de historial reciente: "
            f"{n_with_nulls} / {df.height} "
            f"({n_with_nulls / df.height:.1%})"
        )
    else:
        p("[WARN] No hay columnas recent_* en ml_lobby_outcome; ¿se generó bien?")


# -------------------- main / CLI -------------------- #

def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Sanity checks para datasets del Kraken",
    )
    parser.add_argument(
        "--player-profile",
        type=Path,
        default=Path("data/ml/player_profile.parquet"),
        help="Ruta a player_profile.parquet",
    )
    parser.add_argument(
        "--team-outcome",
        type=Path,
        default=Path("data/ml/ml_team_outcome.parquet"),
        help="Ruta a ml_team_outcome.parquet",
    )
    parser.add_argument(
        "--lobby-outcome",
        type=Path,
        default=Path("data/ml/ml_lobby_outcome.parquet"),
        help="Ruta a ml_lobby_outcome.parquet",
    )

    # qué comprobar
    parser.add_argument("--check-player-profile", action="store_true")
    parser.add_argument("--check-team-outcome", action="store_true")
    parser.add_argument("--check-lobby-outcome", action="store_true")
    parser.add_argument(
        "--all",
        action="store_true",
        help="Ejecutar todos los checks (por defecto si no se especifica ninguno)",
    )

    # opciones extra
    parser.add_argument(
        "--form",
        action="store_true",
        help="Modo simple: sólo shapes y columnas (sin checks fuertes)",
    )
    parser.add_argument(
        "--history-size",
        type=int,
        default=None,
        help="Valor esperado de history_size utilizado al crear player_profile "
             "(para validar games_used <= history_size).",
    )
    parser.add_argument(
        "--min-matches",
        type=int,
        default=None,
        help="Valor esperado de min_matches utilizado al crear player_profile "
             "(para validar games_used >= min_matches).",
    )

    return parser.parse_args()


def main() -> None:
    args = parse_args()

    # si no se especifica nada, asumimos --all
    if not (args.check_player_profile or args.check_team_outcome or args.check_lobby_outcome):
        args.all = True

    if args.all:
        args.check_player_profile = True
        args.check_team_outcome = True
        args.check_lobby_outcome = True

    if args.check_player_profile:
        check_player_profile(
            args.player_profile,
            form_only=args.form,
            history_size=args.history_size,
            min_matches=args.min_matches,
        )

    if args.check_team_outcome:
        check_team_outcome(args.team_outcome, form_only=args.form)

    if args.check_lobby_outcome:
        check_lobby_outcome(args.lobby_outcome, form_only=args.form)


if __name__ == "__main__":
    main()
