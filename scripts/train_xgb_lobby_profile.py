#!/usr/bin/env python
"""
train_xgb_lobby_profile.py

Ablation experiments for lobby win prediction:
- mode=champs   : only team_side + champs (ally/enemy)
- mode=profiles : only player profile stats (ally/enemy)
- mode=full     : champs + profiles + ally-enemy deltas

Uses:
- data/ml/ml_lobby_outcome.parquet
"""

import argparse
from typing import List

import numpy as np
import polars as pl
from sklearn.metrics import (
    accuracy_score,
    classification_report,
    roc_auc_score,
)
from sklearn.model_selection import GroupShuffleSplit
import xgboost as xgb


def build_feature_columns(mode: str) -> dict:
    """
    Devuelve los nombres base de columnas que vamos a usar según el modo.
    No toca el DataFrame, solo define qué listas de columnas queremos.
    """

    roles = ["top", "jungle", "middle", "bottom", "utility"]

    champ_cols_ally = [
        f"ally_{r}_champion_id" for r in roles
    ]
    champ_cols_enemy = [
        f"enemy_{r}_champion_id" for r in roles
    ]

    # Perfiles recientes: games, winrate, gpm, dpm, vspm
    profile_stats = [
        "recent_games",
        "recent_winrate",
        "recent_gold_per_min",
        "recent_damage_per_min",
        "recent_vision_per_min",
    ]

    ally_profile_cols = [
        f"ally_{role}_{stat}"
        for role in roles
        for stat in profile_stats
    ]
    enemy_profile_cols = [
        f"enemy_{role}_{stat}"
        for role in roles
        for stat in profile_stats
    ]

    # Nombres de las columnas de deltas que crearemos luego
    delta_cols = [
        f"{role}_diff_{stat}"
        for role in roles
        for stat in profile_stats
    ]

    if mode == "champs":
        feature_cols = ["side_blue"] + champ_cols_ally + champ_cols_enemy
    elif mode == "profiles":
        feature_cols = ["side_blue"] + ally_profile_cols + enemy_profile_cols
    elif mode == "full":
        feature_cols = (
            ["side_blue"]
            + champ_cols_ally
            + champ_cols_enemy
            + ally_profile_cols
            + enemy_profile_cols
            + delta_cols
        )
    else:
        raise ValueError(f"Unknown mode: {mode}")

    return {
        "roles": roles,
        "profile_stats": profile_stats,
        "champ_cols_ally": champ_cols_ally,
        "champ_cols_enemy": champ_cols_enemy,
        "ally_profile_cols": ally_profile_cols,
        "enemy_profile_cols": enemy_profile_cols,
        "delta_cols": delta_cols,
        "feature_cols": feature_cols,
    }


def main():
    parser = argparse.ArgumentParser(
        description="Train XGBoost lobby+profile models with ablations."
    )
    parser.add_argument(
        "--lobby-parquet",
        type=str,
        default="data/ml/ml_lobby_outcome.parquet",
        help="Path to ml_lobby_outcome.parquet",
    )
    parser.add_argument(
        "--mode",
        type=str,
        choices=["champs", "profiles", "full"],
        default="full",
        help=(
            "Ablation mode:\n"
            "  champs   -> side + champs ally/enemy\n"
            "  profiles-> side + ally/enemy profile stats\n"
            "  full    -> champs + profiles + ally-enemy deltas"
        ),
    )
    parser.add_argument(
        "--test-size",
        type=float,
        default=0.2,
        help="Test fraction for GroupShuffleSplit",
    )
    parser.add_argument(
        "--random-state",
        type=int,
        default=42,
        help="Random seed for splitting and XGBoost",
    )
    args = parser.parse_args()

    print(f"[config] lobby_parquet = {args.lobby_parquet}")
    print(f"[config] mode          = {args.mode}")
    print()

    # -------------------------------------------------------------------------
    # 1) Cargar datos
    # -------------------------------------------------------------------------
    print(f"[load] Reading parquet from {args.lobby_parquet}")
    lf = pl.scan_parquet(args.lobby_parquet)

    lf = lf.filter(pl.col("queue_id") == 420)  # SoloQ ranked
    df = lf.collect()
    print(f"[load] Raw shape (SoloQ 420): {df.shape}")

    # Pasar a pandas para XGBoost + sklearn
    df_pd = df.to_pandas()

    # side_blue: 1 si team_side == 'blue', 0 si 'red'
    df_pd["side_blue"] = (df_pd["team_side"] == "blue").astype(int)

    # Label
    y = df_pd["team_win"].astype(int).values

    # -------------------------------------------------------------------------
    # 2) Definir columnas de features según el modo
    # -------------------------------------------------------------------------
    col_info = build_feature_columns(args.mode)
    feature_cols: List[str] = col_info["feature_cols"]

    print(f"[features] Mode={args.mode}")
    print(f"[features] Initial feature columns count: {len(feature_cols)}")

    # -------------------------------------------------------------------------
    # 3) En modo 'full', crear columnas de deltas ally–enemy
    # -------------------------------------------------------------------------
    if args.mode == "full":
        roles = col_info["roles"]
        profile_stats = col_info["profile_stats"]

        print("[features] Adding ally–enemy delta features...")
        for role in roles:
            for stat in profile_stats:
                ally_col = f"ally_{role}_{stat}"
                enemy_col = f"enemy_{role}_{stat}"
                delta_col = f"{role}_diff_{stat}"

                if ally_col not in df_pd.columns or enemy_col not in df_pd.columns:
                    # Si algo falta, lo marcamos, pero seguimos
                    print(
                        f"  [warn] Missing columns for delta {delta_col}: "
                        f"{ally_col}, {enemy_col} – will be NaN."
                    )
                    df_pd[delta_col] = np.nan
                else:
                    df_pd[delta_col] = (
                        df_pd[ally_col].astype(float) - df_pd[enemy_col].astype(float)
                    )

    # -------------------------------------------------------------------------
    # 4) Limpiar filas con NaNs en features + label
    # -------------------------------------------------------------------------
    needed_cols = feature_cols + ["team_win"]
    existing = [c for c in needed_cols if c in df_pd.columns]
    missing = [c for c in needed_cols if c not in df_pd.columns]

    if missing:
        print("[error] Missing expected columns in DataFrame:")
        for c in missing:
            print(f"   - {c}")
        raise SystemExit("Aborting due to missing columns.")

    before = len(df_pd)
    mask = df_pd[existing].notnull().all(axis=1)
    df_clean = df_pd.loc[mask].reset_index(drop=True)
    dropped = before - len(df_clean)

    print(
        f"[load] After dropping rows with null in features/label: "
        f"{df_clean.shape} (dropped {dropped})"
    )

    # Si no queda nada, abortamos
    if len(df_clean) == 0:
        print("[error] No rows left after filtering nulls. Check data / features.")
        raise SystemExit(1)

    # -------------------------------------------------------------------------
    # 5) Preparar X, y, grupos (match_id)
    # -------------------------------------------------------------------------
    X = df_clean[feature_cols].values
    y = df_clean["team_win"].astype(int).values
    groups = df_clean["match_id"].values

    # -------------------------------------------------------------------------
    # 6) Train/test split con GroupShuffleSplit por match_id
    # -------------------------------------------------------------------------
    gss = GroupShuffleSplit(
        n_splits=1,
        test_size=args.test_size,
        random_state=args.random_state,
    )
    (train_idx, test_idx) = next(gss.split(X, y, groups))

    X_train, X_test = X[train_idx], X[test_idx]
    y_train, y_test = y[train_idx], y[test_idx]

    print(
        f"[split] Train size: {X_train.shape}, "
        f"Test size: {X_test.shape}, "
        f"Groups train/test ≈ {len(np.unique(groups[train_idx]))}/"
        f"{len(np.unique(groups[test_idx]))}"
    )

    # -------------------------------------------------------------------------
    # 7) Entrenar XGBoost
    # -------------------------------------------------------------------------
    print("[train] Training XGBoost lobby model...")
    model = xgb.XGBClassifier(
        n_estimators=400,
        max_depth=6,
        learning_rate=0.05,
        subsample=0.9,
        colsample_bytree=0.9,
        objective="binary:logistic",
        eval_metric="logloss",
        tree_method="hist",
        random_state=args.random_state,
        n_jobs=4,
    )

    model.fit(X_train, y_train)

    # -------------------------------------------------------------------------
    # 8) Evaluar
    # -------------------------------------------------------------------------
    print("[eval] Evaluating...")
    y_proba = model.predict_proba(X_test)[:, 1]
    y_pred = (y_proba >= 0.5).astype(int)

    acc = accuracy_score(y_test, y_pred)
    try:
        auc = roc_auc_score(y_test, y_proba)
    except ValueError:
        auc = float("nan")

    print()
    print(f"[metrics] Mode={args.mode}")
    print(f"[metrics] Accuracy: {acc:.4f}")
    print(f"[metrics] ROC AUC : {auc:.4f}")
    print()
    print("[metrics] Classification report:")
    print(classification_report(y_test, y_pred, digits=4))

    # -------------------------------------------------------------------------
    # 9) Importancias de features
    # -------------------------------------------------------------------------
    importances = model.feature_importances_
    feat_imp = list(zip(feature_cols, importances))
    feat_imp.sort(key=lambda x: x[1], reverse=True)

    top_k = 25 if len(feat_imp) > 25 else len(feat_imp)
    print()
    print(f"[features] Top {top_k} importances:")
    for name, val in feat_imp[:top_k]:
        print(f"  {name:35s} {val:.4f}")


if __name__ == "__main__":
    main()

