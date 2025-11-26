#!/usr/bin/env python
import argparse
import os

import pandas as pd
import xgboost as xgb
from sklearn.model_selection import GroupShuffleSplit
from sklearn.metrics import accuracy_score, roc_auc_score, classification_report


def load_team_dataset(path: str, only_queue_420: bool = True) -> pd.DataFrame:
    if not os.path.exists(path):
        raise FileNotFoundError(f"Dataset not found: {path}")

    print(f"[load] Reading parquet from {path}")
    df = pd.read_parquet(path)

    print(f"[load] Raw shape: {df.shape}")
    if only_queue_420 and "queue_id" in df.columns:
        df = df[df["queue_id"] == 420].copy()
        print(f"[load] Filtered to SoloQ (queue_id=420): {df.shape}")

    # Basic sanity
    required_cols = [
        "match_id",
        "team_side",
        "team_win",
        "top_champion_id",
        "jungle_champion_id",
        "middle_champion_id",
        "bottom_champion_id",
        "utility_champion_id",
    ]
    missing = [c for c in required_cols if c not in df.columns]
    if missing:
        raise ValueError(f"Missing required columns in dataset: {missing}")

    # Drop rows with null champion IDs or side / label
    df = df.dropna(
        subset=[
            "team_side",
            "team_win",
            "top_champion_id",
            "jungle_champion_id",
            "middle_champion_id",
            "bottom_champion_id",
            "utility_champion_id",
        ]
    ).copy()
    print(f"[load] After dropping rows with null champs/labels: {df.shape}")

    return df


def build_features(df: pd.DataFrame, use_only_draft: bool = True) -> tuple[pd.DataFrame, pd.Series, pd.Series]:
    """
    Returns:
        X: features DataFrame
        y: labels (team_win)
        groups: match_id (for group-wise split)
    """
    # Encode side: blue=1, red=0
    df = df.copy()
    df["side_blue"] = (df["team_side"] == "blue").astype("int8")

    base_features = [
        "side_blue",
        "top_champion_id",
        "jungle_champion_id",
        "middle_champion_id",
        "bottom_champion_id",
        "utility_champion_id",
    ]

    if not use_only_draft:
        # WARNING: esto mete info de “después” de la partida.
        post_game_features = [
            "team_kills",
            "team_deaths",
            "team_assists",
            "team_gold_earned",
            "team_gold_per_min",
            "team_damage_to_champions",
            "team_damage_per_min",
            "team_vision_score",
            "team_vision_score_per_min",
            "team_cs_total",
            "team_cs_per_min",
            "team_towers_destroyed",
            "team_inhibitors_destroyed",
            "team_dragons",
            "team_barons",
            "team_heralds",
            # "team_plates",  # opcional si existe
        ]
        post_game_features = [f for f in post_game_features if f in df.columns]
        features = base_features + post_game_features
    else:
        features = base_features

    print(f"[features] Using {len(features)} features: {features}")

    X = df[features].astype("float32")
    y = df["team_win"].astype("int8")
    groups = df["match_id"].astype("category").cat.codes  # group by match

    return X, y, groups


def train_xgb(X, y, groups, test_size=0.2, random_state=42):
    splitter = GroupShuffleSplit(
        n_splits=1, test_size=test_size, random_state=random_state
    )
    train_idx, test_idx = next(splitter.split(X, y, groups))

    X_train, X_test = X.iloc[train_idx], X.iloc[test_idx]
    y_train, y_test = y.iloc[train_idx], y.iloc[test_idx]

    print(f"[split] Train size: {X_train.shape}, Test size: {X_test.shape}")

    model = xgb.XGBClassifier(
        n_estimators=300,
        max_depth=6,
        learning_rate=0.05,
        subsample=0.8,
        colsample_bytree=0.8,
        objective="binary:logistic",
        n_jobs=-1,
        eval_metric="logloss",
        tree_method="hist",  # suele ir bien en CPU
    )

    print("[train] Training XGBoost model...")
    model.fit(X_train, y_train)

    print("[eval] Evaluating...")
    y_proba = model.predict_proba(X_test)[:, 1]
    y_pred = (y_proba >= 0.5).astype("int8")

    acc = accuracy_score(y_test, y_pred)
    try:
        auc = roc_auc_score(y_test, y_proba)
    except ValueError:
        auc = float("nan")

    print(f"[metrics] Accuracy: {acc:.4f}")
    print(f"[metrics] ROC AUC: {auc:.4f}")
    print("\n[metrics] Classification report:")
    print(classification_report(y_test, y_pred, digits=3))

    # Feature importances
    importances = model.feature_importances_
    feature_names = X.columns.tolist()
    print("\n[features] Importances:")
    for name, imp in sorted(zip(feature_names, importances), key=lambda x: -x[1]):
        print(f"  {name:30s} {imp:.4f}")

    return model


def main():
    parser = argparse.ArgumentParser(
        description="Train a quick XGBoost baseline on ml_team_outcome.parquet"
    )
    parser.add_argument(
        "--team-parquet",
        type=str,
        default="data/ml/ml_team_outcome.parquet",
        help="Path to ml_team_outcome.parquet",
    )
    parser.add_argument(
        "--use-only-draft",
        action="store_true",
        help="Use only pre-game draft features (side + champion_ids).",
    )
    args = parser.parse_args()

    df = load_team_dataset(args.team_parquet)
    X, y, groups = build_features(df, use_only_draft=args.use_only_draft)
    _ = train_xgb(X, y, groups)


if __name__ == "__main__":
    main()

