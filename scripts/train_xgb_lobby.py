#!/usr/bin/env python
import argparse
import os

import pandas as pd
import xgboost as xgb
from sklearn.model_selection import GroupShuffleSplit
from sklearn.metrics import accuracy_score, roc_auc_score, classification_report


def load_lobby_dataset(path: str, only_queue_420: bool = True) -> pd.DataFrame:
    if not os.path.exists(path):
        raise FileNotFoundError(f"Dataset not found: {path}")

    print(f"[load] Reading parquet from {path}")
    df = pd.read_parquet(path)
    print(f"[load] Raw shape: {df.shape}")

    if only_queue_420 and "queue_id" in df.columns:
        df = df[df["queue_id"] == 420].copy()
        print(f"[load] Filtered to SoloQ (queue_id=420): {df.shape}")

    required_cols = [
        "match_id",
        "team_side",
        "team_win",
        "ally_top_champion_id",
        "ally_jungle_champion_id",
        "ally_middle_champion_id",
        "ally_bottom_champion_id",
        "ally_utility_champion_id",
        "enemy_top_champion_id",
        "enemy_jungle_champion_id",
        "enemy_middle_champion_id",
        "enemy_bottom_champion_id",
        "enemy_utility_champion_id",
    ]
    missing = [c for c in required_cols if c not in df.columns]
    if missing:
        raise ValueError(f"Missing required columns in dataset: {missing}")

    # Drop rows with nulls in champs/labels/side
    df = df.dropna(
        subset=[
            "team_side",
            "team_win",
            "ally_top_champion_id",
            "ally_jungle_champion_id",
            "ally_middle_champion_id",
            "ally_bottom_champion_id",
            "ally_utility_champion_id",
            "enemy_top_champion_id",
            "enemy_jungle_champion_id",
            "enemy_middle_champion_id",
            "enemy_bottom_champion_id",
            "enemy_utility_champion_id",
        ]
    ).copy()
    print(f"[load] After dropping rows with null champs/labels: {df.shape}")

    return df


def build_lobby_features(df: pd.DataFrame) -> tuple[pd.DataFrame, pd.Series, pd.Series]:
    df = df.copy()

    # Encode side: blue=1, red=0
    df["side_blue"] = (df["team_side"] == "blue").astype("int8")

    feature_cols = [
        "side_blue",
        "ally_top_champion_id",
        "ally_jungle_champion_id",
        "ally_middle_champion_id",
        "ally_bottom_champion_id",
        "ally_utility_champion_id",
        "enemy_top_champion_id",
        "enemy_jungle_champion_id",
        "enemy_middle_champion_id",
        "enemy_bottom_champion_id",
        "enemy_utility_champion_id",
    ]

    print(f"[features] Using {len(feature_cols)} features: {feature_cols}")

    X = df[feature_cols].astype("float32")
    y = df["team_win"].astype("int8")
    groups = df["match_id"].astype("category").cat.codes  # group = match

    return X, y, groups


def train_xgb_lobby(X, y, groups, test_size=0.2, random_state=42):
    splitter = GroupShuffleSplit(
        n_splits=1, test_size=test_size, random_state=random_state
    )
    train_idx, test_idx = next(splitter.split(X, y, groups))

    X_train, X_test = X.iloc[train_idx], X.iloc[test_idx]
    y_train, y_test = y.iloc[train_idx], y.iloc[test_idx]

    print(f"[split] Train size: {X_train.shape}, Test size: {X_test.shape}")

    model = xgb.XGBClassifier(
        n_estimators=400,
        max_depth=6,
        learning_rate=0.05,
        subsample=0.9,
        colsample_bytree=0.9,
        objective="binary:logistic",
        n_jobs=-1,
        eval_metric="logloss",
        tree_method="hist",
    )

    print("[train] Training XGBoost lobby model...")
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

    importances = model.feature_importances_
    feature_names = X.columns.tolist()
    print("\n[features] Importances:")
    for name, imp in sorted(zip(feature_names, importances), key=lambda x: -x[1]):
        print(f"  {name:35s} {imp:.4f}")

    return model


def main():
    parser = argparse.ArgumentParser(
        description="Train XGBoost on ml_lobby_outcome (10 champs + side)."
    )
    parser.add_argument(
        "--lobby-parquet",
        type=str,
        default="data/ml/ml_lobby_outcome.parquet",
        help="Path to ml_lobby_outcome.parquet",
    )
    args = parser.parse_args()

    df = load_lobby_dataset(args.lobby_parquet)
    X, y, groups = build_lobby_features(df)
    _ = train_xgb_lobby(X, y, groups)


if __name__ == "__main__":
    main()

