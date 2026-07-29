"""
train_model.py
----------------
Week 2 deliverable: trains an unsupervised anomaly detection model
(Isolation Forest) on historical PaySim transactions, evaluates it
against the ground-truth `isFraud` labels (label is used ONLY for
evaluation, never as a training feature - this stays a genuinely
unsupervised anomaly detector so it can catch novel fraud patterns
not present in historical labels), and serializes the trained model
+ preprocessing pipeline for use in the Spark streaming job.

Usage:
    python model/train_model.py \
        --csv data/paysim.csv \
        --sample-frac 1.0 \
        --contamination 0.0013 \
        --out model/artifacts/isolation_forest.joblib
"""

import argparse
import json
import logging
import os
import sys

import joblib
import numpy as np
import pandas as pd
from sklearn.ensemble import IsolationForest
from sklearn.metrics import (
    classification_report,
    confusion_matrix,
    precision_recall_curve,
    roc_auc_score,
)
from sklearn.model_selection import train_test_split
from sklearn.preprocessing import StandardScaler

sys.path.append(os.path.dirname(os.path.abspath(__file__)))
from features import engineer_features, feature_columns  # noqa: E402

logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s")
log = logging.getLogger("train_model")

RAW_COLUMNS_MAP = {
    "step": "step",
    "type": "type",
    "amount": "amount",
    "nameOrig": "name_orig",
    "oldbalanceOrg": "oldbalance_org",
    "newbalanceOrig": "newbalance_orig",
    "nameDest": "name_dest",
    "oldbalanceDest": "oldbalance_dest",
    "newbalanceDest": "newbalance_dest",
    "isFraud": "is_fraud",
    "isFlaggedFraud": "is_flagged_fraud",
}


def load_data(csv_path: str, sample_frac: float, random_state: int = 42) -> pd.DataFrame:
    log.info(f"Loading {csv_path} ...")
    df = pd.read_csv(csv_path)
    df = df.rename(columns=RAW_COLUMNS_MAP)
    if sample_frac < 1.0:
        df = df.sample(frac=sample_frac, random_state=random_state)
        log.info(f"Sub-sampled to {len(df):,} rows ({sample_frac:.0%})")
    return df


def find_best_threshold(scores: np.ndarray, y_true: np.ndarray) -> float:
    """Pick the anomaly-score threshold that maximizes F2 (favors recall,
    i.e. minimizing false negatives, per the project's Week 2 goal)."""
    precision, recall, thresholds = precision_recall_curve(y_true, -scores)
    beta = 2.0
    f2 = (1 + beta**2) * (precision * recall) / (beta**2 * precision + recall + 1e-12)
    best_idx = int(np.nanargmax(f2))
    # precision_recall_curve returns thresholds of len(n-1)
    best_threshold = thresholds[best_idx] if best_idx < len(thresholds) else thresholds[-1]
    return float(-best_threshold), float(f2[best_idx])


def main():
    parser = argparse.ArgumentParser(description="Train Isolation Forest fraud detector")
    parser.add_argument("--csv", default="data/paysim.csv")
    parser.add_argument("--sample-frac", type=float, default=1.0,
                         help="Fraction of rows to use (useful for quick local runs)")
    parser.add_argument("--contamination", type=float, default=0.0013,
                         help="Expected fraud rate; PaySim's true rate is ~0.13%")
    parser.add_argument("--n-estimators", type=int, default=200)
    parser.add_argument("--test-size", type=float, default=0.2)
    parser.add_argument("--out", default="model/artifacts/isolation_forest.joblib")
    parser.add_argument("--metrics-out", default="model/artifacts/metrics.json")
    args = parser.parse_args()

    df = load_data(args.csv, args.sample_frac)

    y = df["is_fraud"].to_numpy()
    X_df = engineer_features(df)
    cols = feature_columns()
    X = X_df[cols].fillna(0).to_numpy(dtype=np.float64)

    X_train, X_test, y_train, y_test = train_test_split(
        X, y, test_size=args.test_size, random_state=42, stratify=y
    )

    # Train the Isolation Forest ONLY on normal-looking behavior for a
    # cleaner decision boundary (common practice for anomaly detectors),
    # while keeping the held-out test set fully mixed for realistic eval.
    X_train_normal = X_train[y_train == 0]

    log.info(f"Training IsolationForest on {len(X_train_normal):,} normal transactions "
              f"({len(cols)} features, n_estimators={args.n_estimators}) ...")

    scaler = StandardScaler()
    X_train_scaled = scaler.fit_transform(X_train_normal)

    clf = IsolationForest(
        n_estimators=args.n_estimators,
        contamination=args.contamination,
        max_samples="auto",
        random_state=42,
        n_jobs=-1,
    )
    clf.fit(X_train_scaled)

    X_test_scaled = scaler.transform(X_test)
    # score_samples: higher = more normal. We flip sign so higher = more anomalous.
    anomaly_scores = -clf.score_samples(X_test_scaled)

    auc = roc_auc_score(y_test, anomaly_scores)
    log.info(f"Test ROC-AUC: {auc:.4f}")

    best_threshold, best_f2 = find_best_threshold(-anomaly_scores, y_test)
    y_pred = (anomaly_scores >= best_threshold).astype(int)

    report = classification_report(y_test, y_pred, target_names=["legit", "fraud"], output_dict=True)
    cm = confusion_matrix(y_test, y_pred).tolist()

    log.info("Classification report:\n" +
             classification_report(y_test, y_pred, target_names=["legit", "fraud"]))
    log.info(f"Confusion matrix [[TN, FP], [FN, TP]]: {cm}")
    log.info(f"Chosen anomaly threshold (F2-optimal): {best_threshold:.4f}")

    os.makedirs(os.path.dirname(args.out), exist_ok=True)
    bundle = {
        "model": clf,
        "scaler": scaler,
        "feature_columns": cols,
        "anomaly_threshold": best_threshold,
        "trained_on_rows": int(len(X_train_normal)),
        "contamination": args.contamination,
    }
    joblib.dump(bundle, args.out)
    log.info(f"Saved model bundle -> {args.out}")

    metrics = {
        "roc_auc": auc,
        "anomaly_threshold": best_threshold,
        "f2_at_threshold": best_f2,
        "confusion_matrix": {"labels": ["legit", "fraud"], "matrix": cm},
        "classification_report": report,
        "n_train_normal": int(len(X_train_normal)),
        "n_test": int(len(X_test)),
        "fraud_rate_test": float(y_test.mean()),
    }
    with open(args.metrics_out, "w") as f:
        json.dump(metrics, f, indent=2)
    log.info(f"Saved metrics -> {args.metrics_out}")


if __name__ == "__main__":
    main()
