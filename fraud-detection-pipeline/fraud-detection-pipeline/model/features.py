"""
features.py
------------
Shared feature engineering logic used by BOTH the offline training
script (model/train_model.py) and the real-time Spark scoring job
(streaming/spark_consumer.py). Keeping this logic in one place avoids
training/serving skew.

Feature design notes:
- We deliberately avoid using nameOrig/nameDest identity directly (too
  high-cardinality / leaks nothing useful raw); instead we derive
  behavioral & balance-consistency features which are far more
  predictive of PaySim-style fraud (cash-out / transfer draining).
- errorBalanceOrig / errorBalanceDest capture bookkeeping
  inconsistencies between the stated balances, which PaySim's
  synthetic fraud generator tends to introduce.
"""

from typing import Dict, List

import numpy as np
import pandas as pd

NUMERIC_FEATURES: List[str] = [
    "amount",
    "oldbalance_org",
    "newbalance_orig",
    "oldbalance_dest",
    "newbalance_dest",
    "error_balance_orig",
    "error_balance_dest",
    "amount_to_oldbalance_ratio",
    "is_merchant_dest",
    "orig_balance_wiped",
]

CATEGORICAL_FEATURES: List[str] = ["type"]

TXN_TYPES = ["CASH_IN", "CASH_OUT", "DEBIT", "PAYMENT", "TRANSFER"]


def engineer_features(df: pd.DataFrame) -> pd.DataFrame:
    """
    Accepts a DataFrame with raw PaySim-style columns (snake_case, as
    produced by the Kafka producer / CSV loader) and returns a
    DataFrame of model-ready numeric features.
    """
    out = df.copy()

    out["error_balance_orig"] = (
        out["newbalance_orig"] + out["amount"] - out["oldbalance_org"]
    )
    out["error_balance_dest"] = (
        out["oldbalance_dest"] + out["amount"] - out["newbalance_dest"]
    )
    out["amount_to_oldbalance_ratio"] = out["amount"] / (out["oldbalance_org"] + 1.0)
    out["is_merchant_dest"] = out["name_dest"].astype(str).str.startswith("M").astype(int)
    out["orig_balance_wiped"] = (
        (out["oldbalance_org"] > 0) & (out["newbalance_orig"] == 0)
    ).astype(int)

    # One-hot encode transaction type against the known fixed vocabulary
    for t in TXN_TYPES:
        out[f"type_{t}"] = (out["type"] == t).astype(int)

    return out


def feature_columns() -> List[str]:
    return NUMERIC_FEATURES + [f"type_{t}" for t in TXN_TYPES]


def to_feature_vector(df: pd.DataFrame) -> np.ndarray:
    engineered = engineer_features(df)
    cols = feature_columns()
    for c in cols:
        if c not in engineered.columns:
            engineered[c] = 0
    return engineered[cols].fillna(0).to_numpy(dtype=np.float64)


def event_to_row(event: Dict) -> Dict:
    """Map a single Kafka JSON event (as produced by kafka_producer.py)
    into the raw column schema expected by engineer_features()."""
    return {
        "amount": event.get("amount", 0.0),
        "oldbalance_org": event.get("oldbalance_org", 0.0),
        "newbalance_orig": event.get("newbalance_orig", 0.0),
        "name_dest": event.get("name_dest", ""),
        "oldbalance_dest": event.get("oldbalance_dest", 0.0),
        "newbalance_dest": event.get("newbalance_dest", 0.0),
        "type": event.get("type", ""),
    }
