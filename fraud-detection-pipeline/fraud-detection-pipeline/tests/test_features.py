"""
test_features.py
------------------
Sanity checks for the shared feature engineering module, run in CI to
catch train/serve schema drift early.

Usage: pytest tests/test_features.py
"""

import os
import sys

import pandas as pd

sys.path.append(os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "model"))
from features import engineer_features, feature_columns, event_to_row  # noqa: E402


SAMPLE_ROW = {
    "amount": 1000.0,
    "oldbalance_org": 5000.0,
    "newbalance_orig": 4000.0,
    "name_dest": "M12345678",
    "oldbalance_dest": 0.0,
    "newbalance_dest": 1000.0,
    "type": "TRANSFER",
}


def test_engineer_features_adds_expected_columns():
    df = pd.DataFrame([SAMPLE_ROW])
    out = engineer_features(df)
    for col in feature_columns():
        assert col in out.columns, f"missing engineered feature: {col}"


def test_is_merchant_dest_flag():
    df = pd.DataFrame([SAMPLE_ROW])
    out = engineer_features(df)
    assert out["is_merchant_dest"].iloc[0] == 1

    df2 = pd.DataFrame([{**SAMPLE_ROW, "name_dest": "C98765432"}])
    out2 = engineer_features(df2)
    assert out2["is_merchant_dest"].iloc[0] == 0


def test_orig_balance_wiped_flag():
    wiped_row = {**SAMPLE_ROW, "oldbalance_org": 5000.0, "newbalance_orig": 0.0}
    df = pd.DataFrame([wiped_row])
    out = engineer_features(df)
    assert out["orig_balance_wiped"].iloc[0] == 1


def test_event_to_row_roundtrip():
    event = {
        "amount": 500.0,
        "oldbalance_org": 1000.0,
        "newbalance_orig": 500.0,
        "name_dest": "C111111",
        "oldbalance_dest": 200.0,
        "newbalance_dest": 700.0,
        "type": "CASH_OUT",
    }
    row = event_to_row(event)
    df = pd.DataFrame([row])
    out = engineer_features(df)
    assert len(out) == 1
    for col in feature_columns():
        assert col in out.columns


def test_feature_vector_no_nans():
    df = pd.DataFrame([SAMPLE_ROW, {**SAMPLE_ROW, "amount": 0.0}])
    out = engineer_features(df)
    cols = feature_columns()
    assert not out[cols].isna().any().any()
