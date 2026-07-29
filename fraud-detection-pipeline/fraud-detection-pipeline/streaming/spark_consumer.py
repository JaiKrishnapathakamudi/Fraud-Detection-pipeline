"""
spark_consumer.py
--------------------
Week 3 deliverable: Apache Spark Structured Streaming job that:
  1. Consumes raw transaction events from the Kafka `transactions` topic
     in micro-batches.
  2. Applies the pre-trained Isolation Forest model (via a pandas UDF,
     using foreachBatch so the model runs efficiently on the driver/
     executors without re-loading it per row).
  3. Writes ALL scored transactions to Cassandra `transactions` table.
  4. Routes transactions whose anomaly score crosses the trained
     threshold to the dedicated `fraud_alerts` table.
  5. Emits basic batch metrics (record count, latency, alert count) to
     `pipeline_metrics` for Grafana/Prometheus visibility.

Run (against the docker-compose Spark cluster):

    docker exec -it fraud-spark-master spark-submit \
        --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1,\
com.datastax.spark:spark-cassandra-connector_2.12:3.5.0 \
        --master spark://spark-master:7077 \
        /opt/app/streaming/spark_consumer.py

Or locally for development:

    spark-submit \
        --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1,\
com.datastax.spark:spark-cassandra-connector_2.12:3.5.0 \
        streaming/spark_consumer.py
"""

import argparse
import json
import logging
import os
import sys
import time

import joblib
import numpy as np
import pandas as pd
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import col, from_json
from pyspark.sql.types import (
    BooleanType, DoubleType, IntegerType, StringType, StructField, StructType,
)

sys.path.append(os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "model"))
from features import engineer_features, feature_columns  # noqa: E402

logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s")
log = logging.getLogger("spark_consumer")

EVENT_SCHEMA = StructType([
    StructField("transaction_id", StringType()),
    StructField("event_time", StringType()),
    StructField("step", IntegerType()),
    StructField("type", StringType()),
    StructField("amount", DoubleType()),
    StructField("name_orig", StringType()),
    StructField("oldbalance_org", DoubleType()),
    StructField("newbalance_orig", DoubleType()),
    StructField("name_dest", StringType()),
    StructField("oldbalance_dest", DoubleType()),
    StructField("newbalance_dest", DoubleType()),
    StructField("is_fraud_label", IntegerType()),
    StructField("is_flagged_fraud_label", IntegerType()),
])


class ModelBroadcastHolder:
    """Loads the joblib model bundle once per executor process and
    caches it, avoiding repeated disk reads inside foreachBatch."""
    _bundle = None
    _path = None

    @classmethod
    def get(cls, path: str):
        if cls._bundle is None or cls._path != path:
            log.info(f"Loading model bundle from {path}")
            cls._bundle = joblib.load(path)
            cls._path = path
        return cls._bundle


def score_partition(pdf: pd.DataFrame, model_path: str) -> pd.DataFrame:
    """Runs on each partition of a micro-batch: engineers features and
    scores rows with the Isolation Forest model."""
    if pdf.empty:
        return pdf

    bundle = ModelBroadcastHolder.get(model_path)
    clf = bundle["model"]
    scaler = bundle["scaler"]
    cols = bundle["feature_columns"]
    threshold = bundle["anomaly_threshold"]

    engineered = engineer_features(pdf)
    for c in cols:
        if c not in engineered.columns:
            engineered[c] = 0
    X = engineered[cols].fillna(0).to_numpy(dtype=np.float64)
    X_scaled = scaler.transform(X)

    anomaly_scores = -clf.score_samples(X_scaled)
    pdf = pdf.copy()
    pdf["anomaly_score"] = anomaly_scores
    pdf["is_flagged"] = anomaly_scores >= threshold
    pdf["model_version"] = os.path.basename(model_path)
    return pdf


def severity_bucket(score: float, threshold: float) -> str:
    if score >= threshold * 1.5:
        return "CRITICAL"
    elif score >= threshold * 1.2:
        return "HIGH"
    return "MEDIUM"


def process_batch(batch_df: DataFrame, batch_id: int, model_path: str,
                   cassandra_keyspace: str):
    start = time.time()
    pdf = batch_df.toPandas()
    if pdf.empty:
        log.info(f"[batch {batch_id}] empty batch, skipping")
        return

    scored = score_partition(pdf, model_path)
    scored["event_time"] = pd.to_datetime(scored["event_time"], utc=True)
    scored["txn_date"] = scored["event_time"].dt.date.astype(str)

    spark = SparkSession.getActiveSession()

    txn_cols = [
        "txn_date", "name_orig", "event_time", "transaction_id", "step", "type",
        "amount", "oldbalance_org", "newbalance_orig", "name_dest",
        "oldbalance_dest", "newbalance_dest", "anomaly_score", "is_flagged",
        "model_version",
    ]
    txn_sdf = spark.createDataFrame(scored[txn_cols])
    (
        txn_sdf.write.format("org.apache.spark.sql.cassandra")
        .options(table="transactions", keyspace=cassandra_keyspace)
        .mode("append")
        .save()
    )

    alerts = scored[scored["is_flagged"]].copy()
    n_alerts = len(alerts)
    if n_alerts > 0:
        bundle = ModelBroadcastHolder.get(model_path)
        threshold = bundle["anomaly_threshold"]
        alerts["alert_date"] = alerts["txn_date"]
        alerts["severity"] = alerts["anomaly_score"].apply(
            lambda s: severity_bucket(s, threshold)
        )
        alerts["reviewed"] = False
        alert_cols = [
            "alert_date", "transaction_id", "event_time", "name_orig", "name_dest",
            "type", "amount", "anomaly_score", "severity", "model_version", "reviewed",
        ]
        alerts_sdf = spark.createDataFrame(alerts[alert_cols])
        (
            alerts_sdf.write.format("org.apache.spark.sql.cassandra")
            .options(table="fraud_alerts", keyspace=cassandra_keyspace)
            .mode("append")
            .save()
        )
        log.warning(f"[batch {batch_id}] {n_alerts} transactions flagged as potential fraud")

    latency_ms = (time.time() - start) * 1000
    metrics_row = spark.createDataFrame(
        [(scored["txn_date"].iloc[0], int(batch_id), pd.Timestamp.utcnow(),
          int(len(scored)), float(latency_ms), int(n_alerts))],
        schema=["metric_date", "batch_id", "processed_at", "records_in_batch",
                "avg_latency_ms", "alerts_in_batch"],
    )
    (
        metrics_row.write.format("org.apache.spark.sql.cassandra")
        .options(table="pipeline_metrics", keyspace=cassandra_keyspace)
        .mode("append")
        .save()
    )

    log.info(f"[batch {batch_id}] processed={len(scored)} alerts={n_alerts} "
              f"latency_ms={latency_ms:.1f}")


def build_spark_session(app_name: str, cassandra_host: str) -> SparkSession:
    return (
        SparkSession.builder.appName(app_name)
        .config("spark.cassandra.connection.host", cassandra_host)
        .config("spark.sql.shuffle.partitions", "6")
        .config("spark.streaming.stopGracefullyOnShutdown", "true")
        .getOrCreate()
    )


def main():
    parser = argparse.ArgumentParser(description="Real-time fraud scoring via Spark Structured Streaming")
    parser.add_argument("--kafka-bootstrap", default="kafka:9092")
    parser.add_argument("--topic", default="transactions")
    parser.add_argument("--cassandra-host", default="cassandra")
    parser.add_argument("--cassandra-keyspace", default="fraud_detection")
    parser.add_argument("--model-path", default="model/artifacts/isolation_forest.joblib")
    parser.add_argument("--checkpoint-dir", default="/tmp/fraud_checkpoints")
    parser.add_argument("--trigger-seconds", type=int, default=5,
                         help="Micro-batch trigger interval, in seconds")
    parser.add_argument("--starting-offsets", default="latest", choices=["latest", "earliest"])
    args = parser.parse_args()

    spark = build_spark_session("FraudDetectionStreaming", args.cassandra_host)
    spark.sparkContext.setLogLevel("WARN")

    raw = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", args.kafka_bootstrap)
        .option("subscribe", args.topic)
        .option("startingOffsets", args.starting_offsets)
        .option("failOnDataLoss", "false")
        .load()
    )

    parsed = (
        raw.selectExpr("CAST(value AS STRING) as json_str")
        .select(from_json(col("json_str"), EVENT_SCHEMA).alias("data"))
        .select("data.*")
    )

    query = (
        parsed.writeStream.foreachBatch(
            lambda batch_df, batch_id: process_batch(
                batch_df, batch_id, args.model_path, args.cassandra_keyspace
            )
        )
        .option("checkpointLocation", args.checkpoint_dir)
        .trigger(processingTime=f"{args.trigger_seconds} seconds")
        .start()
    )

    log.info("Streaming query started. Awaiting termination ...")
    query.awaitTermination()


if __name__ == "__main__":
    main()
