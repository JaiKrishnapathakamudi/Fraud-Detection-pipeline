-- =====================================================================
-- Cassandra schema for the Real-Time Financial Fraud Detection Pipeline
-- Run with: cqlsh -f db/cassandra_schema.cql
-- or via: docker exec -i fraud-cassandra cqlsh < db/cassandra_schema.cql
-- =====================================================================

CREATE KEYSPACE IF NOT EXISTS fraud_detection
  WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1};

USE fraud_detection;

-- All scored transactions (high write-throughput log).
-- Partitioned by day + origin account for efficient recent-activity lookups.
CREATE TABLE IF NOT EXISTS transactions (
    txn_date        date,
    name_orig       text,
    event_time      timestamp,
    transaction_id  text,
    step            int,
    type            text,
    amount          double,
    oldbalance_org  double,
    newbalance_orig double,
    name_dest       text,
    oldbalance_dest double,
    newbalance_dest double,
    anomaly_score   double,
    is_flagged      boolean,
    model_version   text,
    PRIMARY KEY ((txn_date, name_orig), event_time, transaction_id)
) WITH CLUSTERING ORDER BY (event_time DESC);

-- Dedicated fraud alerts table for anything routed above the alert threshold.
-- Partitioned by day so Grafana can query "today's alerts" cheaply.
CREATE TABLE IF NOT EXISTS fraud_alerts (
    alert_date      date,
    transaction_id  text,
    event_time      timestamp,
    name_orig       text,
    name_dest       text,
    type            text,
    amount          double,
    anomaly_score   double,
    severity        text,
    model_version   text,
    reviewed        boolean,
    PRIMARY KEY (alert_date, event_time, transaction_id)
) WITH CLUSTERING ORDER BY (event_time DESC);

-- Rolling per-account behavioral counters, updated as transactions stream in.
-- Used to detect deviation from an account's own historical baseline.
-- Split into separate counter tables per Cassandra's rule that a table
-- with counter columns may contain ONLY counter columns (besides the key).
CREATE TABLE IF NOT EXISTS account_txn_counts (
    name_orig  text PRIMARY KEY,
    txn_count  counter
);

CREATE TABLE IF NOT EXISTS account_fraud_counts (
    name_orig    text PRIMARY KEY,
    fraud_count  counter
);

-- Pipeline-level metrics snapshot (batch id, latency, throughput) that
-- Grafana/Prometheus can also scrape via a small exporter if desired.
CREATE TABLE IF NOT EXISTS pipeline_metrics (
    metric_date     date,
    batch_id        bigint,
    processed_at    timestamp,
    records_in_batch int,
    avg_latency_ms   double,
    alerts_in_batch  int,
    PRIMARY KEY (metric_date, batch_id)
) WITH CLUSTERING ORDER BY (batch_id DESC);
