# System Architecture

## Overview

```
                     ┌─────────────────────┐
 PaySim CSV  ──────► │  Kafka Producer      │
 (historical replay) │  producer/kafka_     │
                      │  producer.py         │
                      └──────────┬───────────┘
                                 │ JSON events
                                 ▼
                     ┌─────────────────────┐
                     │  Apache Kafka        │
                     │  topic: transactions │
                     │  (6 partitions)      │
                     └──────────┬───────────┘
                                 │ micro-batches
                                 ▼
                     ┌─────────────────────────┐
                     │ Spark Structured         │
                     │ Streaming                │
                     │ streaming/               │
                     │ spark_consumer.py         │
                     │                          │
                     │ 1. Parse JSON            │
                     │ 2. Feature engineering   │
                     │    (model/features.py)   │
                     │ 3. Score w/ Isolation     │
                     │    Forest (joblib)        │
                     │ 4. Route flagged txns     │
                     └──────┬─────────────┬──────┘
                            │             │
                 all txns   │             │ flagged only
                            ▼             ▼
                 ┌──────────────┐  ┌──────────────────┐
                 │ Cassandra     │  │ Cassandra         │
                 │ transactions  │  │ fraud_alerts      │
                 └──────┬───────┘  └─────────┬─────────┘
                        │                    │
                        └─────────┬──────────┘
                                  ▼
                     ┌─────────────────────┐
                     │ Grafana + Prometheus │
                     │ dashboards            │
                     └─────────────────────┘
```

## Component responsibilities

| Component | File(s) | Responsibility |
|---|---|---|
| Data simulation | `producer/kafka_producer.py` | Replays PaySim CSV rows as a live JSON event stream into Kafka, keyed by origin account for partition affinity. |
| Offline training | `model/train_model.py`, `model/features.py` | Trains an Isolation Forest on historical data, evaluates against ground-truth labels (F2-optimized threshold to minimize false negatives), serializes model+scaler+threshold as one joblib bundle. |
| Stream processing | `streaming/spark_consumer.py` | Consumes Kafka in micro-batches (`foreachBatch`), re-uses the exact same feature engineering as training, scores each batch, writes results + alerts + metrics to Cassandra. |
| Storage | `db/cassandra_schema.cql` | Three core tables: `transactions` (full log), `fraud_alerts` (flagged only, with severity), `pipeline_metrics` (batch latency/throughput for observability). |
| Monitoring | `monitoring/` | Prometheus scrapes Kafka-exporter + Spark metrics; Grafana visualizes ingest rate, consumer lag, and (via a Cassandra datasource plugin) alert volume/latency. |
| Load testing | `scripts/load_test.py` | Publishes synthetic transactions at a controlled rate to validate sub-second latency under load. |

## Why Isolation Forest (vs. a supervised classifier)

PaySim's fraud is extremely rare (~0.13%) and synthetic fraud generators
tend to only cover known patterns. An unsupervised anomaly detector:

- Doesn't require balancing an extremely skewed label distribution.
- Generalizes better to **novel** fraud patterns not present in the
  historical label set — critical for a production fraud system, since
  fraud tactics evolve faster than labels can be collected.
- Is fast to score at inference time (log-linear per tree), which keeps
  us within the sub-second micro-batch latency target.

`isFraud`/`isFlaggedFraud` labels are used **only** to pick the
F2-optimal anomaly threshold and to report evaluation metrics — never
as training features — so the model stays genuinely unsupervised at
inference time and can still be run on data with no labels at all
(the real streaming case).

## Feature engineering (train/serve parity)

`model/features.py` is imported by both `train_model.py` and
`spark_consumer.py`, so the exact same transformation runs at training
time and at inference time. Key engineered features:

- `error_balance_orig` / `error_balance_dest` — bookkeeping
  inconsistencies between stated balances, which correlate strongly
  with PaySim's synthetic fraud generator (cash-out/transfer draining
  leaves an inconsistent trail).
- `amount_to_oldbalance_ratio` — proportion of an account's balance
  moved in a single transaction.
- `orig_balance_wiped` — binary flag for "account fully drained."
- `is_merchant_dest` — PaySim encodes merchant accounts with a
  leading "M".
- One-hot encoded `type` (CASH_IN / CASH_OUT / DEBIT / PAYMENT / TRANSFER).

## Latency & partition tuning (Week 4)

- Kafka topic is provisioned with 6 partitions (`KAFKA_CFG_NUM_PARTITIONS`
  in `docker-compose.yml`) to allow parallel consumption by multiple
  Spark tasks.
- Spark micro-batch `trigger(processingTime=...)` defaults to 5 seconds;
  lower it (e.g. 1s) once throughput is validated, trading off latency
  against per-batch overhead.
- `spark.sql.shuffle.partitions` is deliberately kept small (6) for a
  small local cluster — raise this for a real multi-worker deployment.
- Run `scripts/load_test.py` at increasing rates while watching Kafka
  consumer lag (Grafana panel) and `pipeline_metrics.avg_latency_ms` in
  Cassandra to find the safe sustained throughput ceiling for your
  hardware, then tune partitions/executor memory accordingly.

## Adding Cassandra-backed Grafana panels

The default dashboard (`monitoring/grafana/dashboards/fraud_dashboard.json`)
ships with Kafka/Prometheus panels out of the box. To add native
Cassandra panels for alert volume and latency:

1. Install the community Cassandra datasource plugin in the Grafana
   container:
   ```yaml
   # add to the grafana service in docker-compose.yml
   environment:
     GF_INSTALL_PLUGINS: hadesarchitect-cassandra-datasource
   ```
2. Add a datasource pointing at `cassandra:9042`, keyspace
   `fraud_detection`.
3. Example CQL-backed panel queries:
   - Alert volume by severity: query `fraud_alerts` filtered on
     `alert_date`, grouped by `severity`.
   - Processing latency: plot `avg_latency_ms` from `pipeline_metrics`
     ordered by `batch_id`.
   - Transaction velocity: count rows in `transactions` bucketed by
     `event_time`.

## Alternative: MongoDB instead of Cassandra

The project spec lists Cassandra **or** MongoDB. This repo ships
Cassandra by default (better fit for the append-heavy, time-series
write pattern here), but `pymongo` is included in `requirements.txt`
if you'd rather swap storage backends — replace the
`spark.write.format("org.apache.spark.sql.cassandra")` calls in
`streaming/spark_consumer.py` with the
[MongoDB Spark Connector](https://www.mongodb.com/docs/spark-connector/current/)
equivalent (`format("mongodb")`), and mirror the three collections
(`transactions`, `fraud_alerts`, `pipeline_metrics`).
