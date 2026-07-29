# Real-Time Financial Fraud Detection Pipeline

A streaming data analytics pipeline that monitors financial transactions in
real time, scoring each transaction against a trained anomaly detection
model and routing suspicious activity to a dedicated fraud-alerts store —
before funds are settled.

Built around the **PaySim synthetic mobile-money dataset**, using
**Kafka → Spark Structured Streaming → Cassandra → Grafana/Prometheus**.

## Architecture

See [`docs/ARCHITECTURE.md`](docs/ARCHITECTURE.md) for the full diagram and
component breakdown. In short:

```
PaySim CSV → Kafka producer → Kafka topic → Spark Structured Streaming
   (scores each micro-batch with a pre-trained Isolation Forest)
      → Cassandra (transactions + fraud_alerts) → Grafana/Prometheus
```

## Tech stack

| Layer | Technology |
|---|---|
| Languages | Python (core), Scala optional for custom Spark jobs |
| Streaming engine | Apache Kafka |
| Stream processing | Apache Spark Structured Streaming |
| ML | scikit-learn (Isolation Forest) |
| Storage | Apache Cassandra (MongoDB supported as an alternative) |
| Monitoring | Prometheus + Grafana |
| Orchestration (local) | Docker Compose |

## Repository layout

```
fraud-detection-pipeline/
├── docker-compose.yml           # Kafka, Zookeeper, Cassandra, Spark, Prometheus, Grafana
├── requirements.txt
├── .env.example
├── data/
│   └── download_data.py         # Pulls PaySim from Kaggle
├── producer/
│   └── kafka_producer.py        # Week 1: streams PaySim rows into Kafka
├── model/
│   ├── features.py              # Shared feature engineering (train/serve parity)
│   └── train_model.py           # Week 2: trains + evaluates Isolation Forest
├── streaming/
│   └── spark_consumer.py        # Week 3: real-time scoring + routing
├── db/
│   ├── cassandra_schema.cql     # Keyspace + tables
│   └── init_cassandra.sh        # Applies schema to the running container
├── monitoring/
│   ├── prometheus.yml
│   └── grafana/                 # Provisioned datasource + dashboard
├── scripts/
│   └── load_test.py             # Week 4: throughput/latency load test
├── demo/
│   ├── live_dashboard.html      # Zero-install live output preview (open in any browser)
│   └── README.md
├── tests/
│   └── test_features.py         # pytest unit tests, also run in CI
├── .github/workflows/ci.yml     # GitHub Actions: pytest + lint on push/PR
└── docs/
    └── ARCHITECTURE.md
```

## See it working right now (no setup)

Open [`demo/live_dashboard.html`](demo/live_dashboard.html) directly in any
browser — no Docker, Kafka, or Python needed. It streams synthetic
PaySim-style transactions, scores them in-browser using the same feature
logic as `model/features.py`, and renders a live ledger, fraud-alerts feed,
and the same KPIs/charts the real Grafana dashboard shows. See
[`demo/README.md`](demo/README.md) for details on exactly how it maps to
the real pipeline.

## Quickstart

### 1. Bring up infrastructure

```bash
docker compose up -d
```

This starts Zookeeper, Kafka (+ Kafka UI on `localhost:8080`), Cassandra,
a Spark master/worker, Prometheus (`localhost:9090`), and Grafana
(`localhost:3000`, default login `admin` / `admin`).

Apply the Cassandra schema once the container is healthy:

```bash
chmod +x db/init_cassandra.sh
./db/init_cassandra.sh
```

### 2. Install Python dependencies

```bash
python -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt
```

### 3. Get the data

```bash
python data/download_data.py        # requires a Kaggle API token, see script docstring
mv data/PS_20174392719_1491204439457_log.csv data/paysim.csv
```

### 4. Train the model

```bash
python model/train_model.py --csv data/paysim.csv --sample-frac 1.0
```

This writes `model/artifacts/isolation_forest.joblib` and
`model/artifacts/metrics.json` (ROC-AUC, confusion matrix, classification
report, chosen anomaly threshold).

### 5. Start the streaming producer

```bash
python producer/kafka_producer.py --csv data/paysim.csv \
    --bootstrap-servers localhost:9094 --rate 200 --loop
```

### 6. Start the Spark streaming scorer

```bash
docker exec -it fraud-spark-master spark-submit \
    --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1,com.datastax.spark:spark-cassandra-connector_2.12:3.5.0 \
    --master spark://spark-master:7077 \
    /opt/app/streaming/spark_consumer.py \
    --kafka-bootstrap kafka:9092 --cassandra-host cassandra

```
> Note: mount the repo into the Spark containers (e.g. add
> `- .:/opt/app` under `volumes:` for `spark-master`/`spark-worker` in
> `docker-compose.yml`) so `/opt/app/streaming/spark_consumer.py` and the
> model artifact are visible inside the container. Alternatively run
> `spark-submit` directly from a local Spark install pointed at
> `localhost:7077`.

### 7. Watch the dashboards

- Kafka UI: http://localhost:8080
- Prometheus: http://localhost:9090
- Grafana: http://localhost:3000 (dashboard "Real-Time Fraud Detection
  Pipeline" is auto-provisioned)

### 8. Load test

```bash
python scripts/load_test.py --rate 500 --duration 60
```

## Development timeline mapping

| Week | Deliverable | Files |
|---|---|---|
| 1 | Kafka/Cassandra infra + PaySim producer | `docker-compose.yml`, `producer/kafka_producer.py`, `data/download_data.py` |
| 2 | Isolation Forest training + evaluation | `model/features.py`, `model/train_model.py` |
| 3 | Spark Structured Streaming scoring | `streaming/spark_consumer.py`, `db/cassandra_schema.cql` |
| 4 | Dashboards, tuning, load testing, docs | `monitoring/`, `scripts/load_test.py`, `docs/ARCHITECTURE.md` |

## Expected impact

By scoring transactions before settlement, the pipeline is designed to
reduce financial losses and regulatory exposure from fraud, while the
F2-optimized decision threshold explicitly prioritizes minimizing missed
fraud (false negatives) over minimizing false positives, per the Week 2
evaluation goal — without generating so many alerts that it erodes
customer trust or analyst throughput.

## License

MIT — see [`LICENSE`](LICENSE).
