# Live Output Demo

`live_dashboard.html` is a **self-contained, zero-install preview** of what
the running pipeline produces — open it directly in any browser, no Docker,
Kafka, Spark, or Python required.

## What it does

- Generates synthetic PaySim-style transactions in-browser (same schema as
  `producer/kafka_producer.py`: `type`, `amount`, `oldbalanceOrg`,
  `newbalanceOrig`, `oldbalanceDest`, `newbalanceDest`, origin/destination
  account ids).
- Scores each transaction with a JavaScript port of the exact feature
  logic in [`model/features.py`](../model/features.py) — balance-error
  terms, amount-to-balance ratio, merchant-destination flag, and the
  "origin balance wiped" flag — combined into a 0–1 anomaly score that
  approximates how the trained Isolation Forest behaves.
- Streams live rows into a scrolling ledger (styled like the `transactions`
  Cassandra table), routes anything above threshold into a **Fraud
  Alerts** panel (styled like the `fraud_alerts` table), and charts
  transaction velocity, type mix, and alert severity — mirroring the
  Grafana dashboard panels in `monitoring/`.

## What it's for / not for

This is a **visual reference for reviewers** (e.g. instructors, teammates,
your GitHub README) to see the pipeline's *shape* of output — ledger feed,
alert cards, KPIs, charts — without standing up infrastructure.

It is **not** the real model. The anomaly score here is a lightweight
heuristic re-implementation for instant, dependency-free rendering in a
browser. For real scoring, train the actual Isolation Forest
(`python model/train_model.py`) and run the real streaming job
(`streaming/spark_consumer.py`) against the Docker Compose stack —
see the root [`README.md`](../README.md).

## Usage

Just open the file:

```bash
open demo/live_dashboard.html      # macOS
xdg-open demo/live_dashboard.html  # Linux
start demo/live_dashboard.html     # Windows
```

Or double-click it in a file browser. Use the **Pause/Resume** and rate
slider controls to control the simulated stream, and **Reset** to clear
all counters.
