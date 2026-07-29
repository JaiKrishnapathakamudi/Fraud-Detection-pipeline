"""
kafka_producer.py
-------------------
Simulates a continuous, real-time stream of financial transactions by
replaying the PaySim dataset row-by-row into a Kafka topic.

Each row is serialized as JSON and published with the origin account
(`nameOrig`) as the message key, so that all transactions belonging to
the same account land on the same Kafka partition (important for
maintaining per-account state/order downstream).

Usage:
    python producer/kafka_producer.py \
        --csv data/paysim.csv \
        --topic transactions \
        --bootstrap-servers localhost:9094 \
        --rate 200 \
        --loop
"""

import argparse
import json
import logging
import signal
import sys
import time
from datetime import datetime, timezone

import pandas as pd
from kafka import KafkaProducer
from kafka.errors import KafkaError

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
)
log = logging.getLogger("producer")

# Columns present in the PaySim CSV
PAYSIM_COLUMNS = [
    "step", "type", "amount", "nameOrig", "oldbalanceOrg", "newbalanceOrig",
    "nameDest", "oldbalanceDest", "newbalanceDest", "isFraud", "isFlaggedFraud",
]

_shutdown = False


def _handle_sigint(signum, frame):
    global _shutdown
    log.info("Shutdown signal received, finishing current batch...")
    _shutdown = True


def build_producer(bootstrap_servers: str) -> KafkaProducer:
    return KafkaProducer(
        bootstrap_servers=bootstrap_servers.split(","),
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        key_serializer=lambda k: str(k).encode("utf-8") if k is not None else None,
        acks="all",
        retries=5,
        linger_ms=5,
        compression_type="gzip",
    )


def row_to_event(row: pd.Series) -> dict:
    """Convert a PaySim row into a realistic streaming transaction event."""
    return {
        "transaction_id": f"{row['nameOrig']}-{row['step']}-{int(time.time() * 1000)}",
        "event_time": datetime.now(timezone.utc).isoformat(),
        "step": int(row["step"]),
        "type": row["type"],
        "amount": float(row["amount"]),
        "name_orig": row["nameOrig"],
        "oldbalance_org": float(row["oldbalanceOrg"]),
        "newbalance_orig": float(row["newbalanceOrig"]),
        "name_dest": row["nameDest"],
        "oldbalance_dest": float(row["oldbalanceDest"]),
        "newbalance_dest": float(row["newbalanceDest"]),
        # Ground-truth labels retained for offline evaluation/monitoring only.
        # The streaming pipeline must NOT use these as model input.
        "is_fraud_label": int(row["isFraud"]),
        "is_flagged_fraud_label": int(row["isFlaggedFraud"]),
    }


def stream(csv_path: str, topic: str, bootstrap_servers: str, rate: float,
           loop: bool, chunk_size: int = 5000):
    producer = build_producer(bootstrap_servers)
    events_sent = 0
    delay = 1.0 / rate if rate > 0 else 0

    def send_chunk(chunk: pd.DataFrame):
        nonlocal events_sent
        for _, row in chunk.iterrows():
            if _shutdown:
                break
            event = row_to_event(row)
            try:
                producer.send(topic, key=event["name_orig"], value=event)
            except KafkaError as e:
                log.error(f"Failed to send event: {e}")
                continue
            events_sent += 1
            if events_sent % 1000 == 0:
                log.info(f"Sent {events_sent} events -> topic '{topic}'")
            if delay:
                time.sleep(delay)

    while True:
        log.info(f"Reading {csv_path} in chunks of {chunk_size} rows ...")
        for chunk in pd.read_csv(csv_path, usecols=PAYSIM_COLUMNS, chunksize=chunk_size):
            send_chunk(chunk)
            if _shutdown:
                break
        producer.flush()
        log.info(f"Finished one pass over dataset. Total events sent: {events_sent}")
        if not loop or _shutdown:
            break

    producer.flush()
    producer.close()
    log.info("Producer closed cleanly.")


def main():
    parser = argparse.ArgumentParser(description="PaySim -> Kafka streaming producer")
    parser.add_argument("--csv", default="data/paysim.csv", help="Path to PaySim CSV file")
    parser.add_argument("--topic", default="transactions", help="Kafka topic name")
    parser.add_argument("--bootstrap-servers", default="localhost:9094",
                         help="Comma-separated Kafka bootstrap servers")
    parser.add_argument("--rate", type=float, default=200,
                         help="Target events per second (0 = as fast as possible)")
    parser.add_argument("--loop", action="store_true",
                         help="Continuously replay the dataset instead of running once")
    parser.add_argument("--chunk-size", type=int, default=5000)
    args = parser.parse_args()

    signal.signal(signal.SIGINT, _handle_sigint)
    signal.signal(signal.SIGTERM, _handle_sigint)

    try:
        stream(args.csv, args.topic, args.bootstrap_servers, args.rate,
               args.loop, args.chunk_size)
    except FileNotFoundError:
        log.error(f"CSV file not found at '{args.csv}'. Run data/download_data.py first, "
                   f"or point --csv at your local copy of the PaySim dataset.")
        sys.exit(1)


if __name__ == "__main__":
    main()
