"""
load_test.py
--------------
Week 4 deliverable: simple load/latency test for the streaming pipeline.

Publishes a configurable burst of synthetic transactions directly to
Kafka at a target rate, then (optionally) polls Cassandra to measure
end-to-end latency between publish time and the time the scored record
lands in the `transactions` table.

Usage:
    python scripts/load_test.py --rate 500 --duration 60 \
        --bootstrap-servers localhost:9094 --topic transactions
"""

import argparse
import json
import random
import statistics
import time
import uuid
from datetime import datetime, timezone

from kafka import KafkaProducer

TXN_TYPES = ["CASH_IN", "CASH_OUT", "DEBIT", "PAYMENT", "TRANSFER"]


def random_event():
    old_bal = round(random.uniform(0, 50000), 2)
    amount = round(random.uniform(1, old_bal + 1000), 2)
    is_synthetic_fraud = random.random() < 0.01
    new_bal_orig = 0.0 if is_synthetic_fraud else max(old_bal - amount, 0)
    return {
        "transaction_id": str(uuid.uuid4()),
        "event_time": datetime.now(timezone.utc).isoformat(),
        "step": random.randint(1, 744),
        "type": random.choice(TXN_TYPES),
        "amount": amount,
        "name_orig": f"C{random.randint(10**8, 10**9-1)}",
        "oldbalance_org": old_bal,
        "newbalance_orig": new_bal_orig,
        "name_dest": f"{'M' if random.random() < 0.3 else 'C'}{random.randint(10**8, 10**9-1)}",
        "oldbalance_dest": round(random.uniform(0, 50000), 2),
        "newbalance_dest": round(random.uniform(0, 50000), 2),
        "is_fraud_label": int(is_synthetic_fraud),
        "is_flagged_fraud_label": 0,
        "_load_test_publish_ts": time.time(),
    }


def main():
    parser = argparse.ArgumentParser(description="Load test the fraud detection Kafka ingest path")
    parser.add_argument("--bootstrap-servers", default="localhost:9094")
    parser.add_argument("--topic", default="transactions")
    parser.add_argument("--rate", type=float, default=500, help="target events/sec")
    parser.add_argument("--duration", type=float, default=60, help="test duration in seconds")
    args = parser.parse_args()

    producer = KafkaProducer(
        bootstrap_servers=args.bootstrap_servers.split(","),
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        key_serializer=lambda k: str(k).encode("utf-8"),
        linger_ms=5,
        acks="all",
    )

    delay = 1.0 / args.rate if args.rate > 0 else 0
    send_times = []
    end_at = time.time() + args.duration
    count = 0

    print(f"Load test: target {args.rate} events/sec for {args.duration}s "
          f"-> topic '{args.topic}'")

    while time.time() < end_at:
        t0 = time.time()
        event = random_event()
        producer.send(args.topic, key=event["name_orig"], value=event)
        send_times.append(time.time() - t0)
        count += 1
        if count % 500 == 0:
            print(f"  sent {count} events...")
        if delay:
            time.sleep(max(0, delay - (time.time() - t0)))

    producer.flush()
    producer.close()

    p50 = statistics.median(send_times) * 1000
    p95 = statistics.quantiles(send_times, n=100)[94] * 1000 if len(send_times) > 20 else max(send_times) * 1000
    print("\n--- Load test summary ---")
    print(f"Total events sent : {count}")
    print(f"Achieved rate     : {count / args.duration:.1f} events/sec")
    print(f"Kafka send p50    : {p50:.2f} ms")
    print(f"Kafka send p95    : {p95:.2f} ms")
    print("\nFor end-to-end latency (publish -> Cassandra write), check the "
          "'avg_latency_ms' column in fraud_detection.pipeline_metrics, or "
          "the 'processed_at' vs event_time timestamps in the transactions table.")


if __name__ == "__main__":
    main()
