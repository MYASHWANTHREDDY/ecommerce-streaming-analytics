import csv
import itertools
import logging
import os
import sys
import time
from collections import Counter
from pathlib import Path

from confluent_kafka import Producer
from dotenv import load_dotenv

import events

REPO_ROOT = Path(__file__).resolve().parent.parent
load_dotenv(REPO_ROOT / ".env")

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger("producer")

KAFKA_BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP", "localhost:29092")
KAFKA_TOPIC_ORDERS = os.environ.get("KAFKA_TOPIC_ORDERS", "orders")
RATE = float(os.environ.get("RATE", "100"))
CORRUPT_PCT = float(os.environ.get("CORRUPT_PCT", "0.02"))
DATA_FILE = os.environ.get("DATA_FILE", "data/sample_1k.csv")
LOOP = os.environ.get("LOOP", "true").strip().lower() == "true"

LOG_EVERY = 100


def load_rows(path: Path) -> list[dict]:
    with path.open(newline="", encoding="utf-8") as f:
        return list(csv.DictReader(f))


def make_row_iterator(rows: list[dict]):
    return itertools.cycle(rows) if LOOP else iter(rows)


def main():
    data_path = REPO_ROOT / DATA_FILE
    rows = load_rows(data_path)
    if not rows:
        logger.error("No rows loaded from %s", data_path)
        sys.exit(1)
    logger.info(
        "Loaded %d rows from %s | rate=%s/s corrupt_pct=%s loop=%s topic=%s schema_registry=%s",
        len(rows), data_path, RATE, CORRUPT_PCT, LOOP, KAFKA_TOPIC_ORDERS, events.SCHEMA_REGISTRY_URL,
    )

    producer = Producer({"bootstrap.servers": KAFKA_BOOTSTRAP})

    counters = {"sent": 0, "valid": 0, "corrupted": 0}
    variant_counts: Counter = Counter()

    def on_delivery(err, msg):
        if err is not None:
            logger.error("Delivery failed: %s", err)

    interval = 1.0 / RATE
    next_tick = time.perf_counter()

    try:
        for row in make_row_iterator(rows):
            key = events.build_key(row)
            event = events.row_to_event(row)
            payload, was_corrupted, variant = events.maybe_corrupt(event, CORRUPT_PCT)

            while True:
                try:
                    producer.produce(KAFKA_TOPIC_ORDERS, key=key, value=payload, callback=on_delivery)
                    break
                except BufferError:
                    producer.poll(0.1)

            producer.poll(0)

            counters["sent"] += 1
            if was_corrupted:
                counters["corrupted"] += 1
                variant_counts[variant] += 1
            else:
                counters["valid"] += 1

            if counters["sent"] % LOG_EVERY == 0:
                logger.info(
                    "Sent %d events (%d valid, %d corrupted) | topic=%s",
                    counters["sent"], counters["valid"], counters["corrupted"], KAFKA_TOPIC_ORDERS,
                )

            next_tick += interval
            sleep_for = next_tick - time.perf_counter()
            if sleep_for > 0:
                time.sleep(sleep_for)
            else:
                next_tick = time.perf_counter()
    except KeyboardInterrupt:
        logger.info("Shutting down (Ctrl+C received)...")
    finally:
        producer.flush(10)
        logger.info(
            "Final summary: %d sent, %d valid, %d corrupted %s",
            counters["sent"], counters["valid"], counters["corrupted"], dict(variant_counts),
        )


if __name__ == "__main__":
    main()
