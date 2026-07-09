import csv
import heapq
import itertools
import logging
import os
import random
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
KAFKA_TOPIC_ORDER_SHIPPED = os.environ.get("KAFKA_TOPIC_ORDER_SHIPPED", "order_shipped")
RATE = float(os.environ.get("RATE", "100"))
CORRUPT_PCT = float(os.environ.get("CORRUPT_PCT", "0.02"))
DATA_FILE = os.environ.get("DATA_FILE", "data/sample_1k.csv")
LOOP = os.environ.get("LOOP", "true").strip().lower() == "true"

# The source dataset has no real shipping signal, so this simulates one: every
# order_placed event gets a matching order_shipped event some real seconds later,
# emitted as its own genuinely separate Kafka message rather than a field on the first
# one. streaming/stream_orders.py joins the two streams on order_id to compute actual
# fulfillment time from these two real timestamps -- see CHANGELOG.md for why.
SHIP_DELAY_MIN_SECONDS = float(os.environ.get("SHIP_DELAY_MIN_SECONDS", "5"))
SHIP_DELAY_MAX_SECONDS = float(os.environ.get("SHIP_DELAY_MAX_SECONDS", "30"))

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

    counters = {"sent": 0, "valid": 0, "corrupted": 0, "shipped": 0}
    variant_counts: Counter = Counter()
    # Min-heap of (scheduled_unix_time, order_id, placed_event_id) -- cheaper than a
    # sorted list for the "peek smallest, pop if due" pattern this loop does every
    # iteration. placed_event_id (not order_id) is what Spark actually joins on -- see
    # order_shipped_event.avsc's doc field for why.
    pending_ships: list[tuple[float, int, str]] = []

    def on_delivery(err, msg):
        if err is not None:
            logger.error("Delivery failed: %s", err)

    def send_due_ship_events(force: bool = False) -> None:
        """Send every pending order_shipped event whose scheduled time has passed. force
        ignores scheduling and sends everything immediately -- only used if the user hits
        Ctrl+C a second time while the drain loop below is waiting out the last few
        pending delays, as an explicit "stop waiting" escape hatch. Never used just
        because a LOOP=false run's row-sending loop finished early -- doing that
        unconditionally was a real bug: at RATE=200 with the default SHIP_DELAY_MIN=5s,
        1000 rows send in ~5s, faster than any delay could naturally elapse, so an
        unconditional flush at shutdown was sending every single order_shipped event
        seconds early instead of after its real configured delay, silently defeating the
        entire point of this simulation. Confirmed by actually inspecting the resulting
        fulfillment_seconds distribution in bronze_fulfillment/ and finding it clustered
        near zero regardless of what SHIP_DELAY_MIN/MAX_SECONDS was set to."""
        now = time.time()
        while pending_ships and (force or pending_ships[0][0] <= now):
            _, order_id, placed_event_id = heapq.heappop(pending_ships)
            shipped_key = str(order_id).encode("utf-8")
            shipped_payload = events.serialize_shipped_event(
                events.build_shipped_event(order_id, placed_event_id)
            )
            while True:
                try:
                    producer.produce(
                        KAFKA_TOPIC_ORDER_SHIPPED, key=shipped_key, value=shipped_payload, callback=on_delivery
                    )
                    break
                except BufferError:
                    producer.poll(0.1)
            counters["shipped"] += 1

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

            heapq.heappush(
                pending_ships,
                (
                    time.time() + random.uniform(SHIP_DELAY_MIN_SECONDS, SHIP_DELAY_MAX_SECONDS),
                    event["order_id"],
                    event["event_id"],
                ),
            )
            producer.poll(0)
            send_due_ship_events()

            counters["sent"] += 1
            if was_corrupted:
                counters["corrupted"] += 1
                variant_counts[variant] += 1
            else:
                counters["valid"] += 1

            if counters["sent"] % LOG_EVERY == 0:
                logger.info(
                    "Sent %d events (%d valid, %d corrupted, %d shipped) | topic=%s",
                    counters["sent"], counters["valid"], counters["corrupted"], counters["shipped"], KAFKA_TOPIC_ORDERS,
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
        if pending_ships:
            logger.info(
                "Waiting for the last %d pending order_shipped event(s) to reach their "
                "real scheduled delay (up to %.0fs) -- Ctrl+C again to send them early instead.",
                len(pending_ships), SHIP_DELAY_MAX_SECONDS,
            )
        try:
            while pending_ships:
                send_due_ship_events()
                if pending_ships:
                    time.sleep(min(1.0, max(0.0, pending_ships[0][0] - time.time())))
        except KeyboardInterrupt:
            logger.info("Second interrupt -- sending remaining %d order_shipped event(s) now.", len(pending_ships))
            send_due_ship_events(force=True)
        producer.flush(10)
        logger.info(
            "Final summary: %d sent, %d valid, %d corrupted, %d shipped %s",
            counters["sent"], counters["valid"], counters["corrupted"], counters["shipped"], dict(variant_counts),
        )


if __name__ == "__main__":
    main()
