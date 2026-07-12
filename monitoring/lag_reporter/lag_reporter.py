"""Republishes Spark's real bronze-checkpoint offset as a named Kafka consumer group's
committed offset, purely so kafka-exporter (and, through it, Prometheus/Grafana) have a
genuine consumer-lag signal to report.

Why this exists: Structured Streaming's Kafka source never joins a real consumer group --
it tracks progress entirely through its own checkpoint, so there's nothing for
kafka-exporter's lag metric to compute against by default. Setting kafka.group.id
directly on the production stream would fix that, but Spark's own
docs call that "use with extreme caution" since it can interfere with the checkpoint's
own correctness guarantees. This script is the safer alternative: it never subscribes to
or consumes a single message from the orders topic. It only reads the offset Spark's
bronze sink already committed to its checkpoint on disk, and calls the Kafka consumer
API's commit() directly with that value -- a pure republish, with zero influence on the
actual pipeline. If this script were deleted entirely, stream_orders.py would behave
exactly as it does today.
"""
import glob
import json
import logging
import os
import time

from confluent_kafka import Consumer, TopicPartition

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("lag_reporter")

KAFKA_BOOTSTRAP = os.environ["KAFKA_BOOTSTRAP_INTERNAL"]
TOPIC = os.environ.get("KAFKA_TOPIC_ORDERS", "orders")
GROUP_ID = os.environ.get("LAG_MONITOR_GROUP_ID", "spark-pipeline-lag-monitor")
CHECKPOINT_OFFSETS_DIR = os.environ.get("CHECKPOINT_ROOT", "/data/checkpoints") + "/bronze/offsets"
POLL_INTERVAL_SECONDS = 15


def latest_committed_offset() -> int | None:
    files = [f for f in glob.glob(f"{CHECKPOINT_OFFSETS_DIR}/*") if os.path.basename(f).isdigit()]
    if not files:
        return None
    latest = max(files, key=lambda f: int(os.path.basename(f)))
    with open(latest) as fh:
        lines = fh.read().splitlines()
    # Spark's offset log: line 0 is "v1", line 1 is batch metadata, line 2 is the actual
    # per-partition offsets -- e.g. {"orders":{"0": 12345}}. Confirmed against a real
    # checkpoint file, not guessed from docs.
    offsets = json.loads(lines[-1])
    return offsets.get(TOPIC, {}).get("0")


def main() -> None:
    consumer = Consumer(
        {
            "bootstrap.servers": KAFKA_BOOTSTRAP,
            "group.id": GROUP_ID,
            "enable.auto.commit": False,
        }
    )
    log.info("Reporting %s's bronze-checkpoint offset to consumer group %r every %ss", TOPIC, GROUP_ID, POLL_INTERVAL_SECONDS)
    last_reported = None
    while True:
        offset = latest_committed_offset()
        if offset is not None and offset != last_reported:
            consumer.commit(offsets=[TopicPartition(TOPIC, 0, offset)], asynchronous=False)
            log.info("Reported offset %s", offset)
            last_reported = offset
        time.sleep(POLL_INTERVAL_SECONDS)


if __name__ == "__main__":
    main()
