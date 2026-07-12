import json
import os
import urllib.request

from pyspark.sql import SparkSession
from pyspark.sql.avro.functions import from_avro
from pyspark.sql.functions import (
    abs as spark_abs,
    base64,
    col,
    count,
    current_timestamp,
    expr,
    length,
    lit,
    struct,
    substring,
    sum as spark_sum,
    to_date,
    to_json,
    when,
    window,
)

KAFKA_BOOTSTRAP = os.environ["KAFKA_BOOTSTRAP_INTERNAL"]
TOPIC_ORDERS = os.environ["KAFKA_TOPIC_ORDERS"]
TOPIC_ORDER_SHIPPED = os.environ["KAFKA_TOPIC_ORDER_SHIPPED"]
TOPIC_DLQ = os.environ["KAFKA_TOPIC_DLQ"]
SCHEMA_REGISTRY_URL = os.environ["SCHEMA_REGISTRY_URL_INTERNAL"]
POSTGRES_HOST = os.environ["POSTGRES_HOST"]
POSTGRES_PORT = os.environ["POSTGRES_PORT"]
POSTGRES_DB = os.environ["POSTGRES_DB"]
POSTGRES_USER = os.environ["POSTGRES_USER"]
POSTGRES_PASSWORD = os.environ["POSTGRES_PASSWORD"]
BRONZE_PATH = os.environ.get("BRONZE_PATH", "/data/bronze")
BRONZE_FULFILLMENT_PATH = os.environ.get("BRONZE_FULFILLMENT_PATH", "/data/bronze_fulfillment")
CHECKPOINT_ROOT = os.environ.get("CHECKPOINT_ROOT", "/data/checkpoints")

UPSERT_SQL = """
INSERT INTO live_order_metrics (window_start, window_end, region, sales_channel, order_count, revenue)
VALUES %s
ON CONFLICT (window_start, region, sales_channel)
DO UPDATE SET
    window_end = EXCLUDED.window_end,
    order_count = EXCLUDED.order_count,
    revenue = EXCLUDED.revenue;
"""


def fetch_latest_avro_schema(registry_url: str, topic: str) -> tuple[str, int]:
    """Fetch the current Avro schema for a topic's value subject directly from Schema
    Registry via a plain REST call (stdlib urllib, no new dependency) rather than
    hand-duplicating the schema in this file -- the registry is the single source of
    truth for what "valid" means, matching how the producer resolves the same schema.
    Uses the default TopicNameStrategy subject name ("{topic}-value"), confirmed
    empirically against a real AvroSerializer call (see producer/events.py)."""
    subject = f"{topic}-value"
    url = f"{registry_url}/subjects/{subject}/versions/latest"
    with urllib.request.urlopen(url, timeout=30) as resp:
        body = json.loads(resp.read())
    return body["schema"], body["id"]


def upsert_batch(batch_df, batch_id):
    batch_df.foreachPartition(_write_partition)


def _write_partition(rows):
    import psycopg2
    import psycopg2.extras

    records = [
        (r["window_start"], r["window_end"], r["region"], r["sales_channel"], r["order_count"], r["revenue"])
        for r in rows
    ]
    if not records:
        return

    conn = psycopg2.connect(
        host=POSTGRES_HOST,
        port=POSTGRES_PORT,
        dbname=POSTGRES_DB,
        user=POSTGRES_USER,
        password=POSTGRES_PASSWORD,
    )
    try:
        with conn.cursor() as cur:
            psycopg2.extras.execute_values(cur, UPSERT_SQL, records)
        conn.commit()
    finally:
        conn.close()


def main():
    spark = (
        SparkSession.builder.appName("stream_orders")
        # Spark's default of 200 shuffle partitions is a cluster-scale number -- on a
        # single laptop core count, running 4 concurrent stateful queries (windowed agg
        # + the order_placed/order_shipped join both shuffle), it means hundreds of tiny
        # tasks and state-store partitions competing for the same handful of cores. Found
        # this by actually watching a local demo session grind to a halt after running
        # for a while -- ProcessingTimeExecutor logs showed batches taking minutes
        # against a 10s trigger, and CPU was pegged the whole time without the query ever
        # catching up. 8 is enough parallelism for this demo's actual data volume without
        # the per-partition overhead swamping it.
        .config("spark.sql.shuffle.partitions", "8")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    avro_schema_str, schema_id = fetch_latest_avro_schema(SCHEMA_REGISTRY_URL, TOPIC_ORDERS)
    shipped_avro_schema_str, shipped_schema_id = fetch_latest_avro_schema(SCHEMA_REGISTRY_URL, TOPIC_ORDER_SHIPPED)

    # Confluent wire format: [magic byte 0x00][4-byte big-endian schema ID][avro payload].
    # Vanilla OSS Spark has no built-in Confluent-registry awareness, so this header is
    # checked/stripped by hand rather than via a library. Precomputing the exact expected
    # 5-byte header once (rather than decoding the ID from each message) turns the check
    # into a single binary equality, and doubles as the "unrecognized schema ID" check the
    # producer's malformed_bytes corruption variant targets.
    expected_header = bytes([0]) + schema_id.to_bytes(4, "big")

    raw_df = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
        .option("subscribe", TOPIC_ORDERS)
        .option("startingOffsets", "earliest")
        .option("failOnDataLoss", "false")
        .load()
    )

    header_valid = substring(col("value"), 1, 5) == lit(expected_header)
    avro_payload = substring(col("value"), 6, length(col("value")) - 5)

    df = raw_df.select(
        col("key").cast("string").alias("kafka_key"),
        col("value").alias("raw_bytes"),
        header_valid.alias("header_valid"),
        avro_payload.alias("avro_payload"),
        col("partition").alias("source_partition"),
        col("offset").alias("source_offset"),
        col("timestamp").alias("kafka_timestamp"),
    ).withColumn("parsed", from_avro(col("avro_payload"), avro_schema_str))

    p = col("parsed")
    revenue_expected = p["units_sold"] * p["unit_price"]
    dlq_reason = (
        # Bad header (flipped magic byte / unrecognized schema ID -- see
        # producer/events.py's malformed_bytes) is checked first and short-circuits
        # before from_avro's result is even trusted. The isNull()/event_id checks after
        # it are defensive, not expected to fire for any of today's corruption variants:
        # every payload that reaches from_avro here already passed the header check,
        # which this pipeline's Avro schema (all fields required, see
        # producer/schemas/order_event.avsc) guarantees decodes cleanly. Kept anyway,
        # same "design around the unverified case" reasoning this codebase already
        # applied to from_json's actual null-struct-vs-null behavior.
        when(~col("header_valid") | p.isNull() | p["event_id"].isNull(), lit("malformed_bytes"))
        # NOTE: the old null/missing-required-field checks for order_id/region/
        # units_sold/unit_price/total_revenue are gone on purpose, not dropped by
        # accident -- order_event.avsc makes all 16 fields required (no "null" union, no
        # default), so a struct with one of them missing/null can no longer decode via
        # from_avro at all; it would already be caught by the header/isNull check above.
        .when(p["units_sold"] < 0, lit("negative_value:units_sold"))
        .when(p["unit_price"] < 0, lit("negative_value:unit_price"))
        .when(p["total_revenue"] < 0, lit("negative_value:total_revenue"))
        # Absolute-magnitude sanity check, checked before revenue_quantity_mismatch below
        # so it wins the label: producer/events.py's extreme_outlier_numeric sets one field
        # to an absurd fixed value (2e9 units, $1e9/unit, or $1e12 total) without rebalancing
        # the other two, so without this check every extreme_outlier_numeric record was
        # falling through to revenue_quantity_mismatch instead -- caught in the DLQ either
        # way, but mislabeled, confirmed by actually consuming DLQ output and finding
        # revenue_quantity_mismatch at 2x the count the producer's own corruption tally
        # said it should be. Thresholds sit an order of magnitude above data/sample_1k.csv's
        # real max (units_sold ~9991, unit_price ~668, total_revenue ~6.67M) and three
        # orders of magnitude below the corrupted values, so real data can never trip this.
        .when(p["units_sold"] > 100_000, lit("extreme_outlier_numeric"))
        .when(p["unit_price"] > 10_000, lit("extreme_outlier_numeric"))
        .when(p["total_revenue"] > 100_000_000, lit("extreme_outlier_numeric"))
        # Cross-field check: a schema (Avro or otherwise) can only validate one field at
        # a time, so total_revenue can be a perfectly valid, non-negative double while
        # still being inconsistent with units_sold * unit_price. Relative + small
        # absolute tolerance to allow for legitimate rounding, comfortably below the 5x
        # multiplier producer/events.py's revenue_quantity_mismatch variant applies.
        .when(
            spark_abs(p["total_revenue"] - revenue_expected) > ((0.01 * spark_abs(revenue_expected)) + 0.01),
            lit("revenue_quantity_mismatch"),
        )
        # order_date/ship_date are Avro "string" (ISO 8601, e.g. "2026-07-01"), not a date
        # logical type -- see order_event.avsc's doc field for why. ISO 8601 strings sort
        # lexicographically in the same order as the dates they represent, so comparing
        # them directly here is correct without an extra to_date() cast; the cast to a
        # real DATE column happens once, below, for valid_df.
        .when(p["order_date"] > p["ship_date"], lit("invalid_date_order"))
        .otherwise(lit(None).cast("string"))
    )
    df = df.withColumn("dlq_reason", dlq_reason)

    valid_df = (
        df.filter(col("dlq_reason").isNull())
        .select("parsed.*")
        .withColumn("event_ts", col("event_time").cast("timestamp"))
        .withColumn("event_date", to_date(col("event_ts")))
        .withColumn("order_date", to_date(col("order_date")))
        .withColumn("ship_date", to_date(col("ship_date")))
    )

    dlq_df = df.filter(col("dlq_reason").isNotNull())

    # --- Sink 1: bronze Parquet ---
    bronze_query = (
        valid_df.writeStream.format("parquet")
        .option("path", BRONZE_PATH)
        .option("checkpointLocation", f"{CHECKPOINT_ROOT}/bronze")
        .partitionBy("event_date")
        .outputMode("append")
        .start()
    )

    # --- Sink 2: 1-minute windowed aggregates -> Postgres (upsert) ---
    agg_df = (
        valid_df.withWatermark("event_ts", "1 minute")
        .groupBy(window(col("event_ts"), "1 minute"), col("region"), col("sales_channel"))
        .agg(count(lit(1)).alias("order_count"), spark_sum("total_revenue").alias("revenue"))
        .select(
            col("window.start").alias("window_start"),
            col("window.end").alias("window_end"),
            col("region"),
            col("sales_channel"),
            col("order_count"),
            col("revenue"),
        )
    )

    agg_query = (
        agg_df.writeStream.foreachBatch(upsert_batch)
        .option("checkpointLocation", f"{CHECKPOINT_ROOT}/postgres_agg")
        .outputMode("update")
        .trigger(processingTime="10 seconds")
        .start()
    )

    # --- Sink 3: dead-letter queue ---
    # raw_bytes is binary Avro (or corrupted bytes claiming to be), not text -- casting it
    # straight to string the way the old JSON path did would mangle it (invalid UTF-8
    # gets silently replaced), which is exactly wrong for a debugging payload. base64 is
    # lossless and still human-pasteable.
    dlq_payload = to_json(
        struct(
            col("dlq_reason"),
            lit(TOPIC_ORDERS).alias("source_topic"),
            col("source_partition"),
            col("source_offset"),
            col("kafka_timestamp").cast("string").alias("kafka_timestamp"),
            current_timestamp().cast("string").alias("failed_at"),
            base64(col("raw_bytes")).alias("raw_value_base64"),
        )
    )

    dlq_query = (
        dlq_df.select(col("kafka_key").alias("key"), dlq_payload.alias("value"))
        .writeStream.format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
        .option("topic", TOPIC_DLQ)
        .option("checkpointLocation", f"{CHECKPOINT_ROOT}/dlq")
        .outputMode("append")
        .start()
    )

    # --- Sink 4: order_shipped stream-stream join -> fulfillment time ---
    # The dataset's order_date/ship_date are static values baked into the CSV, not
    # anything a streaming system observed -- fulfillment_time.sql used to compute
    # "fulfillment days" straight from those two columns, which isn't a streaming
    # computation at all. This joins the order_placed stream (already-decoded `valid_df`)
    # against a second, separate order_shipped stream on the placed event's own event_id
    # (not order_id -- see order_shipped_event.avsc's doc field for why order_id alone
    # breaks under LOOP=true's repeating replay), and computes fulfillment_seconds from
    # the two Kafka message timestamps.
    shipped_raw_df = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
        .option("subscribe", TOPIC_ORDER_SHIPPED)
        .option("startingOffsets", "earliest")
        .option("failOnDataLoss", "false")
        .load()
    )

    shipped_expected_header = bytes([0]) + shipped_schema_id.to_bytes(4, "big")
    shipped_header_valid = substring(col("value"), 1, 5) == lit(shipped_expected_header)
    shipped_avro_payload = substring(col("value"), 6, length(col("value")) - 5)

    shipped_df = (
        shipped_raw_df.select(
            shipped_header_valid.alias("header_valid"),
            shipped_avro_payload.alias("avro_payload"),
        )
        .withColumn("parsed", from_avro(col("avro_payload"), shipped_avro_schema_str))
        # No DLQ path for this stream -- it isn't user-facing and the producer never
        # corrupts it, so a bad header/decode here just means "can't be joined,"
        # silently dropped rather than routed anywhere. Matches the isNull() defensive
        # pattern the primary stream already uses, not a new idea.
        .filter(col("header_valid") & col("parsed").isNotNull())
        .select("parsed.*")
        .withColumn("shipped_ts", col("shipped_at").cast("timestamp"))
        .withWatermark("shipped_ts", "2 minutes")
    )

    placed_for_join_df = valid_df.withWatermark("event_ts", "2 minutes")

    # A stream-stream join only emits a row once the watermark has passed the join
    # condition's upper time bound -- so this bound directly controls how long results
    # take to appear, not just how much join state Spark retains. 2 minutes is generous
    # relative to SHIP_DELAY_MAX_SECONDS (default 30s) without being the 10-minute
    # window an earlier draft of this used, which -- confirmed by actually running it --
    # made the join produce zero output for the entire duration of a normal test/demo
    # session. event_id (not order_id) is what actually prevents cross-cycle mismatches
    # under LOOP=true, so this bound only needs to comfortably cover the real delay, not
    # be tight for correctness.
    fulfillment_df = (
        placed_for_join_df.alias("placed")
        .join(
            shipped_df.alias("shipped"),
            expr(
                "placed.event_id = shipped.placed_event_id AND "
                "shipped.shipped_ts >= placed.event_ts AND "
                "shipped.shipped_ts <= placed.event_ts + interval 2 minutes"
            ),
            "inner",
        )
        .select(
            col("placed.event_date").alias("event_date"),
            col("placed.region").alias("region"),
            col("placed.sales_channel").alias("sales_channel"),
            (
                col("shipped.shipped_ts").cast("long") - col("placed.event_ts").cast("long")
            ).alias("fulfillment_seconds"),
        )
    )

    fulfillment_query = (
        fulfillment_df.writeStream.format("parquet")
        .option("path", BRONZE_FULFILLMENT_PATH)
        .option("checkpointLocation", f"{CHECKPOINT_ROOT}/bronze_fulfillment")
        .partitionBy("event_date")
        .outputMode("append")
        .start()
    )

    spark.streams.awaitAnyTermination()


if __name__ == "__main__":
    main()
