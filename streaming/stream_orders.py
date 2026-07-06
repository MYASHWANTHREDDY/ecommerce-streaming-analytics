import os

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    count,
    current_timestamp,
    from_json,
    lit,
    struct,
    sum as spark_sum,
    to_date,
    to_json,
    when,
    window,
)

from schemas import ORDER_EVENT_SCHEMA

KAFKA_BOOTSTRAP = os.environ["KAFKA_BOOTSTRAP_INTERNAL"]
TOPIC_ORDERS = os.environ["KAFKA_TOPIC_ORDERS"]
TOPIC_DLQ = os.environ["KAFKA_TOPIC_DLQ"]
POSTGRES_HOST = os.environ["POSTGRES_HOST"]
POSTGRES_PORT = os.environ["POSTGRES_PORT"]
POSTGRES_DB = os.environ["POSTGRES_DB"]
POSTGRES_USER = os.environ["POSTGRES_USER"]
POSTGRES_PASSWORD = os.environ["POSTGRES_PASSWORD"]
BRONZE_PATH = os.environ.get("BRONZE_PATH", "/data/bronze")
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
    spark = SparkSession.builder.appName("stream_orders").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    raw_df = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
        .option("subscribe", TOPIC_ORDERS)
        .option("startingOffsets", "earliest")
        .option("failOnDataLoss", "false")
        .load()
    )

    df = raw_df.select(
        col("key").cast("string").alias("kafka_key"),
        col("value").cast("string").alias("raw_value"),
        col("partition").alias("source_partition"),
        col("offset").alias("source_offset"),
        col("timestamp").alias("kafka_timestamp"),
    ).withColumn("parsed", from_json(col("raw_value"), ORDER_EVENT_SCHEMA))

    p = col("parsed")
    dlq_reason = (
        # from_json in PERMISSIVE mode returns a non-null struct with every field null for
        # unparseable JSON (verified empirically against this Spark version), not a null
        # struct as its docs might suggest. event_id is never touched by any corruption
        # variant, so a null event_id is a reliable signal that parsing failed outright,
        # distinct from "valid JSON, one field happens to be null/missing".
        when(p.isNull() | p["event_id"].isNull(), lit("malformed_json"))
        .when(p["order_id"].isNull(), lit("null_or_missing_required_field:order_id"))
        .when(p["region"].isNull(), lit("null_or_missing_required_field:region"))
        .when(p["units_sold"].isNull(), lit("null_or_missing_required_field:units_sold"))
        .when(p["unit_price"].isNull(), lit("null_or_missing_required_field:unit_price"))
        .when(p["total_revenue"].isNull(), lit("null_or_missing_required_field:total_revenue"))
        .when(p["units_sold"] < 0, lit("negative_value:units_sold"))
        .when(p["unit_price"] < 0, lit("negative_value:unit_price"))
        .when(p["total_revenue"] < 0, lit("negative_value:total_revenue"))
        .when(p["order_date"] > p["ship_date"], lit("invalid_date_order"))
        .otherwise(lit(None).cast("string"))
    )
    df = df.withColumn("dlq_reason", dlq_reason)

    valid_df = (
        df.filter(col("dlq_reason").isNull())
        .select("parsed.*")
        .withColumn("event_ts", col("event_time").cast("timestamp"))
        .withColumn("event_date", to_date(col("event_ts")))
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
    dlq_payload = to_json(
        struct(
            col("dlq_reason"),
            lit(TOPIC_ORDERS).alias("source_topic"),
            col("source_partition"),
            col("source_offset"),
            col("kafka_timestamp").cast("string").alias("kafka_timestamp"),
            current_timestamp().cast("string").alias("failed_at"),
            col("raw_value"),
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

    spark.streams.awaitAnyTermination()


if __name__ == "__main__":
    main()
