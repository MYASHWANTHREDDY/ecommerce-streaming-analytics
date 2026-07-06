from pyspark.sql.types import (
    DateType,
    DoubleType,
    IntegerType,
    LongType,
    StringType,
    StructField,
    StructType,
)

# Mirrors producer/events.py's row_to_event() output. event_time is intentionally
# StringType, not TimestampType — see streaming/stream_orders.py for why it's cast
# to a timestamp after parsing rather than declared as one here.
ORDER_EVENT_SCHEMA = StructType(
    [
        StructField("event_id", StringType(), True),
        StructField("event_time", StringType(), True),
        StructField("order_id", LongType(), True),
        StructField("region", StringType(), True),
        StructField("country", StringType(), True),
        StructField("item_type", StringType(), True),
        StructField("sales_channel", StringType(), True),
        StructField("order_priority", StringType(), True),
        StructField("order_date", DateType(), True),
        StructField("ship_date", DateType(), True),
        StructField("units_sold", IntegerType(), True),
        StructField("unit_price", DoubleType(), True),
        StructField("unit_cost", DoubleType(), True),
        StructField("total_revenue", DoubleType(), True),
        StructField("total_cost", DoubleType(), True),
        StructField("total_profit", DoubleType(), True),
    ]
)

REQUIRED_FIELDS = ["order_id", "region", "units_sold", "unit_price", "total_revenue"]
NON_NEGATIVE_FIELDS = ["units_sold", "unit_price", "total_revenue"]
