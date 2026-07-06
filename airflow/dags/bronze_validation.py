import os

import duckdb
import great_expectations as gx
import pendulum
from great_expectations.checkpoint import UpdateDataDocsAction

BRONZE_PATH = os.environ.get("BRONZE_PATH", "/data/bronze")
BRONZE_GLOB = f"{BRONZE_PATH}/event_date=*/*.parquet"
GX_ROOT = os.environ.get("GX_ROOT", "/opt/airflow/great_expectations")

REQUIRED_FIELDS = ["order_id", "region", "units_sold", "unit_price", "total_revenue"]
NON_NEGATIVE_FIELDS = ["units_sold", "unit_price", "total_revenue"]
VALID_REGIONS = [
    "Asia",
    "Australia and Oceania",
    "Central America and the Caribbean",
    "Europe",
    "Middle East and North Africa",
    "North America",
    "Sub-Saharan Africa",
]
VALID_SALES_CHANNELS = ["Offline", "Online"]
VALID_ORDER_PRIORITIES = ["C", "H", "L", "M"]

MAX_STALENESS = pendulum.duration(hours=6)


def _latest_event_date(con: duckdb.DuckDBPyConnection) -> str:
    row = con.execute(
        f"SELECT DISTINCT event_date FROM read_parquet('{BRONZE_GLOB}', hive_partitioning = true) "
        "ORDER BY event_date DESC LIMIT 1"
    ).fetchone()
    if row is None:
        raise RuntimeError(f"No bronze partitions found at {BRONZE_GLOB}")
    return row[0]


def load_latest_partition_df():
    con = duckdb.connect()
    latest = _latest_event_date(con)
    df = con.execute(f"SELECT * FROM read_parquet('{BRONZE_PATH}/event_date={latest}/*.parquet')").df()
    return df


def check_freshness(df) -> None:
    if df.empty:
        raise RuntimeError("Latest bronze partition is empty — nothing to validate.")
    latest_event_time = df["event_ts"].max() if "event_ts" in df.columns else df["event_time"].max()
    latest_event_time = pendulum.parse(str(latest_event_time))
    age = pendulum.now("UTC") - latest_event_time
    if age > MAX_STALENESS:
        raise RuntimeError(
            f"Latest bronze event is {age} old (max allowed: {MAX_STALENESS}) — "
            "streaming job may be stalled."
        )


def _build_suite(context):
    suite = gx.ExpectationSuite(name="bronze_suite")
    suite = context.suites.add_or_update(suite)

    suite.add_expectation(gx.expectations.ExpectTableRowCountToBeBetween(min_value=1))

    for field in REQUIRED_FIELDS:
        suite.add_expectation(gx.expectations.ExpectColumnValuesToNotBeNull(column=field))

    for field in NON_NEGATIVE_FIELDS:
        suite.add_expectation(gx.expectations.ExpectColumnValuesToBeBetween(column=field, min_value=0))

    suite.add_expectation(gx.expectations.ExpectColumnValuesToBeInSet(column="region", value_set=VALID_REGIONS))
    suite.add_expectation(
        gx.expectations.ExpectColumnValuesToBeInSet(column="sales_channel", value_set=VALID_SALES_CHANNELS)
    )
    suite.add_expectation(
        gx.expectations.ExpectColumnValuesToBeInSet(column="order_priority", value_set=VALID_ORDER_PRIORITIES)
    )
    suite.add_expectation(
        gx.expectations.ExpectColumnPairValuesAToBeGreaterThanB(
            column_A="ship_date", column_B="order_date", or_equal=True
        )
    )
    return suite


def run_bronze_validation(df):
    context = gx.get_context(mode="file", project_root_dir=GX_ROOT)

    data_source = context.data_sources.add_or_update_pandas(name="pandas_bronze")
    data_asset = data_source.add_dataframe_asset(name="bronze_latest_partition")
    batch_definition = data_asset.add_batch_definition_whole_dataframe(name="whole_df")

    suite = _build_suite(context)

    validation_definition = context.validation_definitions.add_or_update(
        gx.ValidationDefinition(name="bronze_hourly_validation", data=batch_definition, suite=suite)
    )

    checkpoint = context.checkpoints.add_or_update(
        gx.Checkpoint(
            name="bronze_hourly_checkpoint",
            validation_definitions=[validation_definition],
            actions=[UpdateDataDocsAction(name="update_data_docs")],
        )
    )

    return checkpoint.run(batch_parameters={"dataframe": df})
