"""Independent export DAG: Postgres gold marts -> GCS (Parquet) -> BigQuery.

Runs hourly, separate from batch_quality_marts.py (which builds the marts via dbt).
This DAG only reads the already-built marts.* tables in Postgres and lands them in
BigQuery for downstream BI/interview-demo purposes -- it does not touch dbt or the
Postgres tables themselves.

Requires (setup steps in docs/gcp-setup.md):
  - A Postgres Airflow connection registered as `postgres_default` (this repo sets it
    via the AIRFLOW_CONN_POSTGRES_DEFAULT env var in docker-compose.yml's
    x-airflow-common block, reusing the same postgres/pipeline/pipeline/analytics
    credentials already used elsewhere in this project -- see docker-compose.yml).
  - GOOGLE_APPLICATION_CREDENTIALS pointing at a mounted service-account key file.
    Airflow's Google provider falls back to Application Default Credentials when the
    `google_cloud_default` connection has no explicit key configured, so no separate
    Airflow Connection needs to be created for GCP -- the env var is enough.
  - GCP_PROJECT_ID / GCP_GCS_BUCKET / GCP_BQ_DATASET env vars (see .env.example).

Uses PostgresToGCSOperator, not SQLToGCSOperator -- the latter doesn't exist in
apache-airflow-providers-google==22.2.1, only the Postgres-specific subclass does.
See docs/gcp-setup.md for the type-inference bug this operator has on DATE columns
and how the DAG's SQL works around it.
"""

from __future__ import annotations

import os

import pendulum
from airflow.decorators import dag, task_group
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from airflow.providers.google.cloud.transfers.postgres_to_gcs import PostgresToGCSOperator

GCP_PROJECT_ID = os.environ["GCP_PROJECT_ID"]
GCP_GCS_BUCKET = os.environ["GCP_GCS_BUCKET"]
GCP_BQ_DATASET = os.environ["GCP_BQ_DATASET"]

# Same 5 gold marts dbt builds in dbt/models/marts/ (see batch_quality_marts.py). Every
# mart's non-event_date columns, listed explicitly rather than SELECT * -- see the
# event_date::text cast below for why.
MART_COLUMNS = {
    "regional_sales": "region, country, order_count, total_units_sold, total_revenue, total_profit",
    "top_items": "item_type, order_count, total_units_sold, total_revenue, total_profit",
    "fulfillment_time": "region, sales_channel, avg_fulfillment_days, min_fulfillment_days, max_fulfillment_days, order_count",
    "profit_margin": "item_type, sales_channel, total_revenue, total_cost, total_profit, profit_margin_pct",
    "channel_performance": "sales_channel, order_priority, order_count, total_revenue, avg_order_value",
}
MART_TABLES = list(MART_COLUMNS)


@dag(
    dag_id="export_to_bigquery",
    schedule="@hourly",
    start_date=pendulum.datetime(2026, 1, 1, tz="UTC"),
    catchup=False,
    max_active_runs=1,
    default_args={
        "retries": 1,
        "retry_delay": pendulum.duration(minutes=5),
    },
    tags=["gcp", "bigquery", "export"],
    doc_md=__doc__,
)
def export_to_bigquery():
    """Postgres marts.* -> GCS Parquet -> BigQuery, one task group per mart."""

    for table in MART_TABLES:

        @task_group(group_id=f"export_{table}")
        def export_mart(table: str = table):
            # {{ ds }} is Jinja (rendered by Airflow at task execution time, so each
            # hourly run's export lands under its own execution-date prefix). The bare
            # `{}` is NOT Jinja -- PostgresToGCSOperator's own internal file-numbering
            # logic replaces it with a part number (0, 1, 2, ...) if the result set is
            # larger than approx_max_file_size_bytes and gets split across files. Both
            # placeholders coexist in the same templated string because Jinja rendering
            # happens first and leaves plain `{}` untouched.
            gcs_object_template = f"marts_export/{table}/{{{{ ds }}}}/data-{{}}.parquet"

            # event_date::text: PostgresToGCSOperator's own convert_type() already turns
            # a Postgres DATE into an ISO string for the Parquet path, but its schema
            # builder separately infers the target column as pyarrow's native date32()
            # for a DATE-typed source column -- string value into a date32 column raises
            # ArrowTypeError: object of type <class 'str'> cannot be converted to int.
            # Confirmed by triggering the DAG for real and reading the traceback, not
            # guessed -- this is a real mismatch between two independent code paths in
            # apache-airflow-providers-google==22.2.1, not something specific to this
            # DAG. Casting to text server-side makes the source column type TEXT, so the
            # operator's own type map (OID 25 has no entry, falls back to its "STRING"
            # default) agrees with what convert_type() already produces.
            export_to_gcs = PostgresToGCSOperator(
                task_id="postgres_to_gcs",
                postgres_conn_id="postgres_default",
                sql=f"SELECT event_date::text AS event_date, {MART_COLUMNS[table]} FROM marts.{table};",
                bucket=GCP_GCS_BUCKET,
                filename=gcs_object_template,
                export_format="parquet",
                gzip=False,
            )

            load_to_bq = GCSToBigQueryOperator(
                task_id="gcs_to_bigquery",
                bucket=GCP_GCS_BUCKET,
                # Wildcard match: normally exactly one part file (data-0.parquet) since
                # these marts are small aggregates, but this also covers the rare case
                # PostgresToGCSOperator splits a mart across multiple part files.
                source_objects=[f"marts_export/{table}/{{{{ ds }}}}/data-*.parquet"],
                destination_project_dataset_table=f"{GCP_BQ_DATASET}.{table}",
                source_format="PARQUET",
                # Parquet is self-describing -- BigQuery reads the embedded schema
                # directly rather than sampling rows, so this is deterministic (not the
                # heuristic row-sampling autodetect does for CSV/JSON).
                autodetect=True,
                create_disposition="CREATE_IF_NEEDED",
                # Full overwrite each run, matching the marts' own rebuild semantics in
                # dbt/profiles.yml's post-hook (DELETE + INSERT, i.e. the mart is always
                # a full snapshot, not an append log) -- WRITE_TRUNCATE mirrors that,
                # not the operator's WRITE_EMPTY default (which would fail on run 2).
                write_disposition="WRITE_TRUNCATE",
                gcp_conn_id="google_cloud_default",
                project_id=GCP_PROJECT_ID,
            )

            export_to_gcs >> load_to_bq

        export_mart()


export_to_bigquery()
