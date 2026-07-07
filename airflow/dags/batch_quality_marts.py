from __future__ import annotations

import os
from pathlib import Path

import pendulum
from airflow.decorators import dag, task
from airflow.exceptions import AirflowFailException

MARTS_SQL_DIR = Path(os.environ.get("MARTS_SQL_DIR", "/opt/airflow/sql/marts"))


@dag(
    dag_id="batch_quality_marts",
    schedule="@hourly",
    start_date=pendulum.datetime(2026, 7, 1, tz="UTC"),
    catchup=False,
    max_active_runs=1,
    default_args={"retries": 1, "retry_delay": pendulum.duration(minutes=5)},
    tags=["gold-marts", "quality"],
)
def batch_quality_marts():
    @task
    def validate_bronze() -> None:
        from bronze_validation import check_freshness, load_latest_partition_df, run_bronze_validation

        df = load_latest_partition_df()
        check_freshness(df)
        result = run_bronze_validation(df)
        if not result.success:
            raise AirflowFailException(
                "GX validation failed on latest bronze partition — see Data Docs at "
                "great_expectations/gx/uncommitted/data_docs/local_site/index.html"
            )

    @task
    def build_gold_marts() -> None:
        import duckdb

        pg_host = os.environ["POSTGRES_HOST"]
        pg_port = os.environ["POSTGRES_PORT"]
        pg_db = os.environ["POSTGRES_DB"]
        pg_user = os.environ["POSTGRES_USER"]
        pg_password = os.environ["POSTGRES_PASSWORD"]

        con = duckdb.connect()
        con.execute("LOAD postgres;")
        con.execute(
            f"ATTACH 'dbname={pg_db} user={pg_user} password={pg_password} "
            f"host={pg_host} port={pg_port}' AS analytics_db (TYPE postgres);"
        )

        for sql_file in sorted(MARTS_SQL_DIR.glob("*.sql")):
            table = sql_file.stem
            select_sql = sql_file.read_text()
            con.execute(f"CREATE OR REPLACE TEMP TABLE mart_result AS {select_sql}")
            con.execute(f"DELETE FROM analytics_db.marts.{table};")
            con.execute(f"INSERT INTO analytics_db.marts.{table} SELECT * FROM mart_result;")

    validate_bronze() >> build_gold_marts()


batch_quality_marts()
