from __future__ import annotations

import os

import pendulum
from airflow.decorators import dag, task
from airflow.exceptions import AirflowFailException

# dbt-core/dbt-duckdb live in an isolated venv baked into the image
# (airflow/Dockerfile), not the Airflow environment itself -- see that file
# for why. Invoke via the full interpreter path, never rely on PATH.
DBT_PROJECT_DIR = os.environ.get("DBT_PROJECT_DIR", "/opt/airflow/dbt")
DBT_BIN = os.environ.get("DBT_BIN", "/opt/dbt-venv/bin/dbt")


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
        import subprocess

        # `dbt build` (not just `dbt run`) so the not_null/composite-uniqueness
        # tests under dbt/models/marts/_marts.yml and dbt/tests/ run in the same
        # task -- check=True means a failing model OR a failing test fails this
        # task exactly like the old hand-rolled loop's exceptions did, so the
        # existing retry policy (retries=1, 5 min) still applies unchanged.
        # Each dbt model reads bronze directly via DuckDB (see
        # dbt/models/marts/_sources.yml) and a post-hook (dbt/dbt_project.yml)
        # pushes the result into Postgres with the same full-rebuild
        # DELETE+INSERT semantics the old loop used.
        subprocess.run(
            [DBT_BIN, "build", "--project-dir", DBT_PROJECT_DIR, "--profiles-dir", DBT_PROJECT_DIR],
            check=True,
        )

    validate_bronze() >> build_gold_marts()


batch_quality_marts()
