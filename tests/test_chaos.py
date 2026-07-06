import os
import subprocess
import time
from datetime import date, timedelta
from pathlib import Path

import pandas as pd
import psycopg2
import pytest

REPO_ROOT = Path(__file__).resolve().parent.parent
BRONZE_DIR = REPO_ROOT / "bronze"
PARQUET_MAGIC = b"PAR1"
pytestmark = pytest.mark.chaos

MART_TABLES = [
    "regional_sales",
    "top_items",
    "fulfillment_time",
    "profit_margin",
    "channel_performance",
]


def _run(cmd, timeout=120):
    result = subprocess.run(cmd, cwd=REPO_ROOT, capture_output=True, text=True, timeout=timeout)
    assert result.returncode == 0, f"{cmd}\nSTDOUT:\n{result.stdout}\nSTDERR:\n{result.stderr}"
    return result


def _pg_conn():
    return psycopg2.connect(
        host=os.environ.get("POSTGRES_HOST_EXTERNAL", "localhost"),
        port=os.environ.get("POSTGRES_PORT_EXTERNAL", "5433"),
        dbname=os.environ.get("POSTGRES_DB", "analytics"),
        user=os.environ.get("POSTGRES_USER", "pipeline"),
        password=os.environ.get("POSTGRES_PASSWORD", "pipeline"),
    )


def _total_order_count() -> int:
    with _pg_conn() as conn:
        df = pd.read_sql("SELECT COALESCE(SUM(order_count), 0) AS total FROM live_order_metrics", conn)
    return int(df["total"].iloc[0])


def _produce_burst(rate=500):
    # LOOP=false + high RATE: the 1k-row sample sends in a couple seconds and the
    # container exits on its own -- avoids signal/Ctrl+C-forwarding complexity around
    # stopping a looping producer mid-test.
    _run(
        [
            "docker", "compose", "--profile", "tools", "run", "--rm",
            "-e", "LOOP=false", "-e", f"RATE={rate}", "producer",
        ],
        timeout=30,
    )


def _remove_orphaned_bronze_files():
    """A `docker compose kill` (ungraceful shutdown, used by the restart test in this same
    file) can leave a partial/corrupt Parquet file behind that Spark's own _spark_metadata
    commit log never counted (confirmed during Milestone 3) -- a non-Spark reader like
    DuckDB chokes on these ("No magic bytes found" / "too small to be a Parquet file").
    Clean up any such orphans before trusting a DuckDB-based read of the whole bronze
    directory, so this test doesn't fail on a side effect of the other chaos test."""
    if not BRONZE_DIR.exists():
        return
    for path in BRONZE_DIR.glob("event_date=*/*.parquet"):
        valid = False
        try:
            with open(path, "rb") as f:
                f.seek(-4, os.SEEK_END)
                valid = f.read(4) == PARQUET_MAGIC
        except OSError:
            valid = False
        if not valid:
            path.unlink(missing_ok=True)
            path.with_name(f".{path.name}.crc").unlink(missing_ok=True)


def _wait_for_running(service: str, timeout: int = 90, poll_interval: int = 5) -> bool:
    """Poll instead of a fixed sleep -- JVM startup + package resolution + Kafka
    reconnect time varies by machine, and a fixed wait is either too slow (wastes time)
    or too short (false failure) depending on the host."""
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        ps = _run(["docker", "compose", "ps", "--status", "running", "--services"])
        if service in ps.stdout:
            return True
        time.sleep(poll_interval)
    return False


def test_restart_spark_recovers_without_losing_or_stalling_data():
    """Automates Milestone 2's manual restart test.

    The Postgres sink already upserts on (window_start, region, sales_channel) with
    SET order_count = EXCLUDED.order_count (overwrite, not increment), so literal
    double-counting from reprocessing is structurally prevented regardless of restarts.
    What a restart can actually break is (a) a crash loop instead of recovery, or
    (b) losing forward progress -- those are what this test actually checks.
    """
    ps = _run(["docker", "compose", "ps", "--status", "running", "--services"])
    for svc in ("kafka", "postgres", "spark"):
        assert svc in ps.stdout, f"chaos tests require `make up` first (missing: {svc})"

    _produce_burst()
    time.sleep(75)  # 1-min tumbling window + watermark + 10s trigger needs ~70-90s to close
    total_before = _total_order_count()

    _run(["docker", "compose", "kill", "spark"])
    _run(["docker", "compose", "up", "-d", "spark"])
    recovered = _wait_for_running("spark", timeout=90)
    assert recovered, "spark did not recover after kill within 90s (crash loop?)"

    _produce_burst()

    # Poll rather than a single fixed-time check: "container reports running" (checked
    # above) is not the same moment as "the streaming query has finished JVM/package-
    # resolution startup and resumed consuming" -- that gap varies by machine. Confirmed
    # empirically that a fixed 75s wait here was sometimes too short purely on timing (the
    # count kept climbing and caught up shortly after) -- polling up to 3 minutes removes
    # that false-failure risk without weakening what's actually being asserted.
    deadline = time.monotonic() + 180
    total_after = total_before
    while time.monotonic() < deadline:
        time.sleep(15)
        total_after = _total_order_count()
        if total_after > total_before:
            break

    assert total_after > total_before, "no forward progress after restart -- job is up but stalled"


def test_build_gold_marts_task_is_idempotent():
    """Honest framing of 'replay events, verify idempotence' for this architecture:
    build_gold_marts globs ALL bronze parquet on every run (DELETE+INSERT full rebuild --
    see airflow/dags/batch_quality_marts.py) and doesn't use the execution date argument
    inside the task itself. So the real idempotence property to prove is: running the exact
    same task against the exact same bronze data twice yields byte-identical gold tables --
    not doubled, not drifted. Forcing literal duplicate Kafka messages through the live
    streaming path wouldn't test anything Milestone 2's restart test didn't already prove.

    The `spark` service runs continuously (restart: unless-stopped) and keeps consuming
    new Kafka messages the whole time this test suite runs -- if left running, bronze
    parquet would keep growing *between* the two snapshots below, making them legitimately
    different for a reason that has nothing to do with build_gold_marts' own idempotence.
    So bronze is frozen (spark stopped) for the duration of both snapshots, then spark is
    restarted afterward regardless of outcome.
    """
    run_date = (date.today() - timedelta(days=1)).isoformat()

    def run_and_snapshot():
        _run(
            [
                "docker", "compose", "run", "--rm", "--no-deps", "airflow-scheduler",
                "airflow", "tasks", "test", "batch_quality_marts", "build_gold_marts", run_date,
            ],
            timeout=120,
        )
        with _pg_conn() as conn:
            return {
                table: pd.read_sql(f"SELECT * FROM marts.{table} ORDER BY 1,2,3", conn)
                .round(6)
                .to_dict("records")
                for table in MART_TABLES
            }

    _run(["docker", "compose", "stop", "spark"])
    try:
        _remove_orphaned_bronze_files()
        first = run_and_snapshot()
        second = run_and_snapshot()
    finally:
        _run(["docker", "compose", "up", "-d", "spark"])
        assert _wait_for_running("spark", timeout=90), "spark did not come back up after the idempotence test"

    assert first == second
