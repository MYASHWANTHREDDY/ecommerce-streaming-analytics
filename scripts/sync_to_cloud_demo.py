"""Push a snapshot of local Postgres data to the cloud Postgres backing the hosted
dashboard demo. Run manually whenever the public snapshot should refresh -- there's no
scheduled job for this, since the hosted dashboard is explicitly labeled a static
snapshot, not a live view (see dashboard/app.py). Requires CLOUD_DATABASE_URL in .env
(the connection string Neon/Supabase give you); see docs/hosting-setup.md.
"""
import os
from pathlib import Path

import psycopg2
import psycopg2.extras
from dotenv import load_dotenv

REPO_ROOT = Path(__file__).resolve().parent.parent
load_dotenv(REPO_ROOT / ".env")

LOCAL_DSN = {
    "host": os.environ.get("POSTGRES_HOST_EXTERNAL", "localhost"),
    "port": os.environ.get("POSTGRES_PORT_EXTERNAL", "5433"),
    "dbname": os.environ.get("POSTGRES_DB", "analytics"),
    "user": os.environ.get("POSTGRES_USER", "pipeline"),
    "password": os.environ.get("POSTGRES_PASSWORD", "pipeline"),
}
CLOUD_DATABASE_URL = os.environ["CLOUD_DATABASE_URL"]

MART_TABLES = [
    "regional_sales",
    "top_items",
    "fulfillment_time",
    "profit_margin",
    "channel_performance",
]

# Full history isn't meaningful for a static snapshot and would keep growing forever if
# resynced repeatedly against a `make produce --loop` session -- 24h is enough to show a
# live-looking chart on the hosted Live tab without the sync growing unbounded.
LIVE_METRICS_LOOKBACK_HOURS = 24

# Same DDL the local stack bootstraps from -- CREATE TABLE/SCHEMA IF NOT EXISTS, so
# running it against an already-initialized cloud DB on every sync is a no-op.
SCHEMA_FILES = [
    REPO_ROOT / "sql" / "init" / "01_schema.sql",
    REPO_ROOT / "sql" / "init" / "02_marts_schema.sql",
]


def ensure_schema(cloud_conn) -> None:
    with cloud_conn.cursor() as cur:
        for path in SCHEMA_FILES:
            cur.execute(path.read_text())
    cloud_conn.commit()


def sync_table(local_conn, cloud_conn, *, select_sql: str, delete_sql: str, insert_sql: str) -> int:
    with local_conn.cursor() as local_cur:
        local_cur.execute(select_sql)
        rows = local_cur.fetchall()

    with cloud_conn.cursor() as cloud_cur:
        cloud_cur.execute(delete_sql)
        if rows:
            psycopg2.extras.execute_values(cloud_cur, insert_sql, rows)
    cloud_conn.commit()
    return len(rows)


def main() -> None:
    local_conn = psycopg2.connect(**LOCAL_DSN)
    cloud_conn = psycopg2.connect(CLOUD_DATABASE_URL)
    try:
        ensure_schema(cloud_conn)

        n = sync_table(
            local_conn,
            cloud_conn,
            select_sql=f"""
                SELECT window_start, window_end, region, sales_channel, order_count, revenue
                FROM live_order_metrics
                WHERE window_start >= NOW() - INTERVAL '{LIVE_METRICS_LOOKBACK_HOURS} hours'
            """,
            delete_sql="DELETE FROM live_order_metrics",
            insert_sql="INSERT INTO live_order_metrics VALUES %s",
        )
        print(f"live_order_metrics: synced {n} rows")

        for table in MART_TABLES:
            n = sync_table(
                local_conn,
                cloud_conn,
                select_sql=f"SELECT * FROM marts.{table}",
                delete_sql=f"DELETE FROM marts.{table}",
                insert_sql=f"INSERT INTO marts.{table} VALUES %s",
            )
            print(f"marts.{table}: synced {n} rows")
    finally:
        local_conn.close()
        cloud_conn.close()


if __name__ == "__main__":
    main()
