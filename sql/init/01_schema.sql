-- Streaming aggregates: written by Spark every 1-minute window
CREATE TABLE IF NOT EXISTS live_order_metrics (
    window_start   TIMESTAMPTZ NOT NULL,
    window_end     TIMESTAMPTZ NOT NULL,
    region         TEXT        NOT NULL,
    sales_channel  TEXT        NOT NULL,
    order_count    BIGINT,
    revenue        NUMERIC(15, 2),
    PRIMARY KEY (window_start, region, sales_channel)
);

-- Gold mart tables (fulfillment time, profit margin, etc.) live in 02_marts_schema.sql,
-- rebuilt hourly by airflow/dags/batch_quality_marts.py.
