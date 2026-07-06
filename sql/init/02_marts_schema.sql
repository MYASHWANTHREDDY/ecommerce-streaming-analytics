-- Gold marts: rebuilt hourly by airflow/dags/batch_quality_marts.py from the full
-- bronze Parquet history via DuckDB. Kept in their own schema (not reusing the
-- "analytics" name, which is already the database name) so they're clearly
-- separate from the streaming speed-layer table (public.live_order_metrics).
CREATE SCHEMA IF NOT EXISTS marts AUTHORIZATION pipeline;

CREATE TABLE IF NOT EXISTS marts.regional_sales (
    event_date        DATE NOT NULL,
    region             TEXT NOT NULL,
    country            TEXT NOT NULL,
    order_count        BIGINT,
    total_units_sold   BIGINT,
    total_revenue      NUMERIC(18, 2),
    total_profit       NUMERIC(18, 2),
    PRIMARY KEY (event_date, region, country)
);

CREATE TABLE IF NOT EXISTS marts.top_items (
    event_date        DATE NOT NULL,
    item_type          TEXT NOT NULL,
    order_count        BIGINT,
    total_units_sold   BIGINT,
    total_revenue      NUMERIC(18, 2),
    total_profit       NUMERIC(18, 2),
    PRIMARY KEY (event_date, item_type)
);

CREATE TABLE IF NOT EXISTS marts.fulfillment_time (
    event_date             DATE NOT NULL,
    region                  TEXT NOT NULL,
    sales_channel           TEXT NOT NULL,
    avg_fulfillment_days    DOUBLE PRECISION,
    min_fulfillment_days    INTEGER,
    max_fulfillment_days    INTEGER,
    order_count             BIGINT,
    PRIMARY KEY (event_date, region, sales_channel)
);

CREATE TABLE IF NOT EXISTS marts.profit_margin (
    event_date         DATE NOT NULL,
    item_type           TEXT NOT NULL,
    sales_channel        TEXT NOT NULL,
    total_revenue        NUMERIC(18, 2),
    total_cost           NUMERIC(18, 2),
    total_profit         NUMERIC(18, 2),
    profit_margin_pct    NUMERIC(6, 3),
    PRIMARY KEY (event_date, item_type, sales_channel)
);

CREATE TABLE IF NOT EXISTS marts.channel_performance (
    event_date       DATE NOT NULL,
    sales_channel      TEXT NOT NULL,
    order_priority     TEXT NOT NULL,
    order_count        BIGINT,
    total_revenue      NUMERIC(18, 2),
    avg_order_value    NUMERIC(18, 2),
    PRIMARY KEY (event_date, sales_channel, order_priority)
);
