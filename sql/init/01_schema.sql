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

-- Gold marts will be created by Airflow DAGs (Milestone 3)
-- Placeholder comment: fulfillment_time_by_region, profit_margin_by_item, etc. to follow
