SELECT
    CAST(event_date AS DATE) AS event_date,
    sales_channel,
    order_priority,
    COUNT(*)             AS order_count,
    SUM(total_revenue)   AS total_revenue,
    SUM(total_revenue) / NULLIF(COUNT(*), 0)  AS avg_order_value
FROM read_parquet('/data/bronze/event_date=*/*.parquet', hive_partitioning = true)
GROUP BY 1, 2, 3
ORDER BY 1, 2, 3;
