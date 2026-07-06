SELECT
    CAST(event_date AS DATE) AS event_date,
    region,
    country,
    COUNT(*)             AS order_count,
    SUM(units_sold)      AS total_units_sold,
    SUM(total_revenue)   AS total_revenue,
    SUM(total_profit)    AS total_profit
FROM read_parquet('/data/bronze/event_date=*/*.parquet', hive_partitioning = true)
GROUP BY 1, 2, 3
ORDER BY 1, 2, 3;
