SELECT
    CAST(event_date AS DATE)                     AS event_date,
    region,
    sales_channel,
    AVG(date_diff('day', order_date, ship_date))  AS avg_fulfillment_days,
    MIN(date_diff('day', order_date, ship_date))  AS min_fulfillment_days,
    MAX(date_diff('day', order_date, ship_date))  AS max_fulfillment_days,
    COUNT(*)                                      AS order_count
FROM read_parquet('/data/bronze/event_date=*/*.parquet', hive_partitioning = true)
-- Defensive: marts rebuild from ALL bronze history, including any partition written
-- before this quality gate existed. GX (bronze_validation.py) only re-checks the
-- newest partition each run, so this isn't redundant with that check.
WHERE ship_date >= order_date
GROUP BY 1, 2, 3
ORDER BY 1, 2, 3;
