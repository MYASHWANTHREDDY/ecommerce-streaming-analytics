SELECT
    CAST(event_date AS DATE) AS event_date,
    item_type,
    COUNT(*)             AS order_count,
    SUM(units_sold)      AS total_units_sold,
    SUM(total_revenue)   AS total_revenue,
    SUM(total_profit)    AS total_profit
FROM {{ source('bronze', 'orders') }}
GROUP BY 1, 2
ORDER BY 1, 2
