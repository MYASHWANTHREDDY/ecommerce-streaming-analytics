SELECT
    CAST(event_date AS DATE) AS event_date,
    item_type,
    sales_channel,
    SUM(total_revenue)   AS total_revenue,
    SUM(total_cost)      AS total_cost,
    SUM(total_profit)    AS total_profit,
    100.0 * SUM(total_profit) / NULLIF(SUM(total_revenue), 0)  AS profit_margin_pct
FROM {{ source('bronze', 'orders') }}
GROUP BY 1, 2, 3
ORDER BY 1, 2, 3
