-- Real streaming-derived fulfillment time, not the dataset's static order_date/
-- ship_date fields: {{ source('bronze', 'fulfillment') }} is the output of
-- streaming/stream_orders.py's order_placed/order_shipped stream-stream join, one row
-- per order with fulfillment_seconds computed from the two events' actual Kafka
-- timestamps. Units are seconds (not days) on purpose -- the synthesized ship delay
-- (SHIP_DELAY_MIN/MAX_SECONDS in producer/producer.py) is real seconds within a runnable
-- demo, not simulated days, and labeling it "days" would misrepresent what's actually
-- being measured. See CHANGELOG.md for the full design.
SELECT
    CAST(event_date AS DATE)                AS event_date,
    region,
    sales_channel,
    AVG(fulfillment_seconds)                AS avg_fulfillment_seconds,
    MIN(fulfillment_seconds)                AS min_fulfillment_seconds,
    MAX(fulfillment_seconds)                AS max_fulfillment_seconds,
    COUNT(*)                                AS order_count
FROM {{ source('bronze', 'fulfillment') }}
GROUP BY 1, 2, 3
ORDER BY 1, 2, 3
