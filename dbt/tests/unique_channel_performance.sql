-- Composite-key uniqueness for channel_performance, mirroring the PRIMARY KEY
-- (event_date, sales_channel, order_priority) in sql/init/02_marts_schema.sql.
-- See unique_regional_sales.sql for why this is a singular test rather than a
-- dbt_utils generic test.
select event_date, sales_channel, order_priority, count(*) as n
from {{ ref('channel_performance') }}
group by 1, 2, 3
having count(*) > 1
