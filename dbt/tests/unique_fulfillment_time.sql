-- Composite-key uniqueness for fulfillment_time, mirroring the PRIMARY KEY
-- (event_date, region, sales_channel) in sql/init/02_marts_schema.sql. See
-- unique_regional_sales.sql for why this is a singular test rather than a
-- dbt_utils generic test.
select event_date, region, sales_channel, count(*) as n
from {{ ref('fulfillment_time') }}
group by 1, 2, 3
having count(*) > 1
