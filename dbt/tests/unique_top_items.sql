-- Composite-key uniqueness for top_items, mirroring the PRIMARY KEY
-- (event_date, item_type) in sql/init/02_marts_schema.sql. See
-- unique_regional_sales.sql for why this is a singular test rather than a
-- dbt_utils generic test.
select event_date, item_type, count(*) as n
from {{ ref('top_items') }}
group by 1, 2
having count(*) > 1
