-- Composite-key uniqueness for regional_sales, mirroring the PRIMARY KEY
-- (event_date, region, country) in sql/init/02_marts_schema.sql. Plain
-- dbt-core has no built-in composite-unique generic test (that's a dbt_utils
-- macro), and pulling in dbt_utils would require `dbt deps` to hit the
-- network at container-start time, since the dbt/ project is volume-mounted
-- at runtime rather than baked into the image at build time — breaking the
-- no-internet-at-DAG-run-time constraint airflow/Dockerfile already follows
-- for the DuckDB postgres extension. A singular test is dependency-free and
-- does the same job: dbt fails the test if this query returns any rows.
select event_date, region, country, count(*) as n
from {{ ref('regional_sales') }}
group by 1, 2, 3
having count(*) > 1
