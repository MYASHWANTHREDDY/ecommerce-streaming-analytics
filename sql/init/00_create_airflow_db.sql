-- Airflow's own metadata (DAG runs, task instances, connections, etc.) lives in a
-- dedicated database, kept separate from the `analytics` business database.
CREATE DATABASE airflow OWNER pipeline;
