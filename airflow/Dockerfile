FROM apache/airflow:2.11.1-python3.10
USER airflow
COPY airflow/requirements.txt /tmp/requirements.txt
RUN pip install --no-cache-dir -r /tmp/requirements.txt
# Bakes the DuckDB postgres extension in at build time so containers don't need
# internet access at DAG-run time (mirrors the ivy2-cache pattern from streaming/).
RUN python -c "import duckdb; c = duckdb.connect(); c.execute('INSTALL postgres')"
