FROM apache/airflow:2.11.1-python3.10
USER airflow
COPY airflow/requirements.txt /tmp/requirements.txt
RUN pip install --no-cache-dir -r /tmp/requirements.txt
# Bakes the DuckDB postgres extension in at build time so containers don't need
# internet access at DAG-run time (mirrors the ivy2-cache pattern from streaming/).
RUN python -c "import duckdb; c = duckdb.connect(); c.execute('INSTALL postgres')"

# dbt-core/dbt-duckdb go in an isolated venv, NOT airflow/requirements.txt --
# they pin their own dependency set (jinja2, click, protobuf, etc.) that
# collides with Airflow's own pinned constraints if installed into the same
# environment. This is the same fix astronomer-cosmos uses for the identical
# problem. Invoke via the full interpreter path (/opt/dbt-venv/bin/dbt) from
# the DAG task -- never rely on PATH picking the right one up.
# /opt is root-owned in the base image -- the non-root `airflow` user (still
# active from USER airflow above) can't create a directory there. Root only
# for this venv-creation step, same USER root/...(steps).../USER <app-user>
# pattern streaming/Dockerfile already uses for its own root-only steps.
USER root
RUN python -m venv /opt/dbt-venv && \
    /opt/dbt-venv/bin/pip install --no-cache-dir dbt-core==1.11.12 dbt-duckdb==1.10.1
# Same reasoning as the airflow-level INSTALL above, but the dbt venv bundles
# its own separate `duckdb` package/version, so the extension has to be baked
# in for that copy too -- it is not shared with the airflow-level duckdb install.
RUN /opt/dbt-venv/bin/python -c "import duckdb; c = duckdb.connect(); c.execute('INSTALL postgres')"
USER airflow
