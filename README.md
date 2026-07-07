# Streaming E-Commerce Analytics Pipeline

[![CI](https://github.com/MYASHWANTHREDDY/ecommerce-streaming-analytics/actions/workflows/ci.yml/badge.svg)](https://github.com/MYASHWANTHREDDY/ecommerce-streaming-analytics/actions/workflows/ci.yml)

A real-time event pipeline for e-commerce order analytics — Kafka, Spark Structured Streaming, Airflow, Great Expectations, and a Streamlit dashboard, all wired together with Docker Compose.

## Quick Start

```bash
# Terminal 1: start everything
make up

# Terminal 2: stream order events
make produce

# Terminal 3: dashboard
make demo
```

Visit `http://localhost:8501` to watch order metrics update in real time.

![Live dashboard demo](docs/dashboard-demo.gif)

*(GIF not recorded yet — see [docs/dashboard-demo-instructions.md](docs/dashboard-demo-instructions.md).)*

## Architecture

![Architecture diagram](docs/architecture.svg)

<details>
<summary>Text version</summary>

```
[Event Producer] → [Kafka] → [Spark Structured Streaming] → [Bronze Parquet] + [Postgres Aggregates]
                                        ↓
                              [Dead-Letter Topic]
                                        ↓
                              [Invalid Events (DLQ)]

[Airflow DAG (hourly)] → [Great Expectations] → [SQL Marts] → [Postgres] → [Streamlit]
```

</details>

**Speed layer:** Spark reads from Kafka continuously, computes 1-minute windowed metrics (order count, revenue by region/channel), and writes them into `live_order_metrics` in Postgres.

**Batch layer:** An hourly Airflow DAG validates the newest bronze partition with Great Expectations, then rebuilds the gold marts (fulfillment time, profit margins, top items) with DuckDB.

**Serving layer:** A Streamlit dashboard — a Live tab reading the speed layer, an Analytics tab reading the batch layer.

## Why these tools

- **Kafka** as the ingestion buffer — decouples the producer's pace from whatever's consuming it, gives durable replay, and makes a dead-letter pattern natural (bad records get their own topic instead of being dropped).
- **Spark Structured Streaming** for the speed layer — native Kafka source/sink, and checkpoint-based recovery means killing the Spark container and bringing it back resumes from the last committed offset instead of reprocessing or losing data.
- **Airflow 2.x with `LocalExecutor`, not Airflow 3** — Airflow 3's `LocalExecutor` needs an api-server, scheduler, dag-processor, and triggerer all running at once, which felt like a lot of extra containers for what's really just one hourly DAG.
- **DuckDB, not Spark, for the gold marts** — the batch layer just needs SQL over a Parquet lake. DuckDB reads the bronze files directly and writes into Postgres through its own `postgres` extension, no second JVM job required.
- **Two-tier data quality** — Spark's in-stream checks (schema, nulls, ranges) are fast but only structural. Great Expectations re-checks the newest bronze partition with business-rule checks (valid categories, date ordering, freshness) as a second, independent gate. The gold marts, by contrast, rebuild from the full bronze history every run.
- **A speed layer *and* a batch layer**, instead of one streaming-only pipeline — the speed layer optimizes for freshness (seconds-old numbers), the batch layer for richer aggregates that don't need to be real-time.
- **Docker Compose** for the whole stack — one command to bring everything up or down.

## Tech Stack

- **Ingestion:** Apache Kafka 3.8.0 (KRaft mode, no ZooKeeper)
- **Streaming:** Apache Spark Structured Streaming
- **Orchestration:** Apache Airflow (LocalExecutor)
- **Data Quality:** Great Expectations
- **Storage:** PostgreSQL 16 + Parquet (bronze layer)
- **Dashboard:** Streamlit
- **Containers:** Docker Compose
- **Language:** Python 3.10+

## Key Features

1. Producer replays a dataset as JSON events into Kafka at a configurable rate, deliberately corrupting ~2% of records to exercise the quality checks downstream.
2. In-stream validation in Spark (schema, nulls, ranges) — bad records go to a dead-letter topic instead of being silently dropped.
3. 1-minute windowed aggregations by region and sales channel, written to Postgres.
4. Hourly Great Expectations validation before rebuilding the gold marts.
5. SQL-based analytics: fulfillment time, profit margins, regional sales, top items, channel performance.
6. Live dashboard, auto-refreshing every 5 seconds.

## Getting Started

### Prerequisites

- Docker Desktop
- Python 3.10+
- ~4GB RAM free for the Spark container

### Setup

1. Clone the repo.
2. Grab the dataset (a 1k-row sample is already committed for quick testing; the full version is optional):
   ```bash
   # https://excelbianalytics.com/wp/downloads-18-sample-csv-files-data-sets-for-testing-sales/
   # unzip and place at data/dataset.csv
   ```
3. Install dependencies:
   ```bash
   python -m venv .venv
   .venv\Scripts\activate   # or `source .venv/bin/activate` on Mac/Linux
   pip install -r requirements.txt
   cp .env.example .env
   ```
4. `make up`, then `make produce` in another terminal, then `make demo` in a third.

### Verify it's working

```bash
docker compose ps
```

Watch events flow through Kafka:
```bash
docker compose exec kafka /opt/kafka/bin/kafka-console-consumer.sh \
  --bootstrap-server localhost:9092 --topic orders --from-beginning --max-messages 20
```

Check the live aggregates:
```bash
psql -h localhost -p 5433 -U pipeline -d analytics -c "SELECT * FROM live_order_metrics ORDER BY window_start DESC LIMIT 10;"
```

Check bronze output:
```bash
ls bronze/
```

Watch the dead-letter queue:
```bash
docker compose exec kafka /opt/kafka/bin/kafka-console-consumer.sh \
  --bootstrap-server localhost:9092 --topic orders_dlq --from-beginning --max-messages 20
```

Dashboard: `http://localhost:8501` — Live tab should update every 5s, Analytics tab should show all 5 gold marts (trigger the DAG manually with `docker compose exec airflow-scheduler airflow dags trigger batch_quality_marts` if you don't want to wait for the hourly schedule).

## Testing

- `make test` — fast unit tests for the producer's event/corruption logic. No Docker needed, runs in seconds.
- `make chaos-test` — restarts the Spark container mid-stream and re-runs the Airflow DAG twice to check idempotence. Needs `make up` first; takes a few minutes.

## Dataset

E-commerce transactions from [ExcelBI Analytics](https://excelbianalytics.com/wp/downloads-18-sample-csv-files-data-sets-for-testing-sales/) — region, country, item type, sales channel, priority, units/pricing, order and ship dates.

The producer converts rows into JSON events with fresh timestamps and deliberately corrupts ~2% of them (missing fields, negative prices, type mismatches, bad date ordering, malformed JSON) to exercise the validation and dead-letter logic.

## Limitations

- Single Kafka broker, replication factor 1 — no fault tolerance if it dies.
- No schema registry — raw JSON over Kafka, so schema drift only ever gets caught downstream (Spark / Great Expectations), never at publish time.
- The dead-letter sink is at-least-once, not exactly-once — fine since it's a diagnostic side channel, not the primary data path.
- A non-Spark reader of `bronze/` (DuckDB) can see uncommitted files if Spark shuts down ungracefully, since only Spark's own metadata log tracks what's actually committed. Ran into this a couple of times while testing restarts and worked around it, but it's not fixed at the architecture level.
- Single Postgres instance, no replication.
- Dev-grade secrets — plaintext `.env`, Airflow's default `admin`/`admin` login. Fine locally, not how this would run in production.
- No dashboard auth, no CI, no monitoring/alerting beyond logs and the test suites.
- Single-node Spark — demonstrates the Structured Streaming API correctly, not distributed scale.

## Ideas for later

- Schema registry (Avro/Protobuf) instead of raw JSON
- Multi-broker Kafka
- CI running the test suite on every push
- BigQuery export + a Looker Studio dashboard on top
- A real two-event design (`order_placed` + `order_shipped`) so fulfillment time comes from actual streaming events instead of the dataset's static dates
