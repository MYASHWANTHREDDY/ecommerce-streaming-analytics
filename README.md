# Streaming E-Commerce Analytics Pipeline

[![CI](https://github.com/MYASHWANTHREDDY/ecommerce-streaming-analytics/actions/workflows/ci.yml/badge.svg)](https://github.com/MYASHWANTHREDDY/ecommerce-streaming-analytics/actions/workflows/ci.yml)

A real-time event pipeline for e-commerce order analytics — Kafka + Avro/Schema Registry, Spark Structured Streaming, Airflow, dbt, Great Expectations, Prometheus/Grafana, a BigQuery export, and a Streamlit dashboard, all wired together with Docker Compose.

**Hosted demo:** https://ecommerce-streaming-analytics.streamlit.app/ (static snapshot, synced periodically from the real pipeline — see [Hosted dashboard](#hosted-dashboard) below)

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
[Producer] --schema--> [Schema Registry]
    |
    v
[Kafka: orders (Avro)] --> [Spark Structured Streaming] --> [Bronze Parquet] + [Postgres: live_order_metrics]
                                      |
                                      v
                          [Kafka: orders_dlq] (bad records + reason)

[Airflow: batch_quality_marts (@hourly)] -> [Great Expectations] -> [dbt/DuckDB] -> [Postgres: marts.*]
[Airflow: export_to_bigquery (@hourly)]  -> reads marts.* -> [GCS Parquet] -> [BigQuery]

[kafka-exporter + Spark metrics] -> [Prometheus] -> [Grafana]

[Postgres] -> [Streamlit: local dashboard]
[Postgres] --sync_to_cloud_demo.py (manual)--> [Neon Postgres] -> [Streamlit Community Cloud, hosted]
```

</details>

**Speed layer:** Spark reads Avro-encoded events from Kafka continuously, computes 1-minute windowed metrics (order count, revenue by region/channel), and writes them into `live_order_metrics` in Postgres.

**Batch layer:** An hourly Airflow DAG validates the newest bronze partition with Great Expectations, then rebuilds the gold marts with dbt (5 models running on DuckDB against the full bronze history). A second hourly DAG exports those same marts to GCS and loads them into BigQuery.

**Monitoring:** Prometheus scrapes Kafka topic/offset metrics (via `kafka-exporter`) and Spark's per-executor JVM/task stats, visualized in a Grafana dashboard.

**Serving layer:** A Streamlit dashboard — a Live tab reading the speed layer, an Analytics tab reading the batch layer — running locally against the real pipeline, or hosted on Streamlit Community Cloud against a periodically-synced snapshot.

## Why these tools

- **Kafka** as the ingestion buffer — decouples the producer's pace from whatever's consuming it, gives durable replay, and makes a dead-letter pattern natural (bad records get their own topic instead of being dropped).
- **Avro + Schema Registry, not raw JSON** — the primary `orders` topic carries a registered schema, so a producer sending a malformed record (missing a required field, wrong type) gets rejected client-side before it ever reaches Kafka, instead of quietly polluting the topic. That does mean a schema can't catch everything — negative prices, absurd values, or a ship date before the order date are all schema-valid but still wrong, so Spark still runs its own semantic checks on top.
- **Spark Structured Streaming** for the speed layer — native Kafka source/sink, and checkpoint-based recovery means killing the Spark container and bringing it back resumes from the last committed offset instead of reprocessing or losing data.
- **Airflow 2.x with `LocalExecutor`, not Airflow 3** — Airflow 3's `LocalExecutor` needs an api-server, scheduler, dag-processor, and triggerer all running at once, which felt like a lot of extra containers for what's really just two hourly DAGs.
- **dbt on DuckDB for the gold marts**, not raw DuckDB scripts — the batch layer just needs SQL over a Parquet lake, and dbt gives that SQL actual schema tests (`not_null`, `unique`) instead of trusting it silently. DuckDB reads the bronze files directly and writes into Postgres through its own `postgres` extension, no second JVM job required.
- **Two-tier data quality** — Spark's in-stream checks (schema, ranges, cross-field consistency) are fast but only see one record at a time. Great Expectations re-checks the newest bronze partition with business-rule checks (valid categories, date ordering, freshness) as a second, independent gate. The gold marts, by contrast, rebuild from the full bronze history every run.
- **A speed layer *and* a batch layer**, instead of one streaming-only pipeline — the speed layer optimizes for freshness (seconds-old numbers), the batch layer for richer aggregates that don't need to be real-time.
- **Prometheus + Grafana** for the operational side of things — throughput and topic offsets are genuinely observable this way; consumer lag isn't (see [Limitations](#limitations)), which turned out to be a useful lesson about how Structured Streaming's Kafka source actually behaves.
- **BigQuery as a second sink for the marts** — the same lake-then-warehouse pattern a lot of real analytics stacks use, and a reason to touch Airflow's Google provider operators instead of hand-rolling API calls.
- **Docker Compose** for the whole stack — one command to bring everything up or down.

## Tech Stack

- **Ingestion:** Apache Kafka 3.8.0 (KRaft mode, no ZooKeeper) + Confluent Schema Registry, Avro
- **Streaming:** Apache Spark Structured Streaming
- **Orchestration:** Apache Airflow (LocalExecutor), 2 hourly DAGs
- **Transformation:** dbt (dbt-duckdb)
- **Data Quality:** Great Expectations
- **Storage:** PostgreSQL 16 + Parquet (bronze layer)
- **Cloud:** GCP (GCS + BigQuery export), Neon Postgres (hosted dashboard snapshot)
- **Monitoring:** Prometheus + Grafana
- **CI:** GitHub Actions
- **Dashboard:** Streamlit, deployed on Streamlit Community Cloud
- **Containers:** Docker Compose
- **Language:** Python 3.10+

## Key Features

1. Producer replays a dataset as Avro-encoded events into Kafka at a configurable rate, deliberately corrupting ~2% of records across 5 distinct failure modes to exercise the quality checks downstream.
2. Schema Registry rejects structurally invalid records (missing/wrong-typed fields) at serialize time; Spark separately catches the corruption Avro can't — negative values, absurd magnitudes, cross-field inconsistencies, bad date ordering — and routes it to a dead-letter topic tagged with the specific reason.
3. 1-minute windowed aggregations by region and sales channel, written to Postgres.
4. Hourly Great Expectations validation before rebuilding the gold marts with dbt.
5. dbt-based analytics: fulfillment time, profit margins, regional sales, top items, channel performance — 5 models, 19 schema tests.
6. A second hourly DAG exports the same marts to GCS and BigQuery.
7. Prometheus + Grafana for live throughput visibility.
8. Live dashboard, auto-refreshing every 5 seconds, plus a hosted static-snapshot version.

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

Cloud pieces (BigQuery export, hosted dashboard) are optional and need your own GCP/Neon/Streamlit accounts — see [CHANGELOG.md](CHANGELOG.md) items 5 and 6 for exact setup steps if you want those running too. Everything else works fully offline.

### Verify it's working

```bash
docker compose ps
```

Watch events flow through Kafka (binary Avro, not readable JSON — pipe through `kafka-avro-console-consumer` against `localhost:8081` if you want it decoded):
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

Grafana: `http://localhost:3000` (admin/admin) — a Message Throughput panel that moves while `make produce` runs.

## Testing

- `make test` — 15 unit tests for the producer's event/corruption logic and the Avro serializer's rejection behavior. No Docker needed, runs in about a second and a half.
- `make chaos-test` — kills the Spark container mid-stream and confirms it resumes from checkpoint with no data loss or duplicate processing, then re-runs the gold-marts build twice to check idempotence. Needs `make up` first; both tests together take under 6 minutes, most of it built-in waiting for 1-minute windows to close.
- GitHub Actions runs the unit test suite plus a `docker compose config` validation on every push.

## Hosted dashboard

The [hosted version](https://ecommerce-streaming-analytics.streamlit.app/) is a static snapshot, not a live connection — there's no way to run Kafka/Spark/Airflow behind a free static host. `scripts/sync_to_cloud_demo.py` pushes the last 24h of `live_order_metrics` plus all 5 marts tables to a small Neon Postgres instance whenever I want to refresh the public snapshot. The dashboard code is identical either way; it just tries `st.secrets` before falling back to `.env`, and shows a caption on the hosted version so it's clear what you're looking at.

## Dataset

E-commerce transactions from [ExcelBI Analytics](https://excelbianalytics.com/wp/downloads-18-sample-csv-files-data-sets-for-testing-sales/) — region, country, item type, sales channel, priority, units/pricing, order and ship dates.

The producer converts rows into Avro events with fresh timestamps and deliberately corrupts ~2% of them across 5 variants: negative numeric values, bad date ordering, a corrupted wire-format header, absurd-magnitude values, and a revenue/quantity mismatch that no single-field check can catch. Three more variants that existed before the Avro migration (missing field, null field, wrong type) are now structurally impossible — the schema rejects them before they can be serialized at all, which is really the point of having one.

## By the numbers

Measured directly against this repo, not estimated:

- **~4,500-5,000 events/sec** sustained from a single producer process against local Kafka, uncapped rate.
- **15 unit tests** in ~1.5s, **2 integration/chaos tests** (restart-recovery + idempotence) in under 6 minutes total, all currently green.
- **5 dbt models, 19 schema tests**, rebuilding 5 gold marts from full bronze history every run.
- **5 corruption variants**, each landing in the dead-letter topic under its own distinct, correctly-attributed reason — verified by consuming the DLQ directly and tallying reasons against what the producer actually sent, not just trusting the code.
- CI green on every push, consistently finishing in under 35 seconds.

## Limitations

- Single Kafka broker, replication factor 1 — no fault tolerance if it dies.
- Consumer lag isn't observable in Grafana: Structured Streaming's Kafka source tracks progress through its own checkpoint and never joins a real consumer group, so `kafka-exporter` has nothing to report lag against. Message throughput is the metric that's actually live.
- Spark's built-in Prometheus servlet only exposes per-executor JVM/task stats, not structured-streaming query metrics (batch latency, rows/sec) — those exist internally but aren't on any Prometheus-format path without a custom metrics sink.
- The dead-letter sink is at-least-once, not exactly-once — fine since it's a diagnostic side channel, not the primary data path.
- A non-Spark reader of `bronze/` (dbt/DuckDB) can see uncommitted files if Spark shuts down ungracefully, since only Spark's own metadata log tracks what's actually committed. Ran into this a couple of times while testing restarts and worked around it, but it's not fixed at the architecture level.
- Single Postgres instance, no replication.
- Dev-grade secrets — plaintext `.env`, Airflow's default `admin`/`admin` login. Fine locally, not how this would run in production.
- No dashboard auth. The hosted version is read-only and shows non-sensitive demo data, so this hasn't mattered in practice.
- Single-node Spark — demonstrates the Structured Streaming API correctly, not distributed scale.
- The hosted dashboard's snapshot only updates when I manually run the sync script — there's no scheduled job for it, on purpose.

## Ideas for later

- Multi-broker Kafka
- A real two-event design (`order_placed` + `order_shipped`) so fulfillment time comes from actual streaming events instead of the dataset's static dates
- A proper consumer-group-based lag metric, if it can be added without risking the correctness guarantees the checkpoint-based approach already has

## Changelog

See [CHANGELOG.md](CHANGELOG.md) for what's changed over time.
