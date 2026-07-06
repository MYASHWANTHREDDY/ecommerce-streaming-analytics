# Streaming E-Commerce Analytics Pipeline

A real-time event-streaming platform for e-commerce order analytics, built with Apache Kafka, Spark Structured Streaming, Apache Airflow, and Great Expectations.

**Status:** Complete — all 5 milestones shipped (Kafka → Spark → Postgres/bronze → Airflow/GE → gold marts → Streamlit).

## Quick Start

```bash
# Terminal 1: Start all services
make up

# Terminal 2: Stream order events
make produce

# Terminal 3: Open the dashboard
make demo
```

Then visit `http://localhost:8501` to see live order metrics updating in real time.

![Live dashboard demo](docs/dashboard-demo.gif)

*(Not recorded yet in this environment — see [docs/dashboard-demo-instructions.md](docs/dashboard-demo-instructions.md) for how to capture it.)*

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

**Speed layer (Kafka → Spark):** Always-running stream computing 1-minute windowed metrics (order count, revenue by region/channel). Results land in `live_order_metrics` table.

**Batch layer (Airflow → GE → SQL):** Hourly DAG validates bronze parquet with Great Expectations, then builds gold marts (fulfillment time, profit margins, top items) via SQL.

**Serving layer (Postgres → Streamlit):** Two-page dashboard — live metrics (speed layer) and analytics (batch layer).

## Design Decisions

- **Kafka as the ingestion buffer.** Decouples the producer's rate from whatever's consuming it, gives durable replay (useful for the chaos/restart tests), and makes a dead-letter pattern natural — bad records get a topic of their own instead of being silently dropped.
- **Spark Structured Streaming for the speed layer.** Native Kafka source/sink, and its checkpoint-based recovery is what actually let the Milestone 2/4 restart tests pass — kill the container, bring it back, it resumes from the last committed offset instead of reprocessing or losing data.
- **Airflow with `LocalExecutor`, not Airflow 3.** Airflow 3's `LocalExecutor` needs an api-server, scheduler, dag-processor, and triggerer running concurrently — real overhead for what's ultimately one hourly DAG. Airflow 2.11's classic webserver+scheduler pair was the right amount of machinery for this job, made explicitly during Milestone 3 planning after checking what Airflow 3 would have actually cost to run here.
- **DuckDB, not Spark, for the gold marts.** The batch layer needs real SQL over a Parquet lake, not a second Spark job — DuckDB reads the Hive-partitioned bronze files directly and writes straight into Postgres via its `postgres` extension, with no JVM and no coupling to the streaming job's lifecycle.
- **Two-tier data quality: in-stream validation + Great Expectations.** Spark's in-stream checks (schema, nulls, ranges) are fast and cheap but purely structural. Great Expectations re-validates the *newest* bronze partition with business-rule checks (categorical membership, date ordering, freshness) as a second, independent gate — deliberately scoped to only the newest partition, so it acts as a freshness/quality gate on new data rather than re-litigating history on every run. The gold marts, by contrast, rebuild from the *full* bronze history every run — a different table gets rebuilt completely, quality gets checked incrementally.
- **Lambda-style dual layers, not a single streaming-only (Kappa) design.** The speed layer optimizes for freshness — seconds-old order counts and revenue. The batch layer optimizes for richer, less time-sensitive aggregates (fulfillment time, profit margin) that don't need to exist in real time and are cheaper to compute correctly over the full dataset on a schedule. Running both is more moving parts than picking one, but it's also what actually demonstrates the tradeoff rather than asserting it.
- **Docker Compose for everything.** Every service was added incrementally, one per milestone, and `make up`/`make down` staying a one-command operation throughout was a deliberate constraint, not an afterthought.

## Tech Stack

- **Ingestion:** Apache Kafka 3.8.0 (KRaft mode, no ZooKeeper)
- **Streaming:** Apache Spark Structured Streaming (Kafka source, Parquet + JDBC sinks)
- **Orchestration:** Apache Airflow (LocalExecutor)
- **Data Quality:** Great Expectations (expectations + data-docs)
- **Storage:** PostgreSQL 16 (serving), Parquet files (bronze)
- **Dashboard:** Streamlit
- **Containerization:** Docker Compose
- **Language:** Python 3.10+

## Key Features

1. **Real-time event ingestion:** Python producer replays the dataset as JSON events into Kafka at configurable rate (default 100 events/sec).
2. **In-stream validation:** Kafka → Spark checks for schema violations, nulls, value ranges; invalid records routed to dead-letter queue.
3. **Windowed aggregations:** 1-minute tumbling windows on order count and revenue, partitioned by region and sales channel, written to Postgres.
4. **Scheduled quality gates:** Hourly Airflow DAG runs Great Expectations suite on bronze parquet before building gold marts.
5. **Automated analytics:** SQL transforms compute core e-commerce KPIs (fulfillment time, profit margins, regional sales, etc.) as scheduled jobs.
6. **Live dashboard:** Streamlit app auto-refreshes metrics from both speed and batch layers.

## Getting Started

### Prerequisites

- Docker Desktop (Windows/Mac/Linux)
- Python 3.10+ (for running producer locally)
- 4GB RAM (recommended for Spark container)

### Setup

1. Clone the repo.
2. Download the dataset:
   ```bash
   # Download from: https://excelbianalytics.com/wp/downloads-18-sample-csv-files-data-sets-for-testing-sales/
   # Unzip and place at: data/dataset.csv
   ```
   (A 1k-row sample is committed; the full 1M-row file is optional.)

3. Install Python dependencies (ideally in a venv):
   ```bash
   python -m venv .venv
   .venv\Scripts\activate   # Windows; use `source .venv/bin/activate` on Mac/Linux
   pip install -r requirements.txt
   cp .env.example .env
   ```

4. Start services:
   ```bash
   make up
   ```

5. In another terminal, stream events:
   ```bash
   make produce
   ```

6. In a third terminal, open the dashboard:
   ```bash
   make demo
   ```

### Verify Setup

Check that all services are running:
```bash
docker compose ps
```

Watch Kafka events:
```bash
docker compose exec kafka /opt/kafka/bin/kafka-console-consumer.sh \
  --bootstrap-server localhost:9092 --topic orders --from-beginning --max-messages 20
```

Check Postgres (live windowed aggregates, written by the Spark job):
```bash
psql -h localhost -p 5433 -U pipeline -d analytics -c "SELECT * FROM live_order_metrics ORDER BY window_start DESC LIMIT 10;"
```

Check bronze Parquet output (valid events, partitioned by date):
```bash
ls bronze/
```

Watch the dead-letter queue (records that failed validation, with a `dlq_reason`):
```bash
docker compose exec kafka /opt/kafka/bin/kafka-console-consumer.sh \
  --bootstrap-server localhost:9092 --topic orders_dlq --from-beginning --max-messages 20
```

Open the dashboard: visit `http://localhost:8501` after running `make demo` — the Live tab should visibly update every 5s, and the Analytics tab should show all 5 gold marts.

## Development

### Testing

- `make test` — fast pytest unit suite (`tests/test_validation.py`, the producer's event/corruption logic). No Docker required, runs in seconds.
- `make chaos-test` — restart-resilience + gold-marts idempotence tests (`tests/test_chaos.py`). Requires `make up` (and ideally the stack having processed some traffic) first; restarts the `spark` container and re-runs the Airflow DAG's `build_gold_marts` task twice, so it takes a few minutes. Excluded from `make test` by default via `pytest.ini`'s `chaos` marker.

### Milestones

See ROADMAP.md for the step-by-step build plan.

### Commits

Each milestone has a clear commit:
- `M1: Kafka + producer streaming order events`
- `M2: Spark Structured Streaming with validation + dead-letter queue`
- `M3: Airflow DAGs + Great Expectations quality checks`
- `M4: Streamlit dashboard + chaos testing + hardening`
- `M5: Documentation + final deployment`

### Explanations

For each piece, I can explain:
- **Kafka listeners & advertised-listeners:** Why three listener protocols and how they route traffic.
- **Checkpointing:** How Spark resumes from Kafka offsets after a crash.
- **Exactly-once semantics:** The combination of Spark checkpoints + Postgres upsert keys.
- **Dead-letter queue:** Why invalid events are routed separately instead of dropped.
- **Two-tier quality:** Why validation is in-stream (fast, cheap) and expectations are batch (thorough, consistent).
- **Lambda architecture:** Why some metrics live in the speed layer (freshness) and others in batch (correctness).

## Dataset

E-commerce transactions from ExcelBI Analytics (https://excelbianalytics.com/wp/downloads-18-sample-csv-files-data-sets-for-testing-sales/).

Columns: Region, Country, Item Type, Sales Channel, Order Priority, Units Sold, Unit Price, Unit Cost, Order Date, Ship Date.

The producer converts rows into timestamped JSON events and deliberately corrupts ~2% (missing fields, negative prices, type mismatches) to test data quality handling.

## Limitations

Honest gaps, most of them found by actually running the chaos tests and restart drills rather than reasoned about in the abstract:

- **Single Kafka broker, replication factor 1.** No fault tolerance if the broker itself dies — a real deployment needs a multi-broker cluster.
- **No schema registry.** Events are raw JSON over Kafka; there's no Avro/Protobuf + compatibility enforcement at publish time. Schema drift is only ever caught downstream, by Spark's `from_json` or Great Expectations — never at the point of ingestion.
- **The dead-letter Kafka sink is at-least-once, not exactly-once.** A crash mid-batch could in theory duplicate a DLQ message. Acceptable here since it's a diagnostic side-channel, not the primary data path (documented in `streaming/stream_orders.py`).
- **A non-Spark reader of `bronze/` can see uncommitted files after an ungraceful Spark shutdown.** Only Spark's own `_spark_metadata` commit log knows which Parquet files are actually committed; DuckDB's plain glob doesn't. Hit this twice in practice (once manually in Milestone 3, once inside the Milestone 4 chaos test) and worked around it both times — not fixed at the architecture level.
- **Single Postgres instance.** No replication, no HA.
- **Dev-grade secrets.** Plaintext `.env`, Airflow's default `admin`/`admin` login. Fine for local development, not how this would ship to production.
- **No dashboard authentication, no CI/CD, no monitoring/alerting stack** beyond container logs and the pytest/chaos suites — nothing runs those automatically on push.
- **Single-node Spark.** Demonstrates the Structured Streaming API correctly, but not distributed scale.

**Future work:** schema registry, multi-broker Kafka, CI/CD — plus the stretch goals already tracked in [ROADMAP.md](ROADMAP.md#stretch-goals-after-m5) (BigQuery export, Looker Studio, a real two-event-type fulfillment design).

## Next Steps

- [x] Milestone 1: Kafka + producer
- [x] Milestone 2: Spark Structured Streaming
- [x] Milestone 3: Airflow + Great Expectations
- [x] Milestone 4: Dashboard + hardening
- [x] Milestone 5: Documentation

## Contact & Questions

For interview walkthroughs of any component, see the "Explanations" section above.
