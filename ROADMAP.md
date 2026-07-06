# Milestone Roadmap

Track progress through the project build here.

## Milestone 1: Kafka + Producer
- [x] docker-compose.yml running (Kafka + Postgres)
- [x] producer/producer.py reading dataset and emitting JSON events to Kafka
- [x] Events visible in kafka-console-consumer
- [x] Commit: "M1: Kafka + producer streaming order events"

## Milestone 2: Spark Structured Streaming
- [x] streaming/schemas.py with explicit StructType for order events
- [x] streaming/stream_orders.py reading from Kafka
- [x] In-stream validation (schema, nulls, ranges)
- [x] Invalid records → orders_dlq topic
- [x] Valid records → bronze/ parquet + live_order_metrics aggregates → Postgres
- [x] Checkpointing + restart test (kill job, restart, verify no duplicates)
- [x] Commit: "M2: Spark Structured Streaming with validation + dead-letter queue"

## Milestone 3: Airflow + Great Expectations
- [ ] Airflow service in compose (LocalExecutor)
- [ ] airflow/dags/batch_quality_marts.py (hourly DAG)
- [ ] Great Expectations suite on bronze data
- [ ] Gold marts built from sql/marts/*.sql (fulfillment time, profit margins, etc.)
- [ ] GE Data Docs generated
- [ ] Test with bad data → DAG fails as expected
- [ ] Commit: "M3: Airflow DAGs + Great Expectations quality checks"

## Milestone 4: Dashboard + Hardening
- [ ] dashboard/app.py in Streamlit (Live page + Analytics page)
- [ ] Live page reads live_order_metrics, auto-refreshes every 5s
- [ ] Analytics page reads gold marts
- [ ] tests/test_validation.py with pytest
- [ ] Chaos tests: restart containers, replay events, verify idempotence
- [ ] Containerize producer, wire up Makefile (make up/produce/demo)
- [ ] Commit: "M4: Streamlit dashboard + chaos testing + hardening"

## Milestone 5: Documentation + Ship
- [ ] README.md with architecture diagram
- [ ] Design Decisions section (why Kafka/Spark/Airflow, DLQ+GE two-tier quality, Lambda-style layers)
- [ ] Limitations section (single broker, no schema registry, future work)
- [ ] 20-30s GIF of live dashboard updating
- [ ] Git repo cleaned up and pushed
- [ ] Commit: "M5: Documentation + final deployment"

## Stretch Goals (after M5)
- [ ] BigQuery export task (Airflow)
- [ ] Looker Studio report on top of BigQuery
- [ ] Two-event-type design (order_placed + order_shipped) for real fulfillment-time streaming
