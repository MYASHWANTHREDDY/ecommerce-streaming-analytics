.PHONY: up down produce demo test chaos-test clean logs

up:
	docker compose up -d
	@echo "Services starting... waiting 10s for Kafka/Postgres to be ready"
	sleep 10
	@echo "Ready. Run 'docker compose ps' to verify, or 'make logs' to tail output"

down:
	docker compose down

logs:
	docker compose logs -f

produce:
	docker compose --profile tools run --rm producer

demo:
	streamlit run dashboard/app.py

test:
	pytest tests/ -v

chaos-test:
	pytest tests/test_chaos.py -v -m chaos

clean:
	docker compose down -v
	# pgdata is a named Docker volume (removed above by `down -v`, not a host folder);
	# these are the actual gitignored host-side directories that accumulate over time.
	rm -rf bronze/ bronze_fulfillment/ checkpoints/ ivy2-cache/ airflow/logs/ great_expectations/gx/
	find . -type d -name __pycache__ -exec rm -rf {} + 2>/dev/null || true
	find . -name "*.pyc" -delete

.DEFAULT_GOAL := help
help:
	@echo "Streaming E-Commerce Analytics Pipeline"
	@echo ""
	@echo "Usage: make [target]"
	@echo ""
	@echo "Targets:"
	@echo "  up         - Start all services (Kafka, Postgres, Spark, Airflow)"
	@echo "  down       - Stop all services"
	@echo "  logs       - Tail service logs"
	@echo "  produce    - Run the event producer (stream orders)"
	@echo "  demo       - Open the Streamlit dashboard"
	@echo "  test       - Run pytest (fast, unit tests only, no Docker needed)"
	@echo "  chaos-test - Restart-resilience + gold-marts idempotence tests (requires 'make up' first)"
	@echo "  clean      - Tear down containers/volumes and remove all generated data"
