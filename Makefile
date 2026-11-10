UV ?= uv
TF := $(or $(wildcard .tools/terraform),terraform)

.PHONY: install test lint tf bench bench-kafka clean

install:  ## .venv with runtime + dev + airflow groups (needs Java 11+, see README)
	$(UV) sync --all-groups

test: lint  ## pytest: real local Spark + Iceberg, librdkafka mock cluster, moto S3, DAG import
	$(UV) run pytest

lint:
	$(UV) run ruff check .
	$(UV) run ruff format --check .

tf:  ## fmt + validate the AWS module (no credentials, no apply)
	$(TF) -chdir=terraform fmt -check -recursive
	$(TF) -chdir=terraform init -backend=false -input=false >/dev/null
	$(TF) -chdir=terraform validate

bench:  ## end-to-end benchmark, replaying files (no broker needed) -> reports/bench-files.json
	$(UV) run streamhouse bench --source files

bench-kafka:  ## same, through a real Kafka broker at $$KAFKA_BOOTSTRAP -> reports/bench-kafka.json
	$(UV) run streamhouse bench --source kafka

clean:
	rm -rf lake spark-warehouse .pytest_cache .ruff_cache
