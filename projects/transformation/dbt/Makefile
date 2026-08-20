.PHONY: help build dbt-debug dbt-run dbt-test dbt-build query clean

help:
	@echo "Available commands:"
	@echo "  make build       - Build the dbt-duckdb Docker container"
	@echo "  make dbt-debug   - Validate dbt connection to DuckDB"
	@echo "  make dbt-run     - Execute dbt run (builds views & tables)"
	@echo "  make dbt-test    - Execute dbt test assertions"
	@echo "  make dbt-build   - Run dbt build (runs models + runs tests)"
	@echo "  make query       - Open interactive DuckDB terminal on dev.duckdb"
	@echo "  make clean       - Remove dev.duckdb, target/, and dbt_packages/"

build:
	docker compose build

dbt-debug:
	docker compose run --rm dbt debug

dbt-run:
	docker compose run --rm dbt run

dbt-test:
	docker compose run --rm dbt test

dbt-build:
	docker compose run --rm dbt build

query:
	docker compose run --rm --entrypoint duckdb dbt dev.duckdb

clean:
	rm -rf dev.duckdb dev.duckdb.wal target dbt_packages
