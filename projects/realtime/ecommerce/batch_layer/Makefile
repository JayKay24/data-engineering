.PHONY: help build dbt-debug dbt-run dbt-test dbt-build dbt-clean query clean

help:
	@echo "Available batch layer commands:"
	@echo "  make build       - Build the dbt-duckdb Docker container"
	@echo "  make dbt-debug   - Validate dbt connection to DuckDB"
	@echo "  make dbt-run     - Execute dbt run (builds staging & marts)"
	@echo "  make dbt-test    - Execute dbt test assertions"
	@echo "  make dbt-build   - Run dbt build (runs models + tests)"
	@echo "  make dbt-clean   - Run dbt clean inside container"
	@echo "  make query       - Open interactive DuckDB terminal on ecommerce.duckdb"
	@echo "  make clean       - Remove ecommerce.duckdb, target/, and dbt_packages/"

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

dbt-clean:
	docker compose run --rm dbt clean

query:
	docker compose run --rm --entrypoint duckdb dbt data/ecommerce.duckdb

clean:
	rm -rf data/ecommerce.duckdb data/ecommerce.duckdb.wal target dbt_packages logs
