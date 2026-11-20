# Agent Context & Operating Instructions

Welcome, Agent! This file serves as the source of truth for the codebase architecture, environment setup, and rules of engagement for this repository.

> [!IMPORTANT]
> **Rule of Preservation:** When making significant updates to the codebase (e.g., adding modules, updating dependencies, changing configurations, adding or removing tools of significance, or introducing new pipelines), you **MUST** update `README.md` (including its tech stack shield badges), `projects/ecommerce/README.md`, the relevant Wiki pages in `docs/wiki/` (validated with `./pants run docs:validate_wiki`), and this `AGENTS.md` file to keep documentation and agent context synchronized.

---

## 1. Project Context & Architecture

This repository is a **Python Data Engineering Monorepo** managed by the **Pants Build System**, centered on a unified **E-Commerce Real-Time Lambda Architecture** (`projects/ecommerce`) and shared platform utilities (`projects/common`).

### Core Stack
* **Build System:** Pants (v2.22.0, with Python, Docker, and Ruff backends)
* **Python Version:** strictly `3.10.9` (managed via `pyenv`)
* **Linter/Formatter:** Ruff (integrated via Pants backend)
* **Data Processing:** PySpark (`3.5.8`), Delta Lake (`3.2.0`), DuckDB, dbt-duckdb
* **Streaming & Schema Governance:** Apache Kafka, Confluent Schema Registry, Avro, ABRiS
* **OLTP & Cloud Storage:** PostgreSQL 16 (`psycopg2-binary`), Google Cloud Storage & BigQuery
* **Data Quality:** Amazon PyDeequ (Spark layer) & Great Expectations (batch/Gold layer)
* **Orchestration & BI:** Apache Airflow (`2.9.3`), Apache Superset (`6.1.0`), Redis (`7`), `duckdb-engine`, Docker

### Project Structure
* `.github/pull_request_template.md`: Template for standardizing PR descriptions.
* `.github/workflows/wiki-sync.yml`: CI workflow validating `docs/wiki/` on PRs and mirroring changes to `data-engineering.wiki.git` on pushes to `develop` and `master`.
* `.pre-commit-config.yaml`: Pre-commit hooks configuration to run `./pants fmt` and `./pants lint` locally.
* `pyproject.toml`: Ruff linting rules enforcing Google-style docstrings (`D` / `D417`) via `./pants lint ::`.
* `3rdparty/`: Dependency management (`requirements.txt`, Pants `user_reqs.lock`, `extract_lock_constraints.py` for lockfile-pinned Docker builds, and `BUILD`).
* `docs/`: Version-controlled GitHub Wiki source (`docs/wiki/*.md`), zero-dependency link/anchor/Mermaid validator (`docs/validate_wiki.py`), and Pants `BUILD` (`docs:validate_wiki`).
* `projects/common/`: Shared monorepo utilities.
  * `logger.py`: Centralized structured logging factory (`get_logger`).
  * `kafka_avro.py`: Shared Avro producer serialization, schema loader, delivery callbacks, and PySpark ABRiS deserialization.
  * `docker/docker-compose.yml`: Shared Zookeeper, Kafka, and Confluent Schema Registry Compose setup.
  * `BUILD`: Pants build definition with `python_sources` target.
* `projects/ecommerce/`: Unified E-Commerce Real-Time Lambda Architecture platform.
  * `streaming_layer/`: PySpark Structured Streaming with Avro, Schema Registry, PyDeequ verification, and Delta Lake.
    * `config/ecommerce_config.yml`: Configuration for Kafka, watermarking, sliding windows, and Delta Lake sinks.
    * `schemas/ecommerce_event.avsc`: Avro schema contract for e-commerce user activity and purchase events.
    * `input_data/ecommerce_events.json`: Sample raw e-commerce event stream dataset.
    * `aggregations.py`: Spark sliding window, product sales, session funnel, and conversion KPI aggregations.
    * `quality_checks.py`: Distributed Spark data quality verification using Amazon PyDeequ (`projects/ecommerce/streaming_layer:quality_checks`).
    * `sinks.py`: Multi-table streaming and batch sink adapters for Delta Lake and console outputs.
    * `ecommerce_producer.py`: Kafka Avro message producer script (`projects/ecommerce/streaming_layer:producer`).
    * `ecommerce_aggregation_job.py`: PySpark Structured Streaming & batch orchestrator (`projects/ecommerce/streaming_layer:stream_job`).
    * `BUILD`: Pants build definition with `python_sources`, `resources`, and `pex_binary` targets.
  * `oltp_sync/`: PostgreSQL OLTP dimension management & Parquet export (`psycopg2-binary`, `pandas`, `pyarrow`).
    * `config.py`: Thread-safe PostgreSQL connection pool factory.
    * `sync_dimensions.py`: Seeds and extracts `dim_customers` and `dim_products` to Parquet (`projects/ecommerce/oltp_sync:sync_dimensions`).
    * `BUILD`: Pants build definition (`lib`, `sync_dimensions`).
  * `batch_layer/`: Modular SQL transformations, OLTP-enriched dimensional marts, and freshness monitors with dbt and DuckDB.
    * `Dockerfile`, `docker-compose.yml`, `Makefile`, `BUILD`: Lockfile-pinned container runner (`make dbt-build`, `make query`, `projects/ecommerce/batch_layer:dbt_image`).
    * `macros/`: Centralized `get_stream_path` SQL macro for dynamic Parquet pathing.
    * `models/`: Staging views with column integrity tests, analytical & OLTP-enriched marts (`cumulative_users`, `daily_category_sales`, `daily_url_conversion`, `customer_segment_spending`, `brand_category_performance`), and freshness/quality monitors (`last_ingest`, `gx_quality_checks`, `pydeequ_quality_checks`).
  * `quality/`: Batch and warehouse data quality gates using Great Expectations.
    * `validate_ecommerce.py`: Validates raw events and DuckDB Gold marts (`projects/ecommerce/quality:validate`).
    * `BUILD`: Pants build definition (`lib`, `validate`).
  * `cloud_sync/`: Optional Google Cloud lakehouse and warehouse sync (`google-cloud-storage`, `google-cloud-bigquery`, `db-dtypes`).
    * `config.py`: GCP client configuration for GCS and BigQuery.
    * `export_to_gcp.py`: Uploads Parquet artifacts to GCS and loads Gold DuckDB marts to BigQuery (`projects/ecommerce/cloud_sync:export`).
    * `BUILD`: Pants build definition (`lib`, `export`).
  * `orchestration/`: Containerized workflow orchestration and executive business intelligence.
    * `Dockerfile`, `docker-compose.yml`, `Makefile`, `BUILD`: Lockfile-pinned Airflow (LocalExecutor + Postgres 16), Redis 7, and Apache Superset 6.1.0 services (`make up`, `make down`, `make status`).
    * `dags/ecommerce_pipeline.py` & `dags/BUILD`: Airflow DAG using `DeltaStreamSensor` to coordinate OLTP sync, dbt staging/marts/tests, Great Expectations validation, dbt monitor views, and optional GCP sync.
    * `superset/`: Apache Superset configuration (`superset_config.py`), bootstrap (`bootstrap.sh`), Dashboards-as-Code YAML bundle (`assets/`), asset validator/packager (`package_assets.py`), lockfile-pinned `Dockerfile`, and `BUILD` (`package_assets`, `superset_image`).
* `.venv`: A root-level symlink pointing to the current active Pants-generated virtual environment.

---

## 2. Local Environment Setup & IDE Integration

This project is bootstrapped to work seamlessly with VS Code / Antigravity IDE without writing any configurations outside the project directory:

* **`.vscode/settings.json`**: Configured to resolve the interpreter and paths dynamically:
  ```json
  {
    "python.defaultInterpreterPath": "${workspaceFolder}/.venv/bin/python",
    "python.analysis.extraPaths": [
      "${workspaceFolder}/projects"
    ]
  }
  ```
* **`.vscode/launch.json`**: Debugging configurations mapped to `projects/ecommerce/` entry points, leveraging the local `.env` file.
* **`.env` (Git Ignored)**: Sets `JAVA_HOME` to **Java 17** (for PySpark 3.x compatibility), cloud project credentials, and `AIRFLOW_FERNET_KEY` for Airflow orchestration.

---

## 3. Mandatory Agent Rules

1. **STRICTLY LOCAL SCOPE:** Do not write or modify any VS Code/IDE configurations outside the `data-engineering` project directory.
2. **PANTS COMPLIANCE:** Always follow the target-based workflow for Pants commands (`./pants fmt ::`, `./pants lint ::`, `./pants package ::`, `./pants run <target>`).
3. **SPARK COMPATIBILITY:** Run PySpark tasks using Java 17 via the `JAVA_HOME` configuration found in `.env`.
4. **DOCUMENTATION & WIKI SYNCHRONIZATION:** Always synchronize `README.md` (including badges and project structure), `projects/ecommerce/README.md`, `docs/wiki/` (verified with `./pants run docs:validate_wiki`), and `AGENTS.md` whenever architectural, schema, dependency, or pipeline changes occur.
   * **Schemas / Avro / SQL Models** $\rightarrow$ `docs/wiki/Data-Catalog-and-Lineage.md` & `docs/wiki/Data-Contracts-and-Governance.md`
   * **Architecture / Pants / Docker / Modules** $\rightarrow$ `docs/wiki/Architecture-Overview.md` & `docs/wiki/Architecture-Decision-Records.md`
   * **Requirements / Quality Gates / Tests** $\rightarrow$ `docs/wiki/Platform-Requirements.md`, `docs/wiki/Testing-and-Quality-Gates.md`, & `docs/wiki/Requirements-Traceability.md`
   * **Milestones / Edge-Case Decisions** $\rightarrow$ `docs/wiki/Project-History.md` & `docs/wiki/Engineering-Design-Decisions.md`
5. **LOCAL SELF-REVIEW & ITERATION:** Before committing or opening a PR, review `git diff` locally for PySpark/data engineering best practices (partitioning, avoiding redundant `.collect()` on large datasets), Ruff/PEP8 compliance, edge cases, and test coverage, and iterate locally until all checks pass cleanly.

---

## 4. Pull Request & Bookkeeping Workflow

When implementing new features or bug fixes, follow this workflow:

1. **Local Implementation & Verification:** Implement changes locally on a feature branch, verify they run/package/lint cleanly (`./pants fmt ::`, `./pants lint ::`, `./pants package ::`, `./pants run docs:validate_wiki`, and pipeline verification), and synchronize `README.md`, `projects/ecommerce/README.md`, `docs/wiki/`, and `AGENTS.md`.
2. **Local Review & Iteration:** Inspect the full `git diff` yourself to catch bugs, edge cases, unused dependencies, or formatting/lint issues, and iterate locally until the diff is clean.
3. **Commit & Push:** Stage and commit your verified changes (`git commit`) and push the feature branch to `origin`.
4. **PR Creation Against `develop`:** Once done with the changes, use the `github` MCP server to create a Pull Request targeting the **`develop`** branch (`base: "develop"`). The PR description must follow `.github/pull_request_template.md`:
   * **Summary:** A clear explanation of *why* the changes were made.
   * **Key Changes:** A bulleted list of modified modules/files and what was updated.
   * **Verification:** Documentation of successful test/run commands executed during verification.
