# Agent Context & Operating Instructions

Welcome, Agent! This file serves as the source of truth for the codebase architecture, environment setup, and rules of engagement for this repository. 

> [!IMPORTANT]
> **Rule of Preservation:** When making significant updates to the codebase (e.g., adding sub-projects, updating dependencies, changing configurations, adding or removing tools of significance, or introducing new pipelines), you **MUST** update both the main `README.md` (including its tech stack shield badges) and this `agents.md` file to keep the documentation and agent context synchronized.

---

## 1. Project Context & Architecture

This repository is a **Python Data Engineering Monorepo** managed by the **Pants Build System**.

### Core Stack
* **Build System:** Pants (v2.22.0)
* **Python Version:** strictly `3.10.9` (managed via `pyenv`)
* **Linter/Formatter:** Ruff (integrated via Pants backend)
* **Data Processing:** PySpark (`3.5.8`)

### Project Structure
* `.github/workflows/ai-review.yml`: Automated Gemini AI PR reviewer workflow on GitHub Actions.
* `.github/pull_request_template.md`: Template for standardizing PR descriptions.
* `.pre-commit-config.yaml`: Pre-commit hooks configuration to run `./pants fmt` and `./pants lint` locally.
* `3rdparty/`: Contains requirements (`requirements.txt`) and Pants lockfiles (`user_reqs.lock`).
* `projects/common/`: Shared monorepo utilities.
  * `logger.py`: Centralized structured logging factory (`get_logger`).
  * `kafka_avro.py`: Shared Avro producer serialization, schema loader, delivery callbacks, and PySpark ABRiS deserialization.
  * `docker/docker-compose.yml`: Shared Zookeeper, Kafka, and Confluent Schema Registry Compose setup.
  * `BUILD`: Pants build definition with `python_sources` target.
* `projects/storage/postgres/`: Relational transactional storage with PostgreSQL 16.
  * `docker/docker-compose.yml` & `docker/init.sql`: Containerized PostgreSQL 16-alpine with automated DDL schema bootstrap.
  * `config.py`: Environment variable connection factory with sensible defaults.
  * `insert_transactions.py` & `fetch_transactions.py`: Context-managed transactional CRUD operations.
  * `BUILD`: Pants build definition with `python_sources` and `pex_binary` targets.
* `projects/storage/cloud/`: Cloud lakehouse and warehouse storage access with Google Cloud.
  * `config.py`: GCP client configuration for GCS and BigQuery.
  * `read_gcs_data.py`: Object listing and streaming/pandas reader for GCS data lakes.
  * `query_bigquery.py`: Parameterized analytical queries against BigQuery data warehouse.
  * `BUILD`: Pants build definition with `python_sources` and `pex_binary` targets.
* `projects/quality/`: Data quality, governance, and validation pipelines.
  * `data/raw/user_data.csv`: Shared raw dataset with intentional edge cases for validation tests.
  * `great_expectations/validate_user_data.py`: Automated assertion validation using Great Expectations Fluent API (`projects/quality/great_expectations:validate`).
  * `pydeequ/validate_pyspark_deequ.py`: Distributed data quality checks on PySpark using Amazon Deequ (`projects/quality/pydeequ:validate`).
  * `BUILD`: Pants build definitions for shared resources and execution binaries.
* `projects/essentials/`: Core batch processing utilities & local PySpark foundations.
  * `word_count.py`: Local text processing WordCount Spark script (`projects/essentials:word_count`).
  * `employee_partition_by_hire_date.py`: Local partitioning Spark script (`projects/essentials:employee_partition`).
  * `input_data/`: Small CSV/txt sample inputs.
  * `output_data/`: Automatically generated Spark output targets (ignored by git).
  * `BUILD`: Pants build definition with `python_sources` and `pex_binary` targets.
* `projects/ingestion/`: Real-time streaming & event-driven ingestion pipeline with Avro & Schema Registry.
  * `config/input_config.yml`: Spark Ingestion configuration YAML file with Schema Registry URL.
  * `schemas/user_event.avsc`: Avro schema contract for event data.
  * `input_data/user_events.json`: Sample event stream dataset.
  * `json_producer.py`: Compact Kafka Avro message producer script (`projects/ingestion:producer`).
  * `kafka_json_to_file_job.py`: PySpark ingestion script using ABRiS and Confluent Schema Registry (`projects/ingestion:ingest_job`).
  * `BUILD`: Pants build definition with `python_sources`, `resources`, and `pex_binary` targets.
* `projects/realtime/ecommerce/`: E-Commerce Lambda Architecture platform (Streaming + Batch + Orchestration).
  * `streaming_layer/`: PySpark Structured Streaming with Avro, Schema Registry, and Delta Lake.
    * `config/ecommerce_config.yml`: Configuration for Kafka, watermarking, sliding windows, and Delta Lake sinks.
    * `schemas/ecommerce_event.avsc`: Avro schema contract for e-commerce user activity and purchase events.
    * `input_data/ecommerce_events.json`: Sample raw e-commerce event stream dataset.
    * `aggregations.py`: Spark sliding window, session funnel, and conversion KPI aggregations.
    * `sinks.py`: Multi-table streaming and batch sink adapters for Delta Lake and console outputs.
    * `ecommerce_producer.py`: Kafka Avro message producer script (`projects/realtime/ecommerce/streaming_layer:producer`).
    * `ecommerce_aggregation_job.py`: PySpark Structured Streaming orchestrator into Delta Lake (`projects/realtime/ecommerce/streaming_layer:stream_job`).
    * `BUILD`: Pants build definition with `python_sources`, `resources`, and `pex_binary` targets.
  * `batch_layer/`: Modular SQL transformations, dimensional marts, and freshness monitors with dbt and DuckDB.
    * `Dockerfile`, `docker-compose.yml`, `Makefile`: Containerized runner (`make dbt-build`, `make query`).
    * `macros/`: Centralized `get_stream_path` SQL macro for dynamic parquet pathing.
    * `models/`: Staging views with column integrity tests, analytical marts (`cumulative_users`, `daily_category_sales`, `daily_url_conversion`), and freshness monitors (`last_ingest`).
  * `orchestration/`: Containerized workflow orchestration and executive business intelligence.
    * `docker-compose.yml` & `Makefile`: Containerized Airflow (LocalExecutor + Postgres 16) and Streamlit dashboard (`make up`, `make down`, `make status`).
    * `dags/ecommerce_pipeline.py`: Airflow DAG using a custom `DeltaStreamSensor` to monitor streaming Delta commits and trigger dbt pipelines.
    * `viz/`: Interactive real-time Streamlit dashboard (`app.py`, `viz.Dockerfile`) with cached DuckDB queries.
* `projects/transformation/spark/`: Lakehouse data transformation, dimensional enrichment, and Delta Lake curation.
  * `data/raw/`: Raw sample datasets (`customers.json`, `products.json`, `purchases.json`).
  * `data/curated/`: Output directory for generated Delta Lake tables (ignored by git).
  * `data_processing_job.py`: Batch transformation and Delta Lake table curation PySpark script (`projects/transformation/spark:data_processing_job`).
  * `BUILD`: Pants build definition with `python_sources`, `resources`, and `pex_binary` targets.
* `projects/transformation/dbt/`: Modular SQL transformation, dimensional modeling, and testing with dbt and DuckDB in an isolated Docker container.
  * `Dockerfile` & `docker-compose.yml`: Containerized runtime with `dbt-duckdb` and DuckDB CLI.
  * `Makefile`: Shortcut commands (`make dbt-build`, `make query`).
  * `models/`: Staging, intermediate, and dimensional marts (`fct_customer_spending`, `fct_category_revenue`).
  * `data/raw/`: Sample raw input JSON datasets.
  * `BUILD`: Pants build definition for tracking project files.
* `scripts/`: Python utility scripts.
  * `ai_pr_reviewer.py`: The AI code reviewer script powered by the Gemini API.
  * `BUILD`: Pants build definition for the scripts directory.
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
* **`.vscode/launch.json`**: Debugging configurations mapped to current files, leveraging the local `.env` file.
* **`.env` (Git Ignored)**: Sets the correct `JAVA_HOME` to **Java 17** (overriding system default Java 24 to maintain PySpark 3.x compatibility), cloud project credentials, and the `AIRFLOW_FERNET_KEY` encryption key for Airflow container orchestration.

---

## 3. Mandatory Agent Rules

1. **NO GIT COMMITS:** Do not run any `git commit` commands. The user commits all files manually.
2. **STRICTLY LOCAL SCOPE:** Do not write or modify any VS Code/IDE configurations outside the `data-engineering` project directory.
3. **PANTS COMPLIANCE:** Always follow the target-based workflow for Pants commands.
4. **SPARK COMPATIBILITY:** Run PySpark tasks using Java 17 via the `JAVA_HOME` configuration found in `.env`.

---

## 4. Pull Request & Bookkeeping Workflow

When implementing new features or bug fixes, follow this workflow to coordinate PR creation:

1. **Local Implementation & Verification:** Implement changes locally, verify they run/test successfully, and document them in `README.md` and `agents.md`.
2. **User Review & Local Commit:** Present the changes to the user. The user will review the code locally and run `git commit` manually.
3. **PR Creation:** Once the user commits the changes, they will instruct you to create the Pull Request (or you can offer to do so).
4. **PR Formatting:** Use the `github` MCP server to create the PR. The PR must have a detailed description containing:
   * **Summary:** A clear explanation of *why* the changes were made.
   * **Key Changes:** A bulleted list of modified modules/files and what was updated.
   * **Verification:** Documentation of successful test/run commands executed during verification.

