# Agent Context & Operating Instructions

Welcome, Agent! This file serves as the source of truth for the codebase architecture, environment setup, and rules of engagement for this repository.

> [!IMPORTANT]
> **Rule of Preservation:** When making significant updates to the codebase (e.g., adding modules, updating dependencies, changing configurations, adding or removing tools of significance, or introducing new pipelines), you **MUST** update `README.md` (including its tech stack shield badges), `projects/ecommerce/README.md`, the relevant Wiki pages in `docs/wiki/` (validated with `./pants run docs:validate_wiki`), and this `AGENTS.md` file to keep documentation and agent context synchronized.

---

## 1. Project Context & Architecture

This repository is a **Python Data Engineering Monorepo** managed by the **Pants Build System**, centered on a unified **E-Commerce Medallion Lakehouse & Real-Time Streaming Platform** (`projects/ecommerce`) and shared platform utilities (`projects/common`).

### Core Stack
* **Build & Test System:** Pants (v2.22.0, with Python, Pytest, Docker, and Ruff backends)
* **Python Version:** strictly `3.10.9` (managed via `pyenv`)
* **Linter/Formatter:** Ruff (integrated via Pants backend)
* **Data Processing:** PySpark (`3.5.8`), Delta Lake (`3.2.0`), DuckDB (`1.5.6` with native `delta` extension), dbt (`1.12.x` + `dbt-duckdb`)
* **Streaming & Schema Governance:** Redpanda (`v24.2`, Kafka API + Confluent-compatible Schema Registry), Avro, ABRiS
* **OLTP & Cloud Storage:** PostgreSQL 16 (`psycopg2-binary`, zero-Pandas PyArrow export), Google Cloud Storage & BigQuery, Terraform / OpenTofu IaC (`infra/terraform/`)
* **Data Quality:** Amazon PyDeequ (micro-batch Spark `foreachBatch` + Quarantine DLQ) & Great Expectations (live Silver Delta + WAP Gold warehouse)
* **Orchestration & BI:** Apache Airflow (`2.9.3`), Astronomer Cosmos (`1.15.x`), Apache Superset (`6.1.0`), Redis (`7`), `duckdb-engine`, Docker

### Project Structure
* `.github/pull_request_template.md`: Template for standardizing PR descriptions.
* `.github/workflows/ci.yml`: CI pipeline running Ruff lint/format, `./pants test ::`, dbt unit tests, Superset YAML validation, Wiki validation, and Terraform validation.
* `.github/workflows/wiki-sync.yml`: CI workflow validating `docs/wiki/` on PRs and mirroring changes to `data-engineering.wiki.git` on pushes to `develop` and `master`.
* `.pre-commit-config.yaml`: Pre-commit hooks configuration to run `./pants fmt` and `./pants lint` locally.
* `pyproject.toml`: Ruff linting rules enforcing Google-style docstrings (`D` / `D417`) via `./pants lint ::`.
* `3rdparty/`: Dependency management (`requirements.txt`, Pants `user_reqs.lock`, `extract_lock_constraints.py` for lockfile-pinned Docker builds, and `BUILD`).
* `infra/terraform/`: Declarative Free-Tier GCP IaC (7-day GCS lifecycle bucket, partitioned/clustered BigQuery dataset & tables, least-privilege IAM).
* `docs/`: Version-controlled GitHub Wiki source (`docs/wiki/*.md`), zero-dependency link/anchor/Mermaid validator (`docs/validate_wiki.py`), and Pants `BUILD` (`docs:validate_wiki`).
* `projects/common/`: Shared monorepo utilities.
  * `logger.py`: Centralized structured logging factory (`get_logger`).
  * `kafka_avro.py`: Shared Avro producer serialization, schema loader, delivery callbacks, and PySpark ABRiS deserialization.
  * `docker/docker-compose.yml`: Single-source lightweight single-binary Redpanda Compose stack (`:9092` Kafka + `:8081` Schema Registry), included by `projects/ecommerce/orchestration/docker-compose.yml`.
  * `BUILD`: Pants build definition with `python_sources` target.
* `projects/ecommerce/`: Unified E-Commerce Medallion Lakehouse Platform.
  * `duckdb_utils.py` & `wap_duckdb.py`: Lock-resilient read-only DuckDB snapshot reader and atomic Write-Audit-Publish (WAP) staging/promotion CLI (`projects/ecommerce:wap_duckdb`).
  * `streaming_layer/`: Single-consumer PySpark `foreachBatch` Medallion Silver (`silver_ecommerce_events`) & Quarantine DLQ (`quarantine_events`), live PyDeequ verification, non-overlapping `10s` tumbling KPIs, and synthetic/file Redpanda producer (`--mode {available-now,streaming,batch}`).
  * `oltp_sync/`: Zero-Pandas PostgreSQL OLTP dimension management & ZSTD PyArrow Parquet export (`--simulate-scd-change` supported).
  * `batch_layer/`: `dbt-duckdb` transformations using ACID `delta_scan()` (`get_delta_path`), SCD Type 2 snapshots (`snap_customers`, `snap_products`), incremental & contract-enforced Gold marts, native dbt `unit_tests.yml`, and materialized monitor tables (`last_ingest`, `gx_quality_checks`, `pydeequ_quality_checks`, `quarantine_summary`).
  * `quality/`: Great Expectations live Silver Delta, quarantine DLQ ratio, and staging Gold mart validation gates (`projects/ecommerce/quality:validate`).
  * `cloud_sync/`: Optional GCP lakehouse sync exporting compacted ZSTD Gold Parquet snapshots to GCS and loading time-partitioned, clustered BigQuery tables with a `100 MB` query guardrail (`projects/ecommerce/cloud_sync:export`).
  * `orchestration/`: Containerized Airflow 2.9 (`LocalExecutor` + `duckdb_write_pool` + Java 17/Spark `availableNow`) with Astronomer Cosmos `DbtTaskGroup`, Redis 7, Apache Superset 6.1.0 Dashboards-as-Code, Compose `include:` for `projects/common/docker/docker-compose.yml`, and a lean `docker-compose.prod.yml` override for low-RAM (<2.6 GB) hosting.
  * `tests/`: Hermetic Pants `pytest` suite covering PySpark aggregations/quarantine, WAP & DuckDB `delta_scan()`, OLTP/GX gates, BigQuery FinOps specs, Superset YAML validation, and Airflow Cosmos DAG parsing (`./pants test ::`).
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
2. **PANTS COMPLIANCE:** Always follow the target-based workflow for Pants commands (`./pants fmt ::`, `./pants lint ::`, `./pants test ::`, `./pants package ::`, `./pants run <target>`).
3. **SPARK COMPATIBILITY:** Run PySpark tasks and `./pants test ::` using Java 17 via the `JAVA_HOME` configuration found in `.env`.
4. **DOCUMENTATION & WIKI SYNCHRONIZATION:** Always synchronize `README.md` (including badges and project structure), `projects/ecommerce/README.md`, `docs/wiki/` (verified with `./pants run docs:validate_wiki`), and `AGENTS.md` whenever architectural, schema, dependency, or pipeline changes occur.
   * **Schemas / Avro / SQL Models** $\rightarrow$ `docs/wiki/Data-Catalog-and-Lineage.md` & `docs/wiki/Data-Contracts-and-Governance.md`
   * **Architecture / Pants / Docker / Modules** $\rightarrow$ `docs/wiki/Architecture-Overview.md` & `docs/wiki/Architecture-Decision-Records.md`
   * **Requirements / Quality Gates / Tests** $\rightarrow$ `docs/wiki/Platform-Requirements.md`, `docs/wiki/Testing-and-Quality-Gates.md`, & `docs/wiki/Requirements-Traceability.md`
   * **Milestones / Edge-Case Decisions** $\rightarrow$ `docs/wiki/Project-History.md` & `docs/wiki/Engineering-Design-Decisions.md`
   * **New Acronyms / Domain Concepts** $\rightarrow$ `docs/wiki/Glossary.md`
5. **LOCAL SELF-REVIEW & ITERATION:** Before committing or opening a PR, review `git diff` locally for PySpark/data engineering best practices (partitioning, avoiding redundant `.collect()` on large datasets), Ruff/PEP8 compliance, edge cases, and test coverage, and iterate locally until all checks pass cleanly.

---

## 4. Pull Request & Bookkeeping Workflow

When implementing new features or bug fixes, follow this workflow:

1. **Local Implementation & Verification:** Implement changes locally on a feature branch, verify they run/test/package/lint cleanly (`./pants fmt ::`, `./pants lint ::`, `./pants test ::`, `./pants package ::`, `./pants run docs:validate_wiki`, and pipeline verification), and synchronize `README.md`, `projects/ecommerce/README.md`, `docs/wiki/`, and `AGENTS.md`.
2. **Local Review & Iteration:** Inspect the full `git diff` yourself to catch bugs, edge cases, unused dependencies, or formatting/lint issues, and iterate locally until the diff is clean.
3. **Commit & Push:** Stage and commit your verified changes (`git commit`) and push the feature branch to `origin`.
4. **PR Creation Against `develop`:** Once done with the changes, use the `github` MCP server or `gh pr create` to create a Pull Request targeting the **`develop`** branch (`base: "develop"`). The PR description must follow `.github/pull_request_template.md`:
   * **Summary:** A clear explanation of *why* the changes were made.
   * **Key Changes:** A bulleted list of modified modules/files and what was updated.
   * **Verification:** Documentation of successful test/run commands executed during verification.
