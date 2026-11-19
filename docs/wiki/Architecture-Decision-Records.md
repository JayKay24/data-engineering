# Architecture Decision Records (ADRs)

This catalog documents the foundational Architecture Decision Records (ADRs) for **`JayKay24/data-engineering`**, detailing context, alternatives considered, decisions, consequences, and pull request references.

---

## 📑 ADR Index

| ADR         | Title                                                                                                                                      |   Status   | Phase / Context | Primary Reference                                                                                  |
| :---------- | :----------------------------------------------------------------------------------------------------------------------------------------- | :--------: | :-------------- | :------------------------------------------------------------------------------------------------- |
| **ADR-001** | [Pants Build System & Single Shared Lockfile](#adr-001-pants-build-system--single-shared-lockfile)                                         | `Accepted` | Genesis         | PR [#1](https://github.com/JayKay24/data-engineering/pull/1)–[#5](https://github.com/JayKay24/data-engineering/pull/5) |
| **ADR-002** | [Confluent Schema Registry & PySpark ABRiS Py4J Deserialization](#adr-002-confluent-schema-registry--pyspark-abris-py4j-deserialization) | `Accepted` | Ingestion       | PR [#13](https://github.com/JayKay24/data-engineering/pull/13)                                     |
| **ADR-003** | [Shared Platform Utilities & Infrastructure in `projects/common`](#adr-003-shared-platform-utilities--infrastructure-in-projectscommon)    | `Accepted` | Platform        | PR [#10](https://github.com/JayKay24/data-engineering/pull/10), [#14](https://github.com/JayKay24/data-engineering/pull/14) |
| **ADR-004** | [Delta Lake ACID Sinks for Multi-Metric Structured Streaming](#adr-004-delta-lake-acid-sinks-for-multi-metric-structured-streaming)        | `Accepted` | Speed Layer     | PR [#6](https://github.com/JayKay24/data-engineering/pull/6), [#14](https://github.com/JayKay24/data-engineering/pull/14) |
| **ADR-005** | [E-Commerce Lambda Architecture with DuckDB & `dbt-duckdb`](#adr-005-e-commerce-lambda-architecture-with-duckdb--dbt-duckdb)               | `Accepted` | Batch Layer     | PR [#7](https://github.com/JayKay24/data-engineering/pull/7), [#15](https://github.com/JayKay24/data-engineering/pull/15) |
| **ADR-006** | [Dynamic Parquet Resolution via `get_stream_path` dbt Macro](#adr-006-dynamic-parquet-resolution-via-get_stream_path-dbt-macro)          | `Accepted` | Batch Layer     | PR [#15](https://github.com/JayKay24/data-engineering/pull/15)                                     |
| **ADR-007** | [Event-Driven Airflow Orchestration with `DeltaStreamSensor`](#adr-007-event-driven-airflow-orchestration-with-deltastreamsensor)         | `Accepted` | Orchestration   | PR [#15](https://github.com/JayKay24/data-engineering/pull/15)                                     |
| **ADR-008** | [Thread-Safe PostgreSQL Connection Pooling & OLTP Dimension Sync](#adr-008-thread-safe-postgresql-connection-pooling--oltp-dimension-sync) | `Accepted` | OLTP / Storage  | PR [#10](https://github.com/JayKay24/data-engineering/pull/10), [#24](https://github.com/JayKay24/data-engineering/pull/24) |
| **ADR-009** | [Dual-Engine Data Quality Governance (PyDeequ + Great Expectations)](#adr-009-dual-engine-data-quality-governance-pydeequ--great-expectations) | `Accepted` | Quality     | PR [#11](https://github.com/JayKay24/data-engineering/pull/11), [#12](https://github.com/JayKay24/data-engineering/pull/12), [#24](https://github.com/JayKay24/data-engineering/pull/24) |
| **ADR-010** | [Lock-Resilient Read-Only DuckDB Snapshot Fallback](#adr-010-lock-resilient-read-only-duckdb-snapshot-fallback)                            | `Accepted` | Reliability     | PR [#24](https://github.com/JayKay24/data-engineering/pull/24)                                     |
| **ADR-011** | [Lockfile-Driven Docker Builds & Isolated `/opt/pipeline-venv`](#adr-011-lockfile-driven-docker-builds--isolated-optpipeline-venv)        | `Accepted` | Containers      | PR [#24](https://github.com/JayKay24/data-engineering/pull/24)                                     |
| **ADR-012** | [Monorepo Consolidation into `projects/ecommerce` & Local Agent Review](#adr-012-monorepo-consolidation-into-projectsecommerce--local-agent-review) | `Accepted` | Consolidation | PR [#24](https://github.com/JayKay24/data-engineering/pull/24), [#25](https://github.com/JayKay24/data-engineering/pull/25) |
| **ADR-013** | [Migrating Executive BI from Streamlit to Apache Superset (Dashboards-as-Code)](#adr-013-migrating-executive-bi-from-streamlit-to-apache-superset-dashboards-as-code) | `Accepted` | Serving / BI | Apache Superset Migration |

---

## ADR-001: Pants Build System & Single Shared Lockfile

### Status
`Accepted` (PRs [#1](https://github.com/JayKay24/data-engineering/pull/1)–[#5](https://github.com/JayKay24/data-engineering/pull/5))

### Context
Data engineering pipelines combine heavy Python frameworks (PySpark, DuckDB, dbt, Airflow, Great Expectations, GCP SDKs). Uncoordinated virtual environments lead to version drift, broken imports, and slow manual dependency tracking.

### Decision
Adopt **Pantsbuild (v2.22.0)** targeting **Python `3.10.9`** with a single global resolve (`shared-lock` $\rightarrow$ `3rdparty/user_reqs.lock`), automatic dependency inference, Ruff formatting/linting backends, and target-based `pex_binary` packaging.

### Alternatives Considered
- _Poetry / pip-tools per project_: Duplicated lockfiles across sub-projects and no unified monorepo dependency graph or build caching.

### Consequences
- **Positive**: Fine-grained target caching, zero manual `BUILD` dependency lists for Python imports, unified `.venv` export.
- **Negative**: Requires familiarity with target addresses (`./pants run ...`) and lockfile regeneration (`./pants generate-lockfiles`).

---

## ADR-002: Confluent Schema Registry & PySpark ABRiS Py4J Deserialization

### Status
`Accepted` (PR [#13](https://github.com/JayKay24/data-engineering/pull/13))

### Context
Schemaless JSON over Kafka leads to silent null-column regressions when upstream event producers evolve fields.

### Decision
Enforce an explicit Avro contract (`ecommerce_event.avsc`) backed by **Confluent Schema Registry (`:8081`)** and deserialize binary Confluent Avro payloads inside PySpark using **ABRiS (`za.co.absa:abris_2.12:6.4.0`)** via the Py4J gateway (`get_abris_from_avro_column`).

### Alternatives Considered
- _Raw JSON + Spark `from_json`_: No centralized schema registry validation or wire-format evolution guarantees.

### Consequences
- **Positive**: Strong producer-side schema rejection, compact Avro wire payloads, dynamic reader schema resolution in Spark.

---

## ADR-003: Shared Platform Utilities & Infrastructure in `projects/common`

### Status
`Accepted` (PR [#10](https://github.com/JayKay24/data-engineering/pull/10), [#14](https://github.com/JayKay24/data-engineering/pull/14))

### Context
Multiple pipelines required identical structured logging, Kafka Avro producer/ABRiS helpers, and local Zookeeper/Kafka/Schema Registry Compose definitions.

### Decision
Consolidate shared logging (`logger.py`), Kafka Avro/ABRiS adapters (`kafka_avro.py`), and shared Kafka Compose services (`projects/common/docker/docker-compose.yml`) under `projects/common/`.

### Consequences
- **Positive**: Eliminated duplicated boilerplate and port collisions across modules.

---

## ADR-004: Delta Lake ACID Sinks for Multi-Metric Structured Streaming

### Status
`Accepted` (PR [#6](https://github.com/JayKay24/data-engineering/pull/6), [#14](https://github.com/JayKay24/data-engineering/pull/14))

### Context
Writing raw streaming JSON/Parquet files directly without transaction logs causes partial reads when batch consumers scan directories mid-commit.

### Decision
Standardize speed-layer persistence on **Delta Lake (`3.2.0`)** (`sinks.py`) with isolated per-sink Spark checkpoint directories (`checkpoints/delta_<name>`), while preserving a `--sink console` adapter for local debugging.

### Consequences
- **Positive**: ACID transaction logs (`_delta_log`), exactly-once checkpoint recovery, and Parquet-native compatibility for DuckDB.

---

## ADR-005: E-Commerce Lambda Architecture with DuckDB & `dbt-duckdb`

### Status
`Accepted` (PR [#7](https://github.com/JayKay24/data-engineering/pull/7), [#15](https://github.com/JayKay24/data-engineering/pull/15))

### Context
Serving executive dashboards directly from PySpark incurs high JVM startup latency and memory overhead for ad-hoc analytical SQL.

### Decision
Adopt a **Lambda Architecture** pairing PySpark Structured Streaming (Speed Layer) with **DuckDB + `dbt-duckdb`** (Batch Layer) to materialize Gold analytical tables inside `ecommerce.duckdb` for sub-second BI queries.

### Consequences
- **Positive**: Zero-infrastructure analytical warehouse, modular SQL transformations, and native Parquet scanning without an external warehouse cluster.

---

## ADR-006: Dynamic Parquet Resolution via `get_stream_path` dbt Macro

### Status
`Accepted` (PR [#15](https://github.com/JayKay24/data-engineering/pull/15))

### Context
Hardcoding `/opt/project/output_data/...` inside SQL staging views prevented running dbt across both containerized Airflow (`output_data` mounted at `/opt/project/output_data`) and local host runs.

### Decision
Centralize Parquet source resolution in `batch_layer/macros/get_stream_path.sql` reading `env_var("CLICK_STREAM_OUTPUT_PREFIX", "/opt/project/output_data")`.

### Consequences
- **Positive**: Identical SQL models run seamlessly inside `batch_layer` Docker, Airflow containers, and local `.venv` CLI executions.

---

## ADR-007: Event-Driven Airflow Orchestration with `DeltaStreamSensor`

### Status
`Accepted` (PR [#15](https://github.com/JayKay24/data-engineering/pull/15))

### Context
Running `dbt build` on a fixed cron schedule before Spark commits Delta/Parquet outputs fails with missing file errors on cold starts.

### Decision
Implement a custom `DeltaStreamSensor` (`orchestration/dags/ecommerce_pipeline.py`) that loads sink names dynamically from `ecommerce_config.yml` and polls `output_data/` in `reschedule` mode until at least 3 sinks have committed data.

### Consequences
- **Positive**: Resilient cold-start behavior without holding Airflow worker slots idle.

---

## ADR-008: Thread-Safe PostgreSQL Connection Pooling & OLTP Dimension Sync

### Status
`Accepted` (PR [#10](https://github.com/JayKay24/data-engineering/pull/10), [#24](https://github.com/JayKay24/data-engineering/pull/24))

### Context
Joining streaming clickstream purchases against customer segments and product brands requires OLTP master data without coupling PySpark streaming tasks to synchronous JDBC lookups.

### Decision
Leverage the PostgreSQL 16 service in `orchestration/docker-compose.yml` via `oltp_sync/` (`ThreadedConnectionPool`, idempotent `ON CONFLICT DO UPDATE` upserts, and PyArrow Parquet export of `dim_customers` and `dim_products`), joining dimensions cleanly in dbt Gold marts (`customer_segment_spending`, `brand_category_performance`).

### Consequences
- **Positive**: Decoupled OLTP/OLAP boundary, resilient `--seed-from-defaults-on-offline` local fallback, and enriched BI marts.

---

## ADR-009: Dual-Engine Data Quality Governance (PyDeequ + Great Expectations)

### Status
`Accepted` (PR [#11](https://github.com/JayKay24/data-engineering/pull/11), [#12](https://github.com/JayKay24/data-engineering/pull/12), [#24](https://github.com/JayKay24/data-engineering/pull/24))

### Context
No single data quality library natively spans distributed Spark DataFrames and DuckDB/Pandas Gold marts without serialization overhead.

### Decision
Use **Amazon PyDeequ** inside `streaming_layer/quality_checks.py` for Spark-level batch verification and **Great Expectations** inside `quality/validate_ecommerce.py` for raw JSON feeds and Gold DuckDB marts, persisting both JSON and flat Parquet reports surfaced through dbt monitor views in Apache Superset.

### Consequences
- **Positive**: Zero-copy Spark validation at ingress and comprehensive Gold warehouse assertions prior to optional BigQuery export.

---

## ADR-010: Lock-Resilient Read-Only DuckDB Snapshot Fallback

### Status
`Accepted` (PR [#24](https://github.com/JayKay24/data-engineering/pull/24))

### Context
DuckDB raises `IOException: Could not set lock` even on `read_only=True` connections when an external tool (such as DBeaver) holds an active lock on `ecommerce.duckdb`.

### Decision
Introduce `projects/ecommerce/duckdb_utils.py` (`connect_duckdb_readonly`) which falls back to copying `ecommerce.duckdb` (and `.wal`) into a `tempfile.TemporaryDirectory()` whenever a lock conflict occurs.

### Consequences
- **Positive**: `quality:validate` and `cloud_sync:export` never fail due to concurrent developer GUI inspection.

---

## ADR-011: Lockfile-Driven Docker Builds & Isolated `/opt/pipeline-venv`

### Status
`Accepted` (PR [#24](https://github.com/JayKay24/data-engineering/pull/24))

### Context
Hardcoded `pip install` lists across three Dockerfiles drifted (`duckdb==1.1.0` vs `1.1.3`), while installing modern `user_reqs.lock` versions directly into `apache/airflow:2.9.3-python3.10` conflicts with Airflow 2.9.3 constraints.

### Decision
Enable `pants.backend.docker`, introduce `3rdparty/extract_lock_constraints.py`, pin all three Dockerfiles directly to `3rdparty/user_reqs.lock` (plus matching DuckDB CLI release binary), and isolate Airflow's task runtime inside `/opt/pipeline-venv`.

### Consequences
- **Positive**: 100% version parity across local `.venv`, Pants PEX binaries, and Linux Docker containers with zero Airflow dependency conflicts.

---

## ADR-012: Monorepo Consolidation into `projects/ecommerce` & Local Agent Review

### Status
`Accepted` (PR [#24](https://github.com/JayKay24/data-engineering/pull/24), [#25](https://github.com/JayKay24/data-engineering/pull/25))

### Context
Standalone exploratory sub-projects (`essentials`, `ingestion`, `transformation`, `storage`, `quality`) duplicated patterns already unified in the e-commerce platform and held unused dependencies (`openpyxl`, or libraries isolated from `ecommerce`). Meanwhile, CI-based AI PR review (`scripts/ai_pr_reviewer.py`) added external CI coupling.

### Decision
Consolidate the platform under top-level `projects/ecommerce/`, fold all retained capabilities (`pydeequ`, `great-expectations`, `psycopg2-binary`, GCP SDKs) into the e-commerce architecture, remove retired sub-projects and `ai_pr_reviewer.py`, and standardize `agents.md` on local self-review, direct commits, and PRs targeting `develop`.

### Consequences
- **Positive**: 100% dependency utilization in `3rdparty/requirements.txt`, cleaner paths (`projects/ecommerce`), and unified end-to-end architecture.

---

## ADR-013: Migrating Executive BI from Streamlit to Apache Superset (Dashboards-as-Code)

### Status
`Accepted`

### Context
Streamlit + Plotly served well for early prototyping, but lacked production BI capabilities (SQL Lab, RBAC, semantic virtual datasets, cross-filtering, and declarative multi-user caching) and required custom Python UI code for every chart. Meanwhile, open-core BI tools such as Metabase gate Git serialization and granular security behind paid licenses.

### Decision
Replace `orchestration/viz/` (Streamlit + Plotly) with **Apache Superset (`6.1.0`)** backed by the shared PostgreSQL 16 metadata store (`superset` database), **Redis 7** query/filter caching, a read-only `duckdb-engine` connection pinned to Pants's locked `duckdb` version, flat Parquet dbt monitor views (`gx_quality_checks`, `pydeequ_quality_checks`), and Git-versioned **Dashboards-as-Code** YAML assets validated by `package_assets.py` and auto-imported via `bootstrap.sh`.

### Alternatives Considered
- _Metabase OSS_: Simpler GUI onboarding, but lacks free Git-based dashboard serialization and relies on a third-party JDBC DuckDB driver.
- _Retain Streamlit_: Poor fit for production self-serve BI and ad-hoc SQL exploration.

### Consequences
- **Positive**: 100% free Apache 2.0 BI suite with SQL Lab, Redis caching, version-controlled YAML dashboards, and exact DuckDB lockfile parity.
- **Negative**: Slightly higher container memory footprint (Superset + Redis) compared to a single Streamlit script, mitigated by scripted `bootstrap.sh` provisioning.
