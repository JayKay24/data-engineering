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
| **ADR-014** | [Replacing ZooKeeper/Kafka/Schema Registry with Single-Binary Redpanda](#adr-014-replacing-zookeeperkafkaschema-registry-with-single-binary-redpanda) | `Accepted` | Cost / Broker | Production Medallion Modernization |
| **ADR-015** | [Single-Consumer `foreachBatch` Medallion Silver/DLQ, Live PyDeequ & Tumbling Windows](#adr-015-single-consumer-foreachbatch-medallion-silverdlq-live-pydeequ--tumbling-windows) | `Accepted` | Speed / Quality | Production Medallion Modernization |
| **ADR-016** | [DuckDB `delta_scan()`, Materialized Monitor Tables & Atomic Write-Audit-Publish (WAP)](#adr-016-duckdb-delta_scan-materialized-monitor-tables--atomic-write-audit-publish-wap) | `Accepted` | Warehouse / Reliability | Production Medallion Modernization |
| **ADR-017** | [SCD Type 2 Snapshots, Contracted Incremental Marts, Astronomer Cosmos & BigQuery FinOps](#adr-017-scd-type-2-snapshots-contracted-incremental-marts-astronomer-cosmos--bigquery-finops) | `Accepted` | Orchestration / Cloud | Production Medallion Modernization |

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

---

## ADR-014: Replacing ZooKeeper/Kafka/Schema Registry with Single-Binary Redpanda

### Status
`Accepted`

### Context
Running separate Confluent `cp-zookeeper`, `cp-kafka`, and `cp-schema-registry` JVM containers consumed ~2.5–3.0 GB RAM before ingesting a single event, making \$0–\$6/month cloud hosting impractical while relying on deprecated ZooKeeper metadata orchestration.

### Decision
Replace the 3-container Confluent stack in `projects/common/docker/docker-compose.yml` with **Redpanda (`v24.2.11`)** (`--smp=1 --memory=350M`), exposing 100% wire-compatible Kafka (`:9092` / `:19092`) and Schema Registry (`:8081`) endpoints without modifying producer (`confluent-kafka[avro]`) or Spark ABRiS consumer code.

### Consequences
- **Positive**: Cuts broker RAM footprint by ~85% (~2.5 GB $\rightarrow$ ~350 MB) and eliminates ZooKeeper altogether.
- **Negative**: Uses Redpanda rather than Apache Kafka KRaft JVM images; Kafka client protocol parity remains 100% preserved.

---

## ADR-015: Single-Consumer `foreachBatch` Medallion Silver/DLQ, Live PyDeequ & Tumbling Windows

### Status
`Accepted`

### Context
Launching 8 separate `writeStream` queries against Kafka caused 8x redundant deserialization passes, generated ~96 tiny files per minute, bypassed PyDeequ in streaming mode, and produced 2x inflated downstream revenue totals because `10s` windows with `5s` slides assigned every purchase event to two overlapping windows.

### Decision
1. Consolidate Kafka consumption into a single ABRiS stream (`--mode available-now` default for lean scheduled catch-up, plus `streaming` and `batch`) using `foreachBatch` with `batch_df.persist()`.
2. Execute live PyDeequ verification on every micro-batch and route domain violations (invalid `event_type`, negative `price`, missing required fields) to a `quarantine_events` Delta DLQ table while appending valid records to date-partitioned `silver_ecommerce_events`.
3. Roll up all 8 KPI Delta sinks in one cached pass over Silver using non-overlapping `10s` tumbling windows (`window_duration == slide_duration == "10 seconds"`).

### Consequences
- **Positive**: Eliminates 2x revenue double-counting, drops Kafka read amplification 8x $\rightarrow$ 1x, enables live streaming PyDeequ + quarantine observability, and frees JVM memory between Airflow `availableNow` runs.

---

## ADR-016: DuckDB `delta_scan()`, Materialized Monitor Tables & Atomic Write-Audit-Publish (WAP)

### Status
`Accepted`

### Context
Globbing `read_parquet('.../*.parquet')` over Delta Lake tables ignored `_delta_log`, reading tombstoned Parquet files left behind by overwrites or compaction. Simultaneously, writing directly to `ecommerce.duckdb` caused single-writer lock collisions with Apache Superset.

### Decision
1. Enable DuckDB's native `delta` extension in `profiles.yml` and switch Delta-backed dbt staging views to `delta_scan()` (`get_delta_path`), reserving `read_parquet()` (`get_parquet_path`) for Parquet dimensions and quality reports.
2. Materialize `models/monitors/` as `table`s instead of external-file `view`s so `ecommerce.duckdb` is self-contained.
3. Introduce `projects/ecommerce/wap_duckdb.py` (`prepare` / `publish`) so Airflow builds and validates against `ecommerce_staging.duckdb` before running `CHECKPOINT;` and atomically promoting via `os.replace()` to `ecommerce.duckdb` (paired with `pool_recycle: 30` and `pool_pre_ping: true` in Superset).

### Consequences
- **Positive**: Strictly ACID Delta reads, zero Superset lock contention, and unvalidated data never reaches BI dashboards.

---

## ADR-017: SCD Type 2 Snapshots, Contracted Incremental Marts, Astronomer Cosmos & BigQuery FinOps

### Status
`Accepted`

### Context
Full-refresh dbt tables, opaque Airflow `BashOperator` dbt invocations, Pandas-based OLTP/BigQuery copies, and unpartitioned BigQuery `WRITE_TRUNCATE` loads lacked historical dimension tracking, column-level lineage, and cloud FinOps guardrails.

### Decision
1. Refactor `oltp_sync/sync_dimensions.py` to build typed ZSTD Parquet directly via PyArrow without Pandas.
2. Add dbt SCD Type 2 snapshots (`snap_customers`, `snap_products`), incremental (`delete+insert`) time-series marts, enforced dbt Model Contracts (`contract: {enforced: true}`), and native dbt 1.12 `unit_tests.yml`.
3. Integrate **Astronomer Cosmos (`1.15.x`)** `DbtTaskGroup` serialized on `duckdb_write_pool` (`slots=1`) in `ecommerce_pipeline.py`.
4. Update `cloud_sync/export_to_gcp.py` to export 5 compacted ZSTD Gold Parquet files to GCS and load time-partitioned, clustered BigQuery tables with a `100 MB` `maximum_bytes_billed` guardrail, backed by Terraform IaC (`infra/terraform/`) and automated `./pants test ::` CI (`.github/workflows/ci.yml`).

### Consequences
- **Positive**: Point-in-time historical accuracy, task-level dbt observability in Airflow, minimal GCS Class A operations, bounded BigQuery scan costs, and full automated test coverage.
