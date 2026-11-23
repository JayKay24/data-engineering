# Glossary

A plain-English reference for the acronyms, data engineering patterns, and platform concepts used across this repository's Wiki.

**Quick Jump:** [A](#a) · [B](#b) · [C](#c) · [D](#d) · [F](#f) · [G](#g) · [I](#i) · [J](#j) · [K](#k) · [L](#l) · [M](#m) · [O](#o) · [P](#p) · [R](#r) · [S](#s) · [T](#t) · [W](#w) · [Z](#z)

---

## A

### ABRiS
* **Stands for:** Avro Bridge for Spark (`za.co.absa:abris`)
* **Meaning:** An open-source library that lets Apache Spark deserialize binary Confluent-format Avro messages (each prefixed with a 5-byte Schema Registry magic byte + schema ID) directly into structured Spark SQL columns.
* **In this repo:** Used in `projects/common/kafka_avro.py` (`get_abris_from_avro_column`) and `streaming_layer/ecommerce_aggregation_job.py` to decode `ecommerce_events` from Redpanda. See [Architecture Overview](Architecture-Overview).

### ACID
* **Stands for:** Atomicity, Consistency, Isolation, Durability
* **Meaning:** Database transaction guarantees ensuring that a write either succeeds completely or fails cleanly without leaving partial/corrupt data, and that concurrent readers never see half-written state.
* **In this repo:** Provided by **Delta Lake** (`_delta_log` transaction log) for streaming tables and by atomic **`CHECKPOINT;` + `os.replace()`** swaps for DuckDB serving files. See [Architecture Decision Records](Architecture-Decision-Records).

### ADR
* **Stands for:** Architecture Decision Record
* **Meaning:** A short, numbered document capturing an important architectural choice, the context behind it, alternatives considered, and its trade-offs/consequences.
* **In this repo:** Maintained in [Architecture Decision Records](Architecture-Decision-Records) (`ADR-001` through `ADR-017`).

### Astronomer Cosmos
* **Category:** Orchestration Tooling
* **Meaning:** An open-source library that dynamically parses a `dbt` project (`manifest.json`) and renders each dbt model, snapshot, and test as an individual Airflow task node with full lineage in the Airflow UI.
* **In this repo:** Used in `projects/ecommerce/orchestration/dags/ecommerce_pipeline.py` (`DbtTaskGroup`) with `duckdb_write_pool` (`slots=1`) to serialize DuckDB writes. See [Architecture Overview](Architecture-Overview).

### availableNow
* **Category:** Spark Structured Streaming Trigger
* **Meaning:** A Spark trigger mode (`Trigger.availableNow()`) that processes all new Kafka/Redpanda offsets since the last checkpoint in bounded micro-batches and then exits cleanly, freeing JVM memory between scheduled runs.
* **In this repo:** Default mode (`--mode available-now`) for `streaming_layer/ecommerce_aggregation_job.py` and in-Airflow Spark runs (`SPARK_IN_AIRFLOW=true`). See [Platform Requirements](Platform-Requirements).

### Avro
* **Category:** Data Serialization Format
* **Meaning:** A compact, row-oriented binary serialization format with a JSON-defined schema, supporting strict schema evolution (backward/forward compatibility) when paired with a Schema Registry.
* **In this repo:** Defined in `projects/ecommerce/streaming_layer/schemas/ecommerce_event.avsc` and enforced on every event produced to Redpanda. See [Data Contracts and Governance](Data-Contracts-and-Governance).

---

## B

### BI
* **Stands for:** Business Intelligence
* **Meaning:** Dashboards, charts, and self-service SQL tools used by analysts and stakeholders to explore curated metrics and KPIs.
* **In this repo:** Served by **Apache Superset `6.1.0`** (`projects/ecommerce/orchestration/superset/`) backed by read-only DuckDB (`ecommerce.duckdb`) and Redis caching. See [Architecture Overview](Architecture-Overview).

---

## C

### CI
* **Stands for:** Continuous Integration
* **Meaning:** Automated checks triggered on pull requests and pushes to verify formatting, linting, unit tests, and infrastructure definitions before code is merged.
* **In this repo:** Configured in `.github/workflows/ci.yml` (Ruff lint/format, `./pants test ::`, dbt unit tests, Superset YAML validation, Wiki validation, and Terraform validation). See [Testing and Quality Gates](Testing-and-Quality-Gates).

---

## D

### DAG
* **Stands for:** Directed Acyclic Graph
* **Meaning:** A workflow graph where tasks have directional dependencies and no circular loops, ensuring a deterministic execution order.
* **In this repo:** Implemented as `ecommerce_batch_refresh` in `projects/ecommerce/orchestration/dags/ecommerce_pipeline.py` running every 15 minutes (`*/15 * * * *`). See [Architecture Overview](Architecture-Overview).

### dbt
* **Stands for:** data build tool
* **Meaning:** A SQL-first transformation framework that compiles modular `SELECT` queries into views, tables, incremental models, snapshots, and data tests.
* **In this repo:** Located in `projects/ecommerce/batch_layer/` using `dbt-duckdb` (`1.12.x`) to transform Delta Lake Silver/KPI tables and OLTP dimensions into Gold marts. See [Data Catalog and Lineage](Data-Catalog-and-Lineage).

### Delta Lake
* **Category:** Open Table Format
* **Meaning:** A storage layer on top of Parquet files that adds an ACID transaction log (`_delta_log/`), schema enforcement, and time travel. Reading via `delta_scan()` consults `_delta_log` so [tombstoned files](#tombstoned-files) from overwrites or compaction are never read.
* **In this repo:** Used for `silver_ecommerce_events`, `quarantine_events`, and all 8 streaming KPI tables under `projects/ecommerce/output_data/`, queried from dbt/DuckDB via `get_delta_path` (`delta_scan(...)`). See [Architecture Decision Records](Architecture-Decision-Records).

### DLQ
* **Stands for:** Dead-Letter Queue
* **Meaning:** A dedicated quarantine table or topic where malformed, schema-invalid, or domain-violating records are routed instead of crashing the pipeline or polluting downstream analytics.
* **In this repo:** Implemented as the `quarantine_events` Delta table (`projects/ecommerce/output_data/quarantine_events`), populated by `split_valid_and_quarantine_events` in `streaming_layer/aggregations.py` and surfaced in Superset via `quarantine_summary`. See [Data Contracts and Governance](Data-Contracts-and-Governance).

---

## F

### FinOps
* **Stands for:** Financial Operations (Cloud Cost Governance)
* **Meaning:** Practices and guardrails that keep cloud storage and query costs predictable and bounded (e.g., lifecycle expiration rules, partitioning/clustering, and query byte limits).
* **In this repo:** Enforced via a 7-day GCS lifecycle rule in `infra/terraform/`, `DAY`-partitioned and clustered BigQuery tables, and a `100 MB` `maximum_bytes_billed` guardrail in `cloud_sync/export_to_gcp.py`. See [Platform Requirements](Platform-Requirements).

### foreachBatch
* **Category:** Spark Structured Streaming API
* **Meaning:** A Spark sink hook that passes each micro-batch `DataFrame` to a custom Python function, allowing you to cache (`persist()`) the batch once and write to multiple outputs (e.g., Silver, DLQ, and KPIs) plus run quality checks in a single pass.
* **In this repo:** Used in `streaming_layer/sinks.py` (`write_silver_and_quarantine_microbatch`) to eliminate 8x redundant Kafka reads. See [Architecture Decision Records](Architecture-Decision-Records).

---

## G

### GCS
* **Stands for:** Google Cloud Storage
* **Meaning:** Google Cloud's object storage service, commonly used as a cloud data lake landing zone for Parquet snapshots.
* **In this repo:** Receives compacted ZSTD Gold Parquet snapshots (`ecommerce/gold/*.parquet`) from `cloud_sync/export_to_gcp.py` with a 7-day lifecycle rule managed in `infra/terraform/`.

### GX
* **Stands for:** Great Expectations
* **Meaning:** A Python data quality framework for defining declarative "Expectations" (assertions on row counts, nulls, value ranges, and ratios) against tables and DataFrames.
* **In this repo:** Implemented in `projects/ecommerce/quality/validate_ecommerce.py` to validate live `silver_ecommerce_events`, `quarantine_events` DLQ ratios, and staging Gold marts before [WAP](#wap) promotion. See [Testing and Quality Gates](Testing-and-Quality-Gates).

---

## I

### IaC
* **Stands for:** Infrastructure as Code
* **Meaning:** Managing cloud resources (buckets, datasets, tables, IAM roles) through version-controlled declarative configuration files rather than manual console clicks.
* **In this repo:** Defined in `infra/terraform/` (`main.tf`, `variables.tf`, `outputs.tf`) and validated in CI with `terraform fmt -check` and `terraform validate`.

### IAM
* **Stands for:** Identity and Access Management
* **Meaning:** Cloud permissions and service-account policies that enforce least-privilege access to storage and warehouse resources.
* **In this repo:** Configured in `infra/terraform/` for the GCP lakehouse sync service account.

### Incremental Model
* **Category:** dbt Materialization Strategy
* **Meaning:** A dbt table materialization that inserts or updates only recent/changed rows (e.g., via `delete+insert` on a date key) instead of rebuilding the entire table from scratch on every run.
* **In this repo:** Used in `projects/ecommerce/batch_layer/models/marts/` (`daily_category_sales`, `daily_url_conversion`, `daily_top_urls`, `daily_top_urls_per_user`). See [Data Catalog and Lineage](Data-Catalog-and-Lineage).

---

## J

### JSONL
* **Stands for:** JSON Lines (newline-delimited JSON)
* **Meaning:** A text format where each line is a standalone valid JSON object, ideal for streaming replay and log files.
* **In this repo:** Used by `projects/ecommerce/streaming_layer/input_data/ecommerce_events.json` for deterministic producer replay (`--mode file`).

### JVM
* **Stands for:** Java Virtual Machine
* **Meaning:** The runtime environment required by Java/Scala engines such as Apache Spark, Delta Lake, ABRiS, and Deequ.
* **In this repo:** Configured for **Java 17** (`JAVA_HOME`) for PySpark `3.5.8` compatibility.

---

## K

### KPI
* **Stands for:** Key Performance Indicator
* **Meaning:** A quantifiable business metric used to track performance (e.g., total revenue, units sold, conversion rate, cart abandonment).
* **In this repo:** Computed in `streaming_layer/aggregations.py` (8 Delta KPI tables) and exposed in dbt Gold marts and Apache Superset dashboards.

---

## L

### Lambda Architecture
* **Category:** Data Platform Pattern
* **Meaning:** A hybrid architecture combining a low-latency **Speed Layer** (streaming) with a durable **Batch Layer** (historical/dimensional joins) and a unified **Serving Layer** for queries.
* **In this repo:** Forms the foundation of `projects/ecommerce/` (Redpanda + Spark Speed Layer $\rightarrow$ dbt + DuckDB Batch Layer $\rightarrow$ DuckDB/Superset Serving Layer). See [Architecture Overview](Architecture-Overview).

---

## M

### Medallion Architecture
* **Category:** Lakehouse Data Organization Pattern
* **Meaning:** A multi-hop data refinement pattern: **Bronze** (raw ingested events), **Silver** (validated, typed, deduplicated records + DLQ), and **Gold** (business-ready dimensional marts and aggregates).
* **In this repo:** Raw Avro events in Redpanda (Bronze) $\rightarrow$ `silver_ecommerce_events` + `quarantine_events` (Silver) $\rightarrow$ dbt `models/marts/` in DuckDB & BigQuery (Gold). See [Architecture Overview](Architecture-Overview).

### Model Contract
* **Category:** dbt Governance Feature
* **Meaning:** A dbt configuration (`contract: {enforced: true}`) that validates exact column names and data types at build time, failing the run if upstream SQL drifts from the declared schema.
* **In this repo:** Enforced on Gold marts in `projects/ecommerce/batch_layer/models/marts/schema.yml`. See [Data Contracts and Governance](Data-Contracts-and-Governance).

---

## O

### OLAP
* **Stands for:** Online Analytical Processing
* **Meaning:** Systems optimized for fast analytical scans, aggregations, and joins over large columnar datasets (e.g., DuckDB, BigQuery).
* **In this repo:** Powered by **DuckDB** (`ecommerce.duckdb`) for local/Superset serving and **BigQuery** for optional cloud warehousing.

### OLTP
* **Stands for:** Online Transaction Processing
* **Meaning:** Row-oriented databases optimized for fast, concurrent operational reads/writes (e.g., customer profiles, product catalogs).
* **In this repo:** Simulated in **PostgreSQL 16** (`dim_customers`, `dim_products`) and exported via zero-Pandas PyArrow ZSTD Parquet in `projects/ecommerce/oltp_sync/sync_dimensions.py`.

---

## P

### PEX
* **Stands for:** Python Executable
* **Meaning:** A self-contained executable archive bundling Python code and its pinned dependencies, built hermetically by Pants.
* **In this repo:** Every `./pants run <target>` command (e.g., `streaming_layer:producer`, `quality:validate`, `projects/ecommerce:wap_duckdb`) builds and runs a hermetic PEX binary from `3rdparty/user_reqs.lock`.

### Py4J
* **Category:** Python–JVM Bridge
* **Meaning:** The bridge library used by PySpark (`spark._jvm`) to invoke Scala/Java classes such as ABRiS and Amazon Deequ from Python.
* **In this repo:** Used in `projects/common/kafka_avro.py` and `streaming_layer/quality_checks.py` (which cleanly shuts down `spark_session.sparkContext._gateway.shutdown_callback_server()` after PyDeequ checks).

### PyDeequ
* **Category:** Distributed Spark Data Quality Library
* **Meaning:** Python bindings for Amazon Deequ, running declarative completeness, uniqueness, and range checks directly inside Spark executors.
* **In this repo:** Executed inside `foreachBatch` in `streaming_layer/quality_checks.py`, writing results to `pydeequ_checks.parquet` for the `pydeequ_quality_checks` monitor table. See [Testing and Quality Gates](Testing-and-Quality-Gates).

---

## R

### RBAC
* **Stands for:** Role-Based Access Control
* **Meaning:** Restricting UI and API actions based on assigned user roles (e.g., Admin vs. Public read-only guest).
* **In this repo:** Enabled in Airflow (`AIRFLOW__WEBSERVER__RBAC=true`) and Superset (`SUPERSET_PUBLIC_READONLY=true` guest mode).

---

## S

### SCD Type 2
* **Also written as:** `SCD2` (Slowly Changing Dimension Type 2)
* **Meaning:** A dimensional modeling technique that preserves full historical attribute changes by adding a new row with `dbt_valid_from` and `dbt_valid_to` timestamps whenever a tracked column (e.g., customer segment or product price) changes.
* **In this repo:** Implemented in `projects/ecommerce/batch_layer/snapshots/snap_customers.sql` and `snap_products.sql`, joined point-in-time in Gold marts (`customer_segment_spending`, `brand_category_performance`). See [Data Catalog and Lineage](Data-Catalog-and-Lineage).

### Schema Registry
* **Also written as:** `SR`
* **Meaning:** A central HTTP service (`:8081`) that stores versioned Avro schemas and enforces compatibility rules between producers and consumers.
* **In this repo:** Provided by **Redpanda** (`projects/common/docker/docker-compose.yml`) on port `8081`.

### Session Window
* **Category:** Stream Windowing Strategy
* **Meaning:** A dynamic event-time window that groups a user's activity together until a gap of inactivity (e.g., 15 minutes / `900s`) occurs.
* **In this repo:** Used in `streaming_layer/aggregations.py` to compute `session_funnels` (`view -> add_to_cart -> purchase`).

---

## T

### Tombstoned Files
* **Category:** Delta Lake Storage Concept
* **Meaning:** Old Parquet files that have been logically deleted or replaced in `_delta_log` (e.g., after an `overwrite` or compaction) but still exist physically on disk until `VACUUM`. Globbing `*.parquet` reads them by mistake, causing duplicate/stale data; `delta_scan()` skips them.
* **In this repo:** Avoided by using `delta_scan()` (`get_delta_path`) in `batch_layer/macros/get_stream_path.sql` and `quality/validate_ecommerce.py`. See [Architecture Decision Records](Architecture-Decision-Records).

### Tumbling Window
* **Category:** Stream Windowing Strategy
* **Meaning:** Fixed-size, **non-overlapping** time buckets (e.g., `10 seconds` duration with `10 seconds` slide) so every event belongs to exactly one window, preventing double-counting.
* **In this repo:** Used in `streaming_layer/config/ecommerce_config.yml` and `aggregations.py` for exact revenue and click aggregations. See [Architecture Decision Records](Architecture-Decision-Records).

---

## W

### WAP
* **Stands for:** Write-Audit-Publish
* **Meaning:** A data engineering pattern where transformations **Write** to an isolated staging database/branch, **Audit** the staged data with automated quality gates, and atomically **Publish** (swap) it into production only if all checks pass—ensuring readers never see partial or invalid data.
* **In this repo:** Implemented in `projects/ecommerce/wap_duckdb.py` (`prepare` copies `ecommerce.duckdb` to `ecommerce_staging.duckdb`; dbt & GX run against staging; `publish` runs `CHECKPOINT;` + atomic `os.replace()`). See [Engineering Design Decisions](Engineering-Design-Decisions).

### Watermark
* **Category:** Stream Processing Concept
* **Meaning:** A moving event-time threshold (`event_time - delay`) telling Spark how long to wait for late-arriving events before finalizing a window and dropping older state.
* **In this repo:** Configured as `10 seconds` in `streaming_layer/config/ecommerce_config.yml`.

---

## Z

### ZSTD
* **Stands for:** Zstandard
* **Meaning:** A fast, high-ratio lossless compression algorithm developed by Meta, ideal for Parquet files.
* **In this repo:** Used for ZSTD-compressed Parquet exports in `oltp_sync/sync_dimensions.py` and `cloud_sync/export_to_gcp.py`.
