# E-Commerce Real-Time Lambda Architecture & Orchestration

This project implements a complete **Lambda Architecture** for high-volume e-commerce intelligence, combining real-time Kafka/PySpark streaming, PostgreSQL OLTP dimensional enrichment, dbt + DuckDB batch curation, dual-layer data quality governance (PyDeequ + Great Expectations), optional GCP lakehouse sync, and automated Airflow orchestration with an interactive Streamlit dashboard.

```mermaid
flowchart TD
    subgraph OLTP [OLTP Dimension Source]
        PG[(PostgreSQL 16: dim_customers & dim_products)] -->|oltp_sync: Seed & Export| DIM[(Parquet Dimensions)]
    end

    subgraph Speed Layer [Streaming / Speed Layer]
        K[Kafka: ecommerce_events] -->|PySpark Structured Streaming + ABRiS| SS[Multi-Metric Window Aggregations]
        SS -->|PyDeequ VerificationSuite| DEEQU[Spark Quality Metrics]
        SS -->|Low-Latency Writes| DL[(Delta Lake Storage Sinks)]
    end

    subgraph Serving & Orchestration [Serving & Orchestration Layer]
        AF[Apache Airflow DAG: ecommerce_batch_refresh] --> SENSOR[DeltaStreamSensor: wait_for_stream_outputs]
        DL -.->|Polls for Delta commits| SENSOR
        SENSOR --> SYNC[sync_postgres_dimensions]
        SYNC --> DBT[dbt build & test]
        DBT --> GX[Great Expectations Quality Gate]
        GX --> GCP[Optional GCS & BigQuery Sync]
        DBT -->|Build Staging & Marts| DUCK[(DuckDB Gold Warehouse)]
        DUCK -->|Queries| VIZ[Streamlit Executive Dashboard :8501]
        GX -.->|Quality Report| VIZ
        DEEQU -.->|Quality Report| VIZ
    end

    subgraph Batch Layer [Batch Layer - dbt & DuckDB]
        DBT -.->|Executes| STG[Staging Views]
        DBT -.->|Executes| MARTS[Analytical & Enriched Marts]
        DL -.->|Source Parquet Data| STG
        DIM -.->|Dimension Parquet Data| STG
        STG --> MARTS
    end
```

---

## 📁 Project Architecture & Components

*   [projects/common/docker/docker-compose.yml](../common/docker/docker-compose.yml): Shared monorepo Zookeeper, Kafka, and Confluent Schema Registry (`:8081`) containers.

### 1. Speed Layer (`streaming_layer/`)
*   [config/ecommerce_config.yml](streaming_layer/config/ecommerce_config.yml): Configuration defining Kafka topics, Schema Registry URL, sliding windows (`10s` duration, `5s` slide), `10s` watermark, and Delta Lake sinks.
*   [schemas/ecommerce_event.avsc](streaming_layer/schemas/ecommerce_event.avsc): Avro schema contract supporting `user_id`, `url`, `event_type`, `product_id`, `category`, `price`, and device headers.
*   [aggregations.py](streaming_layer/aggregations.py): Real-time metrics computation (`url_counts`, `user_counts`, `url_conversion`, `category_sales`, `product_sales`, `cart_metrics`, `session_funnels`, `top_urls_per_user`).
*   [quality_checks.py](streaming_layer/quality_checks.py): Distributed Spark data quality checks with Amazon PyDeequ (`projects/ecommerce/streaming_layer:quality_checks`).
*   [sinks.py](streaming_layer/sinks.py): Modular streaming and batch writer adapters for Delta Lake and Console outputs.
*   [ecommerce_producer.py](streaming_layer/ecommerce_producer.py): Lightweight Avro event producer (`projects/ecommerce/streaming_layer:producer`).
*   [ecommerce_aggregation_job.py](streaming_layer/ecommerce_aggregation_job.py): PySpark streaming & batch orchestrator (`projects/ecommerce/streaming_layer:stream_job`).

### 2. OLTP Dimension Sync (`oltp_sync/`)
*   [config.py](oltp_sync/config.py): Thread-safe PostgreSQL connection pool (`psycopg2-binary`).
*   [sync_dimensions.py](oltp_sync/sync_dimensions.py): Seeds `dim_customers` and `dim_products` in PostgreSQL and exports Parquet dimension snapshots for dbt (`projects/ecommerce/oltp_sync:sync_dimensions`).

### 3. Batch Layer (`batch_layer/`)
*   [dbt_project.yml](batch_layer/dbt_project.yml) & [profiles.yml](batch_layer/profiles.yml): dbt configuration targeting DuckDB (`ecommerce.duckdb`).
*   [Dockerfile](batch_layer/Dockerfile) & [BUILD](batch_layer/BUILD): Lockfile-pinned dbt + DuckDB CLI container driven by `3rdparty/user_reqs.lock`.
*   [Makefile](batch_layer/Makefile): Shortcuts for dbt execution (`make dbt-build`, `make query`).
*   [macros/](batch_layer/macros/): Centralized SQL macros (`get_stream_path.sql` for dynamic Parquet source resolution).
*   [models/staging/](batch_layer/models/staging/): Staging views over streaming Delta/Parquet and OLTP dimension outputs with integrity tests.
*   [models/marts/](batch_layer/models/marts/): Analytical models (`cumulative_users`, `daily_category_sales`, `monthly_category_sales`, `daily_top_urls`, `daily_top_urls_per_user`, `daily_url_conversion`, `new_vs_returning_users`, `customer_segment_spending`, `brand_category_performance`).
*   [models/monitors/](batch_layer/models/monitors/): Freshness and latency monitor (`last_ingest.sql`).

### 4. Data Quality Gate (`quality/`)
*   [validate_ecommerce.py](quality/validate_ecommerce.py): Great Expectations validation suites for raw event JSON feeds and DuckDB Gold marts (`projects/ecommerce/quality:validate`).

### 5. Cloud Sync (`cloud_sync/`)
*   [config.py](cloud_sync/config.py) & [export_to_gcp.py](cloud_sync/export_to_gcp.py): Optional Google Cloud Storage Parquet export and BigQuery warehouse loader (`projects/ecommerce/cloud_sync:export`). Controlled via `GCP_SYNC_ENABLED=true`.

### 6. Orchestration & Serving Layer (`orchestration/`)
*   [docker-compose.yml](orchestration/docker-compose.yml): Airflow (LocalExecutor), PostgreSQL 16 backend, and Streamlit containers.
*   [Dockerfile](orchestration/Dockerfile) & [viz/Dockerfile](orchestration/viz/Dockerfile): Lockfile-pinned containers deriving package constraints from `3rdparty/user_reqs.lock`.
*   [Makefile](orchestration/Makefile): Cluster lifecycle commands (`make up`, `make down`, `make logs`).
*   [dags/ecommerce_pipeline.py](orchestration/dags/ecommerce_pipeline.py): Airflow DAG orchestrating `DeltaStreamSensor`, OLTP dimension sync, `dbt build` & `dbt test`, Great Expectations validation, and optional GCP sync.
*   [viz/app.py](orchestration/viz/app.py): Interactive Streamlit dashboard visualizer (`:8501`) featuring Category Sales, URL Conversion, User Growth, OLTP Customer & Brand Intelligence, and Data Quality & Pipeline Health tabs.

---

## 🚀 Quickstart & Execution

### 1. Start Shared Kafka Infrastructure
```bash
docker compose -f projects/common/docker/docker-compose.yml up -d
```

### 2. Stream E-Commerce Events to Kafka
```bash
./pants run projects/ecommerce/streaming_layer:producer
```

### 3. Run the Real-Time Streaming Aggregator
```bash
# Streaming mode (continuous execution with Delta Lake sinks)
./pants run projects/ecommerce/streaming_layer:stream_job -- --sink delta

# Batch mode (single-pass execution + PyDeequ quality checks)
./pants run projects/ecommerce/streaming_layer:stream_job -- --batch --sink delta
```

### 4. Run OLTP Sync, Quality Checks, or Cloud Sync via Pants
```bash
./pants run projects/ecommerce/oltp_sync:sync_dimensions -- --seed-from-defaults-on-offline
./pants run projects/ecommerce/quality:validate
./pants run projects/ecommerce/cloud_sync:export
```

### 5. Execute Batch Transformations via dbt (Optional Standalone)
```bash
cd projects/ecommerce/batch_layer
make build      # Builds the lockfile-pinned dbt container
make dbt-build  # Runs dbt models and tests against DuckDB
make query      # Interactive DuckDB CLI on data/ecommerce.duckdb
```

### 6. Launch Orchestration & Dashboard Cluster
> **Prerequisite:** Ensure `AIRFLOW_FERNET_KEY` is present in your local `.env` file (e.g. `AIRFLOW_FERNET_KEY=IOYiXlIcJ8QVhBT2iVyuKc2ehyX7OcBt-_f1EPDyNqM=`).

```bash
cd projects/ecommerce/orchestration
make build  # Builds lockfile-pinned Airflow & Streamlit container images
make up     # Starts Airflow, Postgres, and Streamlit in background
make status # Checks container health status
```
- **Airflow Webserver UI:** `http://localhost:8080` (`admin` / `admin`)
- **Streamlit Dashboard:** `http://localhost:8501`

To stop all orchestration services:
```bash
make down
```
