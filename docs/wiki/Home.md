# Welcome to the Data Engineering Wiki

Welcome to the official engineering documentation and architecture wiki for **`JayKay24/data-engineering`**, a **Python Data Engineering Monorepo** managed by **Pantsbuild (v2.22.0)** and centered on a unified **E-Commerce Real-Time Lambda Architecture** (`projects/ecommerce`).

Modern e-commerce analytics require both **sub-minute operational visibility** (live conversion funnels, cart abandonment, and session behavior) and **curated dimensional warehousing** (customer segment spending, brand performance, and historical cohort retention). This platform unifies Kafka/Avro event streaming, PySpark Structured Streaming, Delta Lake ACID sinks, PostgreSQL 16 OLTP dimensional enrichment, dbt + DuckDB analytical modeling, dual-layer data quality governance (PyDeequ + Great Expectations), optional GCP lakehouse sync, and Apache Airflow + Apache Superset orchestration under a single hermetic lockfile (`3rdparty/user_reqs.lock`).

---

## 🧭 Wiki Navigation

Explore the Data Engineering knowledge base by section:

### 📋 Domain & Data Contracts

- **[Platform Requirements](Platform-Requirements)**: Complete functional and non-functional specification across all 6 e-commerce layers (`streaming_layer`, `oltp_sync`, `batch_layer`, `quality`, `cloud_sync`, and `orchestration`).
- **[Data Contracts & Governance](Data-Contracts-and-Governance)**: Deep dive into Confluent Avro Schema Registry contracts, event-time watermark & sliding/session window mathematics, KPI formulas, quality rules, and the 6 core operational verification scenarios.
- **[Engineering Design Decisions](Engineering-Design-Decisions)**: Detailed resolutions for complex distributed engineering edge cases, including DuckDB single-writer lock contention, Airflow 2.9.3 dependency isolation, offline OLTP resilience, and cross-platform Pants lockfile pinning.
- **[Requirements Traceability Matrix](Requirements-Traceability)**: End-to-end matrix mapping every platform requirement to its implementation status (`Implemented`), source modules, Pants targets, and automated verification checks, plus the next-horizon roadmap.
- **[Data Catalog & Lineage](Data-Catalog-and-Lineage)**: Complete living data dictionary and column-level lineage covering the Avro event contract, 8 Delta Lake streaming tables, 2 PostgreSQL OLTP dimension tables, 9 dbt staging views, and 9 Gold DuckDB marts.

### 🏗 Engineering & Architecture

- **[Architecture Overview](Architecture-Overview)**: System design, Pants monorepo target graph, shared `projects/common` utilities (logging and Py4J ABRiS bridge), PySpark + Delta Lake speed layer, dbt + DuckDB batch warehouse, and lockfile-pinned Docker containers.
- **[Testing & Quality Gates](Testing-and-Quality-Gates)**: The 4-tier verification pyramid spanning static Ruff/Pants checks, distributed PyDeequ Spark constraints, dbt schema integrity tests, and Great Expectations Gold mart validation suites.

### 📜 Decisions & History

- **[Architecture Decision Records (ADRs)](Architecture-Decision-Records)**: Catalog of 12 foundational Architecture Decision Records (`ADR-001` through `ADR-012`) documenting context, decisions, alternatives considered, consequences, and PR links.
- **[Project History & Milestones](Project-History)**: Chronological narrative of the monorepo's evolution across 7 phases and 25 pull requests, tracing how standalone data engineering sub-projects matured and amalgamated into `projects/ecommerce`.

---

## 🚀 Quick Reference Links

| Resource                      | Description                                             | Location / Endpoint                                                                                        |
| :---------------------------- | :------------------------------------------------------ | :--------------------------------------------------------------------------------------------------------- |
| **Source Repository**         | Main Pantsbuild monorepo codebase                       | [`JayKay24/data-engineering`](https://github.com/JayKay24/data-engineering)                                |
| **Local Setup & Quick Start** | Pyenv (`3.10.9`), Pants `.venv` export, and pre-commit  | [Repository README](https://github.com/JayKay24/data-engineering#readme)                                   |
| **E-Commerce Platform Docs**  | Lambda Architecture quickstart and execution guide      | [projects/ecommerce/README.md](https://github.com/JayKay24/data-engineering/blob/develop/projects/ecommerce/README.md) |
| **Kafka & Schema Registry**   | Shared Zookeeper (`:2181`), Kafka (`:9092`), SR (`:8081`) | [`projects/common/docker/docker-compose.yml`](https://github.com/JayKay24/data-engineering/blob/develop/projects/common/docker/docker-compose.yml) |
| **Airflow Webserver UI**      | Orchestration DAG (`ecommerce_batch_refresh`)           | `http://localhost:8080` (`admin` / `admin`)                                                                |
| **Apache Superset BI**        | Dashboards-as-Code BI, SQL Lab & Data Quality Monitor   | `http://localhost:8088` (`admin` / `admin`)                                                                |
| **Pull Requests**             | Feature and release PR history                          | [GitHub Pull Requests](https://github.com/JayKay24/data-engineering/pulls)                                 |
