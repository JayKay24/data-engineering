# Data Engineering Monorepo

<p align="center">
  <img src="https://img.shields.io/badge/Pants-2.22-294576?style=for-the-badge&logo=pants&logoColor=white" alt="Pants" />
  <img src="https://img.shields.io/badge/Python-3.10.9-3776AB?style=for-the-badge&logo=python&logoColor=white" alt="Python" />
  <img src="https://img.shields.io/badge/Apache_Spark-3.5-E25A1C?style=for-the-badge&logo=apachespark&logoColor=white" alt="Apache Spark" />
  <img src="https://img.shields.io/badge/Delta_Lake-3.2-003366?style=for-the-badge&logo=delta&logoColor=white" alt="Delta Lake" />
  <img src="https://img.shields.io/badge/Redpanda-24.2-E63B2E?style=for-the-badge&logo=redpanda&logoColor=white" alt="Redpanda" />
  <img src="https://img.shields.io/badge/Schema_Registry-Avro-005571?style=for-the-badge&logo=confluent&logoColor=white" alt="Schema Registry" />
  <img src="https://img.shields.io/badge/PostgreSQL-16-4169E1?style=for-the-badge&logo=postgresql&logoColor=white" alt="PostgreSQL" />
  <img src="https://img.shields.io/badge/Google_Cloud-4285F4?style=for-the-badge&logo=googlecloud&logoColor=white" alt="Google Cloud" />
  <img src="https://img.shields.io/badge/Terraform-IaC-844FBA?style=for-the-badge&logo=terraform&logoColor=white" alt="Terraform" />
  <img src="https://img.shields.io/badge/Great_Expectations-1.x-FF671F?style=for-the-badge&logo=greatexpectations&logoColor=white" alt="Great Expectations" />
  <img src="https://img.shields.io/badge/PyDeequ-1.x-FF9900?style=for-the-badge&logo=amazonaws&logoColor=white" alt="PyDeequ" />
  <img src="https://img.shields.io/badge/dbt-1.12-FF694B?style=for-the-badge&logo=dbt&logoColor=white" alt="dbt" />
  <img src="https://img.shields.io/badge/DuckDB-1.5-FFF000?style=for-the-badge&logo=duckdb&logoColor=black" alt="DuckDB" />
  <img src="https://img.shields.io/badge/Apache_Airflow-2.9-017CEE?style=for-the-badge&logo=apacheairflow&logoColor=white" alt="Apache Airflow" />
  <img src="https://img.shields.io/badge/Astronomer_Cosmos-1.15-5E5CE6?style=for-the-badge&logo=apacheairflow&logoColor=white" alt="Astronomer Cosmos" />
  <img src="https://img.shields.io/badge/Apache_Superset-6.1-20A7C9?style=for-the-badge&logo=apachesuperset&logoColor=white" alt="Apache Superset" />
  <img src="https://img.shields.io/badge/Redis-7-DC382D?style=for-the-badge&logo=redis&logoColor=white" alt="Redis" />
  <img src="https://img.shields.io/badge/Pytest-CI-0A9EDC?style=for-the-badge&logo=pytest&logoColor=white" alt="Pytest" />
  <img src="https://img.shields.io/badge/Docker-2496ED?style=for-the-badge&logo=docker&logoColor=white" alt="Docker" />
  <img src="https://img.shields.io/badge/Ruff-D7FF64?style=for-the-badge&logo=ruff&logoColor=black" alt="Ruff" />
</p>

This repository is a Python Data Engineering monorepo consolidated around a unified **E-Commerce Medallion Lakehouse & Real-Time Streaming Platform** (`projects/ecommerce`) and shared infrastructure utilities (`projects/common`).

I manage this monorepo using **Pantsbuild (Pants)**, targeting **Python 3.10.9**, **Ruff** for linting and formatting, **pytest** for automated verification, **Terraform** for GCP IaC, and a single shared lockfile (`3rdparty/user_reqs.lock`) that drives local PEX targets, tests, and Docker container builds.

---

## 🛠️ Tech Stack & Tooling

*   **Build System:** [Pantsbuild](https://www.pantsbuild.org/) (Fast, hermetic caching, Python, Pytest, Docker, and Ruff backends)
*   **Target Language:** Python 3.10.9 (configured via `.python-version` and Pyenv)
*   **Format & Lint:** [Ruff](https://github.com/astral-sh/ruff) (unified linter and formatter)
*   **Dependency Management:** Single shared lockfile (`3rdparty/user_reqs.lock`) pinned across local PEX binaries and Docker images via `3rdparty/extract_lock_constraints.py`
*   **CI & Infrastructure:** GitHub Actions (`.github/workflows/ci.yml`), Terraform / OpenTofu (`infra/terraform/`), and [pre-commit](https://pre-commit.com/)

---

## 📁 Repository Structure

```text
data-engineering/
├── .github/
│   ├── pull_request_template.md       # Template for standardizing PR descriptions
│   └── workflows/
│       ├── ci.yml                     # Automated Ruff, Pants pytest, dbt unit tests, Superset, Wiki & Terraform CI
│       └── wiki-sync.yml              # Validates docs/wiki/ on PRs & mirrors to GitHub Wiki
├── .gitignore                         # Python, Pants, dbt, Spark, Terraform, and OS ignore rules
├── .pre-commit-config.yaml            # Configures the local pre-commit hook
├── .python-version                    # Sets project-local Python to 3.10.9
├── pants                              # Pants launcher binary (scie-pants)
├── pants.toml                         # Main configuration for Pants (Python, Pytest, Docker, Ruff)
├── .venv                              # Local symlink pointing to the Pants-generated virtualenv
├── 3rdparty/
│   ├── BUILD                          # Global dependency & lockfile targets
│   ├── requirements.txt               # Project Python requirements
│   ├── constraints.txt                # Transitive dependency constraints
│   ├── user_reqs.lock                 # Pants generated dependency lockfile
│   └── extract_lock_constraints.py    # Extracts exact lockfile pins for Docker builds
├── infra/
│   └── terraform/                     # Free-tier GCP IaC (7-day GCS lifecycle, partitioned BigQuery, least-privilege IAM)
├── docs/
│   ├── BUILD                          # Pants targets for Wiki validation
│   ├── validate_wiki.py               # Link, GFM anchor, and Mermaid fence validator
│   └── wiki/                          # Canonical source for the GitHub Wiki
└── projects/
    ├── common/                        # Shared utilities (structured logging, Kafka Avro/ABRiS helpers, Redpanda Docker stack)
    └── ecommerce/                     # Unified E-Commerce Medallion Lakehouse Platform
        ├── streaming_layer/           # Single-pass PySpark foreachBatch Silver/DLQ, PyDeequ, tumbling KPIs & Redpanda producer
        ├── oltp_sync/                 # Zero-Pandas PostgreSQL OLTP dimension sync & ZSTD PyArrow Parquet export
        ├── batch_layer/               # dbt + DuckDB delta_scan() staging, SCD2 snapshots, incremental contracted marts & unit tests
        ├── quality/                   # Great Expectations live Silver Delta, quarantine ratio & Gold mart gates
        ├── cloud_sync/                # Optional compacted GCS Parquet & FinOps-partitioned BigQuery export
        ├── orchestration/             # Airflow 2.9 + Astronomer Cosmos DAG, atomic WAP DuckDB, Superset 6.1 BI & Compose prod override
        ├── tests/                     # Hermetic pytest suite (PySpark, DuckDB delta_scan, WAP, OLTP, GX, Cosmos DAG, Superset)
        └── wap_duckdb.py              # Atomic Write-Audit-Publish (WAP) DuckDB staging & promotion CLI
```

---

## 📁 Projects & Documentation

*   **[GitHub Wiki](https://github.com/JayKay24/data-engineering/wiki)** — Comprehensive engineering Wiki (`docs/wiki/`), automatically validated on PRs and synced to GitHub Wiki via `.github/workflows/wiki-sync.yml`.
*   [projects/common/](projects/common/) — Shared monorepo utilities including unified structured logging, Kafka/Avro/ABRiS helpers, and the lightweight single-binary **Redpanda** (`:9092` Kafka + `:8081` Schema Registry) Docker Compose definition (included by `projects/ecommerce/orchestration/docker-compose.yml`).
*   [projects/ecommerce/](projects/ecommerce/) — End-to-end E-Commerce Medallion Lakehouse combining Redpanda/Avro streaming, single-consumer PySpark `foreachBatch` Silver/DLQ + PyDeequ checks, tumbling-window Delta KPIs, zero-Pandas PostgreSQL OLTP sync, dbt + DuckDB `delta_scan()` with SCD Type 2 snapshots, incremental contracted marts & unit tests, Great Expectations validation, atomic Write-Audit-Publish (WAP) DuckDB promotion, FinOps BigQuery export, and Airflow + Astronomer Cosmos + Apache Superset BI (see [projects/ecommerce/README.md](projects/ecommerce/README.md)).

---

## 🚀 Getting Started

### 1. Prerequisites
Ensure you have Pyenv installed and Python `3.10.9` available on your system:
```bash
pyenv install 3.10.9
```

### 2. Set Up the Virtual Environment
Pants manages its own virtual environments, and exports one symlinked to `.venv` for IDE integration:
```bash
# Export the Pants-managed environment
./pants export --resolve=shared-lock

# Activate it in your terminal
source .venv/bin/activate
```

### 3. Set Up Pre-commit Hooks
The project uses `pre-commit` to automatically run Pants formatters and linters on staged files:
```bash
pre-commit install
```

---

## ⚡ CLI Cheatsheet

| Goal | Command | Description |
| :--- | :--- | :--- |
| **Lint & format check** | `./pants lint ::` | Runs Ruff linter and format check on all directories |
| **Format** | `./pants fmt ::` | Runs Ruff formatter to auto-fix styling issues |
| **Test** | `./pants test ::` | Runs the Pytest unit & integration suite (PySpark, DuckDB, GX, Cosmos) |
| **Package** | `./pants package ::` | Packages all PEX binaries and Docker image targets |
| **Validate Wiki** | `./pants run docs:validate_wiki` | Validates `docs/wiki/` links, GFM anchors & fences |
| **Update Lockfile**| `./pants generate-lockfiles` | Re-compiles dependencies in `3rdparty/requirements.txt` |
| **Inspect Graph** | `./pants peek ::` | Lists metadata and inferred dependencies for all targets |

---

## 🙏 Acknowledgements

This workspace and the pipelines within are inspired by the book [*Hello, Modern Data Pipelines*](https://a.co/d/09CDz27y) by Raj Kishore Singh.
