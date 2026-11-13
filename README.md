# Data Engineering Monorepo

<p align="center">
  <img src="https://img.shields.io/badge/Pants-2.22-294576?style=for-the-badge&logo=pants&logoColor=white" alt="Pants" />
  <img src="https://img.shields.io/badge/Python-3.10.9-3776AB?style=for-the-badge&logo=python&logoColor=white" alt="Python" />
  <img src="https://img.shields.io/badge/Apache_Spark-3.5-E25A1C?style=for-the-badge&logo=apachespark&logoColor=white" alt="Apache Spark" />
  <img src="https://img.shields.io/badge/Delta_Lake-3.2-003366?style=for-the-badge&logo=delta&logoColor=white" alt="Delta Lake" />
  <img src="https://img.shields.io/badge/Apache_Kafka-2.14-231F20?style=for-the-badge&logo=apachekafka&logoColor=white" alt="Apache Kafka" />
  <img src="https://img.shields.io/badge/Schema_Registry-7.2-005571?style=for-the-badge&logo=confluent&logoColor=white" alt="Schema Registry" />
  <img src="https://img.shields.io/badge/PostgreSQL-16-4169E1?style=for-the-badge&logo=postgresql&logoColor=white" alt="PostgreSQL" />
  <img src="https://img.shields.io/badge/Google_Cloud-4285F4?style=for-the-badge&logo=googlecloud&logoColor=white" alt="Google Cloud" />
  <img src="https://img.shields.io/badge/Great_Expectations-1.x-FF671F?style=for-the-badge&logo=greatexpectations&logoColor=white" alt="Great Expectations" />
  <img src="https://img.shields.io/badge/PyDeequ-1.x-FF9900?style=for-the-badge&logo=amazonaws&logoColor=white" alt="PyDeequ" />
  <img src="https://img.shields.io/badge/dbt-1.x-FF694B?style=for-the-badge&logo=dbt&logoColor=white" alt="dbt" />
  <img src="https://img.shields.io/badge/DuckDB-1.5-FFF000?style=for-the-badge&logo=duckdb&logoColor=black" alt="DuckDB" />
  <img src="https://img.shields.io/badge/Apache_Airflow-2.9-017CEE?style=for-the-badge&logo=apacheairflow&logoColor=white" alt="Apache Airflow" />
  <img src="https://img.shields.io/badge/Streamlit-1.x-FF4B4B?style=for-the-badge&logo=streamlit&logoColor=white" alt="Streamlit" />
  <img src="https://img.shields.io/badge/Plotly-7.x-3F4F75?style=for-the-badge&logo=plotly&logoColor=white" alt="Plotly" />
  <img src="https://img.shields.io/badge/Docker-2496ED?style=for-the-badge&logo=docker&logoColor=white" alt="Docker" />
  <img src="https://img.shields.io/badge/Ruff-D7FF64?style=for-the-badge&logo=ruff&logoColor=black" alt="Ruff" />
</p>

This repository is a Python Data Engineering monorepo consolidated around a unified **E-Commerce Real-Time Lambda Architecture** platform (`projects/ecommerce`) and shared infrastructure utilities (`projects/common`).

I manage this monorepo using **Pantsbuild (Pants)**, targeting **Python 3.10.9**, **Ruff** for linting and formatting, and a single shared lockfile (`3rdparty/user_reqs.lock`) that drives both local PEX targets and Docker container builds.

---

## 🛠️ Tech Stack & Tooling

*   **Build System:** [Pantsbuild](https://www.pantsbuild.org/) (Fast, hermetic caching, Python + Docker backends)
*   **Target Language:** Python 3.10.9 (configured via `.python-version` and Pyenv)
*   **Format & Lint:** [Ruff](https://github.com/astral-sh/ruff) (unified linter and formatter)
*   **Dependency Management:** Single shared lockfile (`3rdparty/user_reqs.lock`) pinned across local PEX binaries and Docker images via `3rdparty/extract_lock_constraints.py`
*   **Git Hooks:** [pre-commit](https://pre-commit.com/) (runs `./pants fmt` and `./pants lint` locally)

---

## 📁 Repository Structure

```text
data-engineering/
├── .github/
│   └── pull_request_template.md       # Template for standardizing PR descriptions
├── .gitignore                         # Python, Pants, dbt, Spark, and OS ignore rules
├── .pre-commit-config.yaml            # Configures the local pre-commit hook
├── .python-version                    # Sets project-local Python to 3.10.9
├── pants                              # Pants launcher binary (scie-pants)
├── pants.toml                         # Main configuration for Pants (Python, Docker, Ruff)
├── .venv                              # Local symlink pointing to the Pants-generated virtualenv
├── 3rdparty/
│   ├── BUILD                          # Global dependency & lockfile targets
│   ├── requirements.txt               # Project Python requirements
│   ├── user_reqs.lock                 # Pants generated dependency lockfile
│   └── extract_lock_constraints.py    # Extracts exact lockfile pins for Docker builds
└── projects/
    ├── common/                        # Shared utilities (structured logging, Kafka Avro/ABRiS helpers, Kafka Docker stack)
    └── ecommerce/                     # Unified E-Commerce Real-Time Lambda Architecture
        ├── streaming_layer/           # PySpark Structured Streaming, Avro, PyDeequ & Delta Lake sinks
        ├── oltp_sync/                 # PostgreSQL OLTP dimension sync & Parquet export (psycopg2 + pyarrow)
        ├── batch_layer/               # dbt + DuckDB SQL staging, OLTP-enriched marts & monitors
        ├── quality/                   # Great Expectations batch & Gold warehouse validation suites
        ├── cloud_sync/                # Optional Google Cloud Storage & BigQuery lakehouse export
        └── orchestration/             # Apache Airflow DAG + Streamlit/Plotly executive dashboard
```

---

## 📁 Projects

*   [projects/common/](projects/common/) — Shared monorepo utilities including unified structured logging, Kafka/Avro/ABRiS helpers, and the shared Kafka/Schema Registry Docker Compose stack.
*   [projects/ecommerce/](projects/ecommerce/) — End-to-end E-Commerce Lambda Architecture combining Kafka/Avro streaming, PySpark + Delta Lake aggregations, PyDeequ Spark verification, PostgreSQL OLTP dimension sync, dbt + DuckDB analytical marts, Great Expectations quality gates, optional GCP export, and containerized Airflow + Streamlit orchestration (see [projects/ecommerce/README.md](projects/ecommerce/README.md)).

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
| **Lint** | `./pants lint ::` | Runs Ruff linter on all directories |
| **Format check** | `./pants fmt --check ::` | Checks formatting without rewriting files |
| **Format** | `./pants fmt ::` | Runs Ruff formatter to auto-fix styling issues |
| **Package** | `./pants package ::` | Packages all PEX binaries and Docker image targets |
| **Update Lockfile**| `./pants generate-lockfiles` | Re-compiles dependencies in `3rdparty/requirements.txt` |
| **Inspect Graph** | `./pants peek ::` | Lists metadata and inferred dependencies for all targets |

---

## 🙏 Acknowledgements

This workspace and the pipelines within are inspired by the book [*Hello, Modern Data Pipelines*](https://a.co/d/09CDz27y) by Raj Kishore Singh.
