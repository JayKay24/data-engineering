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
  <img src="https://img.shields.io/badge/Great_Expectations-1.2-FF671F?style=for-the-badge&logo=greatexpectations&logoColor=white" alt="Great Expectations" />
  <img src="https://img.shields.io/badge/dbt-1.8-FF694B?style=for-the-badge&logo=dbt&logoColor=white" alt="dbt" />
  <img src="https://img.shields.io/badge/DuckDB-1.1-FFF000?style=for-the-badge&logo=duckdb&logoColor=black" alt="DuckDB" />
  <img src="https://img.shields.io/badge/Apache_Airflow-2.9-017CEE?style=for-the-badge&logo=apacheairflow&logoColor=white" alt="Apache Airflow" />
  <img src="https://img.shields.io/badge/Streamlit-1.38-FF4B4B?style=for-the-badge&logo=streamlit&logoColor=white" alt="Streamlit" />
  <img src="https://img.shields.io/badge/Docker-2496ED?style=for-the-badge&logo=docker&logoColor=white" alt="Docker" />
  <img src="https://img.shields.io/badge/Ruff-D7FF64?style=for-the-badge&logo=ruff&logoColor=black" alt="Ruff" />
</p>

This repository is my monorepo containing my data-engineering projects and pipelines.

I manage this monorepo using **Pantsbuild (Pants)**, targeting **Python 3.10.x** and utilizing **Ruff** for high-performance linting and formatting.

---

## 🛠️ Tech Stack & Tooling

*   **Build System:** [Pantsbuild](https://www.pantsbuild.org/) (Fast, hermetic, caching, and polyglot-ready)
*   **Target Language:** Python 3.10.9 (configured via `.python-version` and Pyenv)
*   **Format & Lint:** [Ruff](https://github.com/astral-sh/ruff) (unified linter and formatter)
*   **Dependency Management:** Single shared lockfile (`3rdparty/user_reqs.lock`)
*   **Git Hooks:** [pre-commit](https://pre-commit.com/) (runs Pants formatting and linting locally)
*   **CI Reviewer:** Gemini API code reviewer via GitHub Actions and `google-generativeai`

---

## 📁 Repository Structure

```text
data-engineering/
├── .github/
│   ├── workflows/
│   │   └── ai-review.yml      # CI pipeline for automated AI code reviews
│   └── pull_request_template.md # Template for standardizing PR descriptions
├── .gitignore                 # Python, Pants, and OS ignore rules
├── .pre-commit-config.yaml    # Configures the local pre-commit hook
├── .python-version            # Sets project-local Python to 3.10.9
├── pants                      # Pants launcher binary (scie-pants)
├── pants.toml                 # Main configuration for Pants and tool backends
├── .venv                      # Local symlink pointing to the Pants-generated virtualenv
├── 3rdparty/
│   ├── BUILD                  # Configures global dependency targets
│   ├── requirements.txt       # Lists project requirements (pandas, PySpark, etc.)
│   └── user_reqs.lock         # Pants generated dependency lockfile
├── projects/                  # Directory containing all sub-projects
│   ├── common/                # Shared utilities (structured logging, Kafka Avro helpers)
│   ├── storage/               # Strategic relational & cloud data storage
│   │   ├── postgres/          # Dockerized PostgreSQL 16 transactional storage & CRUD
│   │   └── cloud/             # Google Cloud Storage & BigQuery analytical queries
│   ├── quality/               # Data quality, governance, and validation pipelines
│   │   ├── great_expectations/# Great Expectations Fluent API validation engine
│   │   └── pydeequ/           # Scalable Apache Spark data quality checks with Amazon Deequ
│   ├── transformation/        # Data transformation and lakehouse curation
│   │   ├── spark/             # PySpark and Delta Lake batch curation
│   │   └── dbt/               # dbt and DuckDB SQL transformation pipeline
│   ├── realtime/              # Real-time event stream aggregation and analytics
│   │   └── ecommerce/         # E-commerce Lambda Architecture: Streaming (PySpark/Delta) + Batch (dbt/DuckDB) + Orchestration (Airflow/Streamlit)
│   ├── ingestion/             # Real-time event streaming and ingestion pipeline
│   └── essentials/            # Core PySpark processing utilities
├── scripts/
│   ├── BUILD                  # Configures scripts targets for Pants
│   └── ai_pr_reviewer.py      # Python script that runs Gemini AI code reviews
└── tests/                     # Monorepo unit/integration test suites
```

---

## 📁 Projects

Each project under the `projects/` directory represents a distinct data platform capability:

*   [projects/common/](projects/common/) — Shared monorepo utilities including unified structured logging and shared Kafka/Avro/ABRiS tools.
*   [projects/realtime/ecommerce/](projects/realtime/ecommerce/) — E-commerce Lambda architecture with real-time PySpark streaming into Delta Lake, dbt DuckDB analytical batch marts, and containerized Airflow + Streamlit orchestration (see [projects/realtime/ecommerce/README.md](projects/realtime/ecommerce/README.md)).
*   [projects/quality/great_expectations/](projects/quality/great_expectations/) — Automated tabular data quality validation using Great Expectations Fluent API (see [projects/quality/great_expectations/README.md](projects/quality/great_expectations/README.md)).
*   [projects/quality/pydeequ/](projects/quality/pydeequ/) — Distributed data quality verification and metric auditing with Amazon Deequ and PySpark (see [projects/quality/pydeequ/README.md](projects/quality/pydeequ/README.md)).

*   [projects/storage/postgres/](projects/storage/postgres/) — Containerized PostgreSQL 16 transactional storage with automated DDL initialization and safe context-managed operations (see [projects/storage/postgres/README.md](projects/storage/postgres/README.md)).
*   [projects/storage/cloud/](projects/storage/cloud/) — Cloud lakehouse data access (GCS) and warehouse analytics (BigQuery) using standard GCP clients (see [projects/storage/cloud/README.md](projects/storage/cloud/README.md)).
*   [projects/essentials/](projects/essentials/) — Core data processing utilities and foundational batch pipelines (see [projects/essentials/README.md](projects/essentials/README.md)).
*   [projects/ingestion/](projects/ingestion/) — Real-time event ingestion using Kafka, Avro, Schema Registry, and Spark Structured Streaming with ABRiS (see [projects/ingestion/README.md](projects/ingestion/README.md)).
*   [projects/transformation/spark/](projects/transformation/spark/) — Batch data transformation, multi-table dimensional enrichment, and Delta Lake table curation (see [projects/transformation/spark/README.md](projects/transformation/spark/README.md)).
*   [projects/transformation/dbt/](projects/transformation/dbt/) — Modular SQL transformation, dimensional modeling, and testing with dbt and DuckDB in an isolated Docker environment (see [projects/transformation/dbt/README.md](projects/transformation/dbt/README.md)).

---

## 🚀 Getting Started

### 1. Prerequisites
Ensure you have Pyenv installed and Python `3.10.9` available on your system:
```bash
pyenv install 3.10.9
```

### 2. Set Up the Virtual Environment
Pants manages its own virtual environments, but I like to export one symlinked to `.venv` for editor integration (autocompletion, diagnostics, etc.):
```bash
# Export the Pants-managed environment
./pants export --resolve=shared-lock

# Activate it in your terminal
source .venv/bin/activate
```

### 3. Set Up Pre-commit Hooks
The project uses `pre-commit` to automatically run Pants formatters and linters on staged files. Run the following command inside the virtual environment to install the hooks:
```bash
pre-commit install
```

---

## ⚡ CLI Cheatsheet

I frequently run these commands from the root directory:

| Goal | Command | Description |
| :--- | :--- | :--- |
| **Lint** | `./pants lint ::` | Runs Ruff linter on all directories |
| **Format check** | `./pants fmt --check ::` | Checks formatting without rewriting files |
| **Format** | `./pants fmt ::` | Runs Ruff formatter to auto-fix styling issues |
| **Update Lockfile**| `./pants generate-lockfiles` | Re-compiles dependencies in `3rdparty/requirements.txt` |
| **Inspect Graph** | `./pants peek ::` | Lists metadata and inferred dependencies for all targets |

> 💡 *Note: The `::` symbol is a wildcard indicating "all directories recursively".*

---

## 📈 Adding a New Project

When I am ready to start coding a new project:

1.  **Create the project directory:**
    ```bash
    mkdir -p projects/my_project
    ```
2.  **Add a `BUILD` file:**
    Create `projects/my_project/BUILD` and define the target sources:
    ```python
    python_sources(
        name="lib",
    )
    ```
3.  **Manage Dependencies:**
    *   If I need a new external library (e.g., `pandas`), I add it to `3rdparty/requirements.txt`.
    *   Regenerate the lockfile: `./pants generate-lockfiles`
    *   Pants will **automatically infer** imports in my Python files—no need to manually declare dependencies in `BUILD` files!

---

## 🙏 Acknowledgements

This workspace and the pipelines within are inspired by the book [*Hello, Modern Data Pipelines*](https://a.co/d/09CDz27y) by Raj Kishore Singh.
