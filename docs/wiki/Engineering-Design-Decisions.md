# Engineering Design Decisions & Edge Case Resolutions

## 1. Overview

Building a unified Lambda Architecture across PySpark, Delta Lake, DuckDB, PostgreSQL, Airflow, and Pants introduces subtle operational boundaries—particularly around streaming vs. batch APIs, embedded file-locked databases, cross-platform container builds, and native wheel loading. This document records the deliberate engineering decisions made to resolve these edge cases cleanly.

---

## 2. Decision Log: Distributed & Tooling Edge Cases

### Decision 1: Unbounded Spark Streaming vs. PyDeequ Batch Verification

- **The Ambiguity**: Amazon PyDeequ (`VerificationSuite`) computes column completeness, domain compliance, and value-range checks across Spark DataFrames. Should PyDeequ run inside the continuous streaming query or strictly on bounded inputs?
- **Engineering Impact**: PyDeequ internally invokes `DataFrame.rdd` and Spark accumulators/aggregations that throw `AnalysisException: Queries with streaming sources must be executed with writeStream.start()` if called directly on an unbounded streaming DataFrame. Running full-table verification on every 5-second micro-batch also adds high JVM overhead for redundant checks.
- **Implemented Resolution** (`streaming_layer/quality_checks.py`, `ecommerce_aggregation_job.py`):
  1. **Streaming Guard**: `run_pydeequ_verification` checks `if df.isStreaming:` and safely skips with a structured warning instead of crashing.
  2. **Batch & Standalone Execution**: PyDeequ executes automatically in `--batch` mode (and via the standalone `./pants run projects/ecommerce/streaming_layer:quality_checks` target), writing both `latest_report.json` and `pydeequ_checks.parquet` for dbt monitor views and Apache Superset observability.

---

### Decision 2: DuckDB Single-Writer File Locks vs. Concurrent Quality/Cloud Readers

- **The Ambiguity**: DuckDB enforces strict process-level file locking on `ecommerce.duckdb`. Even opening a `read_only=True` connection fails with `IOException: Could not set lock on file` if another process (such as DBeaver GUI, dbt, or another container) holds a conflicting lock.
- **Engineering Impact**: Local verification and scheduled Airflow quality/export tasks (`validate_ecommerce.py`, `export_to_gcp.py`) would crash intermittently whenever a developer inspects `ecommerce.duckdb` in DBeaver.
- **Implemented Resolution** (`projects/ecommerce/duckdb_utils.py`):
  - Extracted a shared `connect_duckdb_readonly(duckdb_path)` context manager:
    1. Attempts `duckdb.connect(duckdb_path, read_only=True)` directly (zero overhead in uncontended runs).
    2. If `duckdb.IOException` contains `"Could not set lock"`, copies `ecommerce.duckdb` (and `ecommerce.duckdb.wal` if present) into a `tempfile.TemporaryDirectory()` and opens a read-only connection against the isolated snapshot.

---

### Decision 3: Airflow 2.9.3 Base Image Constraints vs. Modern Pants Lockfile Packages

- **The Ambiguity**: `orchestration/Dockerfile` builds on `apache/airflow:2.9.3-python3.10`, whose official constraints file pins older packages (`pandas==2.1.4`, `pydantic==2.8.2`, `pyarrow==16.1.0`, `apache-airflow-providers-postgres==5.11.2`). Meanwhile, Pants's `3rdparty/user_reqs.lock` resolves modern versions (`duckdb==1.5.6`, `dbt-duckdb==1.11.1`, `pandas==2.3.3`, `pyarrow==25.0.1`, `great-expectations==1.25.0`).
- **Engineering Impact**: Installing `3rdparty/user_reqs.lock` versions directly into Airflow's system Python breaks Airflow 2.9.3 webserver/scheduler dependencies; conversely, downgrading the whole repo to Airflow 2.9.3 constraints breaks modern DuckDB, `duckdb-engine`, and dbt-duckdb compatibility.
- **Implemented Resolution** (`orchestration/Dockerfile`, `orchestration/dags/ecommerce_pipeline.py`):
  1. **System Python (`airflow` user)**: Installs only `apache-airflow-providers-postgres` under Airflow `2.9.3` official constraints so Airflow core stays rock-solid.
  2. **Isolated Pipeline Virtualenv (`/opt/pipeline-venv`)**: Creates a dedicated venv pinned 100% to `3rdparty/user_reqs.lock` (`dbt-core`, `dbt-duckdb`, `duckdb`, `psycopg2-binary`, `pandas`, `pyarrow`, `great-expectations`, `google-cloud-storage`, `google-cloud-bigquery`, `db-dtypes`, `pyyaml`).
  3. **Task Execution**: `BashOperator` tasks in `ecommerce_pipeline.py` automatically prefer `/opt/pipeline-venv/bin/python` and `/opt/pipeline-venv/bin/dbt` when inside the container, falling back to `python`/`dbt` locally.

---

### Decision 4: Cross-Platform Docker Builds on Apple Silicon vs. Linux Containers

- **The Ambiguity**: Developers run Pants on macOS (`darwin_arm64`), while Docker containers run Debian/Ubuntu Linux (`linux/arm64` or `linux/amd64`). Should Dockerfiles `COPY` host-built `.pex` binaries from `dist/` or install from `3rdparty/user_reqs.lock`?
- **Engineering Impact**: A `.pex` built on macOS contains macOS native wheels (`macosx_14_0_arm64.whl`) that fail to execute inside Linux containers unless static `complete_platforms` JSON files are generated and maintained for every container OS/arch. Additionally, older Dockerfiles hardcoded mismatched DuckDB versions (`1.1.0` vs `1.1.3`), causing DuckDB storage format drift.
- **Implemented Resolution** (`3rdparty/extract_lock_constraints.py`, all 3 Dockerfiles):
  - Created a zero-dependency stdlib helper (`3rdparty/extract_lock_constraints.py`) tracked by `3rdparty:lock_files`.
  - All three Dockerfiles (`batch_layer/Dockerfile`, `orchestration/Dockerfile`, `orchestration/superset/Dockerfile`) use the monorepo root as build context, generate `/tmp/pants-constraints.txt` directly from `3rdparty/user_reqs.lock`, and dynamically download the exact matching DuckDB CLI release binary (`v${DUCKDB_VER}` with `amd64`/`arm64` arch mapping). Both `./pants package` and `make build` work natively on any OS/arch with zero version drift.

---

### Decision 5: Transitive SciPy `1.15.x` Mach-O Loader Crash on macOS arm64

- **The Ambiguity**: `great-expectations` pulls `scipy>=1.6.0` transitively. When Pants resolved `scipy==1.15.3`, importing `great_expectations` on macOS (`arm64`) failed with `ImportError: dlopen(.../_spropack.cpython-310-darwin.so): section '__DATA/__thread_bss' has a zero-fill section type, but offset field is not zero`.
- **Engineering Impact**: `projects/ecommerce/quality:validate` could not run locally on macOS via `./pants run`.
- **Implemented Resolution** (`3rdparty/constraints.txt`, `pants.toml`):
  - Rather than polluting top-level `3rdparty/requirements.txt` with a transitive dependency, added `scipy<1.15.0` to `3rdparty/constraints.txt` and registered `[python.resolves_to_constraints_file] shared-lock = "3rdparty/constraints.txt"` in `pants.toml`. Regenerating `user_reqs.lock` pinned `scipy==1.14.1`, resolving the macOS loader crash cleanly.
