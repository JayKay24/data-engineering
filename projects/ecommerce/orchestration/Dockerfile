FROM apache/airflow:2.9.3-python3.10

USER root
RUN apt-get update && \
    apt-get install -y --no-install-recommends gcc python3-dev && \
    apt-get clean && rm -rf /var/lib/apt/lists/*

# Copy Pants lockfile and constraint extractor from monorepo root
COPY 3rdparty/user_reqs.lock 3rdparty/extract_lock_constraints.py /tmp/

# Build isolated pipeline venv pinned to Pants user_reqs.lock so task runtimes
# (dbt-duckdb, duckdb, psycopg2, great-expectations, GCP SDKs) never conflict
# with Airflow 2.9.3 core constraints.
RUN python3 -m venv /opt/pipeline-venv && \
    python3 /tmp/extract_lock_constraints.py --lockfile /tmp/user_reqs.lock --all-constraints > /tmp/pants-constraints.txt && \
    /opt/pipeline-venv/bin/pip install --no-cache-dir --upgrade pip && \
    /opt/pipeline-venv/bin/pip install --no-cache-dir -c /tmp/pants-constraints.txt \
        dbt-core \
        dbt-duckdb \
        duckdb \
        psycopg2-binary \
        pandas \
        pyarrow \
        great-expectations \
        google-cloud-storage \
        google-cloud-bigquery \
        db-dtypes \
        pyyaml && \
    rm -f /tmp/user_reqs.lock /tmp/extract_lock_constraints.py /tmp/pants-constraints.txt

USER airflow
ARG AIRFLOW_VERSION=2.9.3
ARG PYTHON_VERSION=3.10
RUN pip install --no-cache-dir \
    --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-${AIRFLOW_VERSION}/constraints-${PYTHON_VERSION}.txt" \
    apache-airflow-providers-postgres
