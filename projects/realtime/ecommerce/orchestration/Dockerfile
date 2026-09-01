FROM apache/airflow:2.9.3-python3.10

USER root
RUN apt-get update && \
    apt-get install -y --no-install-recommends gcc python3-dev && \
    apt-get clean && rm -rf /var/lib/apt/lists/*

USER airflow
ARG AIRFLOW_VERSION=2.9.3
ARG PYTHON_VERSION=3.10
RUN pip install --no-cache-dir \
    --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-${AIRFLOW_VERSION}/constraints-${PYTHON_VERSION}.txt" \
    duckdb \
    dbt-duckdb \
    apache-airflow-providers-postgres
