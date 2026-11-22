FROM apache/airflow:2.9.3-python3.10

USER root
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
        gcc \
        python3-dev \
        openjdk-17-jre-headless && \
    ln -sfn /usr/lib/jvm/java-17-openjdk-* /usr/lib/jvm/java-17-openjdk && \
    apt-get clean && rm -rf /var/lib/apt/lists/*

ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk
ENV SPARK_IVY_DIR=/opt/spark-ivy

# Copy Pants lockfile and constraint extractor from monorepo root
COPY 3rdparty/user_reqs.lock 3rdparty/extract_lock_constraints.py /tmp/

# Build isolated pipeline venv pinned to Pants user_reqs.lock so task runtimes
# (Spark, Delta Lake, PyDeequ, dbt-duckdb, GX, GCP SDKs) never conflict with
# Airflow 2.9.3 core constraints.
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
        pyspark \
        delta-spark \
        pydeequ \
        confluent-kafka[avro] \
        google-cloud-storage \
        google-cloud-bigquery \
        db-dtypes \
        pyyaml && \
    HOME=/home/airflow /opt/pipeline-venv/bin/python -c "import duckdb; duckdb.execute('INSTALL delta')" && \
    mkdir -p /opt/spark-ivy && \
    chown -R airflow:0 /home/airflow/.duckdb /opt/spark-ivy && \
    rm -f /tmp/user_reqs.lock /tmp/extract_lock_constraints.py /tmp/pants-constraints.txt

USER airflow
ARG AIRFLOW_VERSION=2.9.3
ARG PYTHON_VERSION=3.10
RUN pip install --no-cache-dir \
    --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-${AIRFLOW_VERSION}/constraints-${PYTHON_VERSION}.txt" \
    apache-airflow-providers-postgres && \
    pip install --no-cache-dir "astronomer-cosmos>=1.11.0,<1.16.0"
