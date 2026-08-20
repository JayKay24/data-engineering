FROM python:3.10-slim

# Install system utilities, curl, unzip, and git
RUN apt-get update && apt-get install -y --no-install-recommends \
    curl \
    unzip \
    git \
    build-essential \
    && rm -rf /var/lib/apt/lists/*

# Install DuckDB CLI binary
RUN ARCH=$(uname -m) && \
    if [ "$ARCH" = "x86_64" ]; then DUCKDB_ARCH="amd64"; \
    elif [ "$ARCH" = "aarch64" ] || [ "$ARCH" = "arm64" ]; then DUCKDB_ARCH="aarch64"; \
    else DUCKDB_ARCH="amd64"; fi && \
    curl -LO "https://github.com/duckdb/duckdb/releases/download/v1.1.3/duckdb_cli-linux-${DUCKDB_ARCH}.zip" && \
    unzip "duckdb_cli-linux-${DUCKDB_ARCH}.zip" -d /usr/local/bin && \
    rm "duckdb_cli-linux-${DUCKDB_ARCH}.zip" && \
    chmod +x /usr/local/bin/duckdb

# Install dbt with DuckDB adapter
RUN pip install --no-cache-dir \
    dbt-core~=1.8.0 \
    dbt-duckdb~=1.8.0

# Set workspace directory and dbt profiles path
WORKDIR /usr/app
ENV DBT_PROFILES_DIR=/usr/app

ENTRYPOINT ["dbt"]
CMD ["run"]
