FROM python:3.10-slim

# Install system utilities, curl, unzip, and git
RUN apt-get update && apt-get install -y --no-install-recommends \
    curl \
    unzip \
    git \
    build-essential \
    && rm -rf /var/lib/apt/lists/*

# Copy Pants lockfile and constraint extractor from monorepo root
COPY 3rdparty/user_reqs.lock 3rdparty/extract_lock_constraints.py /tmp/

# Install DuckDB CLI binary matching the exact version locked in Pants
RUN DUCKDB_VER=$(python3 /tmp/extract_lock_constraints.py --lockfile /tmp/user_reqs.lock --get-version duckdb) && \
    ARCH=$(uname -m) && \
    if [ "$ARCH" = "x86_64" ]; then DUCKDB_ARCH="amd64"; \
    elif [ "$ARCH" = "aarch64" ] || [ "$ARCH" = "arm64" ]; then DUCKDB_ARCH="arm64"; \
    else DUCKDB_ARCH="amd64"; fi && \
    curl -fLO "https://github.com/duckdb/duckdb/releases/download/v${DUCKDB_VER}/duckdb_cli-linux-${DUCKDB_ARCH}.zip" && \
    unzip "duckdb_cli-linux-${DUCKDB_ARCH}.zip" -d /usr/local/bin && \
    rm "duckdb_cli-linux-${DUCKDB_ARCH}.zip" && \
    chmod +x /usr/local/bin/duckdb

# Install dbt-core and dbt-duckdb constrained by Pants user_reqs.lock
RUN python3 /tmp/extract_lock_constraints.py --lockfile /tmp/user_reqs.lock --all-constraints > /tmp/pants-constraints.txt && \
    pip install --no-cache-dir -c /tmp/pants-constraints.txt \
    dbt-core \
    dbt-duckdb \
    duckdb && \
    rm -f /tmp/user_reqs.lock /tmp/extract_lock_constraints.py /tmp/pants-constraints.txt

WORKDIR /usr/app
ENV DBT_PROFILES_DIR=/usr/app

ENTRYPOINT ["dbt"]
CMD ["build"]
