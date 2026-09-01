FROM ghcr.io/dbt-labs/dbt-duckdb:1.8.0

WORKDIR /usr/app

RUN apt-get update && \
    apt-get install -y --no-install-recommends wget unzip curl && \
    wget -q https://github.com/duckdb/duckdb/releases/download/v1.1.0/duckdb_cli-linux-amd64.zip && \
    unzip duckdb_cli-linux-amd64.zip -d /usr/local/bin && \
    rm duckdb_cli-linux-amd64.zip && \
    apt-get clean && rm -rf /var/lib/apt/lists/*

COPY . /usr/app/

ENV DBT_PROFILES_DIR=/usr/app

ENTRYPOINT ["dbt"]
CMD ["build"]
