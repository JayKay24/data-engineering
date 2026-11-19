FROM apache/superset:6.1.0

USER root

# Copy Pants lockfile and constraint extractor from monorepo root
COPY 3rdparty/user_reqs.lock 3rdparty/extract_lock_constraints.py /tmp/

# Install DuckDB, duckdb-engine, psycopg2-binary (pinned to Pants lockfile) and Redis client
RUN /app/.venv/bin/python /tmp/extract_lock_constraints.py --lockfile /tmp/user_reqs.lock --all-constraints > /tmp/pants-constraints.txt && \
    uv pip install --python /app/.venv/bin/python --no-cache -c /tmp/pants-constraints.txt \
        duckdb \
        duckdb-engine \
        psycopg2-binary \
        redis && \
    rm -f /tmp/user_reqs.lock /tmp/extract_lock_constraints.py /tmp/pants-constraints.txt

# Copy Superset configuration, bootstrap script, and Dashboards-as-Code YAML assets
COPY projects/ecommerce/orchestration/superset /app/superset_config
RUN chmod +x /app/superset_config/bootstrap.sh

ENV SUPERSET_CONFIG_PATH=/app/superset_config/superset_config.py

USER superset
EXPOSE 8088

ENTRYPOINT ["/app/superset_config/bootstrap.sh"]
