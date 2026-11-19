FROM apache/superset:6.1.0

USER root

# Copy Pants lockfile and constraint extractor from monorepo root
COPY 3rdparty/user_reqs.lock 3rdparty/extract_lock_constraints.py /tmp/

# Install DuckDB, duckdb-engine, and psycopg2-binary pinned to Pants lockfile (without
# overriding Superset's base SQLAlchemy 1.4.x) plus Redis client
RUN /app/.venv/bin/python /tmp/extract_lock_constraints.py --lockfile /tmp/user_reqs.lock --packages duckdb duckdb-engine psycopg2-binary > /tmp/superset-reqs.txt && \
    uv pip install --python /app/.venv/bin/python --no-cache \
        -r /tmp/superset-reqs.txt \
        redis && \
    rm -f /tmp/user_reqs.lock /tmp/extract_lock_constraints.py /tmp/superset-reqs.txt

# Copy Superset configuration, bootstrap script, and Dashboards-as-Code YAML assets
COPY projects/ecommerce/orchestration/superset /app/superset_config
RUN chmod +x /app/superset_config/bootstrap.sh

ENV SUPERSET_CONFIG_PATH=/app/superset_config/superset_config.py

USER superset
EXPOSE 8088

ENTRYPOINT ["/app/superset_config/bootstrap.sh"]
