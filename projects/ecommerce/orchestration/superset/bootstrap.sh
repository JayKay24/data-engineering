#!/usr/bin/env bash
set -euo pipefail

echo "Ensuring PostgreSQL metadata database '${SUPERSET_DB_NAME:-superset}' exists..."
/app/.venv/bin/python - <<'PY'
import os
import psycopg2
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT

db_name = os.environ.get("SUPERSET_DB_NAME", "superset")
conn = psycopg2.connect(
    host=os.environ.get("SUPERSET_DB_HOST", "postgres"),
    port=int(os.environ.get("SUPERSET_DB_PORT", "5432")),
    dbname="postgres",
    user=os.environ.get("SUPERSET_DB_USER", "airflow"),
    password=os.environ.get("SUPERSET_DB_PASSWORD", "airflow"),
)
conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
with conn.cursor() as cur:
    cur.execute("SELECT 1 FROM pg_database WHERE datname = %s;", (db_name,))
    if not cur.fetchone():
        cur.execute(f'CREATE DATABASE "{db_name}";')
        print(f"Created database '{db_name}'.")
    else:
        print(f"Database '{db_name}' already exists.")
conn.close()
PY

echo "Running Superset DB migrations..."
superset db upgrade

echo "Ensuring Superset admin user exists..."
superset fab create-admin \
    --username "${SUPERSET_ADMIN_USER:-admin}" \
    --firstname Admin \
    --lastname User \
    --email admin@example.com \
    --password "${SUPERSET_ADMIN_PASSWORD:-admin}" || true

echo "Initializing Superset roles and permissions..."
superset init

echo "Packaging and importing Dashboards-as-Code YAML assets..."
/app/.venv/bin/python /app/superset_config/package_assets.py \
    --assets-dir /app/superset_config/assets \
    --output /tmp/ecommerce_superset_assets.zip
superset import-dashboards \
    -p /tmp/ecommerce_superset_assets.zip \
    -u "${SUPERSET_ADMIN_USER:-admin}"
rm -f /tmp/ecommerce_superset_assets.zip

echo "Starting Apache Superset server on :8088..."
exec /app/docker/entrypoints/run-server.sh
