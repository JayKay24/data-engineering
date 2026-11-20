"""Apache Superset configuration for the E-Commerce Real-Time Intelligence platform."""

import os

SECRET_KEY = os.environ.get(
    "SUPERSET_SECRET_KEY",
    "ecommerce-superset-dev-secret-key-change-me",
)

# Metadata database hosted in the shared PostgreSQL 16 service
_PG_USER = os.environ.get("SUPERSET_DB_USER", "airflow")
_PG_PASS = os.environ.get("SUPERSET_DB_PASSWORD", "airflow")
_PG_HOST = os.environ.get("SUPERSET_DB_HOST", "postgres")
_PG_PORT = os.environ.get("SUPERSET_DB_PORT", "5432")
_PG_DB = os.environ.get("SUPERSET_DB_NAME", "superset")

SQLALCHEMY_DATABASE_URI = (
    f"postgresql+psycopg2://{_PG_USER}:{_PG_PASS}@{_PG_HOST}:{_PG_PORT}/{_PG_DB}"
)

# Allow file-backed DuckDB connections in Superset
PREVENT_UNSAFE_DB_CONNECTIONS = False

# Redis caching for dashboard filters, chart queries, and SQL Lab state
_REDIS_HOST = os.environ.get("REDIS_HOST", "redis")
_REDIS_PORT = int(os.environ.get("REDIS_PORT", "6379"))

CACHE_CONFIG = {
    "CACHE_TYPE": "RedisCache",
    "CACHE_DEFAULT_TIMEOUT": 300,
    "CACHE_KEY_PREFIX": "superset_meta_",
    "CACHE_REDIS_HOST": _REDIS_HOST,
    "CACHE_REDIS_PORT": _REDIS_PORT,
    "CACHE_REDIS_DB": 0,
}

DATA_CACHE_CONFIG = {
    "CACHE_TYPE": "RedisCache",
    "CACHE_DEFAULT_TIMEOUT": 60,
    "CACHE_KEY_PREFIX": "superset_data_",
    "CACHE_REDIS_HOST": _REDIS_HOST,
    "CACHE_REDIS_PORT": _REDIS_PORT,
    "CACHE_REDIS_DB": 1,
}

FILTER_STATE_CACHE_CONFIG = {
    "CACHE_TYPE": "RedisCache",
    "CACHE_DEFAULT_TIMEOUT": 86400,
    "CACHE_KEY_PREFIX": "superset_filter_",
    "CACHE_REDIS_HOST": _REDIS_HOST,
    "CACHE_REDIS_PORT": _REDIS_PORT,
    "CACHE_REDIS_DB": 2,
}

EXPLORE_FORM_DATA_CACHE_CONFIG = {
    "CACHE_TYPE": "RedisCache",
    "CACHE_DEFAULT_TIMEOUT": 86400,
    "CACHE_KEY_PREFIX": "superset_explore_",
    "CACHE_REDIS_HOST": _REDIS_HOST,
    "CACHE_REDIS_PORT": _REDIS_PORT,
    "CACHE_REDIS_DB": 3,
}

FEATURE_FLAGS = {
    "DASHBOARD_CROSS_FILTERS": True,
    "EMBEDDABLE_CHARTS": True,
}
