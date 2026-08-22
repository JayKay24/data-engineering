import os
from typing import Any
import psycopg2
from psycopg2.extensions import connection as PgConnection


def get_postgres_config() -> dict[str, Any]:
    """Retrieves PostgreSQL connection parameters from environment variables with sensible defaults."""
    return {
        "host": os.getenv("POSTGRES_HOST", "localhost"),
        "port": int(os.getenv("POSTGRES_PORT", "5432")),
        "dbname": os.getenv("POSTGRES_DB", "retail"),
        "user": os.getenv("POSTGRES_USER", "postgres"),
        "password": os.getenv("POSTGRES_PASSWORD", "mysecurepassword"),
    }


def get_connection() -> PgConnection:
    """Establishes and returns a psycopg2 database connection."""
    config = get_postgres_config()
    return psycopg2.connect(**config)
