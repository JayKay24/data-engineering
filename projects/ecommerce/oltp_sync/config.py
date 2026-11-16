from contextlib import contextmanager
import os
import threading
from typing import Generator, TypedDict

from psycopg2.extensions import connection as PgConnection
from psycopg2.pool import ThreadedConnectionPool


class PostgresConfig(TypedDict):
    host: str
    port: int
    dbname: str
    user: str
    password: str


_pool_lock = threading.Lock()
_connection_pool: ThreadedConnectionPool | None = None


def get_postgres_config() -> PostgresConfig:
    """Retrieves PostgreSQL connection parameters from environment variables."""
    return {
        "host": os.getenv("POSTGRES_HOST", "localhost"),
        "port": int(os.getenv("POSTGRES_PORT", "5432")),
        "dbname": os.getenv("POSTGRES_DB", "airflow"),
        "user": os.getenv("POSTGRES_USER", "airflow"),
        "password": os.getenv("POSTGRES_PASSWORD", "airflow"),
    }


def get_pool(minconn: int = 1, maxconn: int = 5) -> ThreadedConnectionPool:
    """Initializes and returns a thread-safe ThreadedConnectionPool singleton."""
    global _connection_pool
    if _connection_pool is None:
        with _pool_lock:
            if _connection_pool is None:
                config = get_postgres_config()
                _connection_pool = ThreadedConnectionPool(minconn, maxconn, **config)
    return _connection_pool


@contextmanager
def get_connection() -> Generator[PgConnection, None, None]:
    """Context manager that borrows a connection, commits on success, rolls back on error, and returns it."""
    pool = get_pool()
    conn = pool.getconn()
    try:
        yield conn
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        pool.putconn(conn)
