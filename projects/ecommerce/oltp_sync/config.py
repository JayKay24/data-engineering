"""Thread-safe PostgreSQL connection pool and configuration factory."""

from contextlib import contextmanager
import os
import threading
from typing import Generator, TypedDict

from psycopg2.extensions import connection as PgConnection
from psycopg2.pool import ThreadedConnectionPool


class PostgresConfig(TypedDict):
    """Typed dictionary holding PostgreSQL connection parameters."""

    host: str
    port: int
    dbname: str
    user: str
    password: str


_pool_lock = threading.Lock()
_connection_pool: ThreadedConnectionPool | None = None


def get_postgres_config() -> PostgresConfig:
    """Retrieve PostgreSQL connection parameters from environment variables.

    Returns:
        Dictionary of host, port, dbname, user, and password settings.
    """
    return {
        "host": os.getenv("POSTGRES_HOST", "localhost"),
        "port": int(os.getenv("POSTGRES_PORT", "5432")),
        "dbname": os.getenv("POSTGRES_DB", "airflow"),
        "user": os.getenv("POSTGRES_USER", "airflow"),
        "password": os.getenv("POSTGRES_PASSWORD", "airflow"),
    }


def get_pool(minconn: int = 1, maxconn: int = 5) -> ThreadedConnectionPool:
    """Initialize and return a thread-safe ``ThreadedConnectionPool`` singleton.

    Args:
        minconn: Minimum number of idle connections maintained in the pool.
        maxconn: Maximum number of concurrent connections allowed by the pool.

    Returns:
        Shared ``ThreadedConnectionPool`` instance.
    """
    global _connection_pool
    if _connection_pool is None:
        with _pool_lock:
            if _connection_pool is None:
                config = get_postgres_config()
                _connection_pool = ThreadedConnectionPool(minconn, maxconn, **config)
    return _connection_pool


@contextmanager
def get_connection() -> Generator[PgConnection, None, None]:
    """Borrow a PostgreSQL connection with automatic commit/rollback handling.

    Yields:
        Active ``psycopg2`` connection borrowed from the shared pool.

    Raises:
        Exception: Re-raises any exception raised inside the context after
            rolling back the transaction.
    """
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
