"""Shared DuckDB connection helpers with lock-safe read-only fallback."""

from contextlib import contextmanager
import os
import shutil
import tempfile
from typing import Generator

import duckdb


@contextmanager
def connect_duckdb_readonly(
    duckdb_path: str,
) -> Generator[duckdb.DuckDBPyConnection, None, None]:
    """Open a DuckDB database in read-only mode with a snapshot fallback.

    Attempts a direct read-only connection first. If another process holds an
    exclusive file lock on ``duckdb_path``, copies the database (and optional
    ``.wal`` file) to a temporary directory and opens the snapshot instead.

    Args:
        duckdb_path: Filesystem path to the DuckDB database file.

    Yields:
        Active read-only ``duckdb.DuckDBPyConnection`` instance.

    Raises:
        duckdb.IOException: If opening DuckDB fails for any reason other than
            a file lock conflict.
    """
    try:
        with duckdb.connect(duckdb_path, read_only=True) as conn:
            yield conn
            return
    except duckdb.IOException as exc:
        if "Could not set lock" not in str(exc):
            raise

    # DuckDB holds exclusive file locks during active writes; copy both the DB and WAL
    # into an ephemeral directory so concurrent readers (e.g. Superset/GX) see a consistent snapshot.
    with tempfile.TemporaryDirectory() as tmpdir:
        snapshot_path = os.path.join(tmpdir, os.path.basename(duckdb_path))
        shutil.copy2(duckdb_path, snapshot_path)
        wal_path = f"{duckdb_path}.wal"
        if os.path.exists(wal_path):
            shutil.copy2(wal_path, f"{snapshot_path}.wal")
        with duckdb.connect(snapshot_path, read_only=True) as conn:
            yield conn
