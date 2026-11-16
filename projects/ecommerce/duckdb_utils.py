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
    """Opens DuckDB read-only, falling back to a temp snapshot if another process holds a file lock."""
    try:
        with duckdb.connect(duckdb_path, read_only=True) as conn:
            yield conn
            return
    except duckdb.IOException as exc:
        if "Could not set lock" not in str(exc):
            raise

    with tempfile.TemporaryDirectory() as tmpdir:
        snapshot_path = os.path.join(tmpdir, os.path.basename(duckdb_path))
        shutil.copy2(duckdb_path, snapshot_path)
        wal_path = f"{duckdb_path}.wal"
        if os.path.exists(wal_path):
            shutil.copy2(wal_path, f"{snapshot_path}.wal")
        with duckdb.connect(snapshot_path, read_only=True) as conn:
            yield conn
