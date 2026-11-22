"""Atomic Write-Audit-Publish (WAP) lifecycle helper for DuckDB warehouses."""

import argparse
import os
import shutil
import sys

import duckdb

from projects.common.logger import get_logger
from projects.ecommerce.duckdb_utils import connect_duckdb_readonly

logger = get_logger("EcommerceWapDuckDB")


def prepare_staging_db(serving_path: str, staging_path: str) -> None:
    """Initialize a fresh WAP staging DuckDB file from the current serving database.

    Copies the existing serving database (when present) into ``staging_path`` so
    dbt incremental marts and SCD Type 2 snapshots preserve prior state while
    keeping ``serving_path`` completely isolated from write locks and unvalidated
    changes.

    Args:
        serving_path: Path to the live read-only serving DuckDB database.
        staging_path: Path to the isolated staging DuckDB database for writes.
    """
    staging_dir = os.path.dirname(os.path.abspath(staging_path))
    os.makedirs(staging_dir, exist_ok=True)

    for stale in (staging_path, f"{staging_path}.wal"):
        if os.path.exists(stale):
            os.remove(stale)

    if os.path.exists(serving_path):
        # Copy safely even if a reader holds a handle, flushing via snapshot copy if needed.
        with connect_duckdb_readonly(serving_path):
            pass
        shutil.copy2(serving_path, staging_path)
        wal_path = f"{serving_path}.wal"
        if os.path.exists(wal_path):
            shutil.copy2(wal_path, f"{staging_path}.wal")
        logger.info(
            "Prepared WAP staging DuckDB at %s from serving DB %s.",
            staging_path,
            serving_path,
        )
    else:
        logger.info(
            "Serving DuckDB not found at %s; starting fresh staging DB at %s.",
            serving_path,
            staging_path,
        )


def publish_staging_db(staging_path: str, serving_path: str) -> None:
    """Checkpoint and atomically promote a validated staging DuckDB to serving.

    Flushes DuckDB's write-ahead log into the main file via ``CHECKPOINT``,
    copies the checkpointed database to a same-directory temporary file, and
    performs an atomic ``os.replace`` onto ``serving_path``.

    Args:
        staging_path: Path to the validated WAP staging DuckDB database.
        serving_path: Destination serving DuckDB path mounted by BI/readers.

    Raises:
        FileNotFoundError: If ``staging_path`` does not exist.
    """
    if not os.path.exists(staging_path):
        raise FileNotFoundError(
            f"Cannot publish WAP DuckDB: staging file not found at {staging_path}"
        )

    serving_dir = os.path.dirname(os.path.abspath(serving_path))
    os.makedirs(serving_dir, exist_ok=True)

    # Flush any WAL changes directly into the staging file so the promoted DB is self-contained.
    with duckdb.connect(staging_path, read_only=False) as conn:
        conn.execute("CHECKPOINT;")

    tmp_path = os.path.join(
        serving_dir, f".{os.path.basename(serving_path)}.tmp.{os.getpid()}"
    )
    try:
        shutil.copy2(staging_path, tmp_path)
        serving_wal = f"{serving_path}.wal"
        if os.path.exists(serving_wal):
            os.remove(serving_wal)
        os.replace(tmp_path, serving_path)
    finally:
        if os.path.exists(tmp_path):
            os.remove(tmp_path)

    logger.info(
        "Atomically published validated staging DuckDB %s -> %s.",
        staging_path,
        serving_path,
    )


if __name__ == "__main__":
    default_serving = os.getenv(
        "DUCKDB_SERVING_PATH",
        os.getenv(
            "DUCKDB_PATH", "projects/ecommerce/batch_layer/data/ecommerce.duckdb"
        ),
    )
    default_staging = os.getenv(
        "DUCKDB_STAGING_PATH",
        "projects/ecommerce/batch_layer/data/ecommerce_staging.duckdb",
    )

    parser = argparse.ArgumentParser(
        description="Prepare or atomically publish a WAP DuckDB warehouse."
    )
    subparsers = parser.add_subparsers(dest="action", required=True)

    for action_name in ("prepare", "publish"):
        sub = subparsers.add_parser(action_name)
        sub.add_argument("--serving-path", default=default_serving)
        sub.add_argument("--staging-path", default=default_staging)

    args = parser.parse_args()
    try:
        if args.action == "prepare":
            prepare_staging_db(args.serving_path, args.staging_path)
        else:
            publish_staging_db(args.staging_path, args.serving_path)
    except Exception as exc:
        logger.error("WAP DuckDB %s failed: %s", args.action, exc, exc_info=True)
        sys.exit(1)
