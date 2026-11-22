"""Unit tests for atomic WAP DuckDB swaps and ACID Delta Lake delta_scan()."""

import json
import os
from pathlib import Path

import duckdb
import pyarrow as pa
import pyarrow.parquet as pq

from projects.ecommerce.duckdb_utils import connect_duckdb_readonly
from projects.ecommerce.wap_duckdb import prepare_staging_db, publish_staging_db


def test_wap_prepare_and_atomic_publish(tmp_path: Path) -> None:
    """Verify WAP staging writes remain isolated until atomic publish.

    Args:
        tmp_path: Pytest temporary directory fixture.
    """
    serving_db = str(tmp_path / "ecommerce.duckdb")
    staging_db = str(tmp_path / "ecommerce_staging.duckdb")

    # Seed initial serving DB
    with duckdb.connect(serving_db) as conn:
        conn.execute("CREATE TABLE sales AS SELECT 'electronics' AS cat, 100.0 AS rev")

    # Prepare staging DB and mutate staging only
    prepare_staging_db(serving_db, staging_db)
    with duckdb.connect(staging_db) as conn:
        conn.execute("INSERT INTO sales VALUES ('home', 50.0)")

    # Serving DB must still see only 1 row prior to publish
    with connect_duckdb_readonly(serving_db) as conn:
        assert conn.execute("SELECT COUNT(*) FROM sales").fetchone()[0] == 1

    # Atomic publish must flush WAL and update serving DB cleanly
    publish_staging_db(staging_db, serving_db)
    with connect_duckdb_readonly(serving_db) as conn:
        rows = conn.execute("SELECT SUM(rev) FROM sales").fetchone()[0]
        assert rows == 150.0


def test_delta_scan_ignores_tombstoned_parquet_files(tmp_path: Path) -> None:
    """Verify DuckDB delta_scan() excludes overwritten Delta Parquet files.

    Args:
        tmp_path: Pytest temporary directory fixture.
    """
    delta_dir = tmp_path / "category_sales"
    log_dir = delta_dir / "_delta_log"
    log_dir.mkdir(parents=True)

    pq_v0 = "part-00000-v0.parquet"
    pq_v1 = "part-00001-v1.parquet"

    table_v0 = pa.table({"category": ["electronics"], "revenue": [100.0]})
    table_v1 = pa.table({"category": ["electronics"], "revenue": [250.0]})
    pq.write_table(table_v0, str(delta_dir / pq_v0))
    pq.write_table(table_v1, str(delta_dir / pq_v1))

    schema_str = json.dumps(
        {
            "type": "struct",
            "fields": [
                {
                    "name": "category",
                    "type": "string",
                    "nullable": True,
                    "metadata": {},
                },
                {"name": "revenue", "type": "double", "nullable": True, "metadata": {}},
            ],
        }
    )

    commit_0 = [
        {"protocol": {"minReaderVersion": 1, "minWriterVersion": 2}},
        {
            "metaData": {
                "id": "test-delta-table",
                "format": {"provider": "parquet", "options": {}},
                "schemaString": schema_str,
                "partitionColumns": [],
                "configuration": {},
            }
        },
        {
            "add": {
                "path": pq_v0,
                "partitionValues": {},
                "size": os.path.getsize(delta_dir / pq_v0),
                "modificationTime": 1,
                "dataChange": True,
            }
        },
    ]
    commit_1 = [
        {"remove": {"path": pq_v0, "deletionTimestamp": 2, "dataChange": True}},
        {
            "add": {
                "path": pq_v1,
                "partitionValues": {},
                "size": os.path.getsize(delta_dir / pq_v1),
                "modificationTime": 2,
                "dataChange": True,
            }
        },
    ]

    with open(log_dir / "00000000000000000000.json", "w", encoding="utf-8") as f:
        for entry in commit_0:
            f.write(json.dumps(entry) + "\n")
    with open(log_dir / "00000000000000000001.json", "w", encoding="utf-8") as f:
        for entry in commit_1:
            f.write(json.dumps(entry) + "\n")

    with duckdb.connect() as conn:
        conn.execute("INSTALL delta; LOAD delta;")
        # Raw globbing (*.parquet) would return 350.0; delta_scan must return exact 250.0.
        delta_sum = conn.execute(
            "SELECT SUM(revenue) FROM delta_scan(?)", [str(delta_dir)]
        ).fetchone()[0]
        assert delta_sum == 250.0
