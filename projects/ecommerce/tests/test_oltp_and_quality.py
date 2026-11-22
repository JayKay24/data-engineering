"""Unit tests for zero-Pandas OLTP Parquet export and live Great Expectations checks."""

import json
import os
from pathlib import Path

import duckdb
import pyarrow as pa
import pyarrow.parquet as pq

from projects.ecommerce.oltp_sync.sync_dimensions import sync_dimensions
from projects.ecommerce.quality.validate_ecommerce import validate_ecommerce_pipeline


def _write_minimal_delta_table(table_dir: Path, arrow_table: pa.Table) -> None:
    """Create a valid single-commit Delta Lake table from a PyArrow table."""
    log_dir = table_dir / "_delta_log"
    log_dir.mkdir(parents=True, exist_ok=True)
    pq_name = "part-00000.parquet"
    pq_path = table_dir / pq_name
    pq.write_table(arrow_table, str(pq_path))

    type_map = {
        pa.string(): "string",
        pa.float64(): "double",
    }
    fields = [
        {
            "name": field.name,
            "type": type_map.get(field.type, "string"),
            "nullable": True,
            "metadata": {},
        }
        for field in arrow_table.schema
    ]
    commit = [
        {"protocol": {"minReaderVersion": 1, "minWriterVersion": 2}},
        {
            "metaData": {
                "id": f"delta-{table_dir.name}",
                "format": {"provider": "parquet", "options": {}},
                "schemaString": json.dumps({"type": "struct", "fields": fields}),
                "partitionColumns": [],
                "configuration": {},
            }
        },
        {
            "add": {
                "path": pq_name,
                "partitionValues": {},
                "size": os.path.getsize(pq_path),
                "modificationTime": 1,
                "dataChange": True,
            }
        },
    ]
    with open(log_dir / "00000000000000000000.json", "w", encoding="utf-8") as f:
        for entry in commit:
            f.write(json.dumps(entry) + "\n")


def test_oltp_sync_offline_and_scd_mutation(tmp_path: Path, monkeypatch) -> None:
    """Verify OLTP offline fallback writes typed ZSTD Parquet and supports SCD simulation.

    Args:
        tmp_path: Pytest temporary directory fixture.
        monkeypatch: Pytest monkeypatch fixture.
    """
    # Force connection failure to trigger offline fallback deterministically.
    monkeypatch.setenv("POSTGRES_HOST", "127.0.0.1")
    monkeypatch.setenv("POSTGRES_PORT", "1")

    out_dir = str(tmp_path / "output_data")
    sync_dimensions(
        output_base_dir=out_dir,
        seed_from_defaults_on_offline=True,
        simulate_scd_change=True,
    )

    cust_table = pq.read_table(
        os.path.join(out_dir, "dim_customers", "dim_customers.parquet")
    )
    prod_table = pq.read_table(
        os.path.join(out_dir, "dim_products", "dim_products.parquet")
    )

    cust_segments = dict(
        zip(cust_table["user_id"].to_pylist(), cust_table["segment"].to_pylist())
    )
    prod_prices = dict(
        zip(prod_table["product_id"].to_pylist(), prod_table["unit_price"].to_pylist())
    )

    assert cust_segments["user_103"] == "Enterprise"
    assert prod_prices["prod_01"] == 1149.99


def test_validate_ecommerce_pipeline_against_live_silver_and_gold(
    tmp_path: Path,
) -> None:
    """Verify Great Expectations validates live Silver Delta events and Gold marts.

    Args:
        tmp_path: Pytest temporary directory fixture.
    """
    output_prefix = tmp_path / "output_data"
    silver_table = pa.table(
        {
            "user_id": ["user_101", "user_102"],
            "url": ["/products/laptop", "/checkout"],
            "event_type": ["view", "purchase"],
            "event_time": ["2026-09-01T10:00:00", "2026-09-01T10:00:05"],
            "price": [1200.0, 1200.0],
        }
    )
    _write_minimal_delta_table(output_prefix / "silver_ecommerce_events", silver_table)

    duckdb_path = str(tmp_path / "ecommerce_staging.duckdb")
    with duckdb.connect(duckdb_path) as conn:
        conn.execute(
            "CREATE TABLE daily_category_sales AS SELECT DATE '2026-09-01' AS sales_date, 'electronics' AS category, 1200.0 AS daily_revenue, 1 AS daily_units"
        )
        conn.execute(
            "CREATE TABLE daily_url_conversion AS SELECT DATE '2026-09-01' AS conversion_date, '/checkout' AS url, 10 AS total_views, 2 AS total_purchases, 0.2 AS avg_conversion_rate"
        )

    report_path = str(output_prefix / "quality_reports" / "gx_validation_results.json")
    ok = validate_ecommerce_pipeline(
        events_json_path=str(tmp_path / "nonexistent.json"),
        duckdb_path=duckdb_path,
        output_report_path=report_path,
        output_prefix=str(output_prefix),
    )

    assert ok is True
    assert os.path.exists(report_path)
    assert os.path.exists(output_prefix / "quality_reports" / "gx_checks.parquet")
    with open(report_path, "r", encoding="utf-8") as f:
        report = json.load(f)
    assert report["overall_success"] is True
    assert report["suites"][0]["suite_name"] == "ecommerce_silver_events_suite"
