"""Unit tests for synthetic producer, Superset assets, BigQuery FinOps, and Airflow Cosmos DAG."""

import os
from pathlib import Path
from unittest.mock import MagicMock

from google.cloud import bigquery

from projects.ecommerce.cloud_sync.export_to_gcp import (
    GOLD_TABLE_SPECS,
    build_bigquery_load_config,
    run_cloud_sync,
)
from projects.ecommerce.orchestration.superset.package_assets import validate_assets
from projects.ecommerce.streaming_layer.ecommerce_producer import (
    generate_synthetic_events,
)


def test_generate_synthetic_events_deterministic_with_anomalies() -> None:
    """Verify deterministic synthetic event generation emits expected anomalies.

    Args:
        None.
    """
    events_a = list(
        generate_synthetic_events(
            count=30, late_event_ratio=0.2, anomaly_ratio=0.2, seed=42
        )
    )
    events_b = list(
        generate_synthetic_events(
            count=30, late_event_ratio=0.2, anomaly_ratio=0.2, seed=42
        )
    )
    assert len(events_a) == 30
    assert [e["event_type"] for e in events_a] == [e["event_type"] for e in events_b]
    assert any(
        (e["price"] is not None and e["price"] < 0) or e["event_type"] == "refund_err"
        for e in events_a
    )


def test_superset_assets_validation_passes() -> None:
    """Verify all Dashboards-as-Code YAML assets validate with zero errors."""
    assets_dir = "projects/ecommerce/orchestration/superset/assets"
    assert validate_assets(assets_dir) == []


def test_bigquery_load_config_finops_partitioning() -> None:
    """Verify Gold tables configure BigQuery DAY partitioning and clustering."""
    spec_map = {s.name: s for s in GOLD_TABLE_SPECS}
    cat_cfg = build_bigquery_load_config(spec_map["daily_category_sales"])
    assert cat_cfg.source_format == bigquery.SourceFormat.PARQUET
    assert cat_cfg.time_partitioning.field == "sales_date"
    assert cat_cfg.clustering_fields == ["category"]


def test_cloud_sync_dry_run_with_duckdb(tmp_path: Path, monkeypatch) -> None:
    """Verify dry-run cloud sync exports compacted Gold Parquet safely without GCP APIs.

    Args:
        tmp_path: Pytest temporary directory fixture.
        monkeypatch: Pytest monkeypatch fixture.
    """
    import duckdb

    monkeypatch.setenv("GCP_SYNC_ENABLED", "false")
    db_path = str(tmp_path / "ecommerce.duckdb")
    with duckdb.connect(db_path) as conn:
        conn.execute(
            "CREATE TABLE daily_category_sales AS SELECT DATE '2026-09-01' AS sales_date, 'electronics' AS category, 1200.0 AS daily_revenue, 1 AS daily_units"
        )

    summary = run_cloud_sync(db_path, dry_run=True)
    assert summary["enabled"] is False
    assert summary["exported_count"] == 1


def test_airflow_cosmos_dag_parses_cleanly(tmp_path: Path, monkeypatch) -> None:
    """Verify Airflow DAG + Astronomer Cosmos DbtTaskGroup parse with expected WAP tasks.

    Args:
        tmp_path: Pytest temporary directory fixture.
        monkeypatch: Pytest monkeypatch fixture.
    """
    from dbt.cli.main import dbtRunner  # pants: no-infer-dep

    dbt_proj = os.path.abspath("projects/ecommerce/batch_layer")
    target_dir = tmp_path / "dbt_target"
    manifest_res = dbtRunner().invoke(
        [
            "parse",
            "--project-dir",
            dbt_proj,
            "--profiles-dir",
            dbt_proj,
            "--target-path",
            str(target_dir),
        ]
    )
    assert manifest_res.success
    manifest_path = str(target_dir / "manifest.json")

    monkeypatch.setenv("AIRFLOW__COSMOS__ENABLE_CACHE_DBT_LS", "False")
    monkeypatch.setenv("DBT_MANIFEST_PATH", manifest_path)
    monkeypatch.setenv("DBT_PROJ_DIR", dbt_proj)

    import importlib
    import projects.ecommerce.orchestration.dags.ecommerce_pipeline as dag_mod

    importlib.reload(dag_mod)
    task_ids = set(dag_mod.dag.task_ids)

    expected_tasks = {
        "wait_for_stream_outputs",
        "sync_postgres_dimensions",
        "prepare_wap_staging_db",
        "run_great_expectations",
        "dbt_build_monitors",
        "publish_wap_duckdb",
        "sync_cloud_warehouse",
    }
    assert expected_tasks.issubset(task_ids)
    # Cosmos expands dbt staging, snapshots, and marts into individual task nodes.
    cosmos_tasks = [t for t in task_ids if t.startswith("dbt_medallion_transform.")]
    assert len(cosmos_tasks) >= 15

    # Verify DeltaStreamSensor detects Delta log commits cleanly.
    out_dir = tmp_path / "output_data"
    for table in ("silver_ecommerce_events", "category_sales", "url_counts"):
        dlog = out_dir / table / "_delta_log"
        dlog.mkdir(parents=True)
        (dlog / "00000000000000000000.json").write_text("{}", encoding="utf-8")

    monkeypatch.setenv("CLICK_STREAM_OUTPUT_PREFIX", str(out_dir))
    sensor = dag_mod.DeltaStreamSensor(
        task_id="test_sensor", output_prefix=str(out_dir), min_ready_tables=3
    )
    assert sensor.poke(context=MagicMock()) is True

    # Verify self-contained SPARK_IN_AIRFLOW + PRODUCE_SYNTHETIC_EVENTS wiring.
    monkeypatch.setenv("SPARK_IN_AIRFLOW", "true")
    monkeypatch.setenv("PRODUCE_SYNTHETIC_EVENTS", "true")
    importlib.reload(dag_mod)
    prod_tasks = set(dag_mod.dag.task_ids)
    assert {"produce_synthetic_events", "run_spark_available_now"}.issubset(prod_tasks)
    assert (
        "run_spark_available_now"
        in dag_mod.dag.get_task("wait_for_stream_outputs").upstream_task_ids
    )
