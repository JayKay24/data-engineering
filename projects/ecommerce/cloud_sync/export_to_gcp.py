"""Google Cloud Platform lakehouse and FinOps warehouse sync utility.

Exports compacted ZSTD Gold Parquet snapshots from DuckDB to Google Cloud
Storage (minimizing GCS Class A operations) and loads them directly into
time-partitioned, clustered BigQuery tables with bounded verification queries.
"""

import argparse
from dataclasses import dataclass
import os
import sys
import tempfile

from google.cloud import bigquery

from projects.common.logger import get_logger
from projects.ecommerce.cloud_sync.config import (
    get_bigquery_client,
    get_gcp_config,
    get_storage_client,
)
from projects.ecommerce.duckdb_utils import connect_duckdb_readonly

logger = get_logger("EcommerceCloudSync")

MAX_VERIFY_BYTES_BILLED = 100 * 1024 * 1024  # 100 MB FinOps query guardrail


@dataclass(frozen=True)
class GoldTableSpec:
    """BigQuery partitioning and clustering specification for a Gold mart."""

    name: str
    partition_field: str | None
    clustering_fields: tuple[str, ...]


GOLD_TABLE_SPECS: tuple[GoldTableSpec, ...] = (
    GoldTableSpec("daily_category_sales", "sales_date", ("category",)),
    GoldTableSpec("daily_url_conversion", "conversion_date", ("url",)),
    GoldTableSpec("cumulative_users", "cohort_date", ()),
    GoldTableSpec("customer_segment_spending", None, ("segment", "region")),
    GoldTableSpec("brand_category_performance", None, ("brand", "category")),
)
GOLD_MART_TABLES = [spec.name for spec in GOLD_TABLE_SPECS]


def export_compacted_gold_parquet(duckdb_path: str, export_dir: str) -> dict[str, str]:
    """Export each available Gold mart from DuckDB into a single ZSTD Parquet file.

    Args:
        duckdb_path: Path to the serving DuckDB database.
        export_dir: Temporary or staging directory for compacted Parquet outputs.

    Returns:
        Mapping of Gold mart table name to exported Parquet file path.
    """
    if not os.path.exists(duckdb_path):
        logger.warning(
            "DuckDB file not found at %s; skipping Gold Parquet export.", duckdb_path
        )
        return {}

    os.makedirs(export_dir, exist_ok=True)
    exported: dict[str, str] = {}

    with connect_duckdb_readonly(duckdb_path) as conn:
        existing_tables = {
            row[0]
            for row in conn.execute(
                "SELECT table_name FROM information_schema.tables WHERE table_schema = 'main'"
            ).fetchall()
        }
        for spec in GOLD_TABLE_SPECS:
            if spec.name not in existing_tables:
                continue
            row_count = conn.execute(f'SELECT COUNT(*) FROM "{spec.name}"').fetchone()[
                0
            ]
            if row_count == 0:
                continue
            parquet_path = os.path.join(export_dir, f"{spec.name}.parquet")
            conn.execute(
                f'COPY (SELECT * FROM "{spec.name}") TO ? (FORMAT PARQUET, COMPRESSION ZSTD)',
                [parquet_path],
            )
            exported[spec.name] = parquet_path

    return exported


def upload_compacted_gold_to_gcs(
    exported_files: dict[str, str],
    bucket_name: str,
    prefix: str = "ecommerce/gold",
) -> int:
    """Upload compacted Gold Parquet snapshots to Google Cloud Storage.

    Uploading one compacted Parquet file per Gold mart keeps Class A ``PUT``
    operations minimal (e.g., 5 ops/run instead of dozens of small streaming files).

    Args:
        exported_files: Mapping of table name to local compacted Parquet path.
        bucket_name: Target GCS bucket name.
        prefix: Destination object key prefix inside the bucket.

    Returns:
        Number of Parquet objects uploaded to GCS.
    """
    if not exported_files:
        logger.warning("No compacted Gold Parquet snapshots available to upload.")
        return 0

    client = get_storage_client()
    bucket = client.bucket(bucket_name)
    uploaded = 0

    for table_name, local_path in sorted(exported_files.items()):
        blob_name = f"{prefix}/{table_name}.parquet"
        blob = bucket.blob(blob_name)
        blob.upload_from_filename(local_path)
        uploaded += 1

    logger.info(
        "Uploaded %d compacted Gold Parquet snapshots to gs://%s/%s",
        uploaded,
        bucket_name,
        prefix,
    )
    return uploaded


def build_bigquery_load_config(spec: GoldTableSpec) -> bigquery.LoadJobConfig:
    """Create a BigQuery Parquet ``LoadJobConfig`` with partitioning and clustering.

    Args:
        spec: Gold mart BigQuery layout specification.

    Returns:
        Configured ``bigquery.LoadJobConfig`` instance.
    """
    kwargs: dict = {
        "source_format": bigquery.SourceFormat.PARQUET,
        "write_disposition": bigquery.WriteDisposition.WRITE_TRUNCATE,
    }
    if spec.partition_field:
        kwargs["time_partitioning"] = bigquery.TimePartitioning(
            type_=bigquery.TimePartitioningType.DAY,
            field=spec.partition_field,
        )
    if spec.clustering_fields:
        kwargs["clustering_fields"] = list(spec.clustering_fields)
    return bigquery.LoadJobConfig(**kwargs)


def sync_gold_parquet_to_bigquery(
    exported_files: dict[str, str],
    project_id: str,
    dataset_id: str,
) -> int:
    """Load compacted Gold Parquet files into BigQuery and run a FinOps-guarded query.

    Args:
        exported_files: Mapping of Gold mart names to local Parquet files.
        project_id: Target GCP project ID owning the BigQuery dataset.
        dataset_id: Target BigQuery dataset name (created if missing).

    Returns:
        Number of Gold mart tables loaded into BigQuery.
    """
    if not exported_files:
        logger.warning("No exported Gold Parquet files; skipping BigQuery sync.")
        return 0

    bq_client = get_bigquery_client(project_id)
    dataset_ref = bigquery.DatasetReference(project_id, dataset_id)
    bq_client.create_dataset(bigquery.Dataset(dataset_ref), exists_ok=True)

    loaded_tables = 0
    for spec in GOLD_TABLE_SPECS:
        parquet_path = exported_files.get(spec.name)
        if not parquet_path:
            continue
        table_id = f"{project_id}.{dataset_id}.{spec.name}"
        job_config = build_bigquery_load_config(spec)
        with open(parquet_path, "rb") as source_file:
            load_job = bq_client.load_table_from_file(
                source_file, table_id, job_config=job_config
            )
        load_job.result()
        loaded_tables += 1
        logger.info(
            "Loaded %s into BigQuery (partition=%s, cluster=%s).",
            table_id,
            spec.partition_field,
            spec.clustering_fields,
        )

    if loaded_tables > 0 and "daily_category_sales" in exported_files:
        verify_sql = f"""
            SELECT category, SUM(daily_revenue) AS total_revenue
            FROM `{project_id}.{dataset_id}.daily_category_sales`
            GROUP BY category
            ORDER BY total_revenue DESC
            LIMIT 10
        """
        query_cfg = bigquery.QueryJobConfig(
            maximum_bytes_billed=MAX_VERIFY_BYTES_BILLED
        )
        rows = list(bq_client.query(verify_sql, job_config=query_cfg).result())
        logger.info("BigQuery verification query returned %d rows.", len(rows))

    return loaded_tables


def run_cloud_sync(
    duckdb_path: str,
    dry_run: bool = False,
) -> dict[str, int | bool]:
    """Execute compacted GCS upload and BigQuery sync, or log a local dry-run summary.

    Args:
        duckdb_path: Path to the serving DuckDB warehouse file.
        dry_run: When ``True``, exports locally into a temp dir and logs the
            sync inventory without calling GCP APIs.

    Returns:
        Summary dictionary with ``enabled``, ``exported_count``,
        ``uploaded_count``, and ``loaded_tables``.
    """
    cfg = get_gcp_config()
    with tempfile.TemporaryDirectory() as tmpdir:
        exported_files = export_compacted_gold_parquet(duckdb_path, tmpdir)

        if dry_run or not cfg["enabled"]:
            logger.info(
                "GCP sync is disabled (set GCP_SYNC_ENABLED=true to enable). "
                "Dry-run inventory: %d compacted Gold Parquet tables from %s (exists=%s), "
                "target=gs://%s/ecommerce/gold & BigQuery %s.%s.",
                len(exported_files),
                duckdb_path,
                os.path.exists(duckdb_path),
                cfg["gcs_bucket"],
                cfg["project_id"],
                cfg["bq_dataset"],
            )
            return {
                "enabled": False,
                "exported_count": len(exported_files),
                "uploaded_count": 0,
                "loaded_tables": 0,
            }

        uploaded = upload_compacted_gold_to_gcs(exported_files, cfg["gcs_bucket"])
        loaded = sync_gold_parquet_to_bigquery(
            exported_files, cfg["project_id"], cfg["bq_dataset"]
        )
        return {
            "enabled": True,
            "exported_count": len(exported_files),
            "uploaded_count": uploaded,
            "loaded_tables": loaded,
        }


if __name__ == "__main__":
    default_duckdb = os.getenv(
        "DUCKDB_SERVING_PATH",
        os.getenv(
            "DUCKDB_PATH", "projects/ecommerce/batch_layer/data/ecommerce.duckdb"
        ),
    )

    parser = argparse.ArgumentParser(
        description="Export compacted Gold DuckDB marts to GCS and partitioned BigQuery tables."
    )
    parser.add_argument("--duckdb-path", default=default_duckdb)
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Log export inventory without calling GCP APIs.",
    )
    args = parser.parse_args()

    try:
        run_cloud_sync(args.duckdb_path, dry_run=args.dry_run)
    except Exception as exc:
        logger.error("Cloud sync failed: %s", exc, exc_info=True)
        sys.exit(1)
