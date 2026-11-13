import argparse
import glob
import os
import sys

import db_dtypes  # noqa: F401 - required for BigQuery DataFrame dtype conversions
from google.cloud import bigquery

from projects.common.logger import get_logger
from projects.ecommerce.cloud_sync.config import (
    get_bigquery_client,
    get_gcp_config,
    get_storage_client,
)
from projects.ecommerce.duckdb_utils import connect_duckdb_readonly

logger = get_logger("EcommerceCloudSync")

GOLD_MART_TABLES = [
    "daily_category_sales",
    "daily_url_conversion",
    "cumulative_users",
    "customer_segment_spending",
    "brand_category_performance",
]


def _discover_parquet_files(output_base_dir: str) -> list[str]:
    """Discovers local Parquet artifacts across streaming and dimension output directories."""
    pattern = os.path.join(output_base_dir, "**", "*.parquet")
    return sorted(glob.glob(pattern, recursive=True))


def upload_parquet_to_gcs(
    output_base_dir: str,
    bucket_name: str,
    prefix: str = "ecommerce/curated",
) -> int:
    """Uploads local Parquet files to Google Cloud Storage."""
    parquet_files = _discover_parquet_files(output_base_dir)
    if not parquet_files:
        logger.warning("No Parquet files found under %s to upload.", output_base_dir)
        return 0

    client = get_storage_client()
    bucket = client.bucket(bucket_name)
    uploaded = 0

    for local_path in parquet_files:
        rel_path = os.path.relpath(local_path, output_base_dir)
        blob_name = f"{prefix}/{rel_path}"
        blob = bucket.blob(blob_name)
        blob.upload_from_filename(local_path)
        uploaded += 1

    logger.info(
        "Uploaded %d Parquet files to gs://%s/%s", uploaded, bucket_name, prefix
    )
    return uploaded


def sync_duckdb_marts_to_bigquery(
    duckdb_path: str,
    project_id: str,
    dataset_id: str,
) -> int:
    """Loads DuckDB Gold analytical marts into BigQuery tables and runs a verification query."""
    if not os.path.exists(duckdb_path):
        logger.warning(
            "DuckDB file not found at %s; skipping BigQuery sync.", duckdb_path
        )
        return 0

    bq_client = get_bigquery_client(project_id)
    dataset_ref = bigquery.DatasetReference(project_id, dataset_id)
    bq_client.create_dataset(bigquery.Dataset(dataset_ref), exists_ok=True)

    loaded_tables = 0
    with connect_duckdb_readonly(duckdb_path) as conn:
        existing_tables = {
            row[0]
            for row in conn.execute(
                "SELECT table_name FROM information_schema.tables WHERE table_schema = 'main'"
            ).fetchall()
        }
        for table in GOLD_MART_TABLES:
            if table not in existing_tables:
                continue
            df = conn.execute(f"SELECT * FROM {table}").fetchdf()
            if df.empty:
                continue
            table_id = f"{project_id}.{dataset_id}.{table}"
            job_config = bigquery.LoadJobConfig(
                write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE
            )
            load_job = bq_client.load_table_from_dataframe(
                df, table_id, job_config=job_config
            )
            load_job.result()
            loaded_tables += 1
            logger.info("Loaded %d rows into BigQuery table %s", len(df), table_id)

    if loaded_tables > 0 and "daily_category_sales" in existing_tables:
        verify_sql = f"""
            SELECT category, SUM(daily_revenue) AS total_revenue
            FROM `{project_id}.{dataset_id}.daily_category_sales`
            GROUP BY category
            ORDER BY total_revenue DESC
            LIMIT 10
        """
        df_verify = bq_client.query(verify_sql).result().to_dataframe()
        logger.info("BigQuery verification query returned %d rows.", len(df_verify))

    return loaded_tables


def run_cloud_sync(
    output_base_dir: str, duckdb_path: str, dry_run: bool = False
) -> None:
    """Executes GCS and BigQuery sync when enabled, or logs a local dry-run summary."""
    cfg = get_gcp_config()
    parquet_files = _discover_parquet_files(output_base_dir)

    if dry_run or not cfg["enabled"]:
        logger.info(
            "GCP sync is disabled (set GCP_SYNC_ENABLED=true to enable). "
            "Dry-run inventory: %d Parquet files in %s, DuckDB=%s (exists=%s), "
            "target=gs://%s & BigQuery %s.%s.",
            len(parquet_files),
            output_base_dir,
            duckdb_path,
            os.path.exists(duckdb_path),
            cfg["gcs_bucket"],
            cfg["project_id"],
            cfg["bq_dataset"],
        )
        return

    upload_parquet_to_gcs(output_base_dir, cfg["gcs_bucket"])
    sync_duckdb_marts_to_bigquery(duckdb_path, cfg["project_id"], cfg["bq_dataset"])


if __name__ == "__main__":
    default_output = os.getenv(
        "CLICK_STREAM_OUTPUT_PREFIX", "projects/ecommerce/output_data"
    )
    default_duckdb = os.getenv(
        "DUCKDB_PATH", "projects/ecommerce/batch_layer/data/ecommerce.duckdb"
    )

    parser = argparse.ArgumentParser(
        description="Export curated e-commerce Parquet files to GCS and Gold marts to BigQuery."
    )
    parser.add_argument("--output-dir", default=default_output)
    parser.add_argument("--duckdb-path", default=default_duckdb)
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Log export inventory without calling GCP APIs.",
    )
    args = parser.parse_args()

    try:
        run_cloud_sync(args.output_dir, args.duckdb_path, dry_run=args.dry_run)
    except Exception as exc:
        logger.error("Cloud sync failed: %s", exc, exc_info=True)
        sys.exit(1)
