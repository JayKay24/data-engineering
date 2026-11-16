import os
from typing import TypedDict

from google.cloud import bigquery, storage


class GCPConfig(TypedDict):
    enabled: bool
    project_id: str
    gcs_bucket: str
    bq_dataset: str


def get_gcp_config() -> GCPConfig:
    """Retrieves GCP lakehouse and warehouse configuration from environment variables."""
    enabled = os.getenv("GCP_SYNC_ENABLED", "false").lower() in ("true", "1", "yes")
    return {
        "enabled": enabled,
        "project_id": os.getenv(
            "GCP_PROJECT", os.getenv("GOOGLE_CLOUD_PROJECT", "demo-data-project")
        ),
        "gcs_bucket": os.getenv("GCS_BUCKET", "demo-ecommerce-datalake"),
        "bq_dataset": os.getenv("BIGQUERY_DATASET", "ecommerce_warehouse"),
    }


def get_storage_client(project_id: str | None = None) -> storage.Client:
    """Returns a Google Cloud Storage client instance."""
    config = get_gcp_config()
    return storage.Client(project=project_id or config["project_id"])


def get_bigquery_client(project_id: str | None = None) -> bigquery.Client:
    """Returns a Google Cloud BigQuery client instance."""
    config = get_gcp_config()
    return bigquery.Client(project=project_id or config["project_id"])
