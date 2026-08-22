import os
from typing import TypedDict
from google.cloud import bigquery, storage


class GCPConfig(TypedDict):
    project_id: str
    gcs_bucket: str
    bq_dataset: str


def get_gcp_config() -> GCPConfig:
    """Retrieves GCP configuration settings from environment variables."""
    return {
        "project_id": os.getenv(
            "GCP_PROJECT", os.getenv("GOOGLE_CLOUD_PROJECT", "demo-data-project")
        ),
        "gcs_bucket": os.getenv("GCS_BUCKET", "demo-datalake-bucket"),
        "bq_dataset": os.getenv("BIGQUERY_DATASET", "retail_warehouse"),
    }


def get_storage_client(project_id: str | None = None) -> storage.Client:
    """Returns a Google Cloud Storage client instance."""
    config = get_gcp_config()
    target_project = project_id or config["project_id"]
    return storage.Client(project=target_project)


def get_bigquery_client(project_id: str | None = None) -> bigquery.Client:
    """Returns a Google Cloud BigQuery client instance."""
    config = get_gcp_config()
    target_project = project_id or config["project_id"]
    return bigquery.Client(project=target_project)
