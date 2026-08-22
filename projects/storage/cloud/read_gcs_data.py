import argparse
import io
import logging
import sys
import pandas as pd
from projects.storage.cloud.config import get_gcp_config, get_storage_client

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
logger = logging.getLogger("GCSReader")


def list_and_read_gcs_data(bucket_name: str, prefix: str = "", limit: int = 5) -> None:
    """Lists blobs within a GCS bucket and reads a sample JSON or CSV file into a pandas DataFrame."""
    client = get_storage_client()
    logger.info(
        "Accessing GCS bucket '%s' with prefix filter '%s'...", bucket_name, prefix
    )
    bucket = client.bucket(bucket_name)

    blobs = list(client.list_blobs(bucket, prefix=prefix, max_results=10))
    if not blobs:
        logger.warning(
            "No blobs found in bucket '%s' with prefix '%s'", bucket_name, prefix
        )
        return

    logger.info("Discovered %d blobs in bucket:", len(blobs))
    for b in blobs:
        logger.info("  - %s (%s bytes, updated: %s)", b.name, b.size, b.updated)

    target_blob = blobs[0]
    logger.info("Reading data from target blob: %s...", target_blob.name)
    data_bytes = target_blob.download_as_bytes()

    if target_blob.name.endswith(".json"):
        df = pd.read_json(io.BytesIO(data_bytes), lines=True)
    elif target_blob.name.endswith(".csv"):
        df = pd.read_csv(io.BytesIO(data_bytes))
    elif target_blob.name.endswith(".parquet"):
        df = pd.read_parquet(io.BytesIO(data_bytes))
    else:
        logger.info("Content preview (raw text):\n%s", data_bytes.decode("utf-8")[:500])
        return

    logger.info(
        "Loaded DataFrame (%d rows x %d cols). Preview:", len(df), len(df.columns)
    )
    print(df.head(limit).to_string())


if __name__ == "__main__":
    config = get_gcp_config()
    parser = argparse.ArgumentParser(
        description="List and read data files from Google Cloud Storage"
    )
    parser.add_argument(
        "--bucket", type=str, default=config["gcs_bucket"], help="GCS Bucket name"
    )
    parser.add_argument(
        "--prefix", type=str, default="", help="Prefix path within bucket"
    )
    parser.add_argument(
        "--limit", type=int, default=5, help="Number of rows to display in preview"
    )

    args = parser.parse_args()

    try:
        list_and_read_gcs_data(args.bucket, args.prefix, args.limit)
    except Exception as e:
        logger.error("GCS operation failed: %s", e, exc_info=True)
        sys.exit(1)
