import argparse
import sys
from projects.common.logger import get_logger
from projects.storage.cloud.config import get_bigquery_client, get_gcp_config

logger = get_logger("BigQueryClient")


def query_bigquery(query_str: str, limit: int = 10) -> None:
    """Executes a SQL query against Google Cloud BigQuery and logs tabular output."""
    client = get_bigquery_client()
    logger.info("Executing BigQuery job...")
    logger.info("Query:\n%s", query_str)

    query_job = client.query(query_str)
    results = query_job.result()

    df = results.to_dataframe()
    logger.info("Query returned %d total rows. Preview (top %d):", len(df), limit)
    print(df.head(limit).to_string(index=False))


if __name__ == "__main__":
    config = get_gcp_config()
    default_query = """
        SELECT
            'sample_txn_01' AS transaction_id,
            'cust_1001' AS customer_id,
            120.50 AS amount,
            CURRENT_TIMESTAMP() AS query_time
    """

    parser = argparse.ArgumentParser(
        description="Query tables and analytical views in Google Cloud BigQuery"
    )
    parser.add_argument(
        "--query", type=str, default=default_query, help="SQL query to execute"
    )
    parser.add_argument(
        "--limit", type=int, default=10, help="Maximum number of rows to display"
    )

    args = parser.parse_args()

    try:
        query_bigquery(args.query, args.limit)
    except Exception as e:
        logger.error("BigQuery query failed: %s", e, exc_info=True)
        sys.exit(1)
