import argparse
import logging
import sys
from projects.storage.postgres.config import get_connection

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
logger = logging.getLogger("PostgresFetch")


def fetch_transaction(transaction_id: str) -> None:
    """Queries a single transaction by ID and displays the record."""
    sql = "SELECT transaction_id, customer_id, amount, created_at FROM transactions WHERE transaction_id = %s;"
    logger.info("Fetching transaction: %s...", transaction_id)
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(sql, (transaction_id,))
            row = cur.fetchone()
            if row:
                logger.info(
                    "Found Transaction -> ID: %s | Customer: %s | Amount: $%.2f | Timestamp: %s",
                    row[0],
                    row[1],
                    row[2],
                    row[3],
                )
            else:
                logger.warning("No transaction found with ID: %s", transaction_id)


def fetch_all_transactions(limit: int = 10) -> None:
    """Queries and displays all recent transactions up to limit."""
    sql = "SELECT transaction_id, customer_id, amount, created_at FROM transactions ORDER BY created_at DESC LIMIT %s;"
    logger.info("Fetching top %d recent transactions...", limit)
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(sql, (limit,))
            rows = cur.fetchall()
            logger.info("Retrieved %d transaction records:", len(rows))
            for r in rows:
                logger.info(
                    "  [%s] Customer: %s | Amount: $%.2f | Created: %s",
                    r[0],
                    r[1],
                    r[2],
                    r[3],
                )


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Fetch transaction records from PostgreSQL database"
    )
    parser.add_argument("--txn-id", type=str, help="Specific Transaction ID to fetch")
    parser.add_argument(
        "--limit", type=int, default=10, help="Maximum number of records to return"
    )

    args = parser.parse_args()

    try:
        if args.txn_id:
            fetch_transaction(args.txn_id)
        else:
            fetch_all_transactions(args.limit)
    except Exception as e:
        logger.error("Failed to fetch transactions: %s", e, exc_info=True)
        sys.exit(1)
