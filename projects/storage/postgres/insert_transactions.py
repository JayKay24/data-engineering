import argparse
import logging
import sys
from projects.storage.postgres.config import get_connection

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
logger = logging.getLogger("PostgresInsert")


def insert_transaction(transaction_id: str, customer_id: str, amount: float) -> None:
    """Inserts a single transactional record into PostgreSQL using context-managed connection."""
    sql = """
        INSERT INTO transactions (transaction_id, customer_id, amount)
        VALUES (%s, %s, %s)
        ON CONFLICT (transaction_id) DO UPDATE
        SET customer_id = EXCLUDED.customer_id,
            amount = EXCLUDED.amount;
    """
    logger.info("Connecting to PostgreSQL to insert transaction: %s...", transaction_id)
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(sql, (transaction_id, customer_id, amount))
    logger.info(
        "Successfully inserted transaction %s (Customer: %s, Amount: $%.2f)",
        transaction_id,
        customer_id,
        amount,
    )


def insert_sample_transactions() -> None:
    """Inserts a standard batch of sample transactions."""
    sample_records = [
        ("txn_1001", "cust_1001", 49.99),
        ("txn_1002", "cust_1002", 19.99),
        ("txn_1003", "cust_1003", 5.99),
        ("txn_1004", "cust_1004", 99.99),
        ("txn_1005", "cust_1005", 15.00),
    ]
    sql = """
        INSERT INTO transactions (transaction_id, customer_id, amount)
        VALUES (%s, %s, %s)
        ON CONFLICT (transaction_id) DO UPDATE
        SET customer_id = EXCLUDED.customer_id,
            amount = EXCLUDED.amount;
    """
    logger.info("Inserting batch of %d sample transactions...", len(sample_records))
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.executemany(sql, sample_records)
    logger.info("Batch insertion completed successfully!")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Insert transaction records into PostgreSQL database"
    )
    parser.add_argument("--txn-id", type=str, help="Transaction ID")
    parser.add_argument("--cust-id", type=str, help="Customer ID")
    parser.add_argument("--amount", type=float, help="Transaction amount")
    parser.add_argument(
        "--sample", action="store_true", help="Insert standard batch of sample records"
    )

    args = parser.parse_args()

    try:
        if args.txn_id and args.cust_id and args.amount is not None:
            insert_transaction(args.txn_id, args.cust_id, args.amount)
        else:
            insert_sample_transactions()
    except Exception as e:
        logger.error("Failed to insert transactions: %s", e, exc_info=True)
        sys.exit(1)
