import argparse
import os
import sys

import pandas as pd
import pyarrow  # noqa: F401 - ensures pyarrow engine is registered for Parquet export
import psycopg2

from projects.common.logger import get_logger
from projects.ecommerce.oltp_sync.config import get_connection

logger = get_logger("EcommerceOltpSync")

DEFAULT_CUSTOMERS: list[tuple[str, str, str, str]] = [
    ("user_101", "Enterprise", "North America", "2025-01-15"),
    ("user_102", "Consumer", "Europe", "2025-03-22"),
    ("user_103", "SMB", "North America", "2025-06-10"),
    ("user_104", "Consumer", "Asia-Pacific", "2025-08-05"),
    ("user_105", "Enterprise", "Europe", "2025-11-19"),
    ("user_106", "SMB", "Asia-Pacific", "2026-01-09"),
    ("user_107", "Consumer", "North America", "2026-02-14"),
    ("user_108", "Enterprise", "Europe", "2026-04-01"),
]

DEFAULT_PRODUCTS: list[tuple[str, str, str, float]] = [
    ("prod_01", "electronics", "ApexTech", 1200.00),
    ("prod_02", "apparel", "StrideWear", 85.00),
    ("prod_03", "home", "CozyNest", 45.00),
    ("prod_04", "electronics", "NovaMobile", 800.00),
    ("prod_05", "home", "ErgoCraft", 150.00),
]

DDL_STATEMENTS = """
CREATE TABLE IF NOT EXISTS dim_customers (
    user_id VARCHAR(64) PRIMARY KEY,
    segment VARCHAR(64) NOT NULL,
    region VARCHAR(64) NOT NULL,
    registration_date DATE NOT NULL
);

CREATE TABLE IF NOT EXISTS dim_products (
    product_id VARCHAR(64) PRIMARY KEY,
    category VARCHAR(64) NOT NULL,
    brand VARCHAR(64) NOT NULL,
    unit_price NUMERIC(10, 2) NOT NULL
);
"""


def seed_postgres_dimensions() -> None:
    """Ensures OLTP dimension tables exist and upserts baseline customer/product records."""
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(DDL_STATEMENTS)
            cur.executemany(
                """
                INSERT INTO dim_customers (user_id, segment, region, registration_date)
                VALUES (%s, %s, %s, %s)
                ON CONFLICT (user_id) DO UPDATE
                SET segment = EXCLUDED.segment,
                    region = EXCLUDED.region,
                    registration_date = EXCLUDED.registration_date;
                """,
                DEFAULT_CUSTOMERS,
            )
            cur.executemany(
                """
                INSERT INTO dim_products (product_id, category, brand, unit_price)
                VALUES (%s, %s, %s, %s)
                ON CONFLICT (product_id) DO UPDATE
                SET category = EXCLUDED.category,
                    brand = EXCLUDED.brand,
                    unit_price = EXCLUDED.unit_price;
                """,
                DEFAULT_PRODUCTS,
            )
    logger.info(
        "Seeded PostgreSQL OLTP dimension tables (%d customers, %d products).",
        len(DEFAULT_CUSTOMERS),
        len(DEFAULT_PRODUCTS),
    )


def extract_dimensions_from_postgres() -> tuple[pd.DataFrame, pd.DataFrame]:
    """Extracts customer and product dimension tables from PostgreSQL into DataFrames."""
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT user_id, segment, region, registration_date::text FROM dim_customers ORDER BY user_id;"
            )
            customer_rows = cur.fetchall()
            cur.execute(
                "SELECT product_id, category, brand, unit_price::float FROM dim_products ORDER BY product_id;"
            )
            product_rows = cur.fetchall()

    customers_df = pd.DataFrame(
        customer_rows, columns=["user_id", "segment", "region", "registration_date"]
    )
    products_df = pd.DataFrame(
        product_rows, columns=["product_id", "category", "brand", "unit_price"]
    )
    return customers_df, products_df


def write_dimension_parquet(
    customers_df: pd.DataFrame,
    products_df: pd.DataFrame,
    output_base_dir: str,
) -> None:
    """Persists customer and product dimension DataFrames as Parquet for dbt/DuckDB consumption."""
    cust_dir = os.path.join(output_base_dir, "dim_customers")
    prod_dir = os.path.join(output_base_dir, "dim_products")
    os.makedirs(cust_dir, exist_ok=True)
    os.makedirs(prod_dir, exist_ok=True)

    cust_path = os.path.join(cust_dir, "dim_customers.parquet")
    prod_path = os.path.join(prod_dir, "dim_products.parquet")

    customers_df.to_parquet(cust_path, index=False, engine="pyarrow")
    products_df.to_parquet(prod_path, index=False, engine="pyarrow")

    logger.info("Wrote %d customer rows to %s", len(customers_df), cust_path)
    logger.info("Wrote %d product rows to %s", len(products_df), prod_path)


def sync_dimensions(
    output_base_dir: str = "projects/ecommerce/output_data",
    seed_from_defaults_on_offline: bool = False,
) -> None:
    """Seeds PostgreSQL OLTP dimension tables and exports them to Parquet."""
    try:
        seed_postgres_dimensions()
        customers_df, products_df = extract_dimensions_from_postgres()
    except psycopg2.OperationalError as exc:
        if not seed_from_defaults_on_offline:
            raise
        logger.warning(
            "PostgreSQL unavailable (%s). Falling back to baseline dimension datasets.",
            exc,
        )
        customers_df = pd.DataFrame(
            DEFAULT_CUSTOMERS,
            columns=["user_id", "segment", "region", "registration_date"],
        )
        products_df = pd.DataFrame(
            DEFAULT_PRODUCTS,
            columns=["product_id", "category", "brand", "unit_price"],
        )

    write_dimension_parquet(customers_df, products_df, output_base_dir)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Seed and extract PostgreSQL OLTP dimension tables to Parquet."
    )
    parser.add_argument(
        "--output-dir",
        default=os.getenv(
            "CLICK_STREAM_OUTPUT_PREFIX", "projects/ecommerce/output_data"
        ),
        help="Base output directory for Parquet dimension files.",
    )
    parser.add_argument(
        "--seed-from-defaults-on-offline",
        action="store_true",
        help="Fallback to default dimension datasets if PostgreSQL is unreachable.",
    )
    args = parser.parse_args()

    try:
        sync_dimensions(
            output_base_dir=args.output_dir,
            seed_from_defaults_on_offline=args.seed_from_defaults_on_offline,
        )
    except Exception as exc:
        logger.error("OLTP dimension sync failed: %s", exc, exc_info=True)
        sys.exit(1)
