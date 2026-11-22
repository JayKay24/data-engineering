"""PostgreSQL OLTP dimension management and zero-Pandas PyArrow Parquet export."""

import argparse
import os
import sys

import psycopg2
import pyarrow as pa
import pyarrow.parquet as pq

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

CUSTOMER_ARROW_SCHEMA = pa.schema(
    [
        ("user_id", pa.string()),
        ("segment", pa.string()),
        ("region", pa.string()),
        ("registration_date", pa.string()),
    ]
)

PRODUCT_ARROW_SCHEMA = pa.schema(
    [
        ("product_id", pa.string()),
        ("category", pa.string()),
        ("brand", pa.string()),
        ("unit_price", pa.float64()),
    ]
)

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


def _build_customer_table(rows: list[tuple[str, str, str, str]]) -> pa.Table:
    """Build a typed PyArrow Table from customer dimension tuples."""
    return pa.Table.from_pydict(
        {
            "user_id": [r[0] for r in rows],
            "segment": [r[1] for r in rows],
            "region": [r[2] for r in rows],
            "registration_date": [r[3] for r in rows],
        },
        schema=CUSTOMER_ARROW_SCHEMA,
    )


def _build_product_table(rows: list[tuple[str, str, str, float]]) -> pa.Table:
    """Build a typed PyArrow Table from product dimension tuples."""
    return pa.Table.from_pydict(
        {
            "product_id": [r[0] for r in rows],
            "category": [r[1] for r in rows],
            "brand": [r[2] for r in rows],
            "unit_price": [float(r[3]) for r in rows],
        },
        schema=PRODUCT_ARROW_SCHEMA,
    )


def _apply_scd_change(
    customers: list[tuple[str, str, str, str]],
    products: list[tuple[str, str, str, float]],
) -> tuple[list[tuple[str, str, str, str]], list[tuple[str, str, str, float]]]:
    """Mutate deterministic dimension attributes to exercise SCD Type 2 snapshots."""
    updated_customers = [
        (uid, "Enterprise", reg, dt) if uid == "user_103" else (uid, seg, reg, dt)
        for uid, seg, reg, dt in customers
    ]
    updated_products = [
        (pid, cat, br, 1149.99) if pid == "prod_01" else (pid, cat, br, price)
        for pid, cat, br, price in products
    ]
    return updated_customers, updated_products


def seed_postgres_dimensions(simulate_scd_change: bool = False) -> None:
    """Ensure OLTP dimension tables exist and upsert baseline records.

    Args:
        simulate_scd_change: When ``True``, applies a deterministic attribute
            update to ``user_103`` and ``prod_01`` so downstream SCD Type 2
            snapshots record historical transitions.

    Raises:
        psycopg2.Error: If the database connection or SQL execution fails.
    """
    customers = list(DEFAULT_CUSTOMERS)
    products = list(DEFAULT_PRODUCTS)
    if simulate_scd_change:
        customers, products = _apply_scd_change(customers, products)

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
                customers,
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
                products,
            )
    logger.info(
        "Seeded PostgreSQL OLTP dimension tables (%d customers, %d products, scd_change=%s).",
        len(customers),
        len(products),
        simulate_scd_change,
    )


def extract_dimensions_from_postgres() -> tuple[pa.Table, pa.Table]:
    """Extract customer and product dimension tables from PostgreSQL into PyArrow.

    Returns:
        A ``(customers_table, products_table)`` tuple of typed ``pyarrow.Table``
        instances ordered by primary key without intermediate Pandas copies.

    Raises:
        psycopg2.Error: If querying PostgreSQL fails.
    """
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

    return _build_customer_table(customer_rows), _build_product_table(product_rows)


def write_dimension_parquet(
    customers_table: pa.Table,
    products_table: pa.Table,
    output_base_dir: str,
) -> None:
    """Persist customer and product PyArrow tables directly as ZSTD Parquet files.

    Args:
        customers_table: Typed PyArrow table for ``dim_customers``.
        products_table: Typed PyArrow table for ``dim_products``.
        output_base_dir: Root directory where dimension subdirectories are created.
    """
    cust_dir = os.path.join(output_base_dir, "dim_customers")
    prod_dir = os.path.join(output_base_dir, "dim_products")
    os.makedirs(cust_dir, exist_ok=True)
    os.makedirs(prod_dir, exist_ok=True)

    cust_path = os.path.join(cust_dir, "dim_customers.parquet")
    prod_path = os.path.join(prod_dir, "dim_products.parquet")

    pq.write_table(customers_table, cust_path, compression="zstd")
    pq.write_table(products_table, prod_path, compression="zstd")

    logger.info("Wrote %d customer rows to %s", customers_table.num_rows, cust_path)
    logger.info("Wrote %d product rows to %s", products_table.num_rows, prod_path)


def sync_dimensions(
    output_base_dir: str = "projects/ecommerce/output_data",
    seed_from_defaults_on_offline: bool = False,
    simulate_scd_change: bool = False,
) -> None:
    """Seed PostgreSQL OLTP dimension tables and export them directly to Parquet.

    Args:
        output_base_dir: Base directory where Parquet dimension subdirectories
            are written. Defaults to ``"projects/ecommerce/output_data"``.
        seed_from_defaults_on_offline: When ``True``, falls back to the
            in-memory ``DEFAULT_CUSTOMERS`` and ``DEFAULT_PRODUCTS`` datasets if
            PostgreSQL is unreachable instead of raising an error.
        simulate_scd_change: When ``True``, applies a deterministic SCD change
            to ``user_103`` and ``prod_01`` for snapshot testing.

    Raises:
        psycopg2.OperationalError: If PostgreSQL is unreachable and
            ``seed_from_defaults_on_offline`` is ``False``.
    """
    try:
        seed_postgres_dimensions(simulate_scd_change=simulate_scd_change)
        customers_table, products_table = extract_dimensions_from_postgres()
    except psycopg2.OperationalError as exc:
        if not seed_from_defaults_on_offline:
            raise
        logger.warning(
            "PostgreSQL unavailable (%s). Falling back to baseline dimension datasets.",
            exc,
        )
        customers = list(DEFAULT_CUSTOMERS)
        products = list(DEFAULT_PRODUCTS)
        if simulate_scd_change:
            customers, products = _apply_scd_change(customers, products)
        customers_table = _build_customer_table(customers)
        products_table = _build_product_table(products)

    write_dimension_parquet(customers_table, products_table, output_base_dir)


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
    parser.add_argument(
        "--simulate-scd-change",
        action="store_true",
        help="Apply a deterministic segment/price update to test SCD Type 2 snapshots.",
    )
    args = parser.parse_args()

    try:
        sync_dimensions(
            output_base_dir=args.output_dir,
            seed_from_defaults_on_offline=args.seed_from_defaults_on_offline,
            simulate_scd_change=args.simulate_scd_change,
        )
    except Exception as exc:
        logger.error("OLTP dimension sync failed: %s", exc, exc_info=True)
        sys.exit(1)
