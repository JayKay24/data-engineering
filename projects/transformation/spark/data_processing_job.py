# %% [markdown]
# # PySpark Batch Transformation Pipeline with Delta Lake
# Cleanses raw transactional purchases, joins with customer and product dimensions,
# calculates business aggregations, and writes curated Delta Lake tables.

# %%
import argparse
import os
import sys
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import col, count, date_format, quarter, sum, to_timestamp
from pyspark.sql.types import (
    DoubleType,
    StringType,
    StructField,
    StructType,
)
from projects.common.logger import get_logger

# --------------------
# Logger Configuration
# --------------------
logger = get_logger("DataProcessingJob")

# --------------------
# Explicit Schema Definitions
# --------------------
PURCHASES_SCHEMA = StructType(
    [
        StructField("purchase_id", StringType(), True),
        StructField("customer_id", StringType(), True),
        StructField("product_id", StringType(), True),
        StructField("purchase_amount", StringType(), True),
        StructField("timestamp", StringType(), True),
        StructField("payment_method", StringType(), True),
    ]
)

CUSTOMERS_SCHEMA = StructType(
    [
        StructField("customer_id", StringType(), True),
        StructField("segment", StringType(), True),
        StructField("region", StringType(), True),
        StructField("registration_date", StringType(), True),
    ]
)

PRODUCTS_SCHEMA = StructType(
    [
        StructField("product_id", StringType(), True),
        StructField("category", StringType(), True),
        StructField("price", DoubleType(), True),
        StructField("brand", StringType(), True),
    ]
)


# %%
def init_spark(app_name: str = "DataProcessingJob") -> SparkSession:
    """Initializes and returns a SparkSession configured with Delta Lake 3.x support."""
    logger.info("Initializing SparkSession with Delta Lake support...")
    return (
        SparkSession.builder.appName(app_name)
        .master("local[*]")
        .config("spark.jars.packages", "io.delta:delta-spark_2.12:3.2.0")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
        .getOrCreate()
    )


# %%
def load_raw_data(
    spark: SparkSession, raw_data_dir: str
) -> tuple[DataFrame, DataFrame, DataFrame]:
    """Loads raw purchases, customers, and products JSON datasets using explicit schemas."""
    purchases_path = os.path.join(raw_data_dir, "purchases.json")
    customers_path = os.path.join(raw_data_dir, "customers.json")
    products_path = os.path.join(raw_data_dir, "products.json")

    for path in (purchases_path, customers_path, products_path):
        if not os.path.exists(path):
            raise FileNotFoundError(f"Input raw dataset not found at: {path}")

    logger.info("Loading raw datasets with schema enforcement from %s...", raw_data_dir)
    purchases_df = spark.read.schema(PURCHASES_SCHEMA).json(purchases_path)
    customers_df = spark.read.schema(CUSTOMERS_SCHEMA).json(customers_path)
    products_df = spark.read.schema(PRODUCTS_SCHEMA).json(products_path)

    return purchases_df, customers_df, products_df


# %%
def cleanse_purchases(purchases_df: DataFrame) -> DataFrame:
    """Cleanses raw purchases data by filtering invalid rows and enriching date attributes."""
    logger.info("Applying cleansing transformations to purchases dataset...")
    return (
        purchases_df.filter(
            "purchase_amount IS NOT NULL AND product_id IS NOT NULL AND customer_id IS NOT NULL"
        )
        .withColumn("purchase_amount", col("purchase_amount").cast("double"))
        .withColumn(
            "timestamp", to_timestamp(col("timestamp"), "yyyy-MM-dd'T'HH:mm:ss'Z'")
        )
        .withColumn("purchase_date", date_format(col("timestamp"), "yyyy-MM-dd"))
        .withColumn("purchase_quarter", quarter(col("timestamp")))
    )


# %%
def transform_and_enrich(
    purchases_clean_df: DataFrame,
    customers_df: DataFrame,
    products_df: DataFrame,
) -> DataFrame:
    """Joins cleansed purchases with customer profiles and product metadata."""
    logger.info("Joining purchases with customer and product dimensions...")
    return purchases_clean_df.join(customers_df, on="customer_id", how="left").join(
        products_df, on="product_id", how="left"
    )


# %%
def aggregate_customer_spending(enriched_df: DataFrame) -> DataFrame:
    """Computes quarterly spending and purchase volume aggregated by customer and region."""
    logger.info("Aggregating quarterly customer spending metrics...")
    return enriched_df.groupBy(
        "customer_id", "segment", "region", "purchase_quarter"
    ).agg(
        sum("purchase_amount").alias("total_spent"),
        count("purchase_id").alias("transaction_count"),
    )


# %%
def aggregate_category_revenue(enriched_df: DataFrame) -> DataFrame:
    """Computes quarterly revenue and transaction count aggregated by product category."""
    logger.info("Aggregating quarterly product category revenue...")
    return enriched_df.groupBy("category", "purchase_quarter").agg(
        sum("purchase_amount").alias("total_revenue"),
        count("purchase_id").alias("units_sold"),
    )


# %%
def write_delta_table(
    df: DataFrame,
    output_path: str,
    partition_cols: list[str] | None = None,
    mode: str = "overwrite",
) -> None:
    """Writes a Spark DataFrame out to disk as a Delta Lake table."""
    logger.info("Writing curated Delta table to: %s (mode: %s)...", output_path, mode)
    writer = df.write.format("delta").mode(mode)
    if partition_cols:
        writer = writer.partitionBy(*partition_cols)
    writer.save(output_path)


# %%
def run_transformation_pipeline(
    raw_data_dir: str, curated_data_dir: str, debug_mode: bool = False
) -> None:
    """Executes the complete end-to-end PySpark transformation pipeline."""
    spark = init_spark()
    try:
        purchases_raw, customers_raw, products_raw = load_raw_data(spark, raw_data_dir)

        # Transformation steps
        purchases_clean = cleanse_purchases(purchases_raw)
        enriched_purchases = transform_and_enrich(
            purchases_clean, customers_raw, products_raw
        )

        customer_spending = aggregate_customer_spending(enriched_purchases)
        category_revenue = aggregate_category_revenue(enriched_purchases)

        # Previews gated behind debug_mode to avoid redundant action executions in production
        if debug_mode:
            logger.info("--- Enriched Purchases Preview ---")
            enriched_purchases.show(5, truncate=False)

            logger.info("--- Customer Spending Aggregations Preview ---")
            customer_spending.show(5, truncate=False)

            logger.info("--- Category Revenue Aggregations Preview ---")
            category_revenue.show(5, truncate=False)

        # Output curated Delta tables
        enriched_out = os.path.join(curated_data_dir, "enriched_purchases")
        spending_out = os.path.join(curated_data_dir, "customer_spending")
        revenue_out = os.path.join(curated_data_dir, "category_revenue")

        write_delta_table(
            enriched_purchases, enriched_out, partition_cols=["purchase_quarter"]
        )
        write_delta_table(customer_spending, spending_out)
        write_delta_table(category_revenue, revenue_out)

        logger.info("Data transformation pipeline completed successfully!")
    finally:
        logger.info("Stopping Spark session...")
        spark.stop()


# %%
if __name__ == "__main__":
    script_dir = os.path.dirname(os.path.abspath(__file__))
    default_raw_dir = os.path.join(script_dir, "data/raw")
    default_curated_dir = os.path.join(script_dir, "data/curated")
    env_debug = os.getenv("DEBUG_MODE", "false").lower() in ("true", "1", "yes")

    parser = argparse.ArgumentParser(
        description="PySpark Batch Data Transformation and Delta Lake Curation Pipeline"
    )
    parser.add_argument(
        "--raw-data-dir",
        type=str,
        default=default_raw_dir,
        help="Path to directory containing raw JSON files (purchases, customers, products).",
    )
    parser.add_argument(
        "--curated-data-dir",
        type=str,
        default=default_curated_dir,
        help="Target output directory for curated Delta Lake tables.",
    )
    parser.add_argument(
        "--debug",
        action="store_true",
        default=env_debug,
        help="Enable DataFrame show() previews for local debugging.",
    )

    args = parser.parse_args()
    try:
        run_transformation_pipeline(
            args.raw_data_dir, args.curated_data_dir, debug_mode=args.debug
        )
    except Exception as e:
        logger.error("Data processing pipeline failed with error: %s", e, exc_info=True)
        sys.exit(1)
