"""Unit tests for Spark Medallion splitting, quarantine DLQ, and tumbling KPIs."""

import os
import subprocess

from pathlib import Path

from delta import configure_spark_with_delta_pip
import duckdb
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_timestamp
from pyspark.sql.types import (
    DoubleType,
    StringType,
    StructField,
    StructType,
)
import pytest

from projects.ecommerce.streaming_layer.aggregations import (
    aggregate_category_sales,
    aggregate_product_sales,
    aggregate_session_funnels,
    aggregate_url_conversion,
    split_valid_and_quarantine_events,
)
from projects.ecommerce.streaming_layer.sinks import (
    materialize_kpi_sinks_from_silver,
    write_silver_and_quarantine_microbatch,
)


def _ensure_java17_home() -> None:
    """Resolve Java 17 JAVA_HOME automatically on macOS/Linux when unset."""
    if os.environ.get("JAVA_HOME"):
        return
    try:
        java_home = subprocess.check_output(
            ["/usr/libexec/java_home", "-v", "17"], text=True
        ).strip()
        if java_home:
            os.environ["JAVA_HOME"] = java_home
    except (OSError, subprocess.CalledProcessError):
        pass


@pytest.fixture(scope="module")
def spark() -> SparkSession:
    """Provide a lightweight local SparkSession configured with Delta Lake."""
    _ensure_java17_home()
    builder = (
        SparkSession.builder.master("local[2]")
        .appName("EcommerceAggregationUnitTests")
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.ui.enabled", "false")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
    )
    session = configure_spark_with_delta_pip(builder).getOrCreate()
    session.sparkContext.setLogLevel("ERROR")
    yield session
    session.stop()


EVENT_SCHEMA = StructType(
    [
        StructField("user_id", StringType(), True),
        StructField("url", StringType(), True),
        StructField("event_type", StringType(), True),
        StructField("event_time", StringType(), True),
        StructField("product_id", StringType(), True),
        StructField("category", StringType(), True),
        StructField("price", DoubleType(), True),
        StructField("ip_address", StringType(), True),
        StructField("user_agent", StringType(), True),
    ]
)


def test_split_valid_and_quarantine_events(spark: SparkSession) -> None:
    """Verify valid events enter Silver while domain violations route to quarantine.

    Args:
        spark: Local SparkSession fixture.
    """
    rows = [
        (
            "user_101",
            "/products/laptop",
            "view",
            "2026-09-01T10:00:01",
            "prod_01",
            "electronics",
            1200.0,
            "10.0.0.1",
            "UA",
        ),
        (
            "user_101",
            "/checkout",
            "purchase",
            "2026-09-01T10:00:05",
            "prod_01",
            "electronics",
            1200.0,
            "10.0.0.1",
            "UA",
        ),
        (
            "user_102",
            "/checkout",
            "purchase",
            "2026-09-01T10:00:06",
            "prod_02",
            "apparel",
            -25.0,
            "10.0.0.2",
            "UA",
        ),
        (
            "user_103",
            "/home",
            "refund_err",
            "2026-09-01T10:00:07",
            None,
            None,
            None,
            "10.0.0.3",
            "UA",
        ),
        (
            None,
            "/cart",
            "view",
            "2026-09-01T10:00:08",
            None,
            None,
            None,
            "10.0.0.4",
            "UA",
        ),
    ]
    df = spark.createDataFrame(rows, schema=EVENT_SCHEMA).withColumn(
        "event_time", to_timestamp(col("event_time"))
    )

    valid_df, quarantine_df = split_valid_and_quarantine_events(df)

    assert valid_df.count() == 2
    assert "event_date" in valid_df.columns

    q_rows = {r["url"]: r["quarantine_reason"] for r in quarantine_df.collect()}
    assert len(q_rows) == 3
    assert "negative_price" in q_rows["/checkout"]
    assert "invalid_event_type" in q_rows["/home"]
    assert "missing_user_id" in q_rows["/cart"]


def test_tumbling_window_prevents_revenue_double_counting(spark: SparkSession) -> None:
    """Assert 10s tumbling windows preserve exact purchase totals with zero inflation.

    Args:
        spark: Local SparkSession fixture.
    """
    rows = [
        (
            "user_101",
            "/checkout",
            "purchase",
            "2026-09-01T10:00:02",
            "prod_01",
            "electronics",
            1200.0,
            "10.0.0.1",
            "UA",
        ),
        (
            "user_102",
            "/checkout",
            "purchase",
            "2026-09-01T10:00:07",
            "prod_04",
            "electronics",
            800.0,
            "10.0.0.2",
            "UA",
        ),
        (
            "user_103",
            "/checkout",
            "purchase",
            "2026-09-01T10:00:13",
            "prod_02",
            "apparel",
            85.0,
            "10.0.0.3",
            "UA",
        ),
    ]
    df = spark.createDataFrame(rows, schema=EVENT_SCHEMA).withColumn(
        "event_time", to_timestamp(col("event_time"))
    )

    cat_df = aggregate_category_sales(df)
    cat_rows = cat_df.collect()
    total_cat_rev = round(sum(r["revenue"] for r in cat_rows), 2)
    total_cat_units = sum(r["units"] for r in cat_rows)

    prod_df = aggregate_product_sales(df)
    total_prod_rev = round(sum(r["revenue"] for r in prod_df.collect()), 2)

    assert total_cat_rev == 2085.0
    assert total_prod_rev == 2085.0
    assert total_cat_units == 3


def test_conversion_and_session_funnels(spark: SparkSession) -> None:
    """Verify URL conversion ratios and user session funnel progression flags.

    Args:
        spark: Local SparkSession fixture.
    """
    rows = [
        (
            "user_101",
            "/products/shoes",
            "view",
            "2026-09-01T10:00:01",
            "prod_02",
            "apparel",
            85.0,
            "10.0.0.1",
            "UA",
        ),
        (
            "user_101",
            "/products/shoes",
            "add_to_cart",
            "2026-09-01T10:00:02",
            "prod_02",
            "apparel",
            85.0,
            "10.0.0.1",
            "UA",
        ),
        (
            "user_101",
            "/products/shoes",
            "purchase",
            "2026-09-01T10:00:03",
            "prod_02",
            "apparel",
            85.0,
            "10.0.0.1",
            "UA",
        ),
        (
            "user_102",
            "/products/shoes",
            "view",
            "2026-09-01T10:00:04",
            "prod_02",
            "apparel",
            85.0,
            "10.0.0.2",
            "UA",
        ),
    ]
    df = spark.createDataFrame(rows, schema=EVENT_SCHEMA).withColumn(
        "event_time", to_timestamp(col("event_time"))
    )

    conv_row = aggregate_url_conversion(df).collect()[0]
    assert conv_row["view_count"] == 2
    assert conv_row["purchase_count"] == 1
    assert conv_row["conversion_rate"] == 0.5

    funnels = {r["user_id"]: r for r in aggregate_session_funnels(df).collect()}
    assert funnels["user_101"]["has_view"] is True
    assert funnels["user_101"]["has_add"] is True
    assert funnels["user_101"]["has_purchase"] is True
    assert funnels["user_102"]["has_purchase"] is False


def test_silver_quarantine_delta_roundtrip_with_duckdb_delta_scan(
    spark: SparkSession, tmp_path: Path
) -> None:
    """Verify Spark Delta Lake Silver/Quarantine/KPIs read cleanly via DuckDB delta_scan().

    Args:
        spark: Local SparkSession fixture.
        tmp_path: Pytest temporary directory fixture.
    """
    out_dir = str(tmp_path / "output_data")
    clean_rows = [
        (
            "user_101",
            "/checkout",
            "purchase",
            "2026-09-01T10:00:05",
            "prod_01",
            "electronics",
            1200.0,
            "10.0.0.1",
            "UA",
        ),
    ]
    df_clean = spark.createDataFrame(clean_rows, schema=EVENT_SCHEMA).withColumn(
        "event_time", to_timestamp(col("event_time"))
    )

    # First batch has 0 invalid rows; quarantine_events must still initialize a valid Delta log.
    stats_1 = write_silver_and_quarantine_microbatch(
        spark, df_clean, batch_id=0, output_base_dir=out_dir, enable_pydeequ=False
    )
    assert stats_1 == {"total_rows": 1, "valid_rows": 1, "quarantine_rows": 0}

    mixed_rows = [
        (
            "user_102",
            "/checkout",
            "purchase",
            "2026-09-01T10:00:15",
            "prod_04",
            "electronics",
            800.0,
            "10.0.0.2",
            "UA",
        ),
        (
            "user_103",
            "/checkout",
            "purchase",
            "2026-09-01T10:00:16",
            "prod_02",
            "apparel",
            -10.0,
            "10.0.0.3",
            "UA",
        ),
    ]
    df_mixed = spark.createDataFrame(mixed_rows, schema=EVENT_SCHEMA).withColumn(
        "event_time", to_timestamp(col("event_time"))
    )
    stats_2 = write_silver_and_quarantine_microbatch(
        spark, df_mixed, batch_id=1, output_base_dir=out_dir, enable_pydeequ=False
    )
    assert stats_2 == {"total_rows": 2, "valid_rows": 1, "quarantine_rows": 1}

    silver_path = os.path.join(out_dir, "silver_ecommerce_events")
    materialize_kpi_sinks_from_silver(
        spark,
        silver_path=silver_path,
        sink_type="delta",
        output_base_dir=out_dir,
        win_cfg={"window_duration": "10 seconds", "slide_duration": "10 seconds"},
    )

    with duckdb.connect() as conn:
        conn.execute("INSTALL delta; LOAD delta;")
        silver_cnt = conn.execute(
            "SELECT COUNT(*) FROM delta_scan(?)", [silver_path]
        ).fetchone()[0]
        quarantine_reasons = conn.execute(
            "SELECT quarantine_reason FROM delta_scan(?)",
            [os.path.join(out_dir, "quarantine_events")],
        ).fetchall()
        cat_rev = conn.execute(
            "SELECT SUM(revenue) FROM delta_scan(?)",
            [os.path.join(out_dir, "category_sales")],
        ).fetchone()[0]

    assert silver_cnt == 2
    assert quarantine_reasons == [("negative_price",)]
    assert cat_rev == 2000.0
