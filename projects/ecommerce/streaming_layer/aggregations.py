"""PySpark Structured Streaming transformations for e-commerce KPI aggregations."""

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.functions import col, date_format, to_timestamp, window

from projects.common.kafka_avro import get_abris_from_avro_column


def build_parsed_stream(
    spark: SparkSession,
    df_raw: DataFrame,
    topic: str,
    schema_registry_url: str,
    timestamp_field: str = "event_time",
    watermark_delay: str = "10 seconds",
) -> DataFrame:
    """Deserialize Avro Kafka payloads via ABRiS, cast timestamps, and watermark.

    Args:
        spark: Active SparkSession providing the Py4J JVM gateway.
        df_raw: Raw Kafka source DataFrame containing binary ``value`` payloads.
        topic: Kafka topic name used for Schema Registry subject lookup.
        schema_registry_url: Base URL of the Confluent Schema Registry.
        timestamp_field: Name of the event timestamp column to cast and
            watermark.
        watermark_delay: Spark watermark delay threshold (e.g., ``"10 seconds"``).

    Returns:
        Parsed and watermarked Spark DataFrame with flattened Avro columns.
    """
    df_deserialized = df_raw.select(
        get_abris_from_avro_column(
            spark,
            data_col="value",
            topic=topic,
            schema_registry_url=schema_registry_url,
        ).alias("data")
    ).select("data.*")

    return df_deserialized.withColumn(
        timestamp_field, to_timestamp(col(timestamp_field))
    ).withWatermark(timestamp_field, watermark_delay)


def aggregate_url_counts(
    df: DataFrame,
    timestamp_field: str = "event_time",
    window_duration: str = "10 seconds",
    slide_duration: str = "5 seconds",
) -> DataFrame:
    """Calculate sliding-window event counts grouped by URL and event type.

    Args:
        df: Parsed and watermarked e-commerce event DataFrame.
        timestamp_field: Name of the event timestamp column for windowing.
        window_duration: Tumbling or sliding window length (e.g., ``"10 seconds"``).
        slide_duration: Window slide interval (e.g., ``"5 seconds"``).

    Returns:
        DataFrame with ``window_start``, ``window_end``, ``url``, ``event_type``,
        and ``count`` columns.
    """
    return (
        df.groupBy(
            window(col(timestamp_field), window_duration, slide_duration),
            col("url"),
            col("event_type"),
        )
        .count()
        .select(
            date_format("window.start", "yyyy-MM-dd HH:mm:ss").alias("window_start"),
            date_format("window.end", "yyyy-MM-dd HH:mm:ss").alias("window_end"),
            col("url"),
            col("event_type"),
            col("count"),
        )
    )


def aggregate_user_activity(
    df: DataFrame,
    timestamp_field: str = "event_time",
    window_duration: str = "10 seconds",
    slide_duration: str = "5 seconds",
) -> DataFrame:
    """Calculate sliding-window event counts grouped by ``user_id``.

    Args:
        df: Parsed and watermarked e-commerce event DataFrame.
        timestamp_field: Name of the event timestamp column for windowing.
        window_duration: Sliding window length (e.g., ``"10 seconds"``).
        slide_duration: Window slide interval (e.g., ``"5 seconds"``).

    Returns:
        DataFrame with ``window_start``, ``window_end``, ``user_id``, and
        ``total_events`` columns.
    """
    return (
        df.groupBy(
            window(col(timestamp_field), window_duration, slide_duration),
            col("user_id"),
        )
        .count()
        .select(
            date_format("window.start", "yyyy-MM-dd HH:mm:ss").alias("window_start"),
            date_format("window.end", "yyyy-MM-dd HH:mm:ss").alias("window_end"),
            col("user_id"),
            col("count").alias("total_events"),
        )
    )


def aggregate_url_conversion(
    df: DataFrame,
    timestamp_field: str = "event_time",
    window_duration: str = "10 seconds",
    slide_duration: str = "5 seconds",
) -> DataFrame:
    """Calculate sliding-window conversion rate (purchases / views) per URL.

    Args:
        df: Parsed and watermarked e-commerce event DataFrame.
        timestamp_field: Name of the event timestamp column for windowing.
        window_duration: Sliding window length (e.g., ``"10 seconds"``).
        slide_duration: Window slide interval (e.g., ``"5 seconds"``).

    Returns:
        DataFrame with ``window_start``, ``window_end``, ``url``, ``view_count``,
        ``purchase_count``, and ``conversion_rate`` columns.
    """
    counts = df.groupBy(
        window(col(timestamp_field), window_duration, slide_duration),
        col("url"),
        col("event_type"),
    ).count()

    wide = (
        counts.groupBy("window", "url")
        .pivot("event_type", ["view", "purchase"])
        .agg(F.first("count"))
        .na.fill(0)
    )

    return wide.withColumn(
        "conversion_rate",
        F.when(col("view") > 0, col("purchase") / col("view")).otherwise(F.lit(0.0)),
    ).select(
        date_format("window.start", "yyyy-MM-dd HH:mm:ss").alias("window_start"),
        date_format("window.end", "yyyy-MM-dd HH:mm:ss").alias("window_end"),
        col("url"),
        col("view").alias("view_count"),
        col("purchase").alias("purchase_count"),
        F.round(col("conversion_rate"), 4).alias("conversion_rate"),
    )


def aggregate_category_sales(
    df: DataFrame,
    timestamp_field: str = "event_time",
    window_duration: str = "10 seconds",
    slide_duration: str = "5 seconds",
) -> DataFrame:
    """Aggregate purchase revenue and unit volume per product category.

    Args:
        df: Parsed and watermarked e-commerce event DataFrame.
        timestamp_field: Name of the event timestamp column for windowing.
        window_duration: Sliding window length (e.g., ``"10 seconds"``).
        slide_duration: Window slide interval (e.g., ``"5 seconds"``).

    Returns:
        DataFrame with ``window_start``, ``window_end``, ``category``,
        ``revenue``, and ``units`` columns.
    """
    purchases = df.filter(col("event_type") == "purchase")
    return (
        purchases.groupBy(
            window(col(timestamp_field), window_duration, slide_duration),
            col("category"),
        )
        .agg(
            F.sum(F.coalesce(col("price"), F.lit(0.0))).alias("revenue"),
            F.count(F.lit(1)).alias("units"),
        )
        .select(
            date_format("window.start", "yyyy-MM-dd HH:mm:ss").alias("window_start"),
            date_format("window.end", "yyyy-MM-dd HH:mm:ss").alias("window_end"),
            col("category"),
            F.round(col("revenue"), 2).alias("revenue"),
            col("units"),
        )
    )


def aggregate_cart_metrics(
    df: DataFrame,
    timestamp_field: str = "event_time",
    window_duration: str = "10 seconds",
    slide_duration: str = "5 seconds",
) -> DataFrame:
    """Calculate add-to-cart rate and cart abandonment rate by URL.

    Args:
        df: Parsed and watermarked e-commerce event DataFrame.
        timestamp_field: Name of the event timestamp column for windowing.
        window_duration: Sliding window length (e.g., ``"10 seconds"``).
        slide_duration: Window slide interval (e.g., ``"5 seconds"``).

    Returns:
        DataFrame with view, add-to-cart, and purchase counts alongside
        ``add_to_cart_rate`` and ``cart_abandonment`` ratios.
    """
    counts = df.groupBy(
        window(col(timestamp_field), window_duration, slide_duration),
        col("url"),
        col("event_type"),
    ).count()

    wide = (
        counts.groupBy("window", "url")
        .pivot("event_type", ["view", "add_to_cart", "purchase"])
        .agg(F.first("count"))
        .na.fill(0)
    )

    return (
        wide.withColumn(
            "add_to_cart_rate",
            F.when(col("view") > 0, col("add_to_cart") / col("view")).otherwise(
                F.lit(0.0)
            ),
        )
        .withColumn(
            "cart_abandonment",
            F.when(
                col("add_to_cart") > 0,
                (col("add_to_cart") - col("purchase")) / col("add_to_cart"),
            ).otherwise(F.lit(0.0)),
        )
        .select(
            date_format("window.start", "yyyy-MM-dd HH:mm:ss").alias("window_start"),
            date_format("window.end", "yyyy-MM-dd HH:mm:ss").alias("window_end"),
            col("url"),
            col("view").alias("view_count"),
            col("add_to_cart").alias("add_to_cart_count"),
            col("purchase").alias("purchase_count"),
            F.round(col("add_to_cart_rate"), 4).alias("add_to_cart_rate"),
            F.round(col("cart_abandonment"), 4).alias("cart_abandonment"),
        )
    )


def aggregate_session_funnels(
    df: DataFrame,
    timestamp_field: str = "event_time",
    session_gap_seconds: int = 900,
) -> DataFrame:
    """Evaluate session funnel progression (view -> add_to_cart -> purchase).

    Args:
        df: Parsed and watermarked e-commerce event DataFrame.
        timestamp_field: Name of the event timestamp column for sessionization.
        session_gap_seconds: Inactivity gap in seconds defining a session
            boundary.

    Returns:
        DataFrame with ``session_start``, ``session_end``, ``user_id``, and
        boolean flags ``has_view``, ``has_add``, and ``has_purchase``.
    """
    return (
        df.groupBy(
            F.session_window(col(timestamp_field), f"{session_gap_seconds} seconds"),
            col("user_id"),
        )
        .agg(F.collect_set(col("event_type")).alias("events"))
        .select(
            date_format(col("session_window.start"), "yyyy-MM-dd HH:mm:ss").alias(
                "session_start"
            ),
            date_format(col("session_window.end"), "yyyy-MM-dd HH:mm:ss").alias(
                "session_end"
            ),
            col("user_id"),
            F.array_contains(col("events"), "view").alias("has_view"),
            F.array_contains(col("events"), "add_to_cart").alias("has_add"),
            F.array_contains(col("events"), "purchase").alias("has_purchase"),
        )
    )


def aggregate_top_urls_per_user(
    df: DataFrame,
    timestamp_field: str = "event_time",
    window_duration: str = "10 seconds",
    slide_duration: str = "5 seconds",
) -> DataFrame:
    """Compute windowed user-URL visit counts for Top-N ranking.

    Args:
        df: Parsed and watermarked e-commerce event DataFrame.
        timestamp_field: Name of the event timestamp column for windowing.
        window_duration: Sliding window length (e.g., ``"10 seconds"``).
        slide_duration: Window slide interval (e.g., ``"5 seconds"``).

    Returns:
        DataFrame with ``window_start``, ``window_end``, ``user_id``, ``url``,
        and ``count`` columns.
    """
    return (
        df.groupBy(
            window(col(timestamp_field), window_duration, slide_duration),
            col("user_id"),
            col("url"),
        )
        .count()
        .select(
            date_format("window.start", "yyyy-MM-dd HH:mm:ss").alias("window_start"),
            date_format("window.end", "yyyy-MM-dd HH:mm:ss").alias("window_end"),
            col("user_id"),
            col("url"),
            col("count"),
        )
    )


def aggregate_product_sales(
    df: DataFrame,
    timestamp_field: str = "event_time",
    window_duration: str = "10 seconds",
    slide_duration: str = "5 seconds",
) -> DataFrame:
    """Aggregate purchase revenue and units by user, product, and category.

    Produces a fact-style windowed table suitable for joining against OLTP
    customer and product dimensions in the dbt Gold layer.

    Args:
        df: Parsed and watermarked e-commerce event DataFrame.
        timestamp_field: Name of the event timestamp column for windowing.
        window_duration: Sliding window length (e.g., ``"10 seconds"``).
        slide_duration: Window slide interval (e.g., ``"5 seconds"``).

    Returns:
        DataFrame with ``window_start``, ``window_end``, ``user_id``,
        ``product_id``, ``category``, ``revenue``, and ``units`` columns.
    """
    purchases = df.filter(
        (col("event_type") == "purchase") & col("product_id").isNotNull()
    )
    return (
        purchases.groupBy(
            window(col(timestamp_field), window_duration, slide_duration),
            col("user_id"),
            col("product_id"),
            col("category"),
        )
        .agg(
            F.sum(F.coalesce(col("price"), F.lit(0.0))).alias("revenue"),
            F.count(F.lit(1)).alias("units"),
        )
        .select(
            date_format("window.start", "yyyy-MM-dd HH:mm:ss").alias("window_start"),
            date_format("window.end", "yyyy-MM-dd HH:mm:ss").alias("window_end"),
            col("user_id"),
            col("product_id"),
            col("category"),
            F.round(col("revenue"), 2).alias("revenue"),
            col("units"),
        )
    )
