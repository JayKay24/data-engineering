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
    """Deserializes Avro Kafka payloads via ABRiS, casts timestamps, and applies watermark."""
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
    """Calculates sliding window event counts grouped by URL and event_type."""
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
    """Calculates sliding window event counts grouped by user_id."""
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
    """Calculates sliding window conversion rate (purchases / views) per URL."""
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
    """Aggregates revenue and unit volume per product category from purchases."""
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
    """Calculates add-to-cart rate and cart abandonment rate by URL."""
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
    """Evaluates session funnel progression (view -> add_to_cart -> purchase) per user session."""
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
    """Computes base user-url visit counts for ranked Top-N extraction."""
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
