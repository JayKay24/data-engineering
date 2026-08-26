from pyspark.sql import DataFrame, SparkSession
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
