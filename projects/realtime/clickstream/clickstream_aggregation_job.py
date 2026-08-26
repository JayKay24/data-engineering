import argparse
import os
from pyspark.sql import Column, DataFrame, SparkSession
from pyspark.sql.functions import col, date_format, to_timestamp, window
import yaml

from projects.common.logger import get_logger

logger = get_logger("ClickstreamAggregationJob")


def load_config(path: str) -> dict:
    """Loads and returns YAML configuration file."""
    with open(path, "r", encoding="utf-8") as f:
        return yaml.safe_load(f)


def get_abris_from_avro_column(
    spark: SparkSession,
    data_col: str,
    topic: str,
    schema_registry_url: str,
    is_key: bool = False,
) -> Column:
    """Invokes ABRiS via Py4J Gateway to deserialize Confluent Avro binary payloads

    into structured Spark SQL columns using the Schema Registry.
    """
    jvm = spark._jvm

    abris_registry_config = (
        jvm.za.co.absa.abris.config.AbrisConfig.fromConfluentAvro()
        .downloadReaderSchemaByLatestVersion()
        .andTopicNameStrategy(topic, is_key)
        .usingSchemaRegistry(schema_registry_url)
    )

    scala_abris_func = jvm.za.co.absa.abris.avro.functions

    return Column(
        scala_abris_func.from_avro(
            col(data_col)._jc,
            abris_registry_config,
        )
    )


def init_spark_session(app_name: str = "ClickstreamAggregationJob") -> SparkSession:
    """Initializes SparkSession with Kafka, ABRiS, and Delta Lake connectors."""
    logger.info(
        "Initializing SparkSession with Kafka, ABRiS, and Delta Lake support..."
    )
    spark = (
        SparkSession.builder.appName(app_name)
        .config("spark.jars.repositories", "https://packages.confluent.io/maven/")
        .config(
            "spark.jars.packages",
            "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.8,"
            "org.apache.spark:spark-avro_2.12:3.5.8,"
            "za.co.absa:abris_2.12:6.4.0,"
            "io.delta:delta-spark_2.12:3.2.0",
        )
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    return spark


def build_parsed_stream(
    spark: SparkSession,
    df_raw: DataFrame,
    topic: str,
    schema_registry_url: str,
    timestamp_field: str,
    watermark_delay: str,
) -> DataFrame:
    """Deserializes Avro payload via ABRiS, casts event_time, and applies watermark."""
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
    timestamp_field: str,
    window_duration: str,
    slide_duration: str,
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
    timestamp_field: str,
    window_duration: str,
    slide_duration: str,
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


def run_streaming_pipeline(
    config_path: str,
    sink: str = "delta",
    is_streaming: bool = True,
) -> None:
    """Executes the clickstream aggregation pipeline in streaming or batch mode."""
    config = load_config(config_path)

    kafka_conf = config.get("kafka_config", {})
    sr_conf = config.get("schema_registry", {})
    win_conf = config.get("windowing", {})
    storage_conf = config.get("storage", {})

    bootstrap_servers = os.getenv(
        "KAFKA_BOOTSTRAP_SERVERS", kafka_conf.get("bootstrap_servers", "localhost:9092")
    )
    topic = os.getenv("KAFKA_TOPIC", kafka_conf.get("topic", "clickstream_events"))
    schema_registry_url = os.getenv(
        "SCHEMA_REGISTRY_URL", sr_conf.get("url", "http://localhost:8081")
    )

    timestamp_field = win_conf.get("timestamp_field", "event_time")
    window_duration = win_conf.get("window_duration", "10 seconds")
    slide_duration = win_conf.get("slide_duration", "5 seconds")
    watermark_delay = win_conf.get("watermark_delay", "10 seconds")

    output_mode = storage_conf.get("output_mode", "append")
    trigger_time = storage_conf.get("trigger_processing_time", "5 seconds")
    url_aggregates_path = storage_conf.get(
        "url_aggregates_path", "projects/realtime/clickstream/output_data/url_counts"
    )
    user_aggregates_path = storage_conf.get(
        "user_aggregates_path", "projects/realtime/clickstream/output_data/user_counts"
    )
    checkpoint_base = storage_conf.get(
        "checkpoint_base_dir", "projects/realtime/clickstream/checkpoints"
    )

    spark = init_spark_session()

    try:
        kafka_options = {
            "kafka.bootstrap.servers": bootstrap_servers,
            "subscribe": topic,
            "startingOffsets": kafka_conf.get("starting_offsets", "earliest"),
            "failOnDataLoss": "false",
        }

        if is_streaming:
            logger.info(
                "Starting real-time streaming pipeline from topic '%s'...", topic
            )
            df_raw = spark.readStream.format("kafka").options(**kafka_options).load()

            parsed_stream = build_parsed_stream(
                spark=spark,
                df_raw=df_raw,
                topic=topic,
                schema_registry_url=schema_registry_url,
                timestamp_field=timestamp_field,
                watermark_delay=watermark_delay,
            )

            url_counts_df = aggregate_url_counts(
                parsed_stream, timestamp_field, window_duration, slide_duration
            )
            user_counts_df = aggregate_user_activity(
                parsed_stream, timestamp_field, window_duration, slide_duration
            )

            queries = []
            if sink == "console":
                logger.info("Configuring console streaming sinks...")
                q1 = (
                    url_counts_df.writeStream.outputMode(output_mode)
                    .format("console")
                    .option("truncate", "false")
                    .option(
                        "checkpointLocation",
                        os.path.join(checkpoint_base, "console_urls"),
                    )
                    .trigger(processingTime=trigger_time)
                    .queryName("url_counts_console")
                    .start()
                )
                q2 = (
                    user_counts_df.writeStream.outputMode(output_mode)
                    .format("console")
                    .option("truncate", "false")
                    .option(
                        "checkpointLocation",
                        os.path.join(checkpoint_base, "console_users"),
                    )
                    .trigger(processingTime=trigger_time)
                    .queryName("user_counts_console")
                    .start()
                )
                queries.extend([q1, q2])
            else:
                logger.info(
                    "Configuring Delta Lake sinks at %s and %s...",
                    url_aggregates_path,
                    user_aggregates_path,
                )
                q1 = (
                    url_counts_df.writeStream.outputMode(output_mode)
                    .format("delta")
                    .option("path", url_aggregates_path)
                    .option(
                        "checkpointLocation",
                        os.path.join(checkpoint_base, "delta_urls"),
                    )
                    .trigger(processingTime=trigger_time)
                    .queryName("url_counts_delta")
                    .start()
                )
                q2 = (
                    user_counts_df.writeStream.outputMode(output_mode)
                    .format("delta")
                    .option("path", user_aggregates_path)
                    .option(
                        "checkpointLocation",
                        os.path.join(checkpoint_base, "delta_users"),
                    )
                    .trigger(processingTime=trigger_time)
                    .queryName("user_counts_delta")
                    .start()
                )
                queries.extend([q1, q2])

            logger.info("Streaming queries active. Waiting for termination...")
            for q in queries:
                q.awaitTermination()
        else:
            logger.info("Executing batch processing mode from topic '%s'...", topic)
            df_raw = spark.read.format("kafka").options(**kafka_options).load()

            parsed_df = build_parsed_stream(
                spark=spark,
                df_raw=df_raw,
                topic=topic,
                schema_registry_url=schema_registry_url,
                timestamp_field=timestamp_field,
                watermark_delay=watermark_delay,
            )

            url_counts_df = aggregate_url_counts(
                parsed_df, timestamp_field, window_duration, slide_duration
            )
            user_counts_df = aggregate_user_activity(
                parsed_df, timestamp_field, window_duration, slide_duration
            )

            if sink == "console":
                logger.info("Displaying batch aggregations to console:")
                url_counts_df.show(truncate=False)
                user_counts_df.show(truncate=False)
            else:
                logger.info("Writing batch aggregations to Delta Lake...")
                url_counts_df.write.format("delta").mode("overwrite").save(
                    url_aggregates_path
                )
                user_counts_df.write.format("delta").mode("overwrite").save(
                    user_aggregates_path
                )
                logger.info("Delta Lake batch tables successfully written.")
    except KeyboardInterrupt:
        logger.info("Pipeline interrupted by user. Stopping queries...")
    finally:
        spark.stop()
        logger.info("Spark session successfully terminated.")


if __name__ == "__main__":
    script_dir = os.path.dirname(os.path.abspath(__file__))
    default_config = os.path.join(script_dir, "config/clickstream_config.yml")

    parser = argparse.ArgumentParser(
        description="Real-Time Clickstream Aggregation Spark Structured Streaming Pipeline."
    )
    parser.add_argument(
        "--config",
        type=str,
        default=default_config,
        help="Path to YAML configuration file.",
    )
    parser.add_argument(
        "--sink",
        type=str,
        choices=["delta", "console"],
        default="delta",
        help="Target streaming sink ('delta' for Delta Lake tables, 'console' for stdout debugging).",
    )
    parser.add_argument(
        "--batch",
        action="store_true",
        help="Run as a one-off batch aggregation query instead of continuous streaming.",
    )

    args = parser.parse_args()
    run_streaming_pipeline(
        config_path=args.config,
        sink=args.sink,
        is_streaming=not args.batch,
    )
