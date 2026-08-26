import argparse
import os
from pyspark.sql import SparkSession
import yaml

from projects.common.logger import get_logger
from projects.realtime.clickstream.aggregations import (
    aggregate_url_counts,
    aggregate_user_activity,
    build_parsed_stream,
)
from projects.realtime.clickstream.sinks import write_batch_sinks, write_streaming_sinks

logger = get_logger("ClickstreamAggregationJob")


def load_config(path: str) -> dict:
    """Loads YAML configuration file."""
    with open(path, "r", encoding="utf-8") as f:
        return yaml.safe_load(f)


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


def run_pipeline(
    config_path: str, sink: str = "delta", is_streaming: bool = True
) -> None:
    """Orchestrates ingestion, aggregation, and sink persistence."""
    cfg = load_config(config_path)
    kafka_cfg, sr_cfg = cfg.get("kafka_config", {}), cfg.get("schema_registry", {})
    win_cfg, storage_cfg = cfg.get("windowing", {}), cfg.get("storage", {})

    bootstrap = os.getenv(
        "KAFKA_BOOTSTRAP_SERVERS", kafka_cfg.get("bootstrap_servers", "localhost:9092")
    )
    topic = os.getenv("KAFKA_TOPIC", kafka_cfg.get("topic", "clickstream_events"))
    sr_url = os.getenv(
        "SCHEMA_REGISTRY_URL", sr_cfg.get("url", "http://localhost:8081")
    )

    spark = init_spark_session()
    try:
        kafka_opts = {
            "kafka.bootstrap.servers": bootstrap,
            "subscribe": topic,
            "startingOffsets": kafka_cfg.get("starting_offsets", "earliest"),
            "failOnDataLoss": "false",
        }
        df_raw = (
            spark.readStream.format("kafka").options(**kafka_opts).load()
            if is_streaming
            else spark.read.format("kafka").options(**kafka_opts).load()
        )

        parsed = build_parsed_stream(
            spark,
            df_raw,
            topic,
            sr_url,
            win_cfg.get("timestamp_field", "event_time"),
            win_cfg.get("watermark_delay", "10 seconds"),
        )
        url_df = aggregate_url_counts(
            parsed,
            win_cfg.get("timestamp_field", "event_time"),
            win_cfg.get("window_duration", "10 seconds"),
            win_cfg.get("slide_duration", "5 seconds"),
        )
        user_df = aggregate_user_activity(
            parsed,
            win_cfg.get("timestamp_field", "event_time"),
            win_cfg.get("window_duration", "10 seconds"),
            win_cfg.get("slide_duration", "5 seconds"),
        )

        if is_streaming:
            queries = write_streaming_sinks(
                url_df,
                user_df,
                sink,
                storage_cfg.get("output_mode", "append"),
                storage_cfg.get("trigger_processing_time", "5 seconds"),
                storage_cfg.get(
                    "url_aggregates_path",
                    "projects/realtime/clickstream/output_data/url_counts",
                ),
                storage_cfg.get(
                    "user_aggregates_path",
                    "projects/realtime/clickstream/output_data/user_counts",
                ),
                storage_cfg.get(
                    "checkpoint_base_dir", "projects/realtime/clickstream/checkpoints"
                ),
            )
            for q in queries:
                q.awaitTermination()
        else:
            write_batch_sinks(
                url_df,
                user_df,
                sink,
                storage_cfg.get(
                    "url_aggregates_path",
                    "projects/realtime/clickstream/output_data/url_counts",
                ),
                storage_cfg.get(
                    "user_aggregates_path",
                    "projects/realtime/clickstream/output_data/user_counts",
                ),
            )
    except KeyboardInterrupt:
        logger.info("Pipeline stopped by user.")
    finally:
        spark.stop()


if __name__ == "__main__":
    script_dir = os.path.dirname(os.path.abspath(__file__))
    parser = argparse.ArgumentParser(
        description="Real-Time Clickstream Aggregation Pipeline."
    )
    parser.add_argument(
        "--config", default=os.path.join(script_dir, "config/clickstream_config.yml")
    )
    parser.add_argument("--sink", choices=["delta", "console"], default="delta")
    parser.add_argument("--batch", action="store_true")
    args = parser.parse_args()

    run_pipeline(args.config, sink=args.sink, is_streaming=not args.batch)
