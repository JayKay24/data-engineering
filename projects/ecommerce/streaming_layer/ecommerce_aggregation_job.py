import argparse
import os
from pyspark.sql import SparkSession
import yaml

# Configure Spark version before importing PyDeequ
os.environ.setdefault("SPARK_VERSION", "3.5")

import pydeequ  # noqa: E402

from projects.common.logger import get_logger  # noqa: E402
from projects.ecommerce.streaming_layer.aggregations import (  # noqa: E402
    aggregate_cart_metrics,
    aggregate_category_sales,
    aggregate_product_sales,
    aggregate_session_funnels,
    aggregate_top_urls_per_user,
    aggregate_url_conversion,
    aggregate_url_counts,
    aggregate_user_activity,
    build_parsed_stream,
)
from projects.ecommerce.streaming_layer.quality_checks import (  # noqa: E402
    run_pydeequ_verification,
)
from projects.ecommerce.streaming_layer.sinks import (  # noqa: E402
    write_batch_sinks,
    write_streaming_sinks,
)

logger = get_logger("EcommerceAggregationJob")


def load_config(path: str) -> dict:
    """Loads YAML configuration file."""
    with open(path, "r", encoding="utf-8") as f:
        return yaml.safe_load(f)


def init_spark_session(
    app_name: str = "EcommerceAggregationJob",
    enable_pydeequ: bool = False,
) -> SparkSession:
    """Initializes SparkSession with Kafka, ABRiS, Delta Lake, and optional PyDeequ connectors."""
    logger.info(
        "Initializing SparkSession with Kafka, ABRiS, and Delta Lake support..."
    )
    packages = [
        "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.8",
        "org.apache.spark:spark-avro_2.12:3.5.8",
        "za.co.absa:abris_2.12:6.4.0",
        "io.delta:delta-spark_2.12:3.2.0",
    ]
    if enable_pydeequ:
        packages.append(pydeequ.deequ_maven_coord)

    builder = (
        SparkSession.builder.appName(app_name)
        .config("spark.jars.repositories", "https://packages.confluent.io/maven/")
        .config("spark.jars.packages", ",".join(packages))
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
    )
    if enable_pydeequ:
        builder = builder.config("spark.jars.excludes", pydeequ.f2j_maven_coord)

    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    return spark


def run_pipeline(
    config_path: str,
    sink: str = "delta",
    is_streaming: bool = True,
    verify_quality: bool = False,
) -> None:
    """Orchestrates ingestion, multi-metric KPI aggregations, quality checks, and Delta Lake persistence."""
    cfg = load_config(config_path)
    kafka_cfg, sr_cfg = cfg.get("kafka_config", {}), cfg.get("schema_registry", {})
    win_cfg, storage_cfg = cfg.get("windowing", {}), cfg.get("storage", {})

    bootstrap = os.getenv(
        "KAFKA_BOOTSTRAP_SERVERS", kafka_cfg.get("bootstrap_servers", "localhost:9092")
    )
    topic = os.getenv("KAFKA_TOPIC", kafka_cfg.get("topic", "ecommerce_events"))
    sr_url = os.getenv(
        "SCHEMA_REGISTRY_URL", sr_cfg.get("url", "http://localhost:8081")
    )

    run_deequ = (not is_streaming) or verify_quality
    spark = init_spark_session(enable_pydeequ=run_deequ)
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

        output_dir = os.getenv(
            "SPARK_OUTPUT_BASE_DIR",
            storage_cfg.get("output_base_dir", "projects/ecommerce/output_data"),
        )
        checkpoint_dir = os.getenv(
            "SPARK_CHECKPOINT_BASE_DIR",
            storage_cfg.get("checkpoint_base_dir", "projects/ecommerce/checkpoints"),
        )

        if run_deequ and not is_streaming:
            run_pydeequ_verification(spark, parsed, output_dir)

        ts_f = win_cfg.get("timestamp_field", "event_time")
        win_d = win_cfg.get("window_duration", "10 seconds")
        slide_d = win_cfg.get("slide_duration", "5 seconds")
        gap_sec = win_cfg.get("session_gap_seconds", 900)

        # Build KPI aggregation streams
        streams = {
            "url_counts": aggregate_url_counts(parsed, ts_f, win_d, slide_d),
            "user_counts": aggregate_user_activity(parsed, ts_f, win_d, slide_d),
            "url_conversion": aggregate_url_conversion(parsed, ts_f, win_d, slide_d),
            "category_sales": aggregate_category_sales(parsed, ts_f, win_d, slide_d),
            "product_sales": aggregate_product_sales(parsed, ts_f, win_d, slide_d),
            "cart_metrics": aggregate_cart_metrics(parsed, ts_f, win_d, slide_d),
            "top_urls_per_user": aggregate_top_urls_per_user(
                parsed, ts_f, win_d, slide_d
            ),
            "session_funnels": aggregate_session_funnels(parsed, ts_f, gap_sec),
        }

        if is_streaming:
            queries = write_streaming_sinks(
                streams,
                sink,
                storage_cfg.get("output_mode", "append"),
                storage_cfg.get("trigger_processing_time", "5 seconds"),
                output_dir,
                checkpoint_dir,
            )
            logger.info(
                "Active streaming queries: %d (%s). Monitoring with fail-fast termination...",
                len(queries),
                ", ".join(streams.keys()),
            )
            spark.streams.awaitAnyTermination()
        else:
            write_batch_sinks(streams, sink, output_dir)
            logger.info("Batch execution completed successfully.")
    except KeyboardInterrupt:
        logger.info("Pipeline terminated by user.")
    finally:
        spark.stop()


if __name__ == "__main__":
    script_dir = os.path.dirname(os.path.abspath(__file__))
    parser = argparse.ArgumentParser(
        description="E-Commerce Real-Time Streaming Aggregation Pipeline."
    )
    parser.add_argument(
        "--config", default=os.path.join(script_dir, "config/ecommerce_config.yml")
    )
    parser.add_argument("--sink", choices=["delta", "console"], default="delta")
    parser.add_argument("--batch", action="store_true")
    parser.add_argument(
        "--verify-quality",
        action="store_true",
        help="Run PyDeequ verification on bounded batch input.",
    )
    args = parser.parse_args()

    run_pipeline(
        args.config,
        sink=args.sink,
        is_streaming=not args.batch,
        verify_quality=args.verify_quality,
    )
