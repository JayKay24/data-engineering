"""PySpark Structured Streaming and batch orchestrator for e-commerce KPIs."""

import argparse
import os
import sys
from pyspark.sql import DataFrame, SparkSession
import yaml

# Configure Spark version and worker Python before importing PyDeequ / launching JVM
os.environ.setdefault("SPARK_VERSION", "3.5")
os.environ.setdefault("PYSPARK_PYTHON", sys.executable)
os.environ.setdefault("PYSPARK_DRIVER_PYTHON", sys.executable)

import pydeequ  # noqa: E402

from projects.common.logger import get_logger  # noqa: E402
from projects.ecommerce.streaming_layer.aggregations import build_parsed_stream  # noqa: E402
from projects.ecommerce.streaming_layer.sinks import (  # noqa: E402
    materialize_kpi_sinks_from_silver,
    write_silver_and_quarantine_microbatch,
)

logger = get_logger("EcommerceAggregationJob")


def load_config(path: str) -> dict:
    """Load and parse a YAML pipeline configuration file.

    Args:
        path: Filesystem path to the YAML configuration file.

    Returns:
        Parsed configuration dictionary.
    """
    with open(path, "r", encoding="utf-8") as f:
        return yaml.safe_load(f)


def init_spark_session(
    app_name: str = "EcommerceAggregationJob",
    enable_pydeequ: bool = True,
) -> SparkSession:
    """Initialize a SparkSession with Kafka, ABRiS, Delta Lake, and PyDeequ support.

    Args:
        app_name: Spark application name.
        enable_pydeequ: Whether to attach the Amazon Deequ Maven package and
            exclusions for micro-batch quality verification.

    Returns:
        Configured ``SparkSession`` instance.
    """
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

    shuffle_partitions = os.getenv("SPARK_SHUFFLE_PARTITIONS", "4")
    builder = (
        SparkSession.builder.appName(app_name)
        .config("spark.jars.repositories", "https://packages.confluent.io/maven/")
        .config("spark.jars.packages", ",".join(packages))
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
        .config("spark.databricks.delta.optimizeWrite.enabled", "true")
        .config("spark.databricks.delta.autoCompact.enabled", "true")
        .config("spark.sql.shuffle.partitions", shuffle_partitions)
    )
    ivy_dir = os.getenv("SPARK_IVY_DIR")
    if ivy_dir:
        builder = builder.config("spark.jars.ivy", ivy_dir)

    if enable_pydeequ:
        # Exclude f2j/arpack native coordinates that fail Ivy resolution in some Spark environments.
        builder = builder.config("spark.jars.excludes", pydeequ.f2j_maven_coord)

    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    return spark


def run_pipeline(
    config_path: str,
    sink: str = "delta",
    mode: str = "available-now",
    verify_quality: bool = True,
) -> None:
    """Orchestrate single-pass Kafka ingestion, Medallion Silver/DLQ, and KPI sinks.

    Uses a single Kafka + ABRiS consumer and ``foreachBatch`` micro-batch caching
    so Kafka offsets are read once per batch, verified with PyDeequ, split into
    ``silver_ecommerce_events`` and ``quarantine_events``, and rolled up into
    compacted Delta KPI tables.

    Args:
        config_path: Path to the YAML pipeline configuration file.
        sink: Target sink backend (``"delta"`` or ``"console"``).
        mode: Execution mode: ``"available-now"`` (incremental catch-up micro-batch
            that terminates cleanly), ``"streaming"`` (continuous micro-batches),
            or ``"batch"`` (single bounded read).
        verify_quality: Whether to execute live PyDeequ verification inside
            micro-batch processing.
    """
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
    output_dir = os.getenv(
        "SPARK_OUTPUT_BASE_DIR",
        storage_cfg.get("output_base_dir", "projects/ecommerce/output_data"),
    )
    checkpoint_dir = os.getenv(
        "SPARK_CHECKPOINT_BASE_DIR",
        storage_cfg.get("checkpoint_base_dir", "projects/ecommerce/checkpoints"),
    )
    silver_table = storage_cfg.get("silver_table", "silver_ecommerce_events")
    quarantine_table = storage_cfg.get("quarantine_table", "quarantine_events")
    silver_path = os.path.join(output_dir, silver_table)
    ts_field = win_cfg.get("timestamp_field", "event_time")

    spark = init_spark_session(enable_pydeequ=verify_quality)
    try:
        kafka_opts = {
            "kafka.bootstrap.servers": bootstrap,
            "subscribe": topic,
            "startingOffsets": kafka_cfg.get("starting_offsets", "earliest"),
            # Tolerate topic resets/offset expiration in local dev and demo runs.
            "failOnDataLoss": "false",
        }
        is_stream_source = mode in ("available-now", "streaming")
        df_raw = (
            spark.readStream.format("kafka").options(**kafka_opts).load()
            if is_stream_source
            else spark.read.format("kafka").options(**kafka_opts).load()
        )

        parsed = build_parsed_stream(
            spark,
            df_raw,
            topic,
            sr_url,
            ts_field,
            win_cfg.get("watermark_delay", "10 seconds"),
        )

        if mode == "batch":
            write_silver_and_quarantine_microbatch(
                spark,
                parsed,
                batch_id=0,
                output_base_dir=output_dir,
                enable_pydeequ=verify_quality,
                silver_table=silver_table,
                quarantine_table=quarantine_table,
                timestamp_field=ts_field,
            )
            materialize_kpi_sinks_from_silver(
                spark, silver_path, sink, output_dir, win_cfg
            )
            logger.info("Batch Medallion execution completed successfully.")
            return

        def _process_microbatch(batch_df: DataFrame, batch_id: int) -> None:
            stats = write_silver_and_quarantine_microbatch(
                spark,
                batch_df,
                batch_id=batch_id,
                output_base_dir=output_dir,
                enable_pydeequ=verify_quality,
                silver_table=silver_table,
                quarantine_table=quarantine_table,
                timestamp_field=ts_field,
            )
            # In continuous streaming mode, keep KPI Delta tables fresh after new Silver commits.
            if mode == "streaming" and stats["valid_rows"] > 0:
                materialize_kpi_sinks_from_silver(
                    spark, silver_path, sink, output_dir, win_cfg
                )

        os.makedirs(checkpoint_dir, exist_ok=True)
        silver_checkpoint = os.path.join(checkpoint_dir, "silver_ingest")
        writer = (
            parsed.writeStream.foreachBatch(_process_microbatch)
            .option("checkpointLocation", silver_checkpoint)
            .queryName("ecommerce_silver_medallion")
        )
        if mode == "available-now":
            query = writer.trigger(availableNow=True).start()
            logger.info(
                "Started incremental availableNow Silver ingestion (checkpoint=%s)...",
                silver_checkpoint,
            )
            query.awaitTermination()
            materialize_kpi_sinks_from_silver(
                spark, silver_path, sink, output_dir, win_cfg
            )
            logger.info("availableNow Medallion ingestion and KPI rollup complete.")
        else:
            trigger_interval = storage_cfg.get("trigger_processing_time", "30 seconds")
            query = writer.trigger(processingTime=trigger_interval).start()
            logger.info(
                "Started continuous Medallion streaming (trigger=%s, checkpoint=%s)...",
                trigger_interval,
                silver_checkpoint,
            )
            query.awaitTermination()
    except KeyboardInterrupt:
        logger.info("Pipeline terminated by user.")
    finally:
        if verify_quality:
            try:
                spark.sparkContext._gateway.shutdown_callback_server()
            except Exception:
                pass
        spark.stop()


if __name__ == "__main__":
    script_dir = os.path.dirname(os.path.abspath(__file__))
    parser = argparse.ArgumentParser(
        description="E-Commerce Real-Time Medallion Aggregation Pipeline."
    )
    parser.add_argument(
        "--config", default=os.path.join(script_dir, "config/ecommerce_config.yml")
    )
    parser.add_argument("--sink", choices=["delta", "console"], default="delta")
    parser.add_argument(
        "--mode",
        choices=["available-now", "streaming", "batch"],
        default="available-now",
        help="Execution mode: incremental availableNow (default), continuous streaming, or bounded batch.",
    )
    parser.add_argument(
        "--batch",
        action="store_true",
        help="Backward-compatible alias for --mode batch.",
    )
    parser.add_argument(
        "--verify-quality",
        dest="verify_quality",
        action="store_true",
        default=True,
        help="Run PyDeequ verification inside micro-batch processing (default: enabled).",
    )
    parser.add_argument(
        "--no-verify-quality",
        dest="verify_quality",
        action="store_false",
        help="Disable PyDeequ verification during ingestion.",
    )
    args = parser.parse_args()

    effective_mode = "batch" if args.batch else args.mode
    run_pipeline(
        args.config,
        sink=args.sink,
        mode=effective_mode,
        verify_quality=args.verify_quality,
    )
    if args.verify_quality:
        # PyDeequ leaves JVM callback daemon threads alive after spark.stop(); force clean exit.
        sys.stdout.flush()
        sys.stderr.flush()
        os._exit(0)
