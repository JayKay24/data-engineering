# %% [markdown]
# # Kafka JSON to File Ingestion Job
# Extracts user events from a Kafka topic, parses them according to a schema config, and writes them to the local filesystem.

# %%
import argparse
import os
import yaml
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json


# %%
def load_config(path):
    """Loads YAML configuration file."""
    with open(path, "r", encoding="utf-8") as f:
        return yaml.safe_load(f)


# %%
def run_ingestion_job(config_path: str, output_dir: str, is_streaming: bool = True):
    """Runs Spark Ingestion job to extract JSON events from Kafka,

    parses them using a JSON schema, and writes them to output_dir.

    Args:
        config_path (str): Path to the ingestion YAML configuration file.
        output_dir (str): Target directory to save the output JSON events.
        is_streaming (bool): If True, run as Structured Streaming job. If False, run as Batch job.
    """
    config = load_config(config_path)

    # Configuration Validation
    if not isinstance(config, dict) or "data_sources" not in config:
        raise ValueError("Invalid configuration file structure: missing 'data_sources'")

    data_sources = config["data_sources"]
    if not data_sources or not isinstance(data_sources, list):
        raise ValueError("'data_sources' must be a non-empty list")

    source_conf = data_sources[0]
    if "kafka_config" not in source_conf:
        raise ValueError("Missing 'kafka_config' in configuration source")

    kafka_conf = source_conf["kafka_config"]
    if "bootstrap_servers" not in kafka_conf or "topic" not in kafka_conf:
        raise ValueError("Missing 'bootstrap_servers' or 'topic' in 'kafka_config'")

    spark = (
        SparkSession.builder.appName("KafkaJsonToFile")
        .master("local[*]")
        .config(
            "spark.jars.packages",
            "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.8",
        )
        .getOrCreate()
    )

    try:
        kafka_options = {
            "kafka.bootstrap.servers": kafka_conf["bootstrap_servers"],
            "subscribe": kafka_conf["topic"],
            "startingOffsets": kafka_conf.get("starting_offsets", "earliest"),
        }

        json_schema_str = source_conf.get("json_schema")
        if not json_schema_str:
            raise ValueError("Missing 'json_schema' in config")

        if is_streaming:
            # Structured Streaming Read
            df_raw = spark.readStream.format("kafka").options(**kafka_options).load()
            df_json = df_raw.selectExpr("CAST(value AS STRING) as json_str")
            df_parsed = df_json.select(
                from_json(col("json_str"), json_schema_str).alias("data")
            ).select("data.*")

            checkpoint_path = os.path.join(output_dir, "_checkpoint")

            # Structured Streaming Write (using Append mode)
            query = (
                df_parsed.writeStream.outputMode("append")
                .format("json")
                .option("path", output_dir)
                .option("checkpointLocation", checkpoint_path)
                .trigger(processingTime="10 seconds")
                .start()
            )
            try:
                query.awaitTermination()
            except KeyboardInterrupt:
                print("Streaming query interrupted by user. Stopping...")
        else:
            # Batch Read
            df_raw = spark.read.format("kafka").options(**kafka_options).load()
            df_json = df_raw.selectExpr("CAST(value AS STRING) as json_str")
            df_parsed = df_json.select(
                from_json(col("json_str"), json_schema_str).alias("data")
            ).select("data.*")

            # Coalesce to control partition count for small datasets
            df_parsed.coalesce(1).write.mode("overwrite").json(output_dir)
    finally:
        spark.stop()


# %%
if __name__ == "__main__":
    script_dir = os.path.dirname(os.path.abspath(__file__))
    default_config = os.path.join(script_dir, "config/input_config.yml")
    default_output = os.path.join(script_dir, "output_json/user_events")

    parser = argparse.ArgumentParser(
        description="Extract user events from Kafka and save as JSON."
    )
    parser.add_argument(
        "--config",
        type=str,
        default=default_config,
        help="Path to the input configuration YAML file.",
    )
    parser.add_argument(
        "--output",
        type=str,
        default=default_output,
        help="Path to save the output JSON events.",
    )
    parser.add_argument(
        "--batch",
        action="store_true",
        help="Run as a one-time batch job instead of a continuous streaming job.",
    )

    args = parser.parse_args()
    run_ingestion_job(args.config, args.output, is_streaming=not args.batch)
