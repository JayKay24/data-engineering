# %% [markdown]
# # Kafka Avro to File Ingestion Job
# Extracts Avro-encoded user events from a Kafka topic, deserializes them using ABRiS and Confluent Schema Registry,
# and writes the structured records to the local filesystem.

# %%
import argparse
import os
import yaml
from pyspark.sql import Column, SparkSession
from pyspark.sql.functions import col


# %%
def load_config(path: str) -> dict:
    """Loads YAML configuration file."""
    with open(path, "r", encoding="utf-8") as f:
        return yaml.safe_load(f)


# %%
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

    # Build ABRiS Schema Registry download configuration
    abris_registry_config = (
        jvm.za.co.absa.abris.config.AbrisConfig.fromConfluentAvro()
        .downloadReaderSchemaByLatestVersion()
        .andTopicNameStrategy(topic, is_key)
        .usingSchemaRegistry(schema_registry_url)
    )

    # za.co.absa.abris.avro.functions.from_avro(data_col, abris_registry_config)
    scala_abris_func = jvm.za.co.absa.abris.avro.functions

    return Column(
        scala_abris_func.from_avro(
            col(data_col)._jc,
            abris_registry_config,
        )
    )


# %%
def run_ingestion_job(config_path: str, output_dir: str, is_streaming: bool = True):
    """Runs Spark Ingestion job to extract Avro events from Kafka,

    deserializes them using ABRiS and Confluent Schema Registry,
    and writes them to output_dir.

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
    if "schema_registry" not in source_conf:
        raise ValueError("Missing 'schema_registry' in configuration source")

    kafka_conf = source_conf["kafka_config"]
    sr_conf = source_conf["schema_registry"]

    if "bootstrap_servers" not in kafka_conf or "topic" not in kafka_conf:
        raise ValueError("Missing 'bootstrap_servers' or 'topic' in 'kafka_config'")
    if "url" not in sr_conf:
        raise ValueError("Missing 'url' in 'schema_registry'")

    spark = (
        SparkSession.builder.appName("KafkaAvroToFileABRiS")
        .config(
            "spark.jars.repositories",
            "https://packages.confluent.io/maven/",
        )
        .config(
            "spark.jars.packages",
            "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.8,org.apache.spark:spark-avro_2.12:3.5.8,za.co.absa:abris_2.12:6.4.0",
        )
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    try:
        kafka_options = {
            "kafka.bootstrap.servers": kafka_conf["bootstrap_servers"],
            "subscribe": kafka_conf["topic"],
            "startingOffsets": kafka_conf.get("starting_offsets", "earliest"),
        }

        topic_name = kafka_conf["topic"]
        schema_registry_url = sr_conf["url"]

        if is_streaming:
            # Structured Streaming Read
            df_raw = spark.readStream.format("kafka").options(**kafka_options).load()

            # Deserialize Avro payload using ABRiS
            df_deserialized = df_raw.select(
                get_abris_from_avro_column(
                    spark,
                    data_col="value",
                    topic=topic_name,
                    schema_registry_url=schema_registry_url,
                ).alias("data")
            ).select("data.*")

            checkpoint_path = os.path.join(output_dir, "_checkpoint")

            # Structured Streaming Write (using Append mode)
            query = (
                df_deserialized.writeStream.outputMode("append")
                .format("json")
                .option("path", output_dir)
                .option("checkpointLocation", checkpoint_path)
                .trigger(processingTime="10 seconds")
                .start()
            )
            try:
                # Blocks the main thread, keeping the streaming query active until interrupted
                query.awaitTermination()
            except KeyboardInterrupt:
                # Triggered when the user presses Ctrl+C in the terminal
                import sys

                print(
                    "Streaming query interrupted by user. Stopping...",
                    file=sys.stderr,
                )
        else:
            # Batch Read
            df_raw = spark.read.format("kafka").options(**kafka_options).load()

            df_deserialized = df_raw.select(
                get_abris_from_avro_column(
                    spark,
                    data_col="value",
                    topic=topic_name,
                    schema_registry_url=schema_registry_url,
                ).alias("data")
            ).select("data.*")

            # Note: coalesce(1) is used here for local demonstration to output a single consolidated JSON file.
            # In production at scale, partition by business keys (e.g., .repartition("event_type").write.partitionBy("event_type"))
            # to distribute writes evenly across executor cores.
            df_deserialized.coalesce(1).write.mode("overwrite").json(output_dir)
    finally:
        spark.stop()


# %%
if __name__ == "__main__":
    script_dir = os.path.dirname(os.path.abspath(__file__))
    default_config = os.path.join(script_dir, "config/input_config.yml")
    default_output = os.path.join(script_dir, "output_json/user_events")

    parser = argparse.ArgumentParser(
        description="Extract user events from Kafka Avro stream and save as JSON."
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
