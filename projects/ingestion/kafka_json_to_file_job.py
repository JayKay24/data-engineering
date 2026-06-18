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
def run_ingestion_job(config_path: str, output_dir: str):
    """Runs Spark Streaming ingestion job to extract JSON events from Kafka,

    parses them using a JSON schema, and writes them to output_dir.

    Args:
        config_path (str): Path to the ingestion YAML configuration file.
        output_dir (str): Target directory to save the output JSON events.
    """
    # Load Kafka connection parameters and schema from config
    config = load_config(config_path)
    source_conf = config["data_sources"][0]

    # Initialize PySpark session with Kafka integration
    spark = (
        SparkSession.builder.appName("KafkaJsonToFile")
        .master("local[*]")
        .config(
            "spark.jars.packages",
            "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.8",
        )
        .getOrCreate()
    )

    # Set up Kafka connection options
    kafka_options = {
        "kafka.bootstrap.servers": source_conf["kafka_config"]["bootstrap_servers"],
        "subscribe": source_conf["kafka_config"]["topic"],
        "startingOffsets": source_conf["kafka_config"].get(
            "starting_offsets", "earliest"
        ),
    }

    json_schema_str = source_conf.get("json_schema")
    if not json_schema_str:
        raise ValueError("Missing 'json_schema' in config")

    # Read raw binary data from Kafka topic into a DataFrame
    df_raw = spark.read.format("kafka").options(**kafka_options).load()

    # Cast raw Kafka message payload from bytes to a JSON string
    df_json = df_raw.selectExpr("CAST(value AS STRING) as json_str")

    # Parse JSON strings into structured columns based on schema
    df_parsed = df_json.select(
        from_json(col("json_str"), json_schema_str).alias("data")
    ).select("data.*")

    # Write the structured data to the local filesystem
    df_parsed.write.mode("overwrite").json(output_dir)

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

    args = parser.parse_args()
    run_ingestion_job(args.config, args.output)
