import argparse
import os
import yaml
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json


def load_config(path):
    with open(path, "r", encoding="utf-8") as f:
        return yaml.safe_load(f)


def run_ingestion_job(config_path: str, output_dir: str):
    config = load_config(config_path)
    source_conf = config["data_sources"][0]

    spark = (
        SparkSession.builder.appName("KafkaJsonToFile")
        .master("local[*]")
        .config(
            "spark.jars.packages",
            "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.8",
        )
        .getOrCreate()
    )

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

    df_raw = spark.read.format("kafka").options(**kafka_options).load()

    df_json = df_raw.selectExpr("CAST(value AS STRING) as json_str")

    df_parsed = df_json.select(
        from_json(col("json_str"), json_schema_str).alias("data")
    ).select("data.*")

    df_parsed.write.mode("overwrite").json(output_dir)

    spark.stop()


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
