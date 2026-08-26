import argparse
import os
import sys
from pyspark.sql import SparkSession
import yaml

from projects.common.kafka_avro import get_abris_from_avro_column
from projects.common.logger import get_logger

logger = get_logger("KafkaAvroIngestJob")


def load_config(path: str) -> dict:
    """Loads YAML configuration file."""
    with open(path, "r", encoding="utf-8") as f:
        return yaml.safe_load(f)


def init_spark_session(app_name: str = "KafkaAvroToFileABRiS") -> SparkSession:
    """Initializes SparkSession with Kafka, Spark-Avro, and ABRiS packages."""
    spark = (
        SparkSession.builder.appName(app_name)
        .config("spark.jars.repositories", "https://packages.confluent.io/maven/")
        .config(
            "spark.jars.packages",
            "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.8,"
            "org.apache.spark:spark-avro_2.12:3.5.8,"
            "za.co.absa:abris_2.12:6.4.0",
        )
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    return spark


def run_ingestion_job(
    config_path: str, output_dir: str, is_streaming: bool = True
) -> None:
    """Extracts Avro events from Kafka, deserializes via ABRiS, and writes JSON files."""
    config = load_config(config_path)
    source_conf = config["data_sources"][0]
    kafka_conf, sr_conf = source_conf["kafka_config"], source_conf["schema_registry"]

    bootstrap = kafka_conf["bootstrap_servers"]
    topic = kafka_conf["topic"]
    sr_url = sr_conf["url"]

    spark = init_spark_session()
    try:
        kafka_options = {
            "kafka.bootstrap.servers": bootstrap,
            "subscribe": topic,
            "startingOffsets": kafka_conf.get("starting_offsets", "earliest"),
        }

        if is_streaming:
            logger.info("Starting streaming ingestion from Kafka topic '%s'...", topic)
            df_raw = spark.readStream.format("kafka").options(**kafka_options).load()
            df_deserialized = df_raw.select(
                get_abris_from_avro_column(spark, "value", topic, sr_url).alias("data")
            ).select("data.*")

            checkpoint_path = os.path.join(output_dir, "_checkpoint")
            query = (
                df_deserialized.writeStream.outputMode("append")
                .format("json")
                .option("path", output_dir)
                .option("checkpointLocation", checkpoint_path)
                .trigger(processingTime="10 seconds")
                .start()
            )
            query.awaitTermination()
        else:
            logger.info("Executing batch ingestion from Kafka topic '%s'...", topic)
            df_raw = spark.read.format("kafka").options(**kafka_options).load()
            df_deserialized = df_raw.select(
                get_abris_from_avro_column(spark, "value", topic, sr_url).alias("data")
            ).select("data.*")

            # Local demo output consolidation
            df_deserialized.coalesce(1).write.mode("overwrite").json(output_dir)
            logger.info("Batch ingestion completed to %s", output_dir)
    except KeyboardInterrupt:
        logger.info("Ingestion job interrupted by user.")
    finally:
        spark.stop()


if __name__ == "__main__":
    script_dir = os.path.dirname(os.path.abspath(__file__))
    parser = argparse.ArgumentParser(
        description="Extract user events from Kafka Avro stream."
    )
    parser.add_argument(
        "--config", default=os.path.join(script_dir, "config/input_config.yml")
    )
    parser.add_argument(
        "--output", default=os.path.join(script_dir, "output_json/user_events")
    )
    parser.add_argument("--batch", action="store_true")
    args = parser.parse_args()

    try:
        run_ingestion_job(args.config, args.output, is_streaming=not args.batch)
    except Exception as exc:
        logger.error("Ingestion job failed: %s", exc)
        sys.exit(1)
