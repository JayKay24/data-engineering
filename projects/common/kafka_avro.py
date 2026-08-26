from collections.abc import Callable
import logging
from typing import Any

from confluent_kafka import Producer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer
from confluent_kafka.serialization import SerializationContext
from pyspark.sql import Column, SparkSession
from pyspark.sql.functions import col


def load_avro_schema(schema_path: str) -> str:
    """Reads and returns the Avro schema string from a given file path."""
    with open(schema_path, "r", encoding="utf-8") as f:
        return f.read()


def kafka_delivery_report(
    err: Any, msg: Any, logger: logging.Logger | None = None
) -> None:
    """Standard Kafka delivery report callback."""
    if err:
        if logger:
            logger.error("Message delivery failed: %s", err)
    else:
        if logger:
            logger.info(
                "Delivered message to %s [%d] @ offset %d",
                msg.topic(),
                msg.partition(),
                msg.offset(),
            )


def create_avro_producer(
    bootstrap_servers: str,
    schema_registry_url: str,
    schema_str: str,
    to_dict_func: Callable[[Any, SerializationContext], dict] | None = None,
) -> tuple[Producer, AvroSerializer]:
    """Creates a configured Confluent Kafka Producer and AvroSerializer."""
    sr_client = SchemaRegistryClient({"url": schema_registry_url})
    serializer = AvroSerializer(
        schema_registry_client=sr_client,
        schema_str=schema_str,
        to_dict=to_dict_func or (lambda event, ctx: dict(event)),
    )
    producer = Producer({"bootstrap.servers": bootstrap_servers})
    return producer, serializer


def get_abris_from_avro_column(
    spark: SparkSession,
    data_col: str,
    topic: str,
    schema_registry_url: str,
    is_key: bool = False,
) -> Column:
    """Invokes ABRiS via Py4J Gateway to deserialize Confluent Avro binary payloads

    into structured Spark SQL columns using Confluent Schema Registry.
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
