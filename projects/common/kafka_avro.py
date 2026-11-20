"""Shared Kafka Avro producer/consumer utilities and PySpark ABRiS helpers."""

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
    """Read and return an Avro schema JSON string from disk.

    Args:
        schema_path: Filesystem path to the ``.avsc`` schema file.

    Returns:
        Raw Avro schema JSON string.
    """
    with open(schema_path, "r", encoding="utf-8") as f:
        return f.read()


def kafka_delivery_report(
    err: Any, msg: Any, logger: logging.Logger | None = None
) -> None:
    """Log Kafka message delivery outcomes from a producer callback.

    Args:
        err: Delivery error reported by ``confluent_kafka.Producer``, or
            ``None`` on success.
        msg: Delivered Kafka message metadata object.
        logger: Optional logger used to record delivery errors or offsets.
    """
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
    """Create a configured Confluent Kafka Producer and AvroSerializer.

    Args:
        bootstrap_servers: Comma-separated Kafka broker host/port list.
        schema_registry_url: Base URL of the Confluent Schema Registry.
        schema_str: Avro schema JSON string used to serialize values.
        to_dict_func: Optional callable converting domain objects to dicts
            before Avro serialization. Defaults to ``dict(event)``.

    Returns:
        Tuple of ``(Producer, AvroSerializer)`` ready for producing events.
    """
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
    """Deserialize Confluent Avro payloads into a Spark SQL column via ABRiS.

    Invokes the JVM ABRiS library through PySpark's Py4J gateway using the
    latest schema registered under the topic name strategy.

    Args:
        spark: Active SparkSession providing the Py4J JVM gateway.
        data_col: Name of the binary DataFrame column containing Avro bytes.
        topic: Kafka topic name used for Schema Registry subject lookup.
        schema_registry_url: Base URL of the Confluent Schema Registry.
        is_key: Whether to resolve the key subject (``-key``) instead of the
            value subject (``-value``).

    Returns:
        Spark SQL ``Column`` representing the deserialized Avro struct.
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
