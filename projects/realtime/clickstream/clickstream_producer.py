import argparse
import json
import os
import sys
import time
from typing import TypedDict

from confluent_kafka import Producer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer
from confluent_kafka.serialization import MessageField, SerializationContext
import yaml

from projects.common.logger import get_logger

logger = get_logger("ClickstreamAvroProducer")


class ClickstreamEvent(TypedDict):
    user_id: str
    url: str
    event_type: str
    event_time: str
    ip_address: str | None
    user_agent: str | None


def load_config(config_path: str) -> dict:
    """Loads and validates YAML configuration."""
    with open(config_path, "r", encoding="utf-8") as f:
        return yaml.safe_load(f)


def load_avro_schema(schema_path: str) -> str:
    """Reads and returns the Avro schema string from file."""
    with open(schema_path, "r", encoding="utf-8") as f:
        return f.read()


def delivery_report(err, msg) -> None:
    """Callback triggered on Kafka message delivery confirmation."""
    if err:
        logger.error("Message delivery failed: %s", err)
    else:
        logger.info(
            "Delivered message to %s [%d] at offset %d",
            msg.topic(),
            msg.partition(),
            msg.offset(),
        )


def produce_events(
    config_path: str,
    schema_path: str,
    input_path: str,
    delay_seconds: float = 0.5,
) -> None:
    """Streams clickstream events from input JSON to Kafka with Avro serialization."""
    config = load_config(config_path)
    kafka_conf = config.get("kafka_config", {})
    sr_conf = config.get("schema_registry", {})

    bootstrap_servers = os.getenv(
        "KAFKA_BOOTSTRAP_SERVERS", kafka_conf.get("bootstrap_servers", "localhost:9092")
    )
    topic = os.getenv("KAFKA_TOPIC", kafka_conf.get("topic", "clickstream_events"))
    schema_registry_url = os.getenv(
        "SCHEMA_REGISTRY_URL", sr_conf.get("url", "http://localhost:8081")
    )

    logger.info("Loading Avro schema from: %s", schema_path)
    schema_str = load_avro_schema(schema_path)

    logger.info("Initializing Schema Registry Client: %s", schema_registry_url)
    sr_client = SchemaRegistryClient({"url": schema_registry_url})
    avro_serializer = AvroSerializer(
        schema_registry_client=sr_client,
        schema_str=schema_str,
        to_dict=lambda event, ctx: dict(event),
    )

    producer_config = {"bootstrap.servers": bootstrap_servers}
    producer = Producer(producer_config)
    serialization_ctx = SerializationContext(topic, MessageField.VALUE)

    logger.info("Reading input clickstream events from: %s", input_path)
    count = 0
    with open(input_path, "r", encoding="utf-8") as f:
        for line in f:
            line_str = line.strip()
            if not line_str:
                continue
            try:
                event_data: ClickstreamEvent = json.loads(line_str)
                serialized_val = avro_serializer(event_data, serialization_ctx)
                key_val = str(event_data.get("user_id", "unknown")).encode("utf-8")

                producer.produce(
                    topic=topic,
                    key=key_val,
                    value=serialized_val,
                    callback=delivery_report,
                )
                producer.poll(0)
                count += 1
                logger.info(
                    "Produced event #%d for user '%s' on url '%s'",
                    count,
                    event_data.get("user_id"),
                    event_data.get("url"),
                )
                if delay_seconds > 0:
                    time.sleep(delay_seconds)
            except json.JSONDecodeError as e:
                logger.warning("Skipping malformed JSON line '%s': %s", line_str, e)
            except Exception as e:
                logger.error("Failed to produce event: %s", e)

    producer.flush()
    logger.info(
        "Successfully flushed %d clickstream events to Kafka topic '%s'.", count, topic
    )


if __name__ == "__main__":
    script_dir = os.path.dirname(os.path.abspath(__file__))
    default_config = os.path.join(script_dir, "config/clickstream_config.yml")
    default_schema = os.path.join(script_dir, "schemas/clickstream_event.avsc")
    default_input = os.path.join(script_dir, "input_data/clickstream_events.json")

    parser = argparse.ArgumentParser(
        description="Produce Avro-serialized clickstream events to Kafka with Schema Registry."
    )
    parser.add_argument(
        "--config",
        type=str,
        default=default_config,
        help="Path to YAML configuration file.",
    )
    parser.add_argument(
        "--schema",
        type=str,
        default=default_schema,
        help="Path to Avro schema (.avsc) file.",
    )
    parser.add_argument(
        "--input",
        type=str,
        default=default_input,
        help="Path to input JSON dataset.",
    )
    parser.add_argument(
        "--delay",
        type=float,
        default=0.5,
        help="Delay in seconds between producing events (default: 0.5s).",
    )

    args = parser.parse_args()
    try:
        produce_events(
            config_path=args.config,
            schema_path=args.schema,
            input_path=args.input,
            delay_seconds=args.delay,
        )
    except FileNotFoundError as fnf_err:
        logger.error("File not found: %s", fnf_err)
        sys.exit(1)
    except Exception as exc:
        logger.error("Fatal error during event production: %s", exc)
        sys.exit(1)
