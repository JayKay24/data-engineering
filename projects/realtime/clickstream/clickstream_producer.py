import argparse
import json
import os
import sys
import time
from typing import TypedDict

from confluent_kafka.serialization import MessageField, SerializationContext
import yaml

from projects.common.kafka_avro import (
    create_avro_producer,
    kafka_delivery_report,
    load_avro_schema,
)
from projects.common.logger import get_logger

logger = get_logger("ClickstreamAvroProducer")


class ClickstreamEvent(TypedDict):
    user_id: str
    url: str
    event_type: str
    event_time: str
    ip_address: str | None
    user_agent: str | None


def produce_events(
    config_path: str, schema_path: str, input_path: str, delay_seconds: float = 0.5
) -> None:
    """Streams clickstream events from JSON dataset to Kafka with Avro serialization."""
    with open(config_path, "r", encoding="utf-8") as f:
        config = yaml.safe_load(f)

    kafka_cfg, sr_cfg = (
        config.get("kafka_config", {}),
        config.get("schema_registry", {}),
    )
    bootstrap = os.getenv(
        "KAFKA_BOOTSTRAP_SERVERS", kafka_cfg.get("bootstrap_servers", "localhost:9092")
    )
    topic = os.getenv("KAFKA_TOPIC", kafka_cfg.get("topic", "clickstream_events"))
    sr_url = os.getenv(
        "SCHEMA_REGISTRY_URL", sr_cfg.get("url", "http://localhost:8081")
    )

    schema_str = load_avro_schema(schema_path)
    producer, serializer = create_avro_producer(bootstrap, sr_url, schema_str)
    ctx = SerializationContext(topic, MessageField.VALUE)

    logger.info("Reading clickstream events from: %s", input_path)
    count = 0
    with open(input_path, "r", encoding="utf-8") as f:
        for line in f:
            if not line.strip():
                continue
            event_data: ClickstreamEvent = json.loads(line.strip())
            serialized = serializer(event_data, ctx)
            key = str(event_data.get("user_id", "unknown")).encode("utf-8")

            producer.produce(
                topic=topic,
                key=key,
                value=serialized,
                callback=lambda err, msg: kafka_delivery_report(err, msg, logger),
            )
            producer.poll(0)
            count += 1
            if delay_seconds > 0:
                time.sleep(delay_seconds)

    producer.flush()
    logger.info("Successfully produced %d events to '%s'.", count, topic)


if __name__ == "__main__":
    script_dir = os.path.dirname(os.path.abspath(__file__))
    parser = argparse.ArgumentParser(description="Produce Avro clickstream events.")
    parser.add_argument(
        "--config", default=os.path.join(script_dir, "config/clickstream_config.yml")
    )
    parser.add_argument(
        "--schema", default=os.path.join(script_dir, "schemas/clickstream_event.avsc")
    )
    parser.add_argument(
        "--input",
        default=os.path.join(script_dir, "input_data/clickstream_events.json"),
    )
    parser.add_argument("--delay", type=float, default=0.5)
    args = parser.parse_args()

    try:
        produce_events(args.config, args.schema, args.input, args.delay)
    except Exception as exc:
        logger.error("Producer error: %s", exc)
        sys.exit(1)
