import argparse
import json
import os
import sys
from typing import TypedDict

from confluent_kafka.serialization import MessageField, SerializationContext

from projects.common.kafka_avro import (
    create_avro_producer,
    kafka_delivery_report,
    load_avro_schema,
)
from projects.common.logger import get_logger

logger = get_logger("KafkaAvroProducer")


class UserEvent(TypedDict):
    user_id: int
    event_type: str
    timestamp: str


def produce_user_events(
    schema_path: str,
    input_path: str,
    topic: str = "user-events-avro",
    bootstrap: str = "localhost:9092",
    sr_url: str = "http://localhost:8081",
) -> None:
    """Streams and serializes user events to Kafka with Schema Registry."""
    schema_str = load_avro_schema(schema_path)
    producer, serializer = create_avro_producer(bootstrap, sr_url, schema_str)
    ctx = SerializationContext(topic, MessageField.VALUE)

    logger.info("Reading user events from %s...", input_path)
    count = 0
    with open(input_path, "r", encoding="utf-8") as f:
        for line in f:
            if not line.strip():
                continue
            event: UserEvent = json.loads(line.strip())
            val = serializer(event, ctx)
            producer.produce(
                topic=topic,
                value=val,
                callback=lambda err, msg: kafka_delivery_report(err, msg, logger),
            )
            count += 1

    producer.flush()
    logger.info("Successfully published %d user events to topic '%s'.", count, topic)


if __name__ == "__main__":
    script_dir = os.path.dirname(os.path.abspath(__file__))
    parser = argparse.ArgumentParser(description="Kafka Avro User Events Producer.")
    parser.add_argument(
        "--schema", default=os.path.join(script_dir, "schemas/user_event.avsc")
    )
    parser.add_argument(
        "--input", default=os.path.join(script_dir, "input_data/user_events.json")
    )
    args = parser.parse_args()

    topic_name = os.getenv("KAFKA_TOPIC", "user-events-avro")
    bootstrap_srv = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
    sr_endpoint = os.getenv("SCHEMA_REGISTRY_URL", "http://localhost:8081")

    try:
        produce_user_events(
            schema_path=args.schema,
            input_path=args.input,
            topic=topic_name,
            bootstrap=bootstrap_srv,
            sr_url=sr_endpoint,
        )
    except Exception as exc:
        logger.error("Failed to produce events: %s", exc)
        sys.exit(1)
