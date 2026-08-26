import json
import os
import sys
from typing import TypedDict

from confluent_kafka import Producer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer
from confluent_kafka.serialization import MessageField, SerializationContext


class UserEvent(TypedDict):
    user_id: int
    event_type: str
    timestamp: str


# Resolve paths relative to this script
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
INPUT_FILE_PATH = os.path.join(SCRIPT_DIR, "input_data/user_events.json")
SCHEMA_FILE_PATH = os.path.join(SCRIPT_DIR, "schemas/user_event.avsc")

# Kafka & Schema Registry configuration
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "user-events-avro")
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
SCHEMA_REGISTRY_URL = os.getenv("SCHEMA_REGISTRY_URL", "http://localhost:8081")


def user_event_to_dict(event: UserEvent, ctx: SerializationContext) -> dict:
    """Converts a UserEvent object/dict to a dictionary for Avro serialization."""
    return dict(event)


def load_avro_schema(schema_path: str) -> str:
    """Reads and returns the Avro schema string from file."""
    with open(schema_path, "r", encoding="utf-8") as f:
        return f.read()


def main() -> None:
    # 1. Load Avro schema
    try:
        schema_str = load_avro_schema(SCHEMA_FILE_PATH)
    except FileNotFoundError:
        print(f"Error: Schema file not found at {SCHEMA_FILE_PATH}", file=sys.stderr)
        sys.exit(1)

    # 2. Configure Schema Registry Client & Avro Serializer
    schema_registry_client = SchemaRegistryClient({"url": SCHEMA_REGISTRY_URL})
    avro_serializer = AvroSerializer(
        schema_registry_client=schema_registry_client,
        schema_str=schema_str,
        to_dict=user_event_to_dict,
    )

    # 3. Configure Kafka Producer
    producer_conf = {"bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS}
    producer = Producer(producer_conf)

    # 4. Read events from input file, serialize with Avro + Schema Registry, and produce
    serialization_ctx = SerializationContext(KAFKA_TOPIC, MessageField.VALUE)

    try:
        with open(INPUT_FILE_PATH, "r", encoding="utf-8") as f:
            for line in f:
                line_str = line.strip()
                if not line_str:
                    continue
                try:
                    event: UserEvent = json.loads(line_str)
                    serialized_value = avro_serializer(event, serialization_ctx)
                    producer.produce(topic=KAFKA_TOPIC, value=serialized_value)
                    print(f"Produced Avro Event: {event} (Topic: {KAFKA_TOPIC})")
                except json.JSONDecodeError as e:
                    print(
                        f"Skipping malformed JSON line: {line_str} - Error: {e}",
                        file=sys.stderr,
                    )
                except Exception as e:
                    print(
                        f"Failed to serialize/produce event {line_str} - Error: {e}",
                        file=sys.stderr,
                    )
    except FileNotFoundError:
        print(f"Error: Input file not found at {INPUT_FILE_PATH}", file=sys.stderr)
        sys.exit(1)
    except IOError as e:
        print(f"Error reading input file {INPUT_FILE_PATH}: {e}", file=sys.stderr)
        sys.exit(1)

    producer.flush()
    print("Flushed all messages to Kafka successfully.")


if __name__ == "__main__":
    main()
