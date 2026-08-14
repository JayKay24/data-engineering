import json
import os
import sys
from typing import TypedDict

from confluent_kafka import Producer


class UserEvent(TypedDict):
    user_id: int
    event_type: str
    timestamp: str


# Resolve paths relative to this script
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
INPUT_FILE_PATH = os.path.join(SCRIPT_DIR, "input_data/user_events.json")

# Kafka producer configuration
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "user-events-json")
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")

producer_conf = {"bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS}
producer = Producer(producer_conf)

# Read events from input file and produce to Kafka line-by-line
try:
    with open(INPUT_FILE_PATH, "r", encoding="utf-8") as f:
        for line in f:
            if line.strip():
                try:
                    event: UserEvent = json.loads(line)
                    json_str = json.dumps(event)
                    producer.produce(topic=KAFKA_TOPIC, value=json_str)
                    print("Produced:", json_str)
                except json.JSONDecodeError as e:
                    print(
                        f"Skipping malformed JSON line: {line.strip()} - Error: {e}",
                        file=sys.stderr,
                    )
except FileNotFoundError:
    print(f"Error: Input file not found at {INPUT_FILE_PATH}", file=sys.stderr)
    sys.exit(1)
except IOError as e:
    print(f"Error reading input file {INPUT_FILE_PATH}: {e}", file=sys.stderr)
    sys.exit(1)

producer.flush()
