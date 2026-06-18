import json
import os
import sys
from confluent_kafka import Producer

# Resolve paths relative to this script
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
INPUT_FILE_PATH = os.path.join(SCRIPT_DIR, "input_data/user_events.json")

# Kafka producer configuration
producer_conf = {"bootstrap.servers": "localhost:9092"}
producer = Producer(producer_conf)

# Read events from input file with error handling
records = []
try:
    with open(INPUT_FILE_PATH, "r", encoding="utf-8") as f:
        for line in f:
            if line.strip():
                try:
                    records.append(json.loads(line))
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

topic = "user-events-json"

# Produce each record to Kafka
for record in records:
    json_str = json.dumps(record)
    producer.produce(topic=topic, value=json_str)
    print("Produced:", json_str)

producer.flush()
