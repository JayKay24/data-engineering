import json
import os
from confluent_kafka import Producer

# Resolve paths relative to this script
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
INPUT_FILE_PATH = os.path.join(SCRIPT_DIR, "input_data/user_events.json")

# Kafka producer configuration
producer_conf = {"bootstrap.servers": "localhost:9092"}
producer = Producer(producer_conf)

# Read events from input file
with open(INPUT_FILE_PATH, "r", encoding="utf-8") as f:
    records = [json.loads(line) for line in f]

topic = "user-events-json"

# Produce each record to Kafka
for record in records:
    json_str = json.dumps(record)
    producer.produce(topic=topic, value=json_str)
    print("Produced:", json_str)

producer.flush()
