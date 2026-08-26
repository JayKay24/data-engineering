# Real-Time Event Ingestion Pipeline

This project implements an event-driven data ingestion pipeline using Apache Kafka and Apache Spark. It demonstrates real-time integration by producing structured event streams to a Kafka broker and streaming or batch-ingesting them into target storage using PySpark.

---

## 📁 Project Contents

*   [docker/docker-compose.yml](projects/ingestion/docker/docker-compose.yml): Launches local Zookeeper, Kafka, and Schema Registry containers.
*   [json_producer.py](projects/ingestion/json_producer.py): Python producer script that publishes mock events to the Kafka broker.
*   [kafka_json_to_file_job.py](projects/ingestion/kafka_json_to_file_job.py): PySpark streaming/batch job that pulls events from Kafka, parses them with a schema, and saves them locally as JSON.
*   [config/input_config.yml](projects/ingestion/config/input_config.yml): Configuration file specifying broker connections, target topics, and JSON parsing schema.
*   [input_data/user_events.json](projects/ingestion/input_data/user_events.json): Sample JSON data file with mock events.

---

## 🚀 How to Run

Ensure your virtual environment is active and `JAVA_HOME` (Java 17) is exported in your environment (as configured in `.env`):
```bash
source .venv/bin/activate
export $(cat .env | xargs)
```

### 1. Spin up Kafka Infrastucture
Start the Zookeeper, Kafka, and Schema Registry containers:
```bash
cd projects/ingestion/docker
docker-compose up -d
cd ../../..
```

### 2. Produce mock JSON events
Publish the sample events from `user_events.json` into Kafka via Pants or Python:
```bash
# Using Pants
./pants run projects/ingestion:producer

# Or using Python directly
python projects/ingestion/json_producer.py
```

### 3. Run Ingestion Spark Job
Extract the events from Kafka and write them to output directories:
```bash
# Using Pants
./pants run projects/ingestion:ingest_job

# Or using Python directly
python projects/ingestion/kafka_json_to_file_job.py
```
Outputs are written locally to `projects/ingestion/output_json/user_events/`.

---

## 📌 TODOs & Roadmap

- [ ] **Schema Registry Integration**:
  - Transition from raw JSON messages to schema-enforced formats (**Avro** or **JSON Schema with Schema Registry**).
  - Update [json_producer.py](projects/ingestion/json_producer.py) to use `confluent_kafka.schema_registry` (`AvroSerializer` / `JSONSerializer`).
  - Update [kafka_json_to_file_job.py](projects/ingestion/kafka_json_to_file_job.py) to deserialize incoming payloads with dynamic schema validation from Schema Registry (using `spark-avro` or ABRiS).
