# Real-Time Event Ingestion Pipeline (Kafka + Avro + Schema Registry + ABRiS)

This project implements an event-driven data ingestion pipeline using Apache Kafka, Confluent Schema Registry, Apache Avro, and Apache Spark. It demonstrates real-time integration by producing schema-validated Avro event streams to a Kafka broker and streaming or batch-ingesting them into target storage using PySpark with ABRiS.

---

## 📁 Project Contents

*   [docker/docker-compose.yml](projects/ingestion/docker/docker-compose.yml): Launches local Zookeeper, Kafka, and Confluent Schema Registry (`:8081`) containers.
*   [schemas/user_event.avsc](projects/ingestion/schemas/user_event.avsc): Apache Avro schema defining the contract for user activity events.
*   [json_producer.py](projects/ingestion/json_producer.py): Python producer script that validates and encodes mock events using `confluent-kafka[avro]` and registers the schema with Schema Registry.
*   [kafka_json_to_file_job.py](projects/ingestion/kafka_json_to_file_job.py): PySpark streaming/batch job that pulls Avro records from Kafka, dynamically resolves schema definitions via Schema Registry and ABRiS (`za.co.absa:abris_2.12`), and saves structured records locally.
*   [config/input_config.yml](projects/ingestion/config/input_config.yml): Configuration file specifying broker connections, target topics, and Schema Registry URL.
*   [input_data/user_events.json](projects/ingestion/input_data/user_events.json): Sample raw events payload.

---

## 🚀 How to Run

Ensure your virtual environment is active and `JAVA_HOME` (Java 17) is exported in your environment (as configured in `.env`):
```bash
source .venv/bin/activate
export $(cat .env | xargs)
```

### 1. Spin up Kafka & Schema Registry Infrastructure
Start the Zookeeper, Kafka, and Schema Registry containers:
```bash
cd projects/ingestion/docker
docker-compose up -d
cd ../../..
```
Schema Registry will be accessible at `http://localhost:8081`.

### 2. Produce Avro Events to Kafka
Publish and register the sample events into Kafka via Pants or Python:
```bash
# Using Pants
./pants run projects/ingestion:producer

# Or using Python directly
python projects/ingestion/json_producer.py
```

### 3. Run Ingestion Spark Job (with ABRiS)
Extract the Avro events from Kafka, deserialize them against Schema Registry, and write them to output directories:
```bash
# Using Pants
./pants run projects/ingestion:ingest_job

# Or using Python directly (Batch mode example)
python projects/ingestion/kafka_json_to_file_job.py --batch
```
Outputs are written locally to `projects/ingestion/output_json/user_events/`.

> [!NOTE]
> **Partitioning Strategy Note**:
> `.coalesce(1)` is used in batch mode strictly for local demonstration purposes to consolidate sample output into a single JSON file. In production environments processing large event streams, dynamic repartitioning by a business key (e.g., `.repartition("event_type").write.partitionBy("event_type")`) should be used to avoid executor bottlenecks and ensure distributed writes.

---

## 📌 Roadmap & Features

- [x] **Schema Registry Integration**:
  - Schema-enforced Avro serialization with `confluent-kafka[avro]` and Schema Registry registration.
  - PySpark Avro deserialization with ABRiS (`za.co.absa:abris_2.12:6.4.0`) resolving schemas dynamically from Schema Registry.
- [x] **Container Health Orchestration**:
  - Automated Docker Compose service readiness health checks (`zookeeper` -> `kafka` -> `schema-registry`) to prevent startup race conditions.
