# Ingestion Project (Chapter 4)

This project implements the advanced data ingestion and integration patterns derived from Chapter 4 of *Hello Modern Data Pipelines*. It demonstrates real-time integration by producing events to a Kafka broker and streaming/reading them into Spark.

---

## 📁 Project Contents

*   [docker/docker-compose.yml](projects/ingestion/docker/docker-compose.yml): Launches local Zookeeper, Kafka, and Schema Registry containers.
*   [json_producer.py](projects/ingestion/json_producer.py): Python producer script that publishes mock events to the Kafka broker.
*   [kafka_json_to_file_job.py](projects/ingestion/kafka_json_to_file_job.py): PySpark streaming/batch job that pulls events from Kafka, parses them with a schema, and saves them locally as JSON.
*   [config/input_config.yml](projects/ingestion/config/input_config.yml): Configuration file specifying broker connections, target topics, and JSON parsing schema.
*   [input_data/user_events.json](projects/ingestion/input_data/user_events.json): Sample JSON data file with mock events.

---

## 🚀 How to Run

Ensure your virtual environment is active:
```bash
source .venv/bin/activate
```

### 1. Spin up Kafka Infrastucture
Start the Zookeeper, Kafka, and Schema Registry containers:
```bash
cd projects/ingestion/docker
docker-compose up -d
cd ../../..
```

### 2. Produce mock JSON events
Publish the sample events from `user_events.json` into Kafka:
```bash
python projects/ingestion/json_producer.py
```

### 3. Run Ingestion Spark Job
Extract the events from Kafka and write them to output directories:
```bash
python projects/ingestion/kafka_json_to_file_job.py
```
Outputs are written locally to `projects/ingestion/output_json/user_events/`.
