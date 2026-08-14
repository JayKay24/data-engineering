python_sources(
    name="lib",
)

pex_binary(
    name="producer",
    entry_point="json_producer.py",
)

pex_binary(
    name="ingest_job",
    entry_point="kafka_json_to_file_job.py",
)
