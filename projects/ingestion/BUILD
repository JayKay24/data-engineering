python_sources(
    name="lib",
)

resources(
    name="data_files",
    sources=["config/*.yml", "input_data/*.json", "schemas/*.avsc"],
)

pex_binary(
    name="producer",
    entry_point="json_producer.py",
    dependencies=[":data_files"],
)

pex_binary(
    name="ingest_job",
    entry_point="kafka_json_to_file_job.py",
    dependencies=[":data_files"],
)
