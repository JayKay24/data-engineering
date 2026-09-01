python_sources(
    name="lib",
)

resources(
    name="data_files",
    sources=["config/*.yml", "input_data/*.json", "schemas/*.avsc"],
)

pex_binary(
    name="producer",
    entry_point="ecommerce_producer.py",
    dependencies=[":data_files", "projects/common:lib"],
)

pex_binary(
    name="stream_job",
    entry_point="ecommerce_aggregation_job.py",
    dependencies=[":data_files", "projects/common:lib"],
)
