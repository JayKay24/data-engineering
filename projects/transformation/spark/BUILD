python_sources(
    name="lib",
)

resources(
    name="data_files",
    sources=["data/raw/*.json"],
)

pex_binary(
    name="data_processing_job",
    entry_point="data_processing_job.py",
    dependencies=[":data_files"],
)
