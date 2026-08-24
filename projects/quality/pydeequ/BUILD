python_sources(
    name="lib",
)

pex_binary(
    name="validate",
    entry_point="validate_pyspark_deequ.py",
    dependencies=[
        "projects/quality:raw_data",
        "3rdparty:reqs#pydeequ",
        "3rdparty:reqs#pyspark",
    ],
)
