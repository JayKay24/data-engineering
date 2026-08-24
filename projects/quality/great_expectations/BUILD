python_sources(
    name="lib",
)

pex_binary(
    name="validate",
    entry_point="validate_user_data.py",
    dependencies=[
        "projects/quality:raw_data",
        "3rdparty:reqs#great-expectations",
        "3rdparty:reqs#pandas",
    ],
)
