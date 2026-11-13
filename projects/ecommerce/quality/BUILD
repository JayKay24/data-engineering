python_sources(
    name="lib",
)

pex_binary(
    name="validate",
    entry_point="validate_ecommerce.py",
    dependencies=[
        "projects/common:lib",
        "projects/ecommerce/streaming_layer:data_files",
    ],
)
