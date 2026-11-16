python_sources(
    name="lib",
)

pex_binary(
    name="export",
    entry_point="export_to_gcp.py",
    dependencies=["projects/common:lib"],
)
