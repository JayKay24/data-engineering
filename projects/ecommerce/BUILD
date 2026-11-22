python_sources(
    name="lib",
    sources=["__init__.py", "duckdb_utils.py", "wap_duckdb.py"],
)

pex_binary(
    name="wap_duckdb",
    entry_point="wap_duckdb.py",
    dependencies=[":lib", "projects/common:lib"],
)
