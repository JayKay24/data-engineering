python_sources(
    name="lib",
)

pex_binary(
    name="insert_transactions",
    entry_point="insert_transactions.py",
)

pex_binary(
    name="fetch_transactions",
    entry_point="fetch_transactions.py",
)

files(
    name="docker_files",
    sources=["docker/*"],
)
