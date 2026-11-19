python_sources(
    name="lib",
    sources=["validate_wiki.py"],
)

files(
    name="wiki_files",
    sources=["wiki/*.md"],
)

pex_binary(
    name="validate_wiki",
    entry_point="validate_wiki.py",
    dependencies=[":wiki_files"],
)
