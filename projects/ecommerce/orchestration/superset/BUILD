python_sources(
    name="lib",
    sources=["superset_config.py", "package_assets.py"],
)

files(
    name="superset_files",
    sources=[
        "superset_config.py",
        "bootstrap.sh",
        "package_assets.py",
        "assets/**/*.yaml",
    ],
)

pex_binary(
    name="package_assets",
    entry_point="package_assets.py",
    dependencies=[":superset_files"],
)

docker_image(
    name="superset_image",
    dependencies=["3rdparty:lock_files", ":superset_files"],
)
