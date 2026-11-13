python_sources(
    name="lib",
)

files(
    name="app_files",
    sources=["app.py"],
)

pex_binary(
    name="app",
    entry_point="app.py",
)

docker_image(
    name="viz_image",
    dependencies=["3rdparty:lock_files", ":app_files"],
)
