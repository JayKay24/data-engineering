files(
    name="compose_files",
    sources=["docker-compose.yml", "Makefile"],
)

docker_image(
    name="airflow_image",
    dependencies=["3rdparty:lock_files"],
)
