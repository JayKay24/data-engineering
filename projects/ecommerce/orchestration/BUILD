files(
    name="compose_files",
    sources=["docker-compose.yml", "docker-compose.prod.yml", "Makefile"],
)

docker_image(
    name="airflow_image",
    dependencies=["3rdparty:lock_files"],
)
