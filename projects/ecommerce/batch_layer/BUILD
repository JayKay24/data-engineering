files(
    name="dbt_files",
    sources=[
        "dbt_project.yml",
        "profiles.yml",
        "Dockerfile",
        "docker-compose.yml",
        "Makefile",
        "macros/**/*.sql",
        "models/**/*.sql",
        "models/**/*.yml",
    ],
)

docker_image(
    name="dbt_image",
    dependencies=["3rdparty:lock_files"],
)
