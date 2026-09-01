.PHONY: help build up down restart logs status airflow-web viz-web clean

help:
	@echo "Available orchestration commands:"
	@echo "  make build       - Build Airflow and Streamlit container images"
	@echo "  make up          - Start Airflow, Postgres, and Streamlit in background"
	@echo "  make down        - Stop all orchestration containers"
	@echo "  make restart     - Restart all orchestration containers"
	@echo "  make logs        - Follow logs from all orchestration containers"
	@echo "  make status      - Check container status and health"
	@echo "  make clean       - Stop containers and remove volumes"

build:
	docker compose build

up:
	docker compose up -d

down:
	docker compose down

restart: down up

logs:
	docker compose logs -f

status:
	docker compose ps

clean:
	docker compose down -v
