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

ENV_FILE := ../../../../.env
COMPOSE := docker compose --env-file $(ENV_FILE)

build:
	$(COMPOSE) build

up:
	$(COMPOSE) up -d

down:
	$(COMPOSE) down

restart: down up

logs:
	$(COMPOSE) logs -f

status:
	$(COMPOSE) ps

clean:
	$(COMPOSE) down -v
