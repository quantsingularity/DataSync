# DataSync - Market Data Layer

.PHONY: help up down down-v logs ps test

help:
	@echo ""
	@echo "  DataSync - Market Data Layer"
	@echo "  ─────────────────────────────────────────────────────"
	@echo "  make up        Start the full stack"
	@echo "  make down      Stop containers"
	@echo "  make down-v    Stop and wipe all volumes"
	@echo "  make logs      Tail all logs"
	@echo "  make test      Run test suite"
	@echo ""
	@echo "  Services after 'make up':"
	@echo "    API:        http://localhost:8000/docs"
	@echo "    Prometheus: http://localhost:9090"
	@echo "    TimescaleDB: localhost:5432"
	@echo ""

up:
	@cp -n .env.example .env 2>/dev/null || true
	docker compose up -d --build
	@echo ""
	@echo "  DataSync is starting..."
	@echo "  API docs:   http://localhost:8000/docs"
	@echo "  Prometheus: http://localhost:9090"

down:
	docker compose down

down-v:
	docker compose down -v

logs:
	docker compose logs -f

logs-%:
	docker compose logs -f $*

ps:
	docker compose ps

test:
	@echo "Running DataSync tests..."
	pip install -q -r requirements.txt websockets
	pytest tests/ -v --tb=short
	@echo "Tests complete."

build-%:
	docker compose up -d --build $*
