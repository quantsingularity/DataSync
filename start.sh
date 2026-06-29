#!/usr/bin/env bash
#
# DataSync - one-command launcher
#
# Usage:
#   ./start.sh           Start the full stack with Docker Compose
#   ./start.sh --down    Stop the stack
#   ./start.sh --down-v  Stop and wipe volumes
#   ./start.sh --logs    Tail logs
#   ./start.sh --test    Run the test suite locally (no Docker)
#
set -euo pipefail
cd "$(dirname "$0")"

if [ ! -f .env ]; then
  cp .env.example .env
  echo "Created .env from .env.example (USE_MOCK_FEEDS=true - no API keys needed)"
fi

case "${1:-up}" in
  --down)   docker compose down ;;
  --down-v) docker compose down -v ;;
  --logs)   docker compose logs -f ;;
  --test)
    python3 -m venv .venv 2>/dev/null || true
    . .venv/bin/activate
    pip install -q -r requirements.txt
    PYTHONPATH=. pytest tests/ -v --tb=short
    ;;
  up|*)
    echo "Starting DataSync with Docker Compose..."
    docker compose up -d --build
    echo ""
    echo "  DataSync is starting."
    echo "  API docs:    http://localhost:8000/docs"
    echo "  Prometheus:  http://localhost:9090"
    echo "  TimescaleDB: localhost:5432   Kafka: localhost:9092"
    echo ""
    echo "  Tail logs:   ./start.sh --logs"
    echo "  Stop:        ./start.sh --down"
    ;;
esac
