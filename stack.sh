#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "$0")" && pwd)"
COMPOSE_FILE="$ROOT_DIR/infrastructure/docker-compose.yml"

cmd="${1:-}"

case "$cmd" in
  start)
    docker compose -f "$COMPOSE_FILE" up -d --build
    ;;
  stop)
    docker compose -f "$COMPOSE_FILE" down
    ;;
  restart)
    docker compose -f "$COMPOSE_FILE" down
    docker compose -f "$COMPOSE_FILE" up -d --build
    ;;
  logs)
    docker compose -f "$COMPOSE_FILE" logs -f
    ;;
  status)
    docker compose -f "$COMPOSE_FILE" ps
    ;;
  *)
    echo "Usage: ./stack.sh {start|stop|restart|logs|status}"
    exit 1
    ;;
esac
