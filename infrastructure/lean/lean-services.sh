#!/usr/bin/env bash
# Start/stop/restart lean stack (local or on server from repo root).
# Usage: ./infrastructure/lean/lean-services.sh start|stop|restart|ps|status|down
set -euo pipefail
ROOT="$(cd "$(dirname "$0")/../.." && pwd)"
cd "${ROOT}"
CMD="${1:-ps}"
COMPOSE=(docker compose -f docker-compose.lean.yml --env-file .env.lean)

case "${CMD}" in
  start)
    "${COMPOSE[@]}" up -d
    ;;
  stop)
    "${COMPOSE[@]}" stop
    ;;
  restart)
    "${COMPOSE[@]}" up -d --build
    ;;
  ps)
    "${COMPOSE[@]}" ps
    ;;
  status)
    "${ROOT}/infrastructure/lean/lean-status.sh"
    ;;
  down)
    "${COMPOSE[@]}" down
    ;;
  *)
    echo "Usage: $0 start|stop|restart|ps|status|down" >&2
    exit 1
    ;;
 esac
