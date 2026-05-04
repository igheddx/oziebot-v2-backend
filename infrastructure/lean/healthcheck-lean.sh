#!/usr/bin/env bash
# Basic health checks for lean compose stack. Run from repo root.
# Optional: LEAN_API_URL=http://127.0.0.1:8000
set -euo pipefail
ROOT="$(cd "$(dirname "$0")/../.." && pwd)"
cd "${ROOT}"
API="${LEAN_API_URL:-http://127.0.0.1:8000}"
FAIL=0

if curl -sf "${API}/v1/ready" >/dev/null; then
  echo "OK API ${API}/v1/ready"
else
  echo "FAIL API ${API}/v1/ready" >&2
  FAIL=1
fi

if docker compose -f docker-compose.lean.yml --env-file .env.lean exec -T redis redis-cli ping 2>/dev/null | grep -q PONG; then
  echo "OK Redis PING"
else
  echo "FAIL Redis" >&2
  FAIL=1
fi

if docker compose -f docker-compose.lean.yml --env-file .env.lean exec -T postgres pg_isready -U oziebot -d oziebot >/dev/null 2>&1; then
  echo "OK Postgres pg_isready"
else
  echo "FAIL Postgres" >&2
  FAIL=1
fi

exit "${FAIL}"
