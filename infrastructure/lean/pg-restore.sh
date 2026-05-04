#!/usr/bin/env bash
# Restore Postgres from gzipped pg_dump into lean-mode Postgres container.
# Usage:
#   ./infrastructure/lean/pg-restore.sh /path/to/oziebot-lean-....sql.gz
#
# Destructive for the target database; backup first.
set -euo pipefail
ROOT="$(cd "$(dirname "$0")/../.." && pwd)"
cd "${ROOT}"
DUMP="${1:?pass path to .sql.gz dump}"

if [[ ! -f "${DUMP}" ]]; then
  echo "File not found: ${DUMP}" >&2
  exit 1
fi

read -r -p "This will replace DB oziebot in the lean postgres container. Continue? [y/N] " ok
if [[ "${ok}" != "y" && "${ok}" != "Y" ]]; then
  exit 1
fi

docker compose -f docker-compose.lean.yml --env-file .env.lean exec -T postgres \
  psql -U oziebot -d postgres -v ON_ERROR_STOP=1 \
  -c "DROP DATABASE IF EXISTS oziebot;"
docker compose -f docker-compose.lean.yml --env-file .env.lean exec -T postgres \
  psql -U oziebot -d postgres -v ON_ERROR_STOP=1 \
  -c "CREATE DATABASE oziebot OWNER oziebot;"

gunzip -c "${DUMP}" | docker compose -f docker-compose.lean.yml --env-file .env.lean exec -T postgres \
  psql -U oziebot -d oziebot -v ON_ERROR_STOP=1

echo "Restore complete. Consider: docker compose ... restart api"
