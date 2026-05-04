#!/usr/bin/env bash
# Backup Postgres used by docker-compose.lean.yml to a gzipped SQL file.
# Usage (on host with compose):
#   cd /path/to/oziebot
#   ./infrastructure/lean/pg-backup.sh [output_dir]
set -euo pipefail
ROOT="$(cd "$(dirname "$0")/../.." && pwd)"
cd "${ROOT}"
OUT_DIR="${1:-${ROOT}/backups/lean-pg}"
mkdir -p "${OUT_DIR}"
STAMP="$(date -u +%Y%m%dT%H%M%SZ)"
FILE="${OUT_DIR}/oziebot-lean-${STAMP}.sql.gz"

docker compose -f docker-compose.lean.yml --env-file .env.lean exec -T postgres \
  pg_dump -U oziebot -d oziebot --no-owner \
  | gzip > "${FILE}"

echo "Wrote ${FILE}"
