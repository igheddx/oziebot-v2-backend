#!/usr/bin/env bash
# Deploy lean stack on a remote Docker host via SSH.
# Prerequisites: Docker + Docker Compose v2 on server; repo copied or cloned there.
#
# Usage:
#   export LEAN_SSH="ubuntu@203.0.113.50"
#   export LEAN_REPO_PATH="/home/ubuntu/oziebot"   # path ON THE SERVER
#   ./infrastructure/lean/deploy-lean-host.sh
#
# Or from your laptop (syncs current directory to server, then compose up):
#   export LEAN_SSH="ubuntu@host"
#   export LEAN_SYNC_LOCAL="/path/to/oziebot"
#   export LEAN_REPO_PATH="/home/ubuntu/oziebot"
#   export LEAN_SH='ssh -i ~/.ssh/key.pem'   # if not using ssh-agent/default key
#   LEAN_USE_EDGE=1 ./infrastructure/lean/deploy-lean-host.sh --rsync   # rsync shares LEAN_SH unless RSYNC_RSH is set
#
set -euo pipefail

LEAN_SSH="${LEAN_SSH:?Set LEAN_SSH=user@host}"
LEAN_REPO_PATH="${LEAN_REPO_PATH:?Set LEAN_REPO_PATH to repo path on server}"
LEAN_SH="${LEAN_SH:-ssh}"
RSYNC_SHELL="${RSYNC_RSH:-$LEAN_SH}"
COMPOSE="docker compose -f docker-compose.lean.yml --env-file .env.lean"
if [[ "${LEAN_USE_EDGE:-0}" == "1" ]]; then
  COMPOSE="docker compose -f docker-compose.lean.yml -f docker-compose.lean.edge.yml --env-file .env.lean"
fi
if [[ "${1:-}" == "--rsync" ]]; then
  LEAN_SYNC_LOCAL="${LEAN_SYNC_LOCAL:-$(cd "$(dirname "$0")/../.." && pwd)}"
  rsync -az --delete \
    --rsh="$RSYNC_SHELL" \
    --exclude '.git' \
    --exclude '.env.lean' \
    --exclude 'node_modules' \
    --exclude '.venv' \
    --exclude '**/__pycache__' \
    --exclude 'frontend' \
    "${LEAN_SYNC_LOCAL}/" "${LEAN_SSH}:${LEAN_REPO_PATH}/"
fi

$LEAN_SH "${LEAN_SSH}" bash -s <<EOF
set -euo pipefail
mkdir -p "${LEAN_REPO_PATH}/infrastructure/lean"
mkdir -p "${LEAN_REPO_PATH}/infrastructure/aws"
cd "${LEAN_REPO_PATH}"
test -f .env.lean || { echo "Missing .env.lean on server"; exit 1; }
${COMPOSE} build
${COMPOSE} up -d
${COMPOSE} ps
EOF

echo "Deploy complete. Run ./infrastructure/lean/healthcheck-lean.sh remotely or via SSH."
