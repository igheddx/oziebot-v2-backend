#!/usr/bin/env bash
# Deploy lean stack on a remote Docker host via SSH.
# Prerequisites: Docker + Docker Compose v2 on server; repo copied or cloned there.
#
# Usage:
#   export LEAN_SSH="ubuntu@203.0.113.50"
#   export LEAN_REPO_PATH="/home/ubuntu/oziebot"   # path ON THE SERVER
#   ./infrastructure/lean/deploy-lean-host.sh              # compose only on remote (no rsync)
#   ./infrastructure/lean/deploy-lean-host.sh --rsync       # rsync local tree, then compose
#   ./infrastructure/lean/deploy-lean-host.sh --sync-only   # rsync only (no compose)
#   ./infrastructure/lean/deploy-lean-host.sh --remote-only # ssh + compose/build only (no rsync)
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

mode="remote_only"
case "${1:-}" in
"") ;;
--rsync) mode="sync_and_deploy" ;;
--sync-only) mode="sync_only" ;;
--remote-only) mode="remote_only" ;;
*)
  echo "usage: $0 [--rsync | --sync-only | --remote-only]" >&2
  exit 1
  ;;
esac

maybe_rsync() {
  if [[ "${mode}" != "sync_only" && "${mode}" != "sync_and_deploy" ]]; then
    return 0
  fi
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
}

maybe_remote_compose() {
  if [[ "${mode}" != "sync_and_deploy" && "${mode}" != "remote_only" ]]; then
    return 0
  fi

  if [[ "${LEAN_REMOTE_VERBOSE:-0}" == "1" ]]; then
    remote_shell_head="set -xeuo pipefail"
  else
    remote_shell_head="set -euo pipefail"
  fi

  $LEAN_SH "${LEAN_SSH}" bash -s <<EOF
${remote_shell_head}
mkdir -p "${LEAN_REPO_PATH}/infrastructure/lean"
mkdir -p "${LEAN_REPO_PATH}/infrastructure/aws"
cd "${LEAN_REPO_PATH}"
test -f .env.lean || { echo "::error::Missing .env.lean under ${LEAN_REPO_PATH} — create it on the host (.env.lean is not rsync'd). Copy from .env.lean.example and fill secrets."; exit 2; }

# Serialize deploys on the host so concurrent workflow/manual runs do not race
# while recreating the same compose containers.
exec 9>/tmp/oziebot-lean-deploy.lock
flock -w 600 9 || { echo "::error::Timed out waiting for /tmp/oziebot-lean-deploy.lock"; exit 3; }

# Drop this compose project so rebuild can replace containers.
${COMPOSE} down --remove-orphans 2>/dev/null || true
${COMPOSE} rm -f -s 2>/dev/null || true

# Edge mode publishes host :80 and :443. Another compose project (or stray Caddy) often
# still holds them — 'up' then fails with "Bind for 0.0.0.0:80 failed". Stop any
# Docker container still publishing those ports (dedicated lean hosts only).
for _lp in 80 443; do
  _ids=\$(docker ps -q --filter "publish=\${_lp}" 2>/dev/null || true)
  if [ -n "\${_ids}" ]; then
    echo "Stopping containers publishing host port \${_lp}: \${_ids}"
    docker stop \${_ids} 2>/dev/null || true
  fi
done

 ${COMPOSE} build
 ${COMPOSE} up -d --force-recreate --remove-orphans
 ${COMPOSE} ps
 ./infrastructure/lean/healthcheck-lean.sh
 ./infrastructure/lean/lean-status.sh
EOF
}

maybe_rsync

maybe_remote_compose

case "${mode}" in
sync_only)
  echo "Rsync complete (compose skipped)."
  ;;
sync_and_deploy | remote_only)
  echo "Deploy complete and health checks passed."
  ;;
esac
