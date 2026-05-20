#!/usr/bin/env bash
# Summarize the active lean deployment topology and key operational checks.
# Usage:
#   ./infrastructure/lean/lean-status.sh
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/../.." && pwd)"
cd "${ROOT}"

ENV_FILE="${LEAN_ENV_FILE:-.env.lean}"
COMPOSE_FILE="docker-compose.lean.yml"

lookup_env() {
  local key="$1"
  local value
  value="$(grep -E "^${key}=" "${ENV_FILE}" 2>/dev/null | tail -n 1 | cut -d= -f2- || true)"
  value="${value%\"}"
  value="${value#\"}"
  value="${value%\'}"
  value="${value#\'}"
  printf '%s' "${value}"
}

file_mode() {
  local path="$1"
  if stat -f '%Lp' "${path}" >/dev/null 2>&1; then
    stat -f '%Lp' "${path}"
  else
    stat -c '%a' "${path}"
  fi
}

warn_if_env_permissions_broad() {
  local mode="$1"
  case "${mode}" in
    600|400)
      echo "Env file permissions: ${mode} (restricted)"
      ;;
    *)
      echo "Env file permissions: ${mode} (consider chmod 600 ${ENV_FILE})"
      ;;
  esac
}

project_name="${COMPOSE_PROJECT_NAME:-}"
if [[ -z "${project_name}" && -f "${ENV_FILE}" ]]; then
  project_name="$(lookup_env COMPOSE_PROJECT_NAME)"
fi
project_name="${project_name:-oziebot}"

api_bind="${LEAN_API_BIND:-}"
if [[ -z "${api_bind}" && -f "${ENV_FILE}" ]]; then
  api_bind="$(lookup_env LEAN_API_BIND)"
fi
api_bind="${api_bind:-127.0.0.1}"

cors_origins="${CORS_ORIGINS:-}"
if [[ -z "${cors_origins}" && -f "${ENV_FILE}" ]]; then
  cors_origins="$(lookup_env CORS_ORIGINS)"
fi

running_services="$(
  docker ps \
    --filter "label=com.docker.compose.project=${project_name}" \
    --format '{{.Label "com.docker.compose.service"}}' 2>/dev/null \
    | sed '/^$/d' \
    | sort -u
)"

ingress_mode="direct-api"
if printf '%s\n' "${running_services}" | grep -qx 'caddy'; then
  ingress_mode="caddy-edge"
fi

echo "Lean topology"
echo "Project: ${project_name}"
echo "Backend stack: docker-compose.lean.yml"
echo "Frontend delivery: CloudFront/S3 static site"
echo "API ingress: ${ingress_mode}"
echo "API bind: ${api_bind}:8000"
if [[ -n "${cors_origins}" ]]; then
  echo "CORS origins: ${cors_origins}"
fi

if [[ -f "${ENV_FILE}" ]]; then
  warn_if_env_permissions_broad "$(file_mode "${ENV_FILE}")"
else
  echo "Env file: missing ${ENV_FILE}"
  exit 1
fi

echo
echo "Compose services"
docker compose -f "${COMPOSE_FILE}" --env-file "${ENV_FILE}" ps || true

BACKUP_DIR="${LEAN_BACKUP_DIR:-${ROOT}/backups/lean-pg}"
latest_backup="$(
  find "${BACKUP_DIR}" -type f -name 'oziebot-lean-*.sql.gz' -print 2>/dev/null | sort | tail -n 1
)"
if [[ -n "${latest_backup}" ]]; then
  echo
  echo "Latest backup: ${latest_backup}"
else
  echo
  echo "Latest backup: none found under ${BACKUP_DIR}"
fi

echo
"${ROOT}/infrastructure/lean/healthcheck-lean.sh"
