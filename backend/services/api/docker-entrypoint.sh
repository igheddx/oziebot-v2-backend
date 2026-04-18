#!/bin/sh
set -e
export PYTHONPATH="${PYTHONPATH:-/app/services/api/src}"
cd /app/services/api

if [ "${RUN_DB_MIGRATIONS_ON_STARTUP:-1}" = "1" ]; then
  alembic upgrade head
fi

exec uvicorn oziebot_api.main:app --host 0.0.0.0 --port 8000
