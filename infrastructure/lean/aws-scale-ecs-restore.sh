#!/usr/bin/env bash
# Restore Oziebot ECS services to run (desired count 1 per service).
# Adjust COUNT if you normally run more API replicas.
set -euo pipefail
REGION="${AWS_REGION:-us-east-1}"
CLUSTER="${OZIEBOT_ECS_CLUSTER:-oziebot-prod}"
COUNT="${ECS_DESIRED_COUNT:-1}"

SERVICES=(
  oziebot-api
  oziebot-market-data-ingestor
  oziebot-strategy-engine
  oziebot-risk-engine
  oziebot-execution-engine
  oziebot-alerts-worker
)

for svc in "${SERVICES[@]}"; do
  echo "Scaling ${CLUSTER}/${svc} -> ${COUNT}"
  aws ecs update-service \
    --region "${REGION}" \
    --cluster "${CLUSTER}" \
    --service "${svc}" \
    --desired-count "${COUNT}"
done

echo "Done. Confirm targets healthy in ECS console / ALB."
