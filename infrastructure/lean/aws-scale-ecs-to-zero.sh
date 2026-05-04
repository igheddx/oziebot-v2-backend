#!/usr/bin/env bash
# Scale Oziebot ECS services to desired-count 0 (stops Fargate charges for tasks).
# Does NOT delete task definitions, ECR images, or ALB. ALB still bills if present.
#
# Usage:
#   export AWS_PROFILE=oziebot
#   export AWS_REGION=us-east-1
#   ./infrastructure/lean/aws-scale-ecs-to-zero.sh
#
# Rollback: use aws-scale-ecs-up.sh (create separately) or console to set desired>=1.

set -euo pipefail
REGION="${AWS_REGION:-us-east-1}"
CLUSTER="${OZIEBOT_ECS_CLUSTER:-oziebot-prod}"

SERVICES=(
  oziebot-api
  oziebot-strategy-engine
  oziebot-risk-engine
  oziebot-execution-engine
  oziebot-alerts-worker
  oziebot-market-data-ingestor
)

for svc in "${SERVICES[@]}"; do
  echo "Scaling ${CLUSTER}/${svc} -> 0"
  aws ecs update-service \
    --region "${REGION}" \
    --cluster "${CLUSTER}" \
    --service "${svc}" \
    --desired-count 0
done

echo "Done. Verify: aws ecs describe-services --cluster ${CLUSTER} --services ${SERVICES[*]}"
