#!/usr/bin/env bash
# Export read-mostly AWS metadata for Oziebot prod (account in service-map.yml).
# Does NOT print secret values unless EXPORT_SECRET_VALUES=1 (use with extreme care).
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
ARCHIVE="${ROOT}/archive/snapshots"
STAMP="$(date -u +%Y%m%d-%H%M%S)"
OUT="${ARCHIVE}/snapshot-${STAMP}"
REGION="${AWS_REGION:-us-east-1}"
CLUSTER="${OZIEBOT_ECS_CLUSTER:-oziebot-prod}"
REDIS_ID="${OZIEBOT_ELASTICACHE_ID:-oziebot-prod-redis}"
CF_DIST="${OZIEBOT_CLOUDFRONT_ID:-E3JE0URVE1J1DJ}"

mkdir -p "${OUT}"/{sts,ecs,ecr,elbv2,elasticache,rds,secretsmanager,ec2,s3,cloudfront,iam}

echo "Writing to ${OUT}"
export AWS_PAGER=""

aws sts get-caller-identity --region "${REGION}" >"${OUT}/sts/caller-identity.json"
echo "{\"exported_at_utc\":\"$(date -u +%Y-%m-%dT%H:%M:%SZ)\",\"region\":\"${REGION}\",\"cluster\":\"${CLUSTER}\"}" \
  >"${OUT}/manifest.json"

# --- ECS ---
aws ecs describe-clusters --region "${REGION}" --clusters "${CLUSTER}" \
  >"${OUT}/ecs/describe-clusters.json" 2>/dev/null || echo "{}" >"${OUT}/ecs/describe-clusters.json"

SERVICES_JSON="${OUT}/ecs/list-services.json"
aws ecs list-services --region "${REGION}" --cluster "${CLUSTER}" >"${SERVICES_JSON}" || echo '{"serviceArns":[]}' >"${SERVICES_JSON}"

if command -v jq >/dev/null 2>&1; then
  mapfile -t SERVICE_ARNS < <(jq -r '.serviceArns[]?' "${SERVICES_JSON}" 2>/dev/null || true)
else
  # Minimal fallback without jq: skip per-service describe
  SERVICE_ARNS=()
fi

if ((${#SERVICE_ARNS[@]} > 0)); then
  aws ecs describe-services --region "${REGION}" --cluster "${CLUSTER}" --services "${SERVICE_ARNS[@]}" \
    >"${OUT}/ecs/describe-services.json"
else
  echo '{"services":[]}' >"${OUT}/ecs/describe-services.json"
fi

mapfile -t TASK_DEF_ARNS < <(
  jq -r '.services[]?.taskDefinition // empty' "${OUT}/ecs/describe-services.json" 2>/dev/null | sort -u || true
)
for arn in "${TASK_DEF_ARNS[@]:-}"; do
  [[ -z "${arn}" ]] && continue
  safe="${arn//[^A-Za-z0-9._-]/_}"
  aws ecs describe-task-definition --region "${REGION}" --task-definition "${arn}" \
    >"${OUT}/ecs/task-definition-${safe}.json" || true
done

# --- ECR (oziebot/*) ---
aws ecr describe-repositories --region "${REGION}" \
  --repository-names oziebot/api oziebot/strategy-engine oziebot/risk-engine oziebot/execution-engine oziebot/alerts-worker oziebot/market-data-ingestor \
  >"${OUT}/ecr/describe-repositories.json" 2>/dev/null || aws ecr describe-repositories --region "${REGION}" >"${OUT}/ecr/describe-repositories-all.json"

# --- ALB / target groups (filter by name substring) ---
aws elbv2 describe-load-balancers --region "${REGION}" >"${OUT}/elbv2/describe-load-balancers.json"
aws elbv2 describe-target-groups --region "${REGION}" >"${OUT}/elbv2/describe-target-groups.json"
if command -v jq >/dev/null 2>&1; then
  mapfile -t TG_ARNS < <(jq -r '.TargetGroups[]? | select(.TargetGroupName? | test("oziebot")) | .TargetGroupArn' "${OUT}/elbv2/describe-target-groups.json" 2>/dev/null || true)
  if ((${#TG_ARNS[@]} > 0)); then
    aws elbv2 describe-target-health --region "${REGION}" --target-group-arns "${TG_ARNS[@]}" \
      >"${OUT}/elbv2/describe-target-health.json" || true
  fi
  mapfile -t LB_ARNS < <(jq -r '.LoadBalancers[]? | select(.LoadBalancerName? | test("oziebot")) | .LoadBalancerArn' "${OUT}/elbv2/describe-load-balancers.json" 2>/dev/null || true)
  for lb in "${LB_ARNS[@]:-}"; do
    [[ -z "${lb}" ]] && continue
    safe="${lb//[^A-Za-z0-9._-]/_}"
    aws elbv2 describe-listeners --region "${REGION}" --load-balancer-arn "${lb}" \
      >"${OUT}/elbv2/listeners-${safe}.json" || true
  done
fi

# --- ElastiCache ---
aws elasticache describe-replication-groups --region "${REGION}" \
  --replication-group-id "${REDIS_ID}" >"${OUT}/elasticache/describe-replication-groups.json" 2>/dev/null \
  || echo "{}" >"${OUT}/elasticache/describe-replication-groups.json"

# --- RDS (all instances in region; filter locally) ---
aws rds describe-db-instances --region "${REGION}" >"${OUT}/rds/describe-db-instances.json" 2>/dev/null || echo "[]" >"${OUT}/rds/describe-db-instances.json"

# --- Secrets Manager (metadata only) ---
aws secretsmanager list-secrets --region "${REGION}" \
  --filters Key=name,Values=oziebot/ >"${OUT}/secretsmanager/list-secrets-oziebot.json" 2>/dev/null \
  || aws secretsmanager list-secrets --region "${REGION}" >"${OUT}/secretsmanager/list-secrets-all.json"

if [[ "${EXPORT_SECRET_VALUES:-0}" == "1" ]]; then
  mkdir -p "${OUT}/secretsmanager/secret-values-REDACT_IN_GIT"
  echo "EXPORT_SECRET_VALUES=1: dumping secret strings to ${OUT}/secretsmanager/secret-values-REDACT_IN_GIT/"
  if command -v jq >/dev/null 2>&1; then
    while IFS= read -r sarn; do
      [[ -z "${sarn}" ]] && continue
      sname="$(aws secretsmanager describe-secret --region "${REGION}" --secret-id "${sarn}" --query Name --output text 2>/dev/null || echo unknown)"
      safe="${sname//[^A-Za-z0-9._-]/_}"
      aws secretsmanager get-secret-value --region "${REGION}" --secret-id "${sarn}" \
        >"${OUT}/secretsmanager/secret-values-REDACT_IN_GIT/${safe}.json" || true
    done < <(jq -r '.SecretList[]?.ARN // empty' "${OUT}/secretsmanager/list-secrets-oziebot.json" 2>/dev/null || true)
  fi
fi

# --- EC2: known subnets & SG from github-actions-config (edit if yours differ) ---
for sid in subnet-029221f44a3665e70 subnet-0670572a23c04d833; do
  aws ec2 describe-subnets --region "${REGION}" --subnet-ids "${sid}" \
    >"${OUT}/ec2/subnet-${sid}.json" 2>/dev/null || true
done
for sgid in sg-0c13b89a1c6c657b9; do
  aws ec2 describe-security-groups --region "${REGION}" --group-ids "${sgid}" \
    >"${OUT}/ec2/security-group-${sgid}.json" 2>/dev/null || true
done

# --- S3 (head / location for known buckets) ---
for bucket in app.oziebot.com oziebot-prod-observability; do
  aws s3api head-bucket --bucket "${bucket}" 2>/dev/null && echo "{\"ok\":true,\"bucket\":\"${bucket}\"}" >"${OUT}/s3/head-${bucket}.json" \
    || echo "{\"ok\":false,\"bucket\":\"${bucket}\"}" >"${OUT}/s3/head-${bucket}.json"
  aws s3api get-bucket-location --bucket "${bucket}" >"${OUT}/s3/location-${bucket}.json" 2>/dev/null || true
done

# --- CloudFront ---
aws cloudfront get-distribution --id "${CF_DIST}" >"${OUT}/cloudfront/get-distribution-${CF_DIST}.json" 2>/dev/null || echo "{}" >"${OUT}/cloudfront/get-distribution.json"

# --- IAM roles referenced in docs ---
for role in oziebot-ecs-execution-role oziebot-ecs-task-role cryptobotty-deploy; do
  aws iam get-role --role-name "${role}" >"${OUT}/iam/role-${role}.json" 2>/dev/null || true
  aws iam list-attached-role-policies --role-name "${role}" >"${OUT}/iam/role-${role}-attached.json" 2>/dev/null || true
  aws iam list-role-policies --role-name "${role}" >"${OUT}/iam/role-${role}-inline-names.json" 2>/dev/null || true
done

# --- Copy versioned infra files into this snapshot (no sibling snapshots) ---
REPO_INFRA_COPY="${OUT}/repo-infrastructure-aws-copy"
mkdir -p "${REPO_INFRA_COPY}"
cp -R "${ROOT}/backend" "${ROOT}/iam" "${REPO_INFRA_COPY}/"
cp "${ROOT}/github-actions-config.md" "${ROOT}/godaddy-dns-template.yml" "${REPO_INFRA_COPY}/" 2>/dev/null || true
mkdir -p "${REPO_INFRA_COPY}/archive"
cp "${ROOT}/archive/README.md" "${ROOT}/archive/restore-checklist.md" "${REPO_INFRA_COPY}/archive/" 2>/dev/null || true

echo "Done. Archive: ${OUT}"
echo "Next: copy ${OUT} to encrypted storage; do not commit snapshots to git."
