#!/usr/bin/env bash
# Set retention on CloudWatch log groups (reduces long-term log storage cost).
# Default: 7 days. Scope: groups matching prefix (env LOG_GROUP_PREFIX).
#
# Usage:
#   export AWS_PROFILE=oziebot
#   export AWS_REGION=us-east-1
#   export RETENTION_DAYS=7
#   export LOG_GROUP_PREFIX=/ecs/
#   ./infrastructure/lean/aws-cloudwatch-retention.sh
#
# Uses JSON output parsing (works on macOS bash 3.2).

set -euo pipefail
REGION="${AWS_REGION:-us-east-1}"
DAYS="${RETENTION_DAYS:-7}"
PREFIX="${LOG_GROUP_PREFIX:-/ecs/}"

TMP="$(mktemp)"
trap 'rm -f "${TMP}"' EXIT

aws logs describe-log-groups --region "${REGION}" --log-group-name-prefix "${PREFIX}" \
  --query 'logGroups[].logGroupName' --output json > "${TMP}" 2>/dev/null || echo "[]" > "${TMP}"

COUNT="$(python3 -c "
import json,sys
with open(sys.argv[1]) as f:
    d=json.load(f)
print(len(d) if isinstance(d,list) else 0)
" "${TMP}")"

if [[ "${COUNT}" == "0" ]]; then
  echo "No log groups found with prefix ${PREFIX}"
  exit 0
fi

python3 -c "
import json, subprocess, sys
REGION, DAYS, TMP = sys.argv[1], sys.argv[2], sys.argv[3]
with open(TMP) as f:
    names = json.load(f)
for g in names:
    print(f'Retention {DAYS}d: {g}', file=sys.stderr)
    subprocess.run(
        ['aws','logs','put-retention-policy',
         '--region', REGION, '--log-group-name', g, '--retention-in-days', str(DAYS)],
        check=True,
    )
" "${REGION}" "${DAYS}" "${TMP}"

echo "Updated ${COUNT} log group(s)."
