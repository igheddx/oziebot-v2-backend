#!/usr/bin/env bash
set -euo pipefail

AWS_REGION="${AWS_REGION:-$(aws configure get region)}"
if [[ -z "${AWS_REGION}" ]]; then
  echo "AWS_REGION is required." >&2
  exit 1
fi

ACCOUNT_ID="${ACCOUNT_ID:-$(aws sts get-caller-identity --query Account --output text)}"
if [[ -z "${ACCOUNT_ID}" ]]; then
  echo "ACCOUNT_ID could not be resolved." >&2
  exit 1
fi

BUCKET_NAME="${BUCKET_NAME:-teacherassist-prod-uploads-${ACCOUNT_ID}-${AWS_REGION}}"
TEMP_EXPIRATION_DAYS="${TEMP_EXPIRATION_DAYS:-3}"
EXPORT_EXPIRATION_DAYS="${EXPORT_EXPIRATION_DAYS:-14}"
PREFIX_ROOT="${PREFIX_ROOT:-teacher-assist}"

LIFECYCLE_FILE="$(mktemp)"
ENCRYPTION_FILE="$(mktemp)"
PUBLIC_ACCESS_FILE="$(mktemp)"
OWNERSHIP_FILE="$(mktemp)"
trap 'rm -f "${LIFECYCLE_FILE}" "${ENCRYPTION_FILE}" "${PUBLIC_ACCESS_FILE}" "${OWNERSHIP_FILE}"' EXIT

cat >"${PUBLIC_ACCESS_FILE}" <<JSON
{
  "BlockPublicAcls": true,
  "IgnorePublicAcls": true,
  "BlockPublicPolicy": true,
  "RestrictPublicBuckets": true
}
JSON

cat >"${OWNERSHIP_FILE}" <<JSON
{
  "Rules": [
    {
      "ObjectOwnership": "BucketOwnerEnforced"
    }
  ]
}
JSON

cat >"${ENCRYPTION_FILE}" <<JSON
{
  "Rules": [
    {
      "ApplyServerSideEncryptionByDefault": {
        "SSEAlgorithm": "AES256"
      },
      "BucketKeyEnabled": true
    }
  ]
}
JSON

cat >"${LIFECYCLE_FILE}" <<JSON
{
  "Rules": [
    {
      "ID": "teacher-assist-temp-expiration",
      "Status": "Enabled",
      "Filter": {
        "Prefix": "${PREFIX_ROOT}/temp/"
      },
      "Expiration": {
        "Days": ${TEMP_EXPIRATION_DAYS}
      }
    },
    {
      "ID": "teacher-assist-exports-expiration",
      "Status": "Enabled",
      "Filter": {
        "Prefix": "${PREFIX_ROOT}/exports/"
      },
      "Expiration": {
        "Days": ${EXPORT_EXPIRATION_DAYS}
      }
    }
  ]
}
JSON

if aws s3api head-bucket --bucket "${BUCKET_NAME}" >/dev/null 2>&1; then
  echo "Bucket already exists: ${BUCKET_NAME}"
else
  if [[ "${AWS_REGION}" == "us-east-1" ]]; then
    aws s3api create-bucket --bucket "${BUCKET_NAME}"
  else
    aws s3api create-bucket \
      --bucket "${BUCKET_NAME}" \
      --create-bucket-configuration "LocationConstraint=${AWS_REGION}"
  fi
  echo "Created bucket: ${BUCKET_NAME}"
fi

aws s3api put-public-access-block \
  --bucket "${BUCKET_NAME}" \
  --public-access-block-configuration "file://${PUBLIC_ACCESS_FILE}"

aws s3api put-bucket-ownership-controls \
  --bucket "${BUCKET_NAME}" \
  --ownership-controls "file://${OWNERSHIP_FILE}"

aws s3api put-bucket-encryption \
  --bucket "${BUCKET_NAME}" \
  --server-side-encryption-configuration "file://${ENCRYPTION_FILE}"

aws s3api put-bucket-lifecycle-configuration \
  --bucket "${BUCKET_NAME}" \
  --lifecycle-configuration "file://${LIFECYCLE_FILE}"

for prefix in \
  "${PREFIX_ROOT}/resources/" \
  "${PREFIX_ROOT}/student-work/" \
  "${PREFIX_ROOT}/print-packets/" \
  "${PREFIX_ROOT}/exports/" \
  "${PREFIX_ROOT}/temp/"; do
  aws s3api put-object --bucket "${BUCKET_NAME}" --key "${prefix}" >/dev/null
done

cat <<EOF
TeacherAssist private bucket bootstrap complete.

Bucket: ${BUCKET_NAME}
Region: ${AWS_REGION}
Prefix root: ${PREFIX_ROOT}
Temp expiration days: ${TEMP_EXPIRATION_DAYS}
Export expiration days: ${EXPORT_EXPIRATION_DAYS}

Recommended backend env:
  TEACHER_ASSIST_STORAGE_BACKEND=s3
  TEACHER_ASSIST_S3_BUCKET=${BUCKET_NAME}
  TEACHER_ASSIST_S3_REGION=${AWS_REGION}
  TEACHER_ASSIST_S3_PREFIX=${PREFIX_ROOT}
  TEACHER_ASSIST_S3_PRESIGN_EXPIRATION_SECONDS=900
EOF
