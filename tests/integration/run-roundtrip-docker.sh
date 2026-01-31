#!/usr/bin/env bash
#
# Run roundtrip.sh locally with MinIO in Docker.
# This is disposable: the container is removed on exit.
#
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT_DIR="$(cd "${SCRIPT_DIR}/../.." && pwd)"
CONTAINER_NAME="s3duct-roundtrip-minio"

if ! command -v docker >/dev/null 2>&1; then
  echo "ERROR: docker is required." >&2
  exit 1
fi

cleanup() {
  echo ""
  echo "--- Docker cleanup ---"
  docker rm -f "${CONTAINER_NAME}" >/dev/null 2>&1 || true
}
trap cleanup EXIT

# Prefer repo venv if present.
if [ -d "${ROOT_DIR}/.venv/bin" ]; then
  export PATH="${ROOT_DIR}/.venv/bin:${PATH}"
fi

if ! command -v s3duct >/dev/null 2>&1; then
  echo "ERROR: s3duct not found in PATH (try: pip install -e .)" >&2
  exit 1
fi

if ! command -v aws >/dev/null 2>&1; then
  echo "ERROR: aws CLI not found in PATH (try: pip install awscli)." >&2
  exit 1
fi

echo "--- Starting MinIO (docker) ---"
docker rm -f "${CONTAINER_NAME}" >/dev/null 2>&1 || true
docker run -d --name "${CONTAINER_NAME}" \
  -p 9000:9000 \
  -e MINIO_ROOT_USER=minioadmin \
  -e MINIO_ROOT_PASSWORD=minioadmin \
  minio/minio server /data

echo "--- Waiting for MinIO ---"
for i in $(seq 1 30); do
  docker exec "${CONTAINER_NAME}" mc ready local 2>/dev/null && break
  echo "  waiting... ($i)"
  sleep 2
done

# Set defaults; allow overrides.
export AWS_ACCESS_KEY_ID="${AWS_ACCESS_KEY_ID:-minioadmin}"
export AWS_SECRET_ACCESS_KEY="${AWS_SECRET_ACCESS_KEY:-minioadmin}"
export S3DUCT_TEST_BUCKET="${S3DUCT_TEST_BUCKET:-roundtrip-test}"
export S3DUCT_ENDPOINT_URL="${S3DUCT_ENDPOINT_URL:-http://localhost:9000}"

echo "--- Creating test bucket ---"
aws --endpoint-url "${S3DUCT_ENDPOINT_URL}" s3 mb "s3://${S3DUCT_TEST_BUCKET}" 2>/dev/null || true

echo "--- Running roundtrip test ---"
exec "${SCRIPT_DIR}/roundtrip.sh"
