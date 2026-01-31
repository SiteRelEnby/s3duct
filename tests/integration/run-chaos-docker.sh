#!/usr/bin/env bash
#
# Run chaos.sh locally with MinIO + toxiproxy in Docker.
# This is disposable: containers and volumes are removed on exit.
#
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
COMPOSE_FILE="${SCRIPT_DIR}/docker-compose.chaos.yml"
ROOT_DIR="$(cd "${SCRIPT_DIR}/../.." && pwd)"

if command -v docker >/dev/null 2>&1; then
  if docker compose version >/dev/null 2>&1; then
    DOCKER_COMPOSE="docker compose"
  elif command -v docker-compose >/dev/null 2>&1; then
    DOCKER_COMPOSE="docker-compose"
  else
    echo "ERROR: docker compose plugin or docker-compose is required." >&2
    exit 1
  fi
else
  echo "ERROR: docker is required." >&2
  exit 1
fi

cleanup() {
  echo ""
  echo "--- Docker cleanup ---"
  ${DOCKER_COMPOSE} -f "${COMPOSE_FILE}" down -v --remove-orphans >/dev/null 2>&1 || true
}
trap cleanup EXIT

echo "--- Starting MinIO + toxiproxy (docker) ---"
${DOCKER_COMPOSE} -f "${COMPOSE_FILE}" up -d --remove-orphans

# Set defaults; allow overrides.
export AWS_ACCESS_KEY_ID="${AWS_ACCESS_KEY_ID:-minioadmin}"
export AWS_SECRET_ACCESS_KEY="${AWS_SECRET_ACCESS_KEY:-minioadmin}"
export S3DUCT_TEST_BUCKET="${S3DUCT_TEST_BUCKET:-chaos-test}"
export MINIO_HOST="${MINIO_HOST:-localhost}"
export MINIO_PORT="${MINIO_PORT:-9000}"
export TOXIPROXY_HOST="${TOXIPROXY_HOST:-localhost}"
export TOXIPROXY_PORT="${TOXIPROXY_PORT:-8474}"
export TOXIC_LISTEN_PORT="${TOXIC_LISTEN_PORT:-19000}"

# Prefer repo venv if present.
if [ -d "${ROOT_DIR}/.venv/bin" ]; then
  export PATH="${ROOT_DIR}/.venv/bin:${PATH}"
fi

if ! command -v s3duct >/dev/null 2>&1; then
  echo "ERROR: s3duct not found in PATH (try: pip install -e .)" >&2
  exit 1
fi

if ! command -v aws >/dev/null 2>&1; then
  echo "ERROR: aws CLI not found in PATH (needed by chaos.sh)." >&2
  exit 1
fi

echo "--- Running chaos test ---"
exec "${SCRIPT_DIR}/chaos.sh"
