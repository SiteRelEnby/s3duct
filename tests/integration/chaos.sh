#!/usr/bin/env bash
#
# Network chaos test using toxiproxy + MinIO.
#
# Requires:
#   - MinIO running on $MINIO_HOST:$MINIO_PORT (default localhost:9000)
#   - toxiproxy running on $TOXIPROXY_HOST:$TOXIPROXY_PORT (default localhost:8474)
#   - toxiproxy proxy "minio" listening on $TOXIC_LISTEN_PORT (default 19000)
#     forwarding to MinIO
#   - AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY set (minioadmin/minioadmin)
#   - S3DUCT_TEST_BUCKET set
#
# Usage:
#   docker compose -f tests/integration/docker-compose.chaos.yml up -d
#   export AWS_ACCESS_KEY_ID=minioadmin AWS_SECRET_ACCESS_KEY=minioadmin
#   export S3DUCT_TEST_BUCKET=chaos-test
#   bash tests/integration/chaos.sh
#
set -euo pipefail

TOXIPROXY_API="${TOXIPROXY_HOST:-localhost}:${TOXIPROXY_PORT:-8474}"
TOXIC_ENDPOINT="http://${TOXIPROXY_HOST:-localhost}:${TOXIC_LISTEN_PORT:-19000}"
MINIO_ENDPOINT="http://${MINIO_HOST:-localhost}:${MINIO_PORT:-9000}"
BUCKET="${S3DUCT_TEST_BUCKET:?S3DUCT_TEST_BUCKET not set}"
PREFIX="chaos-$$"
CHUNK_SIZE="256K"
AES_KEY="hex:$(python3 -c 'import os; print(os.urandom(32).hex())')"

# Test data: ~5MB, enough for ~20 chunks at 256K
DATA_FILE="/tmp/s3duct-chaos-input.bin"
OUT_FILE="/tmp/s3duct-chaos-output.bin"
DATA_SIZE=$((5 * 1024 * 1024))

toxiproxy_api() {
  curl -sf "http://${TOXIPROXY_API}$@"
}

add_toxic() {
  local name="$1"
  local type="$2"
  shift 2
  local attrs="$*"
  echo "  [toxic] Adding ${type}: ${name} ${attrs}"
  curl -sf -X POST "http://${TOXIPROXY_API}/proxies/minio/toxics" \
    -H 'Content-Type: application/json' \
    -d "{\"name\":\"${name}\",\"type\":\"${type}\",\"attributes\":{${attrs}}}" \
    >/dev/null
}

remove_toxic() {
  local name="$1"
  echo "  [toxic] Removing: ${name}"
  curl -sf -X DELETE "http://${TOXIPROXY_API}/proxies/minio/toxics/${name}" >/dev/null 2>&1 || true
}

clear_toxics() {
  # Remove all known toxics
  for name in latency bandwidth_limit slow timeout_kill reset_conn slicer_up; do
    remove_toxic "$name"
  done
}

disable_proxy() {
  echo "  [proxy] DISABLED (total outage)"
  curl -sf -X POST "http://${TOXIPROXY_API}/proxies/minio" \
    -H 'Content-Type: application/json' \
    -d '{"enabled":false}' >/dev/null
}

enable_proxy() {
  echo "  [proxy] ENABLED"
  curl -sf -X POST "http://${TOXIPROXY_API}/proxies/minio" \
    -H 'Content-Type: application/json' \
    -d '{"enabled":true}' >/dev/null
}

cleanup() {
  echo ""
  echo "--- Cleanup ---"
  clear_toxics
  enable_proxy
  aws --endpoint-url "${MINIO_ENDPOINT}" s3 rm "s3://${BUCKET}/${PREFIX}/" --recursive 2>/dev/null || true
  rm -f "${DATA_FILE}" "${OUT_FILE}"
}
trap cleanup EXIT

# Wait for services
echo "--- Waiting for services ---"
for i in $(seq 1 30); do
  if toxiproxy_api "/version" >/dev/null 2>&1; then
    echo "toxiproxy ready"
    break
  fi
  echo "  waiting for toxiproxy... ($i)"
  sleep 2
done

for i in $(seq 1 30); do
  if aws --endpoint-url "${MINIO_ENDPOINT}" s3 ls 2>/dev/null; then
    echo "MinIO ready"
    break
  fi
  echo "  waiting for MinIO... ($i)"
  sleep 2
done

# Create bucket if needed
aws --endpoint-url "${MINIO_ENDPOINT}" s3 mb "s3://${BUCKET}" 2>/dev/null || true

# Generate test data
echo "--- Generate test data (${DATA_SIZE} bytes) ---"
dd if=/dev/urandom of="${DATA_FILE}" bs=1K count=$((DATA_SIZE / 1024)) 2>/dev/null
EXPECTED=$(sha256sum "${DATA_FILE}" | cut -d' ' -f1)
echo "Input SHA256: ${EXPECTED}"

# =========================================================================
# Test 1: Upload with bandwidth throttling
# =========================================================================
echo ""
echo "=== Test 1: Upload under bandwidth throttle ==="
add_toxic "bandwidth_limit" "bandwidth" '"rate":128'  # 128 KB/s

cat "${DATA_FILE}" | s3duct put \
  --bucket "${BUCKET}" \
  --name "${PREFIX}-bw" \
  --chunk-size "${CHUNK_SIZE}" \
  --no-encrypt \
  --endpoint-url "${TOXIC_ENDPOINT}" \
  --summary none

remove_toxic "bandwidth_limit"

echo "--- Downloading (clean connection) ---"
s3duct get \
  --bucket "${BUCKET}" \
  --name "${PREFIX}-bw" \
  --endpoint-url "${MINIO_ENDPOINT}" \
  --summary none \
  > "${OUT_FILE}"

ACTUAL=$(sha256sum "${OUT_FILE}" | cut -d' ' -f1)
if [ "${ACTUAL}" != "${EXPECTED}" ]; then
  echo "FAIL: hash mismatch (bandwidth throttle)"
  exit 1
fi
echo "PASS: bandwidth throttle upload OK"

# =========================================================================
# Test 2: Upload with latency + jitter
# =========================================================================
echo ""
echo "=== Test 2: Upload under high latency + jitter ==="
add_toxic "latency" "latency" '"latency":500,"jitter":300'

cat "${DATA_FILE}" | s3duct put \
  --bucket "${BUCKET}" \
  --name "${PREFIX}-latency" \
  --chunk-size "${CHUNK_SIZE}" \
  --no-encrypt \
  --endpoint-url "${TOXIC_ENDPOINT}" \
  --summary none

remove_toxic "latency"

s3duct get \
  --bucket "${BUCKET}" \
  --name "${PREFIX}-latency" \
  --endpoint-url "${MINIO_ENDPOINT}" \
  --summary none \
  > "${OUT_FILE}"

ACTUAL=$(sha256sum "${OUT_FILE}" | cut -d' ' -f1)
if [ "${ACTUAL}" != "${EXPECTED}" ]; then
  echo "FAIL: hash mismatch (latency)"
  exit 1
fi
echo "PASS: high latency upload OK"

# =========================================================================
# Test 3: Upload with connection resets mid-transfer
# =========================================================================
echo ""
echo "=== Test 3: Upload with periodic connection resets ==="

# Background chaos: toggle reset_peer every few seconds
(
  for i in $(seq 1 6); do
    sleep 3
    add_toxic "reset_conn" "reset_peer" '"timeout":100' 2>/dev/null || true
    sleep 1
    remove_toxic "reset_conn" 2>/dev/null || true
  done
) &
CHAOS_PID=$!

cat "${DATA_FILE}" | s3duct put \
  --bucket "${BUCKET}" \
  --name "${PREFIX}-reset" \
  --chunk-size "${CHUNK_SIZE}" \
  --no-encrypt \
  --endpoint-url "${TOXIC_ENDPOINT}" \
  --summary none || true

wait ${CHAOS_PID} 2>/dev/null || true
clear_toxics

# Verify or resume
s3duct verify \
  --bucket "${BUCKET}" \
  --name "${PREFIX}-reset" \
  --endpoint-url "${MINIO_ENDPOINT}" \
  --summary none 2>/dev/null || {
    echo "  Upload incomplete after resets, re-running..."
    cat "${DATA_FILE}" | s3duct put \
      --bucket "${BUCKET}" \
      --name "${PREFIX}-reset" \
      --chunk-size "${CHUNK_SIZE}" \
      --no-encrypt \
      --endpoint-url "${MINIO_ENDPOINT}" \
      --summary none
  }

s3duct get \
  --bucket "${BUCKET}" \
  --name "${PREFIX}-reset" \
  --endpoint-url "${MINIO_ENDPOINT}" \
  --summary none \
  > "${OUT_FILE}"

ACTUAL=$(sha256sum "${OUT_FILE}" | cut -d' ' -f1)
if [ "${ACTUAL}" != "${EXPECTED}" ]; then
  echo "FAIL: hash mismatch (connection resets)"
  exit 1
fi
echo "PASS: connection reset recovery OK"

# =========================================================================
# Test 4: Total outage mid-upload, then recovery
# =========================================================================
echo ""
echo "=== Test 4: Total outage mid-upload ==="

# Start upload, kill connection after 3 seconds, wait, restore
(
  sleep 3
  disable_proxy
  sleep 8
  enable_proxy
) &
OUTAGE_PID=$!

cat "${DATA_FILE}" | s3duct put \
  --bucket "${BUCKET}" \
  --name "${PREFIX}-outage" \
  --chunk-size "${CHUNK_SIZE}" \
  --no-encrypt \
  --endpoint-url "${TOXIC_ENDPOINT}" \
  --summary none || true

wait ${OUTAGE_PID} 2>/dev/null || true
enable_proxy

# Resume upload via clean connection
echo "  Resuming upload..."
cat "${DATA_FILE}" | s3duct put \
  --bucket "${BUCKET}" \
  --name "${PREFIX}-outage" \
  --chunk-size "${CHUNK_SIZE}" \
  --no-encrypt \
  --endpoint-url "${MINIO_ENDPOINT}" \
  --summary none

s3duct get \
  --bucket "${BUCKET}" \
  --name "${PREFIX}-outage" \
  --endpoint-url "${MINIO_ENDPOINT}" \
  --summary none \
  > "${OUT_FILE}"

ACTUAL=$(sha256sum "${OUT_FILE}" | cut -d' ' -f1)
if [ "${ACTUAL}" != "${EXPECTED}" ]; then
  echo "FAIL: hash mismatch (outage recovery)"
  exit 1
fi
echo "PASS: outage recovery OK"

# =========================================================================
# Test 5: Chaotic download (bandwidth + latency + slicer)
# =========================================================================
echo ""
echo "=== Test 5: Download under combined chaos ==="

# Upload clean first
cat "${DATA_FILE}" | s3duct put \
  --bucket "${BUCKET}" \
  --name "${PREFIX}-dlchaos" \
  --chunk-size "${CHUNK_SIZE}" \
  --key "${AES_KEY}" \
  --endpoint-url "${MINIO_ENDPOINT}" \
  --summary none

# Download through chaos
add_toxic "bandwidth_limit" "bandwidth" '"rate":64'
add_toxic "latency" "latency" '"latency":200,"jitter":150'
add_toxic "slicer_up" "slicer" '"average_size":500,"size_variation":200,"delay":100'

s3duct get \
  --bucket "${BUCKET}" \
  --name "${PREFIX}-dlchaos" \
  --key "${AES_KEY}" \
  --endpoint-url "${TOXIC_ENDPOINT}" \
  --summary none \
  > "${OUT_FILE}"

clear_toxics

ACTUAL=$(sha256sum "${OUT_FILE}" | cut -d' ' -f1)
if [ "${ACTUAL}" != "${EXPECTED}" ]; then
  echo "FAIL: hash mismatch (chaotic download)"
  exit 1
fi
echo "PASS: chaotic download OK"

# =========================================================================
# Test 6: Upload with tight disk + bandwidth chaos (backpressure thrashing)
# =========================================================================
echo ""
echo "=== Test 6: Backpressure thrashing (tight disk + slow network) ==="

# Bandwidth oscillation in background
(
  for i in $(seq 1 10); do
    add_toxic "bandwidth_limit" "bandwidth" '"rate":32' 2>/dev/null || true
    sleep 2
    remove_toxic "bandwidth_limit" 2>/dev/null || true
    add_toxic "bandwidth_limit" "bandwidth" '"rate":512' 2>/dev/null || true
    sleep 1
    remove_toxic "bandwidth_limit" 2>/dev/null || true
  done
) &
BW_PID=$!

cat "${DATA_FILE}" | s3duct put \
  --bucket "${BUCKET}" \
  --name "${PREFIX}-thrash" \
  --chunk-size "${CHUNK_SIZE}" \
  --no-encrypt \
  --endpoint-url "${TOXIC_ENDPOINT}" \
  --diskspace-limit 768K \
  --summary none

wait ${BW_PID} 2>/dev/null || true
clear_toxics

s3duct get \
  --bucket "${BUCKET}" \
  --name "${PREFIX}-thrash" \
  --endpoint-url "${MINIO_ENDPOINT}" \
  --summary none \
  > "${OUT_FILE}"

ACTUAL=$(sha256sum "${OUT_FILE}" | cut -d' ' -f1)
if [ "${ACTUAL}" != "${EXPECTED}" ]; then
  echo "FAIL: hash mismatch (backpressure thrashing)"
  exit 1
fi
echo "PASS: backpressure thrashing OK"

echo ""
echo "=== All chaos tests passed ==="
