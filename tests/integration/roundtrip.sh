#!/usr/bin/env bash
#
# Basic s3duct roundtrip integration test.
#
# Required env vars:
#   AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY
#   S3DUCT_TEST_BUCKET
#
# Optional:
#   S3DUCT_ENDPOINT_URL  — custom endpoint (MinIO, R2, etc.)
#   S3DUCT_TEST_PREFIX   — key prefix for isolation (default: ci-$$)
#
set -euo pipefail

PREFIX="${S3DUCT_TEST_PREFIX:-ci-$$}"
BUCKET="${S3DUCT_TEST_BUCKET:?S3DUCT_TEST_BUCKET not set}"
ENDPOINT_OPT=""
if [ -n "${S3DUCT_ENDPOINT_URL:-}" ]; then
  ENDPOINT_OPT="--endpoint-url ${S3DUCT_ENDPOINT_URL}"
fi

CHUNK_SIZE="32K"
AES_KEY="hex:$(python3 -c 'import os; print(os.urandom(32).hex())')"
STREAM_NAME="${PREFIX}-roundtrip"
STREAM_NAME_ENC="${PREFIX}-roundtrip-enc"
STREAM_NAME_ENCM="${PREFIX}-roundtrip-encmanifest"

cleanup() {
  echo "--- Cleanup ---"
  pip install awscli >/dev/null 2>&1 || true
  local aws_ep=""
  if [ -n "${S3DUCT_ENDPOINT_URL:-}" ]; then
    aws_ep="--endpoint-url ${S3DUCT_ENDPOINT_URL}"
  fi
  aws $aws_ep s3 rm "s3://${BUCKET}/${STREAM_NAME}/" --recursive 2>/dev/null || true
  aws $aws_ep s3 rm "s3://${BUCKET}/${STREAM_NAME_ENC}/" --recursive 2>/dev/null || true
  aws $aws_ep s3 rm "s3://${BUCKET}/${STREAM_NAME_ENCM}/" --recursive 2>/dev/null || true
  rm -f /tmp/s3duct-test-input.bin /tmp/s3duct-test-output.bin
}
trap cleanup EXIT

# Generate test data (multiple chunks worth)
echo "--- Generate test data ---"
dd if=/dev/urandom of=/tmp/s3duct-test-input.bin bs=1K count=128 2>/dev/null
EXPECTED=$(sha256sum /tmp/s3duct-test-input.bin | cut -d' ' -f1)
echo "Input SHA256: ${EXPECTED}"

# =========================================================================
# Test 1: Unencrypted roundtrip
# =========================================================================
echo ""
echo "=== Test 1: Unencrypted upload/download ==="

cat /tmp/s3duct-test-input.bin | s3duct put \
  --bucket "${BUCKET}" \
  --name "${STREAM_NAME}" \
  --chunk-size "${CHUNK_SIZE}" \
  --tag test=roundtrip \
  --tag ci=true \
  --no-encrypt \
  ${ENDPOINT_OPT}

echo "--- Verify ---"
s3duct verify \
  --bucket "${BUCKET}" \
  --name "${STREAM_NAME}" \
  ${ENDPOINT_OPT}

echo "--- List ---"
s3duct list \
  --bucket "${BUCKET}" \
  --prefix "${PREFIX}" \
  ${ENDPOINT_OPT}

echo "--- Download ---"
s3duct get \
  --bucket "${BUCKET}" \
  --name "${STREAM_NAME}" \
  ${ENDPOINT_OPT} \
  > /tmp/s3duct-test-output.bin

ACTUAL=$(sha256sum /tmp/s3duct-test-output.bin | cut -d' ' -f1)
if [ "${ACTUAL}" != "${EXPECTED}" ]; then
  echo "FAIL: hash mismatch (unencrypted)"
  echo "  expected: ${EXPECTED}"
  echo "  actual:   ${ACTUAL}"
  exit 1
fi
echo "PASS: unencrypted roundtrip OK"

# =========================================================================
# Test 2: AES-encrypted roundtrip
# =========================================================================
echo ""
echo "=== Test 2: AES-256-GCM encrypted upload/download ==="

cat /tmp/s3duct-test-input.bin | s3duct put \
  --bucket "${BUCKET}" \
  --name "${STREAM_NAME_ENC}" \
  --chunk-size "${CHUNK_SIZE}" \
  --key "${AES_KEY}" \
  --tag test=encrypted \
  ${ENDPOINT_OPT}

s3duct get \
  --bucket "${BUCKET}" \
  --name "${STREAM_NAME_ENC}" \
  --key "${AES_KEY}" \
  ${ENDPOINT_OPT} \
  > /tmp/s3duct-test-output.bin

ACTUAL=$(sha256sum /tmp/s3duct-test-output.bin | cut -d' ' -f1)
if [ "${ACTUAL}" != "${EXPECTED}" ]; then
  echo "FAIL: hash mismatch (encrypted)"
  echo "  expected: ${EXPECTED}"
  echo "  actual:   ${ACTUAL}"
  exit 1
fi
echo "PASS: AES-encrypted roundtrip OK"

# =========================================================================
# Test 3: Encrypted manifest roundtrip
# =========================================================================
echo ""
echo "=== Test 3: Encrypted manifest ==="

cat /tmp/s3duct-test-input.bin | s3duct put \
  --bucket "${BUCKET}" \
  --name "${STREAM_NAME_ENCM}" \
  --chunk-size "${CHUNK_SIZE}" \
  --key "${AES_KEY}" \
  --encrypt-manifest \
  --tag test=encrypted-manifest \
  ${ENDPOINT_OPT}

s3duct get \
  --bucket "${BUCKET}" \
  --name "${STREAM_NAME_ENCM}" \
  --key "${AES_KEY}" \
  ${ENDPOINT_OPT} \
  > /tmp/s3duct-test-output.bin

ACTUAL=$(sha256sum /tmp/s3duct-test-output.bin | cut -d' ' -f1)
if [ "${ACTUAL}" != "${EXPECTED}" ]; then
  echo "FAIL: hash mismatch (encrypted manifest)"
  echo "  expected: ${EXPECTED}"
  echo "  actual:   ${ACTUAL}"
  exit 1
fi
echo "PASS: encrypted manifest roundtrip OK"

echo ""
echo "=== All integration tests passed ==="
