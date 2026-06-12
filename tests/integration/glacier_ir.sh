#!/usr/bin/env bash
#
# GLACIER_IR (S3 Glacier Instant Retrieval) live integration test.
# Needs REAL AWS — moto and MinIO ignore storage classes entirely.
#
# Validates the things mocks can't:
#   - chunks actually land in GLACIER_IR (HeadObject reports the class)
#   - GETs are served directly, no restore step (verify --deep, get)
#   - `s3duct restore` reports chunks available and does NOT call
#     RestoreObject (S3 rejects RestoreObject on GLACIER_IR)
#   - `s3duct list` shows the storage class
#
# Cost: ~128KB at GLACIER_IR rates with the 90-day minimum-duration charge
# applied on early delete — fractions of a cent per run.
#
# Required env vars:
#   AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY  (and AWS_SESSION_TOKEN if STS)
#   S3DUCT_TEST_BUCKET
#
# Optional:
#   AWS_REGION / AWS_DEFAULT_REGION
#   S3DUCT_TEST_PREFIX   — key prefix for isolation (default: ir-$$)
#
set -euo pipefail

PREFIX="${S3DUCT_TEST_PREFIX:-ir-$$}"
BUCKET="${S3DUCT_TEST_BUCKET:?S3DUCT_TEST_BUCKET not set}"
STREAM="${PREFIX}-glacier-ir"
CHUNK_SIZE="32K"

cleanup() {
  echo "--- Cleanup ---"
  s3duct delete --bucket "${BUCKET}" --name "${STREAM}" --force 2>/dev/null || true
  rm -f /tmp/s3duct-ir-input.bin /tmp/s3duct-ir-output.bin
}
trap cleanup EXIT

echo "--- Generate test data ---"
dd if=/dev/urandom of=/tmp/s3duct-ir-input.bin bs=1K count=128 2>/dev/null
EXPECTED=$(sha256sum /tmp/s3duct-ir-input.bin | cut -d' ' -f1)
echo "Input SHA256: ${EXPECTED}"

echo ""
echo "=== Upload with --storage-class GLACIER_IR ==="
cat /tmp/s3duct-ir-input.bin | s3duct put \
  --bucket "${BUCKET}" \
  --name "${STREAM}" \
  --chunk-size "${CHUNK_SIZE}" \
  --storage-class GLACIER_IR \
  --no-encrypt

echo ""
echo "=== Chunks really are GLACIER_IR (HeadObject) ==="
S3DUCT_IR_BUCKET="${BUCKET}" S3DUCT_IR_STREAM="${STREAM}" python3 - <<'EOF'
import os
import boto3
c = boto3.client("s3")
resp = c.head_object(Bucket=os.environ["S3DUCT_IR_BUCKET"],
                     Key=f"{os.environ['S3DUCT_IR_STREAM']}/chunk-000000")
sc = resp.get("StorageClass")
assert sc == "GLACIER_IR", f"expected GLACIER_IR, got {sc!r}"
print(f"chunk-000000 StorageClass: {sc}")
EOF

echo ""
echo "=== list shows the storage class ==="
LIST_OUT=$(s3duct list --bucket "${BUCKET}" 2>/dev/null)
echo "${LIST_OUT}" | grep "${STREAM}"
echo "${LIST_OUT}" | grep "${STREAM}" | grep -q "GLACIER_IR" \
  || { echo "FAIL: list does not show GLACIER_IR"; exit 1; }

echo ""
echo "=== verify (HEAD path) ==="
s3duct verify --bucket "${BUCKET}" --name "${STREAM}"

echo ""
echo "=== verify --deep (real GETs against GLACIER_IR, no restore) ==="
s3duct verify --bucket "${BUCKET}" --name "${STREAM}" --deep

echo ""
echo "=== restore must report available, not call RestoreObject ==="
RESTORE_OUT=$(s3duct restore --bucket "${BUCKET}" --name "${STREAM}" 2>&1)
echo "${RESTORE_OUT}"
echo "${RESTORE_OUT}" | grep -q "0 initiated" \
  || { echo "FAIL: restore initiated a RestoreObject on GLACIER_IR"; exit 1; }
echo "${RESTORE_OUT}" | grep -q "available" \
  || { echo "FAIL: restore did not report chunks available"; exit 1; }

echo ""
echo "=== get (instant retrieval roundtrip) ==="
s3duct get --bucket "${BUCKET}" --name "${STREAM}" > /tmp/s3duct-ir-output.bin
ACTUAL=$(sha256sum /tmp/s3duct-ir-output.bin | cut -d' ' -f1)
if [ "${ACTUAL}" != "${EXPECTED}" ]; then
  echo "FAIL: hash mismatch"
  echo "  expected: ${EXPECTED}"
  echo "  actual:   ${ACTUAL}"
  exit 1
fi
echo "PASS: GLACIER_IR roundtrip OK"

echo ""
echo "=== All GLACIER_IR tests passed ==="
