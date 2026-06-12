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
STREAM_NAME_AGE="${PREFIX}-roundtrip-age"
STREAM_NAME_AGEM="${PREFIX}-roundtrip-age-encmanifest"
STREAM_NAME_RAW="${PREFIX}-roundtrip-raw"
PRUNE_PREFIX="${PREFIX}-prune"
GC_SCOPE="${PREFIX}-gcscope"

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
  aws $aws_ep s3 rm "s3://${BUCKET}/${STREAM_NAME_AGE}/" --recursive 2>/dev/null || true
  aws $aws_ep s3 rm "s3://${BUCKET}/${STREAM_NAME_AGEM}/" --recursive 2>/dev/null || true
  aws $aws_ep s3 rm "s3://${BUCKET}/${STREAM_NAME_RAW}/" --recursive 2>/dev/null || true
  aws $aws_ep s3 rm "s3://${BUCKET}/${PRUNE_PREFIX}/" --recursive 2>/dev/null || true
  aws $aws_ep s3 rm "s3://${BUCKET}/${GC_SCOPE}/" --recursive 2>/dev/null || true
  rm -f /tmp/s3duct-test-input.bin /tmp/s3duct-test-output.bin /tmp/s3duct-test-age-key.txt \
        /tmp/s3duct-test-raw.bin
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

# =========================================================================
# Test 4: age encrypted roundtrip (skip if age not installed)
# =========================================================================
if command -v age-keygen >/dev/null 2>&1; then
  echo ""
  echo "=== Test 4: age encrypted upload/download ==="

  age-keygen -o /tmp/s3duct-test-age-key.txt 2>/dev/null

  cat /tmp/s3duct-test-input.bin | s3duct put \
    --bucket "${BUCKET}" \
    --name "${STREAM_NAME_AGE}" \
    --chunk-size "${CHUNK_SIZE}" \
    --age-identity /tmp/s3duct-test-age-key.txt \
    --no-encrypt-manifest \
    --tag test=age \
    ${ENDPOINT_OPT}

  s3duct get \
    --bucket "${BUCKET}" \
    --name "${STREAM_NAME_AGE}" \
    --age-identity /tmp/s3duct-test-age-key.txt \
    ${ENDPOINT_OPT} \
    > /tmp/s3duct-test-output.bin

  ACTUAL=$(sha256sum /tmp/s3duct-test-output.bin | cut -d' ' -f1)
  if [ "${ACTUAL}" != "${EXPECTED}" ]; then
    echo "FAIL: hash mismatch (age)"
    echo "  expected: ${EXPECTED}"
    echo "  actual:   ${ACTUAL}"
    exit 1
  fi
  echo "PASS: age encrypted roundtrip OK"

  # =========================================================================
  # Test 5: age encrypted manifest roundtrip
  # =========================================================================
  echo ""
  echo "=== Test 5: age encrypted manifest ==="

  cat /tmp/s3duct-test-input.bin | s3duct put \
    --bucket "${BUCKET}" \
    --name "${STREAM_NAME_AGEM}" \
    --chunk-size "${CHUNK_SIZE}" \
    --age-identity /tmp/s3duct-test-age-key.txt \
    --encrypt-manifest \
    --tag test=age-encrypted-manifest \
    ${ENDPOINT_OPT}

  echo "--- Verify with age identity ---"
  s3duct verify \
    --bucket "${BUCKET}" \
    --name "${STREAM_NAME_AGEM}" \
    --age-identity /tmp/s3duct-test-age-key.txt \
    ${ENDPOINT_OPT}

  s3duct get \
    --bucket "${BUCKET}" \
    --name "${STREAM_NAME_AGEM}" \
    --age-identity /tmp/s3duct-test-age-key.txt \
    ${ENDPOINT_OPT} \
    > /tmp/s3duct-test-output.bin

  ACTUAL=$(sha256sum /tmp/s3duct-test-output.bin | cut -d' ' -f1)
  if [ "${ACTUAL}" != "${EXPECTED}" ]; then
    echo "FAIL: hash mismatch (age encrypted manifest)"
    echo "  expected: ${EXPECTED}"
    echo "  actual:   ${ACTUAL}"
    exit 1
  fi
  echo "PASS: age encrypted manifest roundtrip OK"
else
  echo ""
  echo "=== Skipping age tests (age-keygen not found) ==="
fi

# =========================================================================
# Test 6: deep verify (ciphertext-only without key, full with key)
# =========================================================================
echo ""
echo "=== Test 6: verify --deep ==="

cat /tmp/s3duct-test-input.bin | s3duct put \
  --bucket "${BUCKET}" \
  --name "${STREAM_NAME_RAW}" \
  --chunk-size "${CHUNK_SIZE}" \
  --key "${AES_KEY}" \
  --no-encrypt-manifest \
  ${ENDPOINT_OPT}

echo "--- Deep verify WITHOUT key (ciphertext SHA-256) ---"
s3duct verify \
  --bucket "${BUCKET}" \
  --name "${STREAM_NAME_RAW}" \
  --deep \
  ${ENDPOINT_OPT}

echo "--- Deep verify WITH key (plaintext hashes + chain) ---"
s3duct verify \
  --bucket "${BUCKET}" \
  --name "${STREAM_NAME_RAW}" \
  --deep \
  --key "${AES_KEY}" \
  ${ENDPOINT_OPT}
echo "PASS: deep verify OK"

# =========================================================================
# Test 7: raw mode (--no-decrypt) re-split + offline decrypt
# =========================================================================
echo ""
echo "=== Test 7: raw mode download, re-split, offline decrypt ==="

s3duct get \
  --bucket "${BUCKET}" \
  --name "${STREAM_NAME_RAW}" \
  --no-decrypt \
  ${ENDPOINT_OPT} \
  > /tmp/s3duct-test-raw.bin

S3DUCT_RAW_BUCKET="${BUCKET}" S3DUCT_RAW_STREAM="${STREAM_NAME_RAW}" \
S3DUCT_RAW_KEY="${AES_KEY}" S3DUCT_RAW_EXPECTED="${EXPECTED}" \
python3 - <<'EOF'
import hashlib, os, tempfile
from pathlib import Path
import boto3
from s3duct.manifest import Manifest
from s3duct.encryption import parse_key, aes_decrypt_file

endpoint = os.environ.get("S3DUCT_ENDPOINT_URL") or None
c = boto3.client("s3", endpoint_url=endpoint)
m = Manifest.from_json(c.get_object(
    Bucket=os.environ["S3DUCT_RAW_BUCKET"],
    Key=f"{os.environ['S3DUCT_RAW_STREAM']}/.manifest.json")["Body"].read())
raw = Path("/tmp/s3duct-test-raw.bin").read_bytes()
key = parse_key(os.environ["S3DUCT_RAW_KEY"])

out, offset = b"", 0
with tempfile.TemporaryDirectory() as td:
    for rec in m.chunks:
        assert rec.encrypted_size, f"chunk {rec.index} missing encrypted_size"
        blob = raw[offset:offset + rec.encrypted_size]
        assert hashlib.sha256(blob).hexdigest() == rec.encrypted_sha256, \
            f"chunk {rec.index} ciphertext hash mismatch"
        offset += rec.encrypted_size
        enc = Path(td) / "c.enc"
        dec = Path(td) / "c.dec"
        enc.write_bytes(blob)
        aes_decrypt_file(enc, dec, key)
        out += dec.read_bytes()
assert offset == len(raw), f"raw size mismatch: {offset} != {len(raw)}"
assert hashlib.sha256(out).hexdigest() == os.environ["S3DUCT_RAW_EXPECTED"], \
    "offline decrypt does not match original input"
print(f"re-split {m.chunk_count} chunks, ciphertext hashes verified, offline decrypt matches")
EOF
echo "PASS: raw mode re-split + offline decrypt OK"

# =========================================================================
# Test 8: gc (orphan collection with age gate, scoped via --prefix so a
# shared CI bucket's other runs are never touched)
# =========================================================================
echo ""
echo "=== Test 8: gc ==="

head -c 4096 /dev/urandom | s3duct put \
  --bucket "${BUCKET}" \
  --prefix "${GC_SCOPE}" \
  --name realgc \
  --chunk-size "${CHUNK_SIZE}" \
  --no-encrypt \
  ${ENDPOINT_OPT}

python3 - <<EOF
import os
import boto3
c = boto3.client("s3", endpoint_url=os.environ.get("S3DUCT_ENDPOINT_URL") or None)
b = "${BUCKET}"
c.put_object(Bucket=b, Key="${GC_SCOPE}/dead/chunk-000000", Body=b"orphan")
c.put_object(Bucket=b, Key="${GC_SCOPE}/realgc/chunk-000099", Body=b"leftover")
EOF

echo "--- default age gate must protect fresh orphans ---"
GC_OUT=$(s3duct gc --bucket "${BUCKET}" --prefix "${GC_SCOPE}" --force ${ENDPOINT_OPT} 2>&1)
echo "${GC_OUT}"
echo "${GC_OUT}" | grep -q "No orphaned chunks" || { echo "FAIL: age gate did not protect fresh orphans"; exit 1; }

echo "--- gc --older-than 0 collects them ---"
GC_OUT=$(s3duct gc --bucket "${BUCKET}" --prefix "${GC_SCOPE}" --older-than 0 --force ${ENDPOINT_OPT} 2>&1)
echo "${GC_OUT}"
echo "${GC_OUT}" | grep -q "Deleted 2/2" || { echo "FAIL: gc did not collect 2 orphans"; exit 1; }

echo "--- stream must still deep-verify after gc ---"
s3duct verify --bucket "${BUCKET}" --prefix "${GC_SCOPE}" --name realgc --deep ${ENDPOINT_OPT}
echo "PASS: gc OK"

# =========================================================================
# Test 9: prune (retention)
# =========================================================================
echo ""
echo "=== Test 9: prune ==="

for day in 01 02 03; do
  head -c 4096 /dev/urandom | s3duct put \
    --bucket "${BUCKET}" \
    --name "${PRUNE_PREFIX}/${day}" \
    --chunk-size "${CHUNK_SIZE}" \
    --no-encrypt \
    ${ENDPOINT_OPT}
done

PRUNE_OUT=$(s3duct prune --bucket "${BUCKET}" --stream-prefix "${PRUNE_PREFIX}/" --keep 1 --force ${ENDPOINT_OPT} 2>&1)
echo "${PRUNE_OUT}"
echo "${PRUNE_OUT}" | grep -q "Pruned 2 stream(s), kept 1" || { echo "FAIL: prune did not keep exactly 1"; exit 1; }
# (list --prefix strips the prefix from printed names, so list the whole bucket)
LIST_OUT=$(s3duct list --bucket "${BUCKET}" ${ENDPOINT_OPT} 2>/dev/null)
echo "${LIST_OUT}" | grep -q "${PRUNE_PREFIX}/03" \
  || { echo "FAIL: newest stream was not kept"; exit 1; }
echo "${LIST_OUT}" | grep -q "${PRUNE_PREFIX}/01" \
  && { echo "FAIL: oldest stream was not pruned"; exit 1; }
echo "PASS: prune OK"

echo ""
echo "=== All integration tests passed ==="
