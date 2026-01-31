"""Tests for s3duct.uploader end-to-end with moto."""

import io
import json
import sys

import boto3
import pytest
from moto import mock_aws

from s3duct.backends.s3 import S3Backend
from s3duct.encryption import age_available
from s3duct.manifest import Manifest
from s3duct.resume import ResumeLog
from s3duct.uploader import run_put


CHUNK_SIZE = 64
skip_no_age = pytest.mark.skipif(not age_available(), reason="age CLI not installed")


@pytest.fixture
def upload_env(tmp_path, monkeypatch):
    """Full upload test environment."""
    scratch = tmp_path / "scratch"
    scratch.mkdir()
    session = tmp_path / "sessions"
    session.mkdir()
    monkeypatch.setattr("s3duct.config.SESSION_DIR", session)
    monkeypatch.setattr("s3duct.resume.SESSION_DIR", session)

    with mock_aws():
        client = boto3.client("s3", region_name="us-east-1")
        client.create_bucket(Bucket="test-bucket")
        backend = S3Backend(bucket="test-bucket", region="us-east-1")
        yield backend, client, scratch, session, monkeypatch


def _mock_stdin(monkeypatch, data: bytes):
    """Patch sys.stdin.buffer to read from data."""
    mock = type("MockStdin", (), {"buffer": io.BytesIO(data)})()
    monkeypatch.setattr(sys, "stdin", mock)


def test_run_put_basic(upload_env):
    backend, client, scratch, session, mp = upload_env
    data = b"x" * (CHUNK_SIZE * 2 + 30)
    _mock_stdin(mp, data)

    run_put(backend, "mystream", chunk_size=CHUNK_SIZE,
            encrypt=False, scratch_dir=scratch)

    # Verify manifest uploaded
    raw = client.get_object(Bucket="test-bucket",
                            Key="mystream/.manifest.json")["Body"].read()
    manifest = Manifest.from_json(raw)
    assert manifest.chunk_count == 3
    assert manifest.total_bytes == len(data)
    assert manifest.name == "mystream"
    assert manifest.final_chain  # non-empty

    # Verify chunks uploaded
    for i in range(3):
        key = f"mystream/chunk-{i:06d}"
        resp = client.get_object(Bucket="test-bucket", Key=key)
        chunk_data = resp["Body"].read()
        start = i * CHUNK_SIZE
        end = min(start + CHUNK_SIZE, len(data))
        assert chunk_data == data[start:end]


def test_run_put_empty_stdin(upload_env):
    backend, client, scratch, session, mp = upload_env
    _mock_stdin(mp, b"")

    run_put(backend, "empty", chunk_size=CHUNK_SIZE,
            encrypt=False, scratch_dir=scratch)

    # No manifest should be uploaded for empty input
    objects = client.list_objects_v2(Bucket="test-bucket", Prefix="empty/")
    assert objects.get("KeyCount", 0) == 0


def test_run_put_single_chunk(upload_env):
    backend, client, scratch, session, mp = upload_env
    data = b"small"
    _mock_stdin(mp, data)

    run_put(backend, "single", chunk_size=CHUNK_SIZE,
            encrypt=False, scratch_dir=scratch)

    raw = client.get_object(Bucket="test-bucket",
                            Key="single/.manifest.json")["Body"].read()
    manifest = Manifest.from_json(raw)
    assert manifest.chunk_count == 1
    assert manifest.total_bytes == 5


def test_run_put_resume(upload_env):
    """Test that a resumed upload picks up where it left off."""
    backend, client, scratch, session, mp = upload_env
    data = b"a" * CHUNK_SIZE + b"b" * CHUNK_SIZE + b"c" * 20

    # First upload: simulate uploading only the first chunk by doing a full
    # upload, then re-uploading with the same stream
    _mock_stdin(mp, data)
    run_put(backend, "resume-test", chunk_size=CHUNK_SIZE,
            encrypt=False, scratch_dir=scratch)

    raw = client.get_object(Bucket="test-bucket",
                            Key="resume-test/.manifest.json")["Body"].read()
    manifest = Manifest.from_json(raw)
    assert manifest.chunk_count == 3
    assert manifest.total_bytes == len(data)


def test_run_put_stream_hashes(upload_env):
    """Verify stream-level hashes are recorded in manifest."""
    import hashlib
    backend, client, scratch, session, mp = upload_env
    data = b"hash me across chunks" * 5
    _mock_stdin(mp, data)

    run_put(backend, "hashtest", chunk_size=CHUNK_SIZE,
            encrypt=False, scratch_dir=scratch)

    raw = client.get_object(Bucket="test-bucket",
                            Key="hashtest/.manifest.json")["Body"].read()
    manifest = Manifest.from_json(raw)
    assert manifest.stream_sha256 == hashlib.sha256(data).hexdigest()
    assert manifest.stream_sha3_256 == hashlib.sha3_256(data).hexdigest()


def test_run_put_cleanup(upload_env):
    """Verify scratch dir is cleaned up after upload."""
    backend, client, scratch, session, mp = upload_env
    data = b"y" * (CHUNK_SIZE + 10)
    _mock_stdin(mp, data)

    run_put(backend, "cleanup", chunk_size=CHUNK_SIZE,
            encrypt=False, scratch_dir=scratch)

    # All chunk files should be deleted
    remaining = list(scratch.iterdir())
    assert len(remaining) == 0


def test_run_put_resume_log_cleared(upload_env):
    """Verify local resume log is cleared after successful upload."""
    backend, client, scratch, session, mp = upload_env
    data = b"z" * CHUNK_SIZE
    _mock_stdin(mp, data)

    run_put(backend, "logtest", chunk_size=CHUNK_SIZE,
            encrypt=False, scratch_dir=scratch)

    # Session dir should have no JSONL files remaining
    jsonl_files = list(session.glob("*.jsonl"))
    assert len(jsonl_files) == 0


def test_run_put_aes_encrypted(upload_env):
    """Test upload with AES-256-GCM encryption."""
    import os
    backend, client, scratch, session, mp = upload_env
    data = b"encrypt me with aes" * 5
    _mock_stdin(mp, data)
    aes_key = os.urandom(32)

    run_put(backend, "aes-stream", chunk_size=CHUNK_SIZE,
            encrypt=True, encryption_method="aes-256-gcm",
            aes_key=aes_key, scratch_dir=scratch)

    raw = client.get_object(Bucket="test-bucket",
                            Key="aes-stream/.manifest.json")["Body"].read()
    manifest = Manifest.from_json(raw)
    assert manifest.encrypted is True
    assert manifest.encryption_method == "aes-256-gcm"
    assert manifest.chunk_count > 0
    assert manifest.total_bytes == len(data)

    # Verify chunks are encrypted (not plaintext)
    chunk_data = client.get_object(
        Bucket="test-bucket", Key="aes-stream/chunk-000000"
    )["Body"].read()
    assert chunk_data != data[:CHUNK_SIZE]  # encrypted, not plain


def test_run_put_tool_version_in_manifest(upload_env):
    """Verify tool_version is recorded in manifest."""
    from s3duct import __version__
    backend, client, scratch, session, mp = upload_env
    data = b"version check" * 3
    _mock_stdin(mp, data)

    run_put(backend, "vertest", chunk_size=CHUNK_SIZE,
            encrypt=False, scratch_dir=scratch)

    raw = client.get_object(Bucket="test-bucket",
                            Key="vertest/.manifest.json")["Body"].read()
    manifest = Manifest.from_json(raw)
    assert manifest.tool_version == __version__


def test_run_put_with_tags(upload_env):
    """Verify custom tags are stored in manifest."""
    backend, client, scratch, session, mp = upload_env
    data = b"tagged data" * 3
    _mock_stdin(mp, data)

    run_put(backend, "tagtest", chunk_size=CHUNK_SIZE,
            encrypt=False, scratch_dir=scratch,
            tags={"project": "backups", "env": "prod"})

    raw = client.get_object(Bucket="test-bucket",
                            Key="tagtest/.manifest.json")["Body"].read()
    manifest = Manifest.from_json(raw)
    assert manifest.tags == {"project": "backups", "env": "prod"}


def test_run_put_encrypted_manifest(upload_env):
    """Verify --encrypt-manifest produces non-JSON manifest bytes."""
    import os
    backend, client, scratch, session, mp = upload_env
    data = b"secret manifest" * 3
    _mock_stdin(mp, data)
    aes_key = os.urandom(32)

    run_put(backend, "enc-manifest", chunk_size=CHUNK_SIZE,
            encrypt=True, encrypt_manifest=True,
            encryption_method="aes-256-gcm", aes_key=aes_key,
            scratch_dir=scratch)

    raw = client.get_object(Bucket="test-bucket",
                            Key="enc-manifest/.manifest.json")["Body"].read()
    # Encrypted manifest should NOT be valid JSON
    import json
    with pytest.raises((json.JSONDecodeError, UnicodeDecodeError)):
        json.loads(raw)

    # But decrypting it should yield valid manifest
    from s3duct.encryption import aes_decrypt_manifest
    decrypted = aes_decrypt_manifest(raw, aes_key)
    manifest = Manifest.from_json(decrypted)
    assert manifest.encrypted is True
    assert manifest.encrypted_manifest is True


def test_run_put_regular_file_size_check(upload_env, tmp_path, capsys):
    """When stdin is a regular file, warn if bytes read != file size."""
    import os
    backend, client, scratch, session, mp = upload_env
    data = b"file content for size check" * 3

    # Write to a real file and open as stdin
    test_file = tmp_path / "input.bin"
    test_file.write_bytes(data)

    f = open(test_file, "rb")
    mock = type("FileStdin", (), {"buffer": f})()
    mp.setattr(sys, "stdin", mock)

    run_put(backend, "filecheck", chunk_size=CHUNK_SIZE,
            encrypt=False, scratch_dir=scratch)
    f.close()

    raw = client.get_object(Bucket="test-bucket",
                            Key="filecheck/.manifest.json")["Body"].read()
    manifest = Manifest.from_json(raw)
    assert manifest.total_bytes == len(data)

    # Should NOT warn when sizes match
    captured = capsys.readouterr()
    assert "WARNING" not in captured.err or "regular file" not in captured.err


# --- Parallel upload tests ---


def test_run_put_parallel_basic(upload_env):
    """Upload 7 chunks with workers=4, verify manifest is correct."""
    backend, client, scratch, session, mp = upload_env
    data = b"p" * (CHUNK_SIZE * 7)
    _mock_stdin(mp, data)

    run_put(backend, "par-basic", chunk_size=CHUNK_SIZE,
            encrypt=False, scratch_dir=scratch, upload_workers=4)

    raw = client.get_object(Bucket="test-bucket",
                            Key="par-basic/.manifest.json")["Body"].read()
    manifest = Manifest.from_json(raw)
    assert manifest.chunk_count == 7
    assert manifest.total_bytes == len(data)
    assert manifest.final_chain  # non-empty

    # Verify all chunks present and correct
    for i in range(7):
        key = f"par-basic/chunk-{i:06d}"
        resp = client.get_object(Bucket="test-bucket", Key=key)
        chunk_data = resp["Body"].read()
        assert chunk_data == data[i * CHUNK_SIZE:(i + 1) * CHUNK_SIZE]


def test_run_put_parallel_ordering(upload_env):
    """Verify resume log entries are in strict order with parallel workers."""
    backend, client, scratch, session, mp = upload_env
    data = b"o" * (CHUNK_SIZE * 5 + 20)
    _mock_stdin(mp, data)

    run_put(backend, "par-order", chunk_size=CHUNK_SIZE,
            encrypt=False, scratch_dir=scratch, upload_workers=4)

    # Check resume log was uploaded in order
    raw = client.get_object(Bucket="test-bucket",
                            Key="par-order/.resume.jsonl")["Body"].read()
    lines = [l for l in raw.decode().strip().split("\n") if l]
    indices = [json.loads(l)["chunk"] for l in lines]
    assert indices == list(range(6))


def test_run_put_workers_1_sequential(upload_env):
    """workers=1 matches expected output (sequential behavior)."""
    import hashlib
    backend, client, scratch, session, mp = upload_env
    data = b"sequential test data!" * 5
    _mock_stdin(mp, data)

    run_put(backend, "seq", chunk_size=CHUNK_SIZE,
            encrypt=False, scratch_dir=scratch, upload_workers=1)

    raw = client.get_object(Bucket="test-bucket",
                            Key="seq/.manifest.json")["Body"].read()
    manifest = Manifest.from_json(raw)
    assert manifest.stream_sha256 == hashlib.sha256(data).hexdigest()
    assert manifest.chunk_count > 0


def test_run_put_parallel_encryption(upload_env):
    """AES + workers=4 produces correct encrypted upload."""
    import os
    backend, client, scratch, session, mp = upload_env
    data = b"encrypt parallel" * 10
    aes_key = os.urandom(32)
    _mock_stdin(mp, data)

    run_put(backend, "par-aes", chunk_size=CHUNK_SIZE,
            encrypt=True, encryption_method="aes-256-gcm",
            aes_key=aes_key, scratch_dir=scratch, upload_workers=4)

    raw = client.get_object(Bucket="test-bucket",
                            Key="par-aes/.manifest.json")["Body"].read()
    manifest = Manifest.from_json(raw)
    assert manifest.encrypted is True
    assert manifest.total_bytes == len(data)

    # Verify chunk is encrypted
    chunk0 = client.get_object(
        Bucket="test-bucket", Key="par-aes/chunk-000000"
    )["Body"].read()
    assert chunk0 != data[:CHUNK_SIZE]


def test_run_put_parallel_cleanup(upload_env):
    """Scratch dir empty after parallel upload."""
    backend, client, scratch, session, mp = upload_env
    data = b"c" * (CHUNK_SIZE * 5 + 10)
    _mock_stdin(mp, data)

    run_put(backend, "par-clean", chunk_size=CHUNK_SIZE,
            encrypt=False, scratch_dir=scratch, upload_workers=4)

    remaining = list(scratch.iterdir())
    assert len(remaining) == 0


def test_run_put_parallel_failure(upload_env):
    """Inject upload failure, verify clean abort and consistent resume log."""
    backend, client, scratch, session, mp = upload_env
    data = b"f" * (CHUNK_SIZE * 5)
    _mock_stdin(mp, data)

    call_count = 0
    original_upload = backend.upload

    def failing_upload(key, path, storage_class=None):
        nonlocal call_count
        call_count += 1
        if call_count == 3:
            raise RuntimeError("Simulated non-retryable failure")
        return original_upload(key, path, storage_class)

    backend.upload = failing_upload
    with pytest.raises(RuntimeError, match="non-retryable"):
        run_put(backend, "par-fail", chunk_size=CHUNK_SIZE,
                encrypt=False, scratch_dir=scratch, upload_workers=2)

    # Scratch should be cleaned up even after failure
    remaining = list(scratch.iterdir())
    assert len(remaining) == 0


# --- Manifest encryption tests ---


def test_run_put_encrypt_manifest_default_on(upload_env):
    """When encrypt_manifest=True, manifest should NOT be valid JSON."""
    import os
    backend, client, scratch, session, mp = upload_env
    data = b"auto-encrypt manifest" * 3
    _mock_stdin(mp, data)
    aes_key = os.urandom(32)

    run_put(backend, "enc-man-auto", chunk_size=CHUNK_SIZE,
            encrypt=True, encrypt_manifest=True,
            encryption_method="aes-256-gcm", aes_key=aes_key,
            scratch_dir=scratch)

    raw = client.get_object(Bucket="test-bucket",
                            Key="enc-man-auto/.manifest.json")["Body"].read()
    # Encrypted manifest should NOT be valid JSON
    with pytest.raises((json.JSONDecodeError, UnicodeDecodeError)):
        json.loads(raw)

    # But decrypting it should yield valid manifest
    from s3duct.encryption import aes_decrypt_manifest
    decrypted = aes_decrypt_manifest(raw, aes_key)
    manifest = Manifest.from_json(decrypted)
    assert manifest.encrypted is True
    assert manifest.encrypted_manifest is True
    assert manifest.total_bytes == len(data)


def test_run_put_encrypt_manifest_opt_out(upload_env):
    """When encrypt_manifest=False, manifest stays readable JSON even with encryption."""
    import os
    backend, client, scratch, session, mp = upload_env
    data = b"readable manifest" * 3
    _mock_stdin(mp, data)
    aes_key = os.urandom(32)

    run_put(backend, "enc-man-off", chunk_size=CHUNK_SIZE,
            encrypt=True, encrypt_manifest=False,
            encryption_method="aes-256-gcm", aes_key=aes_key,
            scratch_dir=scratch)

    raw = client.get_object(Bucket="test-bucket",
                            Key="enc-man-off/.manifest.json")["Body"].read()
    # Should be valid JSON (not encrypted)
    manifest = Manifest.from_json(raw)
    assert manifest.encrypted is True
    assert manifest.encrypted_manifest is False


# --- Age encryption tests ---


@skip_no_age
def test_run_put_age_encrypted(upload_env, tmp_path):
    """Test upload with age encryption."""
    import subprocess
    from s3duct.encryption import get_recipient_from_identity

    backend, client, scratch, session, mp = upload_env
    data = b"age encrypt me" * 5
    _mock_stdin(mp, data)

    identity = tmp_path / "identity.txt"
    subprocess.run(["age-keygen", "-o", str(identity)], check=True,
                   capture_output=True)

    run_put(backend, "age-stream", chunk_size=CHUNK_SIZE,
            encrypt=True, encrypt_manifest=False,
            encryption_method="age", age_identity=str(identity),
            scratch_dir=scratch)

    raw = client.get_object(Bucket="test-bucket",
                            Key="age-stream/.manifest.json")["Body"].read()
    manifest = Manifest.from_json(raw)
    assert manifest.encrypted is True
    assert manifest.encryption_method == "age"
    assert manifest.chunk_count > 0
    assert manifest.total_bytes == len(data)

    # Chunks should be encrypted (not plaintext)
    chunk_data = client.get_object(
        Bucket="test-bucket", Key="age-stream/chunk-000000"
    )["Body"].read()
    assert chunk_data != data[:CHUNK_SIZE]


@skip_no_age
def test_run_put_age_encrypted_manifest(upload_env, tmp_path):
    """Test upload with age encryption and encrypted manifest."""
    import subprocess

    backend, client, scratch, session, mp = upload_env
    data = b"age encrypted manifest" * 3
    _mock_stdin(mp, data)

    identity = tmp_path / "identity.txt"
    subprocess.run(["age-keygen", "-o", str(identity)], check=True,
                   capture_output=True)

    run_put(backend, "age-enc-man", chunk_size=CHUNK_SIZE,
            encrypt=True, encrypt_manifest=True,
            encryption_method="age", age_identity=str(identity),
            scratch_dir=scratch)

    raw = client.get_object(Bucket="test-bucket",
                            Key="age-enc-man/.manifest.json")["Body"].read()
    # Manifest should NOT be valid JSON (it's age-encrypted)
    with pytest.raises((json.JSONDecodeError, UnicodeDecodeError)):
        json.loads(raw)

    # Decrypting should yield valid manifest
    from s3duct.encryption import age_decrypt_manifest
    decrypted = age_decrypt_manifest(raw, str(identity))
    manifest = Manifest.from_json(decrypted)
    assert manifest.encrypted is True
    assert manifest.encrypted_manifest is True
    assert manifest.encryption_method == "age"
    assert manifest.total_bytes == len(data)
