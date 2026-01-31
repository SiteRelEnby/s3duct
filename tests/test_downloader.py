"""Tests for s3duct.downloader end-to-end with moto."""

import hashlib
import io
import sys

import boto3
import pytest
from moto import mock_aws

from s3duct.backends.s3 import S3Backend
from s3duct.downloader import run_get, run_list, run_verify
from s3duct.integrity import compute_chain, DualHash
from s3duct.manifest import ChunkRecord, Manifest


CHUNK_SIZE = 64


def _upload_test_stream(client, name, data, chunk_size=CHUNK_SIZE,
                        encrypted=False, encryption_method=None, aes_key=None):
    """Upload a valid test stream directly to S3 for download tests."""
    manifest = Manifest.new(name, chunk_size, encrypted, encryption_method, None, "STANDARD")
    prev_chain = None
    offset = 0
    index = 0

    while offset < len(data):
        chunk_data = data[offset:offset + chunk_size]
        s3_key = f"{name}/chunk-{index:06d}"
        client.put_object(Bucket="test-bucket", Key=s3_key, Body=chunk_data)

        dh = DualHash(
            sha256=hashlib.sha256(chunk_data).hexdigest(),
            sha3_256=hashlib.sha3_256(chunk_data).hexdigest(),
        )
        chain_hex = compute_chain(dh, prev_chain)
        prev_chain = bytes.fromhex(chain_hex)

        resp = client.head_object(Bucket="test-bucket", Key=s3_key)
        manifest.add_chunk(ChunkRecord(
            index=index, s3_key=s3_key, size=len(chunk_data),
            sha256=dh.sha256, sha3_256=dh.sha3_256, etag=resp["ETag"],
        ))

        offset += chunk_size
        index += 1

    manifest.final_chain = prev_chain.hex() if prev_chain else ""
    manifest.stream_sha256 = hashlib.sha256(data).hexdigest()
    manifest.stream_sha3_256 = hashlib.sha3_256(data).hexdigest()

    manifest_key = Manifest.s3_key(name)
    client.put_object(Bucket="test-bucket", Key=manifest_key,
                      Body=manifest.to_json().encode())
    return manifest


@pytest.fixture
def download_env(tmp_path, monkeypatch):
    """Full download test environment."""
    scratch = tmp_path / "scratch"
    scratch.mkdir()

    with mock_aws():
        client = boto3.client("s3", region_name="us-east-1")
        client.create_bucket(Bucket="test-bucket")
        backend = S3Backend(bucket="test-bucket", region="us-east-1")
        yield backend, client, scratch, monkeypatch


def test_run_get_basic(download_env):
    backend, client, scratch, mp = download_env
    data = b"download this data" * 5
    _upload_test_stream(client, "dl-test", data)

    stdout_mock = type("MockStdout", (), {"buffer": io.BytesIO()})()
    mp.setattr(sys, "stdout", stdout_mock)

    run_get(backend, "dl-test", decrypt=False, scratch_dir=scratch)

    stdout_mock.buffer.seek(0)
    assert stdout_mock.buffer.read() == data


def test_run_get_multi_chunk(download_env):
    backend, client, scratch, mp = download_env
    data = b"m" * (CHUNK_SIZE * 3 + 17)
    _upload_test_stream(client, "multi", data)

    stdout_mock = type("MockStdout", (), {"buffer": io.BytesIO()})()
    mp.setattr(sys, "stdout", stdout_mock)

    run_get(backend, "multi", decrypt=False, scratch_dir=scratch)

    stdout_mock.buffer.seek(0)
    assert stdout_mock.buffer.read() == data


def test_run_get_integrity_failure(download_env):
    backend, client, scratch, mp = download_env
    data = b"q" * CHUNK_SIZE
    _upload_test_stream(client, "corrupt", data)

    # Corrupt the chunk in S3
    client.put_object(Bucket="test-bucket", Key="corrupt/chunk-000000",
                      Body=b"CORRUPTED DATA")

    stdout_mock = type("MockStdout", (), {"buffer": io.BytesIO()})()
    mp.setattr(sys, "stdout", stdout_mock)

    with pytest.raises(Exception, match="[Ii]ntegrity|corrupt|mismatch"):
        run_get(backend, "corrupt", decrypt=False, scratch_dir=scratch)


def test_run_get_chain_mismatch(download_env):
    backend, client, scratch, mp = download_env
    data = b"r" * CHUNK_SIZE
    _upload_test_stream(client, "badchain", data)

    # Corrupt final_chain in manifest
    raw = client.get_object(Bucket="test-bucket",
                            Key="badchain/.manifest.json")["Body"].read()
    manifest = Manifest.from_json(raw)
    manifest.final_chain = "0" * 64
    client.put_object(Bucket="test-bucket", Key="badchain/.manifest.json",
                      Body=manifest.to_json().encode())

    stdout_mock = type("MockStdout", (), {"buffer": io.BytesIO()})()
    mp.setattr(sys, "stdout", stdout_mock)

    with pytest.raises(Exception, match="[Cc]hain|tamper|incomplete"):
        run_get(backend, "badchain", decrypt=False, scratch_dir=scratch)


def test_run_get_cleanup(download_env):
    """Scratch dir should be clean after successful download."""
    backend, client, scratch, mp = download_env
    data = b"s" * (CHUNK_SIZE + 10)
    _upload_test_stream(client, "cleanup-dl", data)

    stdout_mock = type("MockStdout", (), {"buffer": io.BytesIO()})()
    mp.setattr(sys, "stdout", stdout_mock)

    run_get(backend, "cleanup-dl", decrypt=False, scratch_dir=scratch)

    remaining = list(scratch.iterdir())
    assert len(remaining) == 0


def test_run_list_empty(download_env, capsys):
    backend, client, scratch, mp = download_env
    run_list(backend)
    # "No streams found" goes to stderr
    captured = capsys.readouterr()
    assert "No streams found" in captured.err or "No streams found" in captured.out


def test_run_list_with_streams(download_env, capsys):
    backend, client, scratch, mp = download_env
    _upload_test_stream(client, "stream-a", b"a" * 10)
    _upload_test_stream(client, "stream-b", b"b" * 20)

    run_list(backend)
    captured = capsys.readouterr()
    assert "stream-a" in captured.out
    assert "stream-b" in captured.out


def test_run_verify_success(download_env):
    backend, client, scratch, mp = download_env
    data = b"v" * (CHUNK_SIZE * 2)
    _upload_test_stream(client, "verify-ok", data)
    # Should not raise
    run_verify(backend, "verify-ok")


def test_run_verify_missing_chunk(download_env):
    backend, client, scratch, mp = download_env
    data = b"w" * (CHUNK_SIZE * 2)
    _upload_test_stream(client, "verify-miss", data)

    # Delete a chunk
    client.delete_object(Bucket="test-bucket", Key="verify-miss/chunk-000001")

    with pytest.raises(SystemExit):
        run_verify(backend, "verify-miss")


def test_run_verify_encrypted_manifest(download_env):
    """Verify should work on streams with encrypted manifests when --key is provided."""
    import os
    from s3duct.uploader import run_put

    backend, client, scratch, mp = download_env
    data = b"verify encrypted manifest" * 5
    aes_key = os.urandom(32)

    session = scratch.parent / "sessions"
    session.mkdir(exist_ok=True)
    mp.setattr("s3duct.config.SESSION_DIR", session)
    mp.setattr("s3duct.resume.SESSION_DIR", session)

    stdin_mock = type("MockStdin", (), {"buffer": io.BytesIO(data)})()
    mp.setattr(sys, "stdin", stdin_mock)

    run_put(
        backend, "verify-enc", chunk_size=CHUNK_SIZE,
        encrypt=True, encryption_method="aes-256-gcm",
        aes_key=aes_key, encrypt_manifest=True, scratch_dir=scratch,
    )

    # Verify with correct key should succeed
    run_verify(backend, "verify-enc", aes_key=aes_key)


def test_run_verify_encrypted_manifest_no_key(download_env):
    """Verify without key on encrypted manifest should fail with helpful message."""
    import os
    from s3duct.uploader import run_put

    backend, client, scratch, mp = download_env
    data = b"verify no key" * 5
    aes_key = os.urandom(32)

    session = scratch.parent / "sessions"
    session.mkdir(exist_ok=True)
    mp.setattr("s3duct.config.SESSION_DIR", session)
    mp.setattr("s3duct.resume.SESSION_DIR", session)

    stdin_mock = type("MockStdin", (), {"buffer": io.BytesIO(data)})()
    mp.setattr(sys, "stdin", stdin_mock)

    run_put(
        backend, "verify-nokey", chunk_size=CHUNK_SIZE,
        encrypt=True, encryption_method="aes-256-gcm",
        aes_key=aes_key, encrypt_manifest=True, scratch_dir=scratch,
    )

    # Verify without key should fail with helpful error
    with pytest.raises(Exception, match="[Ee]ncrypted|key"):
        run_verify(backend, "verify-nokey")


def test_run_get_aes_roundtrip(download_env):
    """Test full upload+download roundtrip with AES-256-GCM encryption."""
    import os
    from s3duct.uploader import run_put
    from s3duct.encryption import aes_encrypt_file

    backend, client, scratch, mp = download_env
    data = b"aes roundtrip test data" * 5
    aes_key = os.urandom(32)

    # Upload with AES encryption using the uploader
    session = scratch.parent / "sessions"
    session.mkdir(exist_ok=True)
    mp.setattr("s3duct.config.SESSION_DIR", session)
    mp.setattr("s3duct.resume.SESSION_DIR", session)

    stdin_mock = type("MockStdin", (), {"buffer": io.BytesIO(data)})()
    mp.setattr(sys, "stdin", stdin_mock)

    run_put(
        backend, "aes-dl-test", chunk_size=CHUNK_SIZE,
        encrypt=True, encryption_method="aes-256-gcm",
        aes_key=aes_key, scratch_dir=scratch,
    )

    # Download and decrypt
    stdout_mock = type("MockStdout", (), {"buffer": io.BytesIO()})()
    mp.setattr(sys, "stdout", stdout_mock)

    run_get(
        backend, "aes-dl-test", decrypt=True,
        encryption_method="aes-256-gcm", aes_key=aes_key,
        scratch_dir=scratch,
    )

    stdout_mock.buffer.seek(0)
    assert stdout_mock.buffer.read() == data


def test_run_get_no_decrypt_raw_download(download_env):
    """--no-decrypt on an encrypted stream should output raw encrypted chunks without failing."""
    import os
    from s3duct.uploader import run_put

    backend, client, scratch, mp = download_env
    data = b"raw encrypted download test" * 5
    aes_key = os.urandom(32)

    session = scratch.parent / "sessions"
    session.mkdir(exist_ok=True)
    mp.setattr("s3duct.config.SESSION_DIR", session)
    mp.setattr("s3duct.resume.SESSION_DIR", session)

    stdin_mock = type("MockStdin", (), {"buffer": io.BytesIO(data)})()
    mp.setattr(sys, "stdin", stdin_mock)

    run_put(
        backend, "raw-dl-test", chunk_size=CHUNK_SIZE,
        encrypt=True, encryption_method="aes-256-gcm",
        aes_key=aes_key, scratch_dir=scratch,
    )

    # Download with decrypt=False (--no-decrypt): should succeed, output is encrypted bytes
    stdout_mock = type("MockStdout", (), {"buffer": io.BytesIO()})()
    mp.setattr(sys, "stdout", stdout_mock)

    run_get(
        backend, "raw-dl-test", decrypt=False,
        scratch_dir=scratch,
    )

    stdout_mock.buffer.seek(0)
    raw_output = stdout_mock.buffer.read()
    # Raw output should NOT equal plaintext (it's encrypted)
    assert raw_output != data
    # Raw output should not be empty
    assert len(raw_output) > 0


def test_run_get_no_decrypt_json_summary(download_env, capsys):
    """JSON summary should report chain_verified=False and raw_mode=True in raw mode."""
    import json
    import os
    from s3duct.uploader import run_put

    backend, client, scratch, mp = download_env
    data = b"json summary raw test" * 5
    aes_key = os.urandom(32)

    session = scratch.parent / "sessions"
    session.mkdir(exist_ok=True)
    mp.setattr("s3duct.config.SESSION_DIR", session)
    mp.setattr("s3duct.resume.SESSION_DIR", session)

    stdin_mock = type("MockStdin", (), {"buffer": io.BytesIO(data)})()
    mp.setattr(sys, "stdin", stdin_mock)

    run_put(
        backend, "raw-json-test", chunk_size=CHUNK_SIZE,
        encrypt=True, encryption_method="aes-256-gcm",
        aes_key=aes_key, scratch_dir=scratch,
    )

    stdout_mock = type("MockStdout", (), {"buffer": io.BytesIO()})()
    mp.setattr(sys, "stdout", stdout_mock)

    # Capture stderr by redirecting it
    import io as _io
    stderr_capture = _io.StringIO()
    mp.setattr("click.utils._default_text_stderr", lambda: stderr_capture)

    run_get(
        backend, "raw-json-test", decrypt=False,
        scratch_dir=scratch, summary="json",
    )

    # Find the JSON line in stderr output
    stderr_capture.seek(0)
    lines = stderr_capture.read().strip().split("\n")
    json_line = [l for l in lines if l.startswith("{")]
    assert json_line, f"No JSON found in stderr: {lines}"
    report = json.loads(json_line[0])
    assert report["chain_verified"] is False
    assert report["raw_mode"] is True
    assert report["status"] == "complete"
