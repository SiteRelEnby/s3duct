"""Stress tests: large data, small chunks, tight disk limits, chain verification."""

import hashlib
import io
import os
import sys

import boto3
import pytest
from moto import mock_aws

from s3duct.backends.s3 import S3Backend
from s3duct.downloader import run_get
from s3duct.manifest import Manifest
from s3duct.uploader import run_put


# 1 MB data, 4 KB chunks = 256 chunks — enough to stress the chain
STRESS_DATA_SIZE = 1 * 1024 * 1024
STRESS_CHUNK_SIZE = 4 * 1024

# Tight disk limit: only 3 chunks worth of scratch space
TIGHT_DISK_LIMIT = STRESS_CHUNK_SIZE * 3


@pytest.fixture
def stress_env(tmp_path, monkeypatch):
    """Environment for stress tests with tight scratch limits."""
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
    mock = type("MockStdin", (), {"buffer": io.BytesIO(data)})()
    monkeypatch.setattr(sys, "stdin", mock)


def _mock_stdout(monkeypatch):
    mock = type("MockStdout", (), {"buffer": io.BytesIO()})()
    monkeypatch.setattr(sys, "stdout", mock)
    return mock


class TestDiskPressureStress:
    """Upload large data through a tight disk budget."""

    def test_many_chunks_tight_disk_unencrypted(self, stress_env):
        """256 chunks, disk limit of 3 chunks, unencrypted."""
        backend, client, scratch, session, mp = stress_env
        data = os.urandom(STRESS_DATA_SIZE)
        _mock_stdin(mp, data)

        run_put(
            backend, "stress-unenc",
            chunk_size=STRESS_CHUNK_SIZE,
            encrypt=False,
            scratch_dir=scratch,
            diskspace_limit=TIGHT_DISK_LIMIT,
        )

        # Verify manifest
        raw = client.get_object(
            Bucket="test-bucket", Key="stress-unenc/.manifest.json"
        )["Body"].read()
        manifest = Manifest.from_json(raw)
        assert manifest.chunk_count == 256
        assert manifest.total_bytes == STRESS_DATA_SIZE
        assert manifest.stream_sha256 == hashlib.sha256(data).hexdigest()

        # Scratch should be clean
        assert len(list(scratch.iterdir())) == 0

    def test_many_chunks_tight_disk_aes(self, stress_env):
        """256 chunks, disk limit of 3 chunks, AES-256-GCM encrypted."""
        backend, client, scratch, session, mp = stress_env
        data = os.urandom(STRESS_DATA_SIZE)
        aes_key = os.urandom(32)
        _mock_stdin(mp, data)

        run_put(
            backend, "stress-aes",
            chunk_size=STRESS_CHUNK_SIZE,
            encrypt=True,
            encryption_method="aes-256-gcm",
            aes_key=aes_key,
            scratch_dir=scratch,
            diskspace_limit=TIGHT_DISK_LIMIT,
        )

        raw = client.get_object(
            Bucket="test-bucket", Key="stress-aes/.manifest.json"
        )["Body"].read()
        manifest = Manifest.from_json(raw)
        assert manifest.chunk_count == 256
        assert manifest.total_bytes == STRESS_DATA_SIZE
        assert manifest.encrypted is True

        # Chunks in S3 must not be plaintext
        chunk0 = client.get_object(
            Bucket="test-bucket", Key="stress-aes/chunk-000000"
        )["Body"].read()
        assert chunk0 != data[:STRESS_CHUNK_SIZE]

    def test_many_chunks_roundtrip(self, stress_env):
        """Full upload + download roundtrip with 256 chunks and tight disk."""
        backend, client, scratch, session, mp = stress_env
        data = os.urandom(STRESS_DATA_SIZE)
        _mock_stdin(mp, data)

        run_put(
            backend, "stress-rt",
            chunk_size=STRESS_CHUNK_SIZE,
            encrypt=False,
            scratch_dir=scratch,
            diskspace_limit=TIGHT_DISK_LIMIT,
        )

        stdout = _mock_stdout(mp)
        run_get(backend, "stress-rt", decrypt=False, scratch_dir=scratch)

        stdout.buffer.seek(0)
        restored = stdout.buffer.read()
        assert len(restored) == STRESS_DATA_SIZE
        assert hashlib.sha256(restored).hexdigest() == hashlib.sha256(data).hexdigest()

    def test_many_chunks_aes_roundtrip(self, stress_env):
        """Full AES roundtrip with 256 chunks and tight disk."""
        backend, client, scratch, session, mp = stress_env
        data = os.urandom(STRESS_DATA_SIZE)
        aes_key = os.urandom(32)
        _mock_stdin(mp, data)

        run_put(
            backend, "stress-aes-rt",
            chunk_size=STRESS_CHUNK_SIZE,
            encrypt=True,
            encryption_method="aes-256-gcm",
            aes_key=aes_key,
            scratch_dir=scratch,
            diskspace_limit=TIGHT_DISK_LIMIT,
        )

        stdout = _mock_stdout(mp)
        run_get(
            backend, "stress-aes-rt",
            decrypt=True,
            encryption_method="aes-256-gcm",
            aes_key=aes_key,
            scratch_dir=scratch,
        )

        stdout.buffer.seek(0)
        restored = stdout.buffer.read()
        assert restored == data


class TestChainIntegrityStress:
    """Verify the HMAC signature chain catches various tampering patterns."""

    def test_chain_catches_deleted_chunk(self, stress_env):
        """Deleting a middle chunk should fail chain verification on download."""
        backend, client, scratch, session, mp = stress_env
        data = os.urandom(STRESS_CHUNK_SIZE * 10)
        _mock_stdin(mp, data)

        run_put(
            backend, "chain-del",
            chunk_size=STRESS_CHUNK_SIZE,
            encrypt=False,
            scratch_dir=scratch,
        )

        # Delete chunk 5 from S3 and remove it from manifest
        raw = client.get_object(
            Bucket="test-bucket", Key="chain-del/.manifest.json"
        )["Body"].read()
        manifest = Manifest.from_json(raw)
        manifest.chunks = [c for c in manifest.chunks if c.index != 5]
        client.put_object(
            Bucket="test-bucket", Key="chain-del/.manifest.json",
            Body=manifest.to_json().encode(),
        )

        stdout = _mock_stdout(mp)
        with pytest.raises(Exception, match="[Cc]hain|tamper|incomplete"):
            run_get(backend, "chain-del", decrypt=False, scratch_dir=scratch)

    def test_chain_catches_swapped_chunks(self, stress_env):
        """Swapping two chunks should fail integrity or chain verification."""
        backend, client, scratch, session, mp = stress_env
        data = os.urandom(STRESS_CHUNK_SIZE * 6)
        _mock_stdin(mp, data)

        run_put(
            backend, "chain-swap",
            chunk_size=STRESS_CHUNK_SIZE,
            encrypt=False,
            scratch_dir=scratch,
        )

        # Swap chunk 1 and chunk 3 data in S3
        c1 = client.get_object(
            Bucket="test-bucket", Key="chain-swap/chunk-000001"
        )["Body"].read()
        c3 = client.get_object(
            Bucket="test-bucket", Key="chain-swap/chunk-000003"
        )["Body"].read()
        client.put_object(Bucket="test-bucket", Key="chain-swap/chunk-000001", Body=c3)
        client.put_object(Bucket="test-bucket", Key="chain-swap/chunk-000003", Body=c1)

        stdout = _mock_stdout(mp)
        with pytest.raises(Exception, match="[Ii]ntegrity|corrupt|mismatch|[Cc]hain"):
            run_get(backend, "chain-swap", decrypt=False, scratch_dir=scratch)

    def test_chain_catches_duplicated_chunk(self, stress_env):
        """Duplicating a chunk (replacing another) should fail verification."""
        backend, client, scratch, session, mp = stress_env
        data = os.urandom(STRESS_CHUNK_SIZE * 4)
        _mock_stdin(mp, data)

        run_put(
            backend, "chain-dup",
            chunk_size=STRESS_CHUNK_SIZE,
            encrypt=False,
            scratch_dir=scratch,
        )

        # Replace chunk 2 with a copy of chunk 0
        c0 = client.get_object(
            Bucket="test-bucket", Key="chain-dup/chunk-000000"
        )["Body"].read()
        client.put_object(Bucket="test-bucket", Key="chain-dup/chunk-000002", Body=c0)

        stdout = _mock_stdout(mp)
        with pytest.raises(Exception, match="[Ii]ntegrity|corrupt|mismatch|[Cc]hain"):
            run_get(backend, "chain-dup", decrypt=False, scratch_dir=scratch)

    def test_chain_catches_appended_chunk(self, stress_env):
        """Adding an extra chunk to the manifest should fail chain verification."""
        backend, client, scratch, session, mp = stress_env
        data = os.urandom(STRESS_CHUNK_SIZE * 3)
        _mock_stdin(mp, data)

        run_put(
            backend, "chain-append",
            chunk_size=STRESS_CHUNK_SIZE,
            encrypt=False,
            scratch_dir=scratch,
        )

        # Upload a fake chunk and add it to the manifest
        fake = os.urandom(STRESS_CHUNK_SIZE)
        client.put_object(
            Bucket="test-bucket", Key="chain-append/chunk-000003", Body=fake,
        )
        raw = client.get_object(
            Bucket="test-bucket", Key="chain-append/.manifest.json"
        )["Body"].read()
        manifest = Manifest.from_json(raw)
        from s3duct.manifest import ChunkRecord
        manifest.add_chunk(ChunkRecord(
            index=3,
            s3_key="chain-append/chunk-000003",
            size=len(fake),
            sha256=hashlib.sha256(fake).hexdigest(),
            sha3_256=hashlib.sha3_256(fake).hexdigest(),
            etag="fake",
        ))
        client.put_object(
            Bucket="test-bucket", Key="chain-append/.manifest.json",
            Body=manifest.to_json().encode(),
        )

        stdout = _mock_stdout(mp)
        with pytest.raises(Exception, match="[Cc]hain|tamper|incomplete"):
            run_get(backend, "chain-append", decrypt=False, scratch_dir=scratch)
