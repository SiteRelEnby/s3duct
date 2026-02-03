"""Tests for s3duct Glacier thaw/restore support."""

import hashlib
import io
import sys
from unittest.mock import patch, MagicMock

import boto3
import pytest
from botocore.exceptions import ClientError
from click.testing import CliRunner
from moto import mock_aws

from s3duct.backends.base import ObjectInfo
from s3duct.backends.s3 import S3Backend, _is_retryable, _RETRYABLE_TRANSPORT
from s3duct.cli import main
from s3duct.downloader import run_get, run_list
from s3duct.integrity import compute_chain, DualHash
from s3duct.manifest import ChunkRecord, Manifest
from s3duct.thaw import run_restore


CHUNK_SIZE = 64


def _make_client_error(code: str, message: str = "error") -> ClientError:
    return ClientError(
        {"Error": {"Code": code, "Message": message}, "ResponseMetadata": {}},
        "TestOp",
    )


def _upload_test_stream(client, name, data, chunk_size=CHUNK_SIZE,
                        storage_class="STANDARD", encrypted=False,
                        encryption_method=None, aes_key=None,
                        encrypt_manifest=False):
    """Upload a valid test stream directly to S3."""
    manifest = Manifest.new(name, chunk_size, encrypted, encryption_method,
                            None, storage_class)
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

    manifest_bytes = manifest.to_json().encode()
    if encrypt_manifest and aes_key:
        from s3duct.encryption import aes_encrypt_manifest
        manifest_bytes = aes_encrypt_manifest(manifest_bytes, aes_key)

    manifest_key = Manifest.s3_key(name)
    client.put_object(Bucket="test-bucket", Key=manifest_key, Body=manifest_bytes)
    return manifest


@pytest.fixture
def thaw_env(tmp_path, monkeypatch):
    scratch = tmp_path / "scratch"
    scratch.mkdir()
    with mock_aws():
        client = boto3.client("s3", region_name="us-east-1")
        client.create_bucket(Bucket="test-bucket")
        backend = S3Backend(bucket="test-bucket", region="us-east-1")
        yield backend, client, scratch, monkeypatch


# --- _is_retryable tests ---

def test_is_retryable_transport_errors():
    from botocore.exceptions import (
        ConnectionClosedError, ConnectTimeoutError,
        EndpointConnectionError, ReadTimeoutError,
    )
    assert _is_retryable(ConnectionError("lost"))
    assert _is_retryable(OSError("disk"))


def test_is_retryable_retryable_client_errors():
    assert _is_retryable(_make_client_error("500"))
    assert _is_retryable(_make_client_error("503"))
    assert _is_retryable(_make_client_error("SlowDown"))
    assert _is_retryable(_make_client_error("Throttling"))
    assert _is_retryable(_make_client_error("InternalError"))
    assert _is_retryable(_make_client_error("RequestTimeout"))


def test_is_retryable_non_retryable_client_errors():
    assert not _is_retryable(_make_client_error("InvalidObjectState"))
    assert not _is_retryable(_make_client_error("NoSuchKey"))
    assert not _is_retryable(_make_client_error("403"))
    assert not _is_retryable(_make_client_error("404"))
    assert not _is_retryable(_make_client_error("AccessDenied"))


def test_is_retryable_non_exception():
    assert not _is_retryable(ValueError("nope"))


# --- restore CLI tests ---

def test_restore_cli_help():
    runner = CliRunner()
    result = runner.invoke(main, ["restore", "--help"])
    assert result.exit_code == 0
    assert "--days" in result.output
    assert "--tier" in result.output
    assert "--wait" in result.output
    assert "--poll-interval" in result.output
    assert "--key" in result.output
    assert "--age-identity" in result.output
    assert "--bucket" in result.output
    assert "--name" in result.output


# --- run_restore tests ---


def test_restore_non_glacier_skips(thaw_env):
    """Restore on a STANDARD stream should skip with helpful message."""
    backend, client, scratch, mp = thaw_env
    data = b"x" * (CHUNK_SIZE * 3)
    _upload_test_stream(client, "std-stream", data, storage_class="STANDARD")

    run_restore(backend, "std-stream")
    # Should complete without error


def test_restore_initiates_all(thaw_env):
    """Restore on GLACIER stream should call initiate_restore for each chunk."""
    backend, client, scratch, mp = thaw_env
    data = b"g" * (CHUNK_SIZE * 3)
    _upload_test_stream(client, "glacier-stream", data, storage_class="GLACIER")

    calls = []
    original_initiate = backend.initiate_restore

    def mock_initiate(key, days, tier):
        calls.append((key, days, tier))

    with patch.object(backend, "initiate_restore", side_effect=mock_initiate):
        # head_object in moto won't return GLACIER storage class, so patch it
        def mock_head(key):
            return ObjectInfo(key=key, size=CHUNK_SIZE, etag='"abc"',
                              storage_class="GLACIER", restore_status=None)

        with patch.object(backend, "head_object", side_effect=mock_head):
            run_restore(backend, "glacier-stream", days=5, tier="Bulk")

    assert len(calls) == 3
    for key, days, tier in calls:
        assert days == 5
        assert tier == "Bulk"


def test_restore_already_in_progress(thaw_env):
    """RestoreAlreadyInProgress error should be handled gracefully."""
    backend, client, scratch, mp = thaw_env
    data = b"r" * (CHUNK_SIZE * 2)
    _upload_test_stream(client, "restore-progress", data, storage_class="GLACIER")

    def mock_head(key):
        return ObjectInfo(key=key, size=CHUNK_SIZE, etag='"abc"',
                          storage_class="GLACIER", restore_status=None)

    def mock_initiate(key, days, tier):
        raise _make_client_error("RestoreAlreadyInProgress")

    with patch.object(backend, "head_object", side_effect=mock_head):
        with patch.object(backend, "initiate_restore", side_effect=mock_initiate):
            # Should not raise
            run_restore(backend, "restore-progress")


def test_restore_already_restored(thaw_env):
    """Chunks with ongoing-request=false should be counted as available."""
    backend, client, scratch, mp = thaw_env
    data = b"d" * (CHUNK_SIZE * 2)
    _upload_test_stream(client, "already-done", data, storage_class="GLACIER")

    initiate_calls = []

    def mock_head(key):
        return ObjectInfo(key=key, size=CHUNK_SIZE, etag='"abc"',
                          storage_class="GLACIER",
                          restore_status='ongoing-request="false", expiry-date="..."')

    def mock_initiate(key, days, tier):
        initiate_calls.append(key)

    with patch.object(backend, "head_object", side_effect=mock_head):
        with patch.object(backend, "initiate_restore", side_effect=mock_initiate):
            run_restore(backend, "already-done")

    # Should not have called initiate_restore for any chunk
    assert len(initiate_calls) == 0


def test_restore_wait_polls(thaw_env):
    """--wait should poll until all chunks are restored."""
    backend, client, scratch, mp = thaw_env
    data = b"w" * (CHUNK_SIZE * 2)
    _upload_test_stream(client, "wait-test", data, storage_class="GLACIER")

    def mock_head(key):
        return ObjectInfo(key=key, size=CHUNK_SIZE, etag='"abc"',
                          storage_class="GLACIER", restore_status=None)

    poll_count = [0]

    def mock_is_complete(key):
        # Return True on second poll cycle
        return poll_count[0] >= 1

    def mock_initiate(key, days, tier):
        pass

    original_sleep = __import__("time").sleep

    def mock_sleep(seconds):
        poll_count[0] += 1

    with patch.object(backend, "head_object", side_effect=mock_head):
        with patch.object(backend, "initiate_restore", side_effect=mock_initiate):
            with patch.object(backend, "is_restore_complete", side_effect=mock_is_complete):
                with patch("s3duct.thaw.time.sleep", side_effect=mock_sleep):
                    run_restore(backend, "wait-test", wait=True, poll_interval=1)

    assert poll_count[0] >= 1


def test_restore_encrypted_manifest(thaw_env):
    """Restore should work with AES-encrypted manifests."""
    backend, client, scratch, mp = thaw_env
    aes_key = b"\x01" * 32
    data = b"e" * (CHUNK_SIZE * 2)
    _upload_test_stream(client, "enc-restore", data, storage_class="STANDARD",
                        encrypted=True, encryption_method="aes-256-gcm",
                        aes_key=aes_key, encrypt_manifest=True)

    # Should succeed with correct key (STANDARD = no restore needed, just tests manifest decrypt)
    run_restore(backend, "enc-restore", aes_key=aes_key)


# --- run_get Glacier error tests ---

def test_get_glacier_error_message(thaw_env):
    """run_get should give helpful error for InvalidObjectState."""
    backend, client, scratch, mp = thaw_env
    data = b"frozen" * 20
    _upload_test_stream(client, "frozen-stream", data)

    stdout_mock = MagicMock()
    stdout_mock.buffer = io.BytesIO()
    mp.setattr(sys, "stdout", stdout_mock)

    def mock_download(key, dest_path):
        raise _make_client_error("InvalidObjectState")

    import click
    with patch.object(backend, "download", side_effect=mock_download):
        with pytest.raises(click.ClickException, match="Glacier"):
            run_get(backend, "frozen-stream", scratch_dir=scratch)


# --- run_list storage class tests ---

def test_list_shows_glacier_storage_class(thaw_env, capsys):
    """run_list should show [GLACIER] for Glacier streams."""
    backend, client, scratch, mp = thaw_env
    data = b"l" * CHUNK_SIZE
    _upload_test_stream(client, "glacier-list", data, storage_class="GLACIER")

    run_list(backend)

    captured = capsys.readouterr()
    assert "[GLACIER]" in captured.out


def test_list_shows_deep_archive_storage_class(thaw_env, capsys):
    """run_list should show [DEEP_ARCHIVE] for Deep Archive streams."""
    backend, client, scratch, mp = thaw_env
    data = b"d" * CHUNK_SIZE
    _upload_test_stream(client, "deep-list", data, storage_class="DEEP_ARCHIVE")

    run_list(backend)

    captured = capsys.readouterr()
    assert "[DEEP_ARCHIVE]" in captured.out


def test_list_hides_standard_storage_class(thaw_env, capsys):
    """run_list should not show storage class for STANDARD streams."""
    backend, client, scratch, mp = thaw_env
    data = b"s" * CHUNK_SIZE
    _upload_test_stream(client, "standard-list", data, storage_class="STANDARD")

    run_list(backend)

    captured = capsys.readouterr()
    assert "[STANDARD]" not in captured.out
    assert "standard-list" in captured.out
