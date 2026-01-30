"""Resume tests: abort mid-upload, continue, wrong stream, divergent stream."""

import hashlib
import io
import os
import sys
from unittest.mock import patch

import boto3
import click
import pytest
from moto import mock_aws

from s3duct.backends.s3 import S3Backend
from s3duct.manifest import Manifest
from s3duct.resume import ResumeLog
from s3duct.uploader import run_put
from s3duct.downloader import run_get


CHUNK_SIZE = 4096  # 4 KB


@pytest.fixture
def resume_env(tmp_path, monkeypatch):
    """Environment for resume/abort tests."""
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


def _count_uploaded_chunks(client, name):
    """Count chunk objects in S3 for a stream."""
    resp = client.list_objects_v2(Bucket="test-bucket", Prefix=f"{name}/chunk-")
    return resp.get("KeyCount", 0)


class TestAbortAndContinue:
    """Kill upload partway through, then resume with same input."""

    def test_abort_resume_completes(self, resume_env):
        """Abort after N chunks, resume with same stream, verify roundtrip."""
        backend, client, scratch, session, mp = resume_env

        # 10 chunks of data
        data = os.urandom(CHUNK_SIZE * 10)
        abort_after = 4

        # Patch backend.upload to raise after N chunks
        real_upload = backend.upload
        call_count = {"n": 0}

        def aborting_upload(key, path, storage_class=None):
            call_count["n"] += 1
            if call_count["n"] > abort_after:
                raise ConnectionError("simulated network failure")
            return real_upload(key, path, storage_class)

        _mock_stdin(mp, data)
        with patch.object(backend, "upload", side_effect=aborting_upload):
            with pytest.raises((ConnectionError, click.ClickException)):
                run_put(
                    backend, "abort-test",
                    chunk_size=CHUNK_SIZE,
                    encrypt=False,
                    scratch_dir=scratch,
                )

        # Verify partial upload: some chunks exist
        uploaded = _count_uploaded_chunks(client, "abort-test")
        assert uploaded == abort_after

        # Resume with same data
        _mock_stdin(mp, data)
        run_put(
            backend, "abort-test",
            chunk_size=CHUNK_SIZE,
            encrypt=False,
            scratch_dir=scratch,
        )

        # Verify complete
        raw = client.get_object(
            Bucket="test-bucket", Key="abort-test/.manifest.json"
        )["Body"].read()
        manifest = Manifest.from_json(raw)
        assert manifest.chunk_count == 10
        assert manifest.total_bytes == len(data)
        assert manifest.stream_sha256 == hashlib.sha256(data).hexdigest()

        # Full roundtrip
        stdout = _mock_stdout(mp)
        run_get(backend, "abort-test", decrypt=False, scratch_dir=scratch)
        stdout.buffer.seek(0)
        assert stdout.buffer.read() == data

    def test_abort_resume_aes_encrypted(self, resume_env):
        """Abort + resume with AES-256-GCM encryption."""
        backend, client, scratch, session, mp = resume_env

        data = os.urandom(CHUNK_SIZE * 8)
        aes_key = os.urandom(32)
        abort_after = 3

        real_upload = backend.upload
        call_count = {"n": 0}

        def aborting_upload(key, path, storage_class=None):
            call_count["n"] += 1
            if call_count["n"] > abort_after:
                raise ConnectionError("simulated failure")
            return real_upload(key, path, storage_class)

        _mock_stdin(mp, data)
        with patch.object(backend, "upload", side_effect=aborting_upload):
            with pytest.raises((ConnectionError, click.ClickException)):
                run_put(
                    backend, "abort-aes",
                    chunk_size=CHUNK_SIZE,
                    encrypt=True,
                    encryption_method="aes-256-gcm",
                    aes_key=aes_key,
                    scratch_dir=scratch,
                )

        # Resume
        _mock_stdin(mp, data)
        run_put(
            backend, "abort-aes",
            chunk_size=CHUNK_SIZE,
            encrypt=True,
            encryption_method="aes-256-gcm",
            aes_key=aes_key,
            scratch_dir=scratch,
        )

        # Roundtrip
        stdout = _mock_stdout(mp)
        run_get(
            backend, "abort-aes",
            decrypt=True,
            encryption_method="aes-256-gcm",
            aes_key=aes_key,
            scratch_dir=scratch,
        )
        stdout.buffer.seek(0)
        assert stdout.buffer.read() == data


class TestResumeMismatch:
    """Attempt to resume with wrong or divergent data."""

    def test_completely_different_stream_fails(self, resume_env):
        """Resume with totally different data should fail."""
        backend, client, scratch, session, mp = resume_env

        original = os.urandom(CHUNK_SIZE * 6)
        different = os.urandom(CHUNK_SIZE * 6)
        abort_after = 3

        real_upload = backend.upload
        call_count = {"n": 0}

        def aborting_upload(key, path, storage_class=None):
            call_count["n"] += 1
            if call_count["n"] > abort_after:
                raise ConnectionError("simulated failure")
            return real_upload(key, path, storage_class)

        _mock_stdin(mp, original)
        with patch.object(backend, "upload", side_effect=aborting_upload):
            with pytest.raises((ConnectionError, click.ClickException)):
                run_put(
                    backend, "mismatch-test",
                    chunk_size=CHUNK_SIZE,
                    encrypt=False,
                    scratch_dir=scratch,
                )

        # Try to resume with completely different data
        _mock_stdin(mp, different)
        with pytest.raises(click.ClickException, match="[Cc]annot resume|does not match"):
            run_put(
                backend, "mismatch-test",
                chunk_size=CHUNK_SIZE,
                encrypt=False,
                scratch_dir=scratch,
            )

    def test_same_prefix_divergent_stream_fails(self, resume_env):
        """Stream starts identical but diverges partway — should fail."""
        backend, client, scratch, session, mp = resume_env

        shared_prefix = os.urandom(CHUNK_SIZE * 3)
        original_tail = os.urandom(CHUNK_SIZE * 5)
        different_tail = os.urandom(CHUNK_SIZE * 5)

        original = shared_prefix + original_tail
        divergent = shared_prefix + different_tail
        # Abort after uploading 5 chunks (past the shared 3-chunk prefix)
        abort_after = 5

        real_upload = backend.upload
        call_count = {"n": 0}

        def aborting_upload(key, path, storage_class=None):
            call_count["n"] += 1
            if call_count["n"] > abort_after:
                raise ConnectionError("simulated failure")
            return real_upload(key, path, storage_class)

        _mock_stdin(mp, original)
        with patch.object(backend, "upload", side_effect=aborting_upload):
            with pytest.raises((ConnectionError, click.ClickException)):
                run_put(
                    backend, "diverge-test",
                    chunk_size=CHUNK_SIZE,
                    encrypt=False,
                    scratch_dir=scratch,
                )

        # Resume with divergent stream — should catch mismatch at chunk 3+
        _mock_stdin(mp, divergent)
        with pytest.raises(click.ClickException, match="[Cc]annot resume|does not match"):
            run_put(
                backend, "diverge-test",
                chunk_size=CHUNK_SIZE,
                encrypt=False,
                scratch_dir=scratch,
            )

    def test_truncated_stream_fails_by_default(self, resume_env):
        """Truncated stream is a fatal error by default (strict_resume=True)."""
        backend, client, scratch, session, mp = resume_env

        original = os.urandom(CHUNK_SIZE * 8)
        truncated = original[:CHUNK_SIZE * 3]
        abort_after = 5

        real_upload = backend.upload
        call_count = {"n": 0}

        def aborting_upload(key, path, storage_class=None):
            call_count["n"] += 1
            if call_count["n"] > abort_after:
                raise ConnectionError("simulated failure")
            return real_upload(key, path, storage_class)

        _mock_stdin(mp, original)
        with patch.object(backend, "upload", side_effect=aborting_upload):
            with pytest.raises((ConnectionError, click.ClickException)):
                run_put(
                    backend, "trunc-strict",
                    chunk_size=CHUNK_SIZE,
                    encrypt=False,
                    scratch_dir=scratch,
                )

        _mock_stdin(mp, truncated)
        with pytest.raises(click.ClickException, match="strict-resume"):
            run_put(
                backend, "trunc-strict",
                chunk_size=CHUNK_SIZE,
                encrypt=False,
                scratch_dir=scratch,
            )

    def test_truncated_stream_warns_with_no_strict(self, resume_env, capsys):
        """With strict_resume=False, truncated stream warns but completes."""
        backend, client, scratch, session, mp = resume_env

        original = os.urandom(CHUNK_SIZE * 8)
        truncated = original[:CHUNK_SIZE * 3]  # only 3 of 8 chunks
        abort_after = 5

        real_upload = backend.upload
        call_count = {"n": 0}

        def aborting_upload(key, path, storage_class=None):
            call_count["n"] += 1
            if call_count["n"] > abort_after:
                raise ConnectionError("simulated failure")
            return real_upload(key, path, storage_class)

        _mock_stdin(mp, original)
        with patch.object(backend, "upload", side_effect=aborting_upload):
            with pytest.raises((ConnectionError, click.ClickException)):
                run_put(
                    backend, "trunc-test",
                    chunk_size=CHUNK_SIZE,
                    encrypt=False,
                    scratch_dir=scratch,
                )

        # Resume with truncated stream + no-strict-resume
        _mock_stdin(mp, truncated)
        run_put(
            backend, "trunc-test",
            chunk_size=CHUNK_SIZE,
            encrypt=False,
            scratch_dir=scratch,
            strict_resume=False,
        )

        captured = capsys.readouterr()
        assert "WARNING" in captured.err
        assert "not re-verified" in captured.err

        raw = client.get_object(
            Bucket="test-bucket", Key="trunc-test/.manifest.json"
        )["Body"].read()
        manifest = Manifest.from_json(raw)
        assert manifest.chunk_count == 5

    def test_resume_same_prefix_exact_boundary(self, resume_env):
        """Stream identical up to exact abort point, different after.

        The first N chunks match the resume log perfectly, but the
        remaining data is different. Upload should succeed because resume
        verification passes (all logged chunks match), and the new chunks
        just continue the chain.
        """
        backend, client, scratch, session, mp = resume_env

        shared = os.urandom(CHUNK_SIZE * 4)
        tail_a = os.urandom(CHUNK_SIZE * 4)
        tail_b = os.urandom(CHUNK_SIZE * 4)

        stream_a = shared + tail_a
        stream_b = shared + tail_b
        abort_after = 4  # abort right at the boundary

        real_upload = backend.upload
        call_count = {"n": 0}

        def aborting_upload(key, path, storage_class=None):
            call_count["n"] += 1
            if call_count["n"] > abort_after:
                raise ConnectionError("simulated failure")
            return real_upload(key, path, storage_class)

        _mock_stdin(mp, stream_a)
        with patch.object(backend, "upload", side_effect=aborting_upload):
            with pytest.raises((ConnectionError, click.ClickException)):
                run_put(
                    backend, "boundary-test",
                    chunk_size=CHUNK_SIZE,
                    encrypt=False,
                    scratch_dir=scratch,
                )

        # Resume with stream_b — shared prefix matches log, new tail uploads
        # This is technically "valid" from the resume log's perspective
        # because the logged chunks are verified and match. The result will
        # be a frankenstein stream (first 4 chunks of A, last 4 of B) but
        # the tool has no way to know the user changed their mind.
        _mock_stdin(mp, stream_b)
        run_put(
            backend, "boundary-test",
            chunk_size=CHUNK_SIZE,
            encrypt=False,
            scratch_dir=scratch,
        )

        raw = client.get_object(
            Bucket="test-bucket", Key="boundary-test/.manifest.json"
        )["Body"].read()
        manifest = Manifest.from_json(raw)
        assert manifest.chunk_count == 8
        # The stream hash will match stream_b since the hasher processes
        # all chunks (fast-forward re-hashes, new chunks hash fresh)
        assert manifest.stream_sha256 == hashlib.sha256(stream_b).hexdigest()


class TestAbortAtEdges:
    """Abort at interesting boundary conditions."""

    def test_abort_after_first_chunk(self, resume_env):
        """Abort after just 1 chunk, resume."""
        backend, client, scratch, session, mp = resume_env
        data = os.urandom(CHUNK_SIZE * 5)

        real_upload = backend.upload
        call_count = {"n": 0}

        def aborting_upload(key, path, storage_class=None):
            call_count["n"] += 1
            if call_count["n"] > 1:
                raise ConnectionError("fail")
            return real_upload(key, path, storage_class)

        _mock_stdin(mp, data)
        with patch.object(backend, "upload", side_effect=aborting_upload):
            with pytest.raises((ConnectionError, click.ClickException)):
                run_put(
                    backend, "abort-first",
                    chunk_size=CHUNK_SIZE,
                    encrypt=False,
                    scratch_dir=scratch,
                )

        _mock_stdin(mp, data)
        run_put(
            backend, "abort-first",
            chunk_size=CHUNK_SIZE,
            encrypt=False,
            scratch_dir=scratch,
        )

        stdout = _mock_stdout(mp)
        run_get(backend, "abort-first", decrypt=False, scratch_dir=scratch)
        stdout.buffer.seek(0)
        assert stdout.buffer.read() == data

    def test_abort_on_last_chunk(self, resume_env):
        """Abort on the very last chunk, resume."""
        backend, client, scratch, session, mp = resume_env
        num_chunks = 6
        data = os.urandom(CHUNK_SIZE * num_chunks)

        real_upload = backend.upload
        call_count = {"n": 0}

        def aborting_upload(key, path, storage_class=None):
            call_count["n"] += 1
            if call_count["n"] == num_chunks:
                raise ConnectionError("fail on last")
            return real_upload(key, path, storage_class)

        _mock_stdin(mp, data)
        with patch.object(backend, "upload", side_effect=aborting_upload):
            with pytest.raises((ConnectionError, click.ClickException)):
                run_put(
                    backend, "abort-last",
                    chunk_size=CHUNK_SIZE,
                    encrypt=False,
                    scratch_dir=scratch,
                )

        _mock_stdin(mp, data)
        run_put(
            backend, "abort-last",
            chunk_size=CHUNK_SIZE,
            encrypt=False,
            scratch_dir=scratch,
        )

        raw = client.get_object(
            Bucket="test-bucket", Key="abort-last/.manifest.json"
        )["Body"].read()
        manifest = Manifest.from_json(raw)
        assert manifest.chunk_count == num_chunks
        assert manifest.stream_sha256 == hashlib.sha256(data).hexdigest()

    def test_multiple_abort_resume_cycles(self, resume_env):
        """Abort several times, resume each time, eventually complete."""
        backend, client, scratch, session, mp = resume_env
        data = os.urandom(CHUNK_SIZE * 12)

        real_upload = backend.upload
        # Abort at chunk 3, then 7, then let it finish
        abort_schedule = [3, 7, None]

        for abort_at in abort_schedule:
            call_count = {"n": 0}

            def make_aborter(limit):
                def aborting_upload(key, path, storage_class=None):
                    call_count["n"] += 1
                    if limit is not None and call_count["n"] > limit:
                        raise ConnectionError(f"abort at {limit}")
                    return real_upload(key, path, storage_class)
                return aborting_upload

            _mock_stdin(mp, data)
            if abort_at is not None:
                with patch.object(backend, "upload",
                                  side_effect=make_aborter(abort_at)):
                    with pytest.raises((ConnectionError, click.ClickException)):
                        run_put(
                            backend, "multi-abort",
                            chunk_size=CHUNK_SIZE,
                            encrypt=False,
                            scratch_dir=scratch,
                        )
            else:
                run_put(
                    backend, "multi-abort",
                    chunk_size=CHUNK_SIZE,
                    encrypt=False,
                    scratch_dir=scratch,
                )

        raw = client.get_object(
            Bucket="test-bucket", Key="multi-abort/.manifest.json"
        )["Body"].read()
        manifest = Manifest.from_json(raw)
        assert manifest.chunk_count == 12
        assert manifest.stream_sha256 == hashlib.sha256(data).hexdigest()

        stdout = _mock_stdout(mp)
        run_get(backend, "multi-abort", decrypt=False, scratch_dir=scratch)
        stdout.buffer.seek(0)
        assert stdout.buffer.read() == data
