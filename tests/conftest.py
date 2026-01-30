"""Shared test fixtures for s3duct."""

import io
import boto3
import pytest
from moto import mock_aws
from pathlib import Path


@pytest.fixture
def scratch_dir(tmp_path):
    """Clean scratch directory for chunk files."""
    d = tmp_path / "scratch"
    d.mkdir()
    return d


@pytest.fixture
def session_dir(tmp_path, monkeypatch):
    """Clean session directory for resume logs, patched into config."""
    d = tmp_path / "sessions"
    d.mkdir()
    monkeypatch.setattr("s3duct.config.SESSION_DIR", d)
    monkeypatch.setattr("s3duct.resume.SESSION_DIR", d)
    return d


@pytest.fixture
def s3_env():
    """Moto-mocked S3 environment yielding (S3Backend, boto3_client)."""
    with mock_aws():
        client = boto3.client("s3", region_name="us-east-1")
        client.create_bucket(Bucket="test-bucket")
        from s3duct.backends.s3 import S3Backend
        backend = S3Backend(bucket="test-bucket", region="us-east-1")
        yield backend, client


class MockStdin:
    """Mock sys.stdin with a .buffer attribute backed by BytesIO."""
    def __init__(self, data: bytes):
        self.buffer = io.BytesIO(data)


class MockStdout:
    """Mock sys.stdout with a .buffer attribute backed by BytesIO."""
    def __init__(self):
        self.buffer = io.BytesIO()
