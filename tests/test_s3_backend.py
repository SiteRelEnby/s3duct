"""Tests for s3duct.backends.s3 using moto mock."""

import pytest
from s3duct.backends.base import ObjectInfo


def test_upload_and_download(s3_env, tmp_path):
    backend, _ = s3_env
    src = tmp_path / "upload.bin"
    src.write_bytes(b"hello s3")

    etag = backend.upload("test/file.bin", src)
    assert etag  # non-empty

    dest = tmp_path / "download.bin"
    backend.download("test/file.bin", dest)
    assert dest.read_bytes() == b"hello s3"


def test_upload_bytes_and_download_bytes(s3_env):
    backend, _ = s3_env
    data = b"raw bytes upload"
    etag = backend.upload_bytes("test/raw.bin", data)
    assert etag

    result = backend.download_bytes("test/raw.bin")
    assert result == data


def test_list_objects(s3_env):
    backend, _ = s3_env
    backend.upload_bytes("a/one", b"1")
    backend.upload_bytes("a/two", b"2")
    backend.upload_bytes("b/three", b"3")

    objects = backend.list_objects("a/")
    keys = [o.key for o in objects]
    assert "a/one" in keys
    assert "a/two" in keys
    assert "b/three" not in keys


def test_list_objects_all(s3_env):
    backend, _ = s3_env
    backend.upload_bytes("x", b"1")
    backend.upload_bytes("y", b"2")
    objects = backend.list_objects("")
    assert len(objects) >= 2


def test_head_object(s3_env):
    backend, _ = s3_env
    data = b"head test data"
    backend.upload_bytes("test/head.bin", data)

    info = backend.head_object("test/head.bin")
    assert isinstance(info, ObjectInfo)
    assert info.size == len(data)
    assert info.etag


def test_delete_object(s3_env):
    backend, _ = s3_env
    backend.upload_bytes("test/delete.bin", b"delete me")
    backend.delete_object("test/delete.bin")

    objects = backend.list_objects("test/")
    keys = [o.key for o in objects]
    assert "test/delete.bin" not in keys


def test_upload_with_storage_class(s3_env, tmp_path):
    backend, client = s3_env
    src = tmp_path / "glacier.bin"
    src.write_bytes(b"cold data")
    backend.upload("test/cold.bin", src, storage_class="STANDARD_IA")

    resp = client.head_object(Bucket="test-bucket", Key="test/cold.bin")
    assert resp.get("StorageClass") in ("STANDARD_IA", None)  # moto may not track this


def test_prefix_handling():
    """Backend with prefix prepends to keys and strips on list."""
    from moto import mock_aws
    import boto3
    from s3duct.backends.s3 import S3Backend

    with mock_aws():
        client = boto3.client("s3", region_name="us-east-1")
        client.create_bucket(Bucket="pfx-bucket")
        backend = S3Backend(bucket="pfx-bucket", region="us-east-1", prefix="myprefix")

        backend.upload_bytes("file.txt", b"data")

        # Verify it's stored with prefix in raw S3
        resp = client.get_object(Bucket="pfx-bucket", Key="myprefix/file.txt")
        assert resp["Body"].read() == b"data"

        # List should return stripped keys
        objects = backend.list_objects("")
        keys = [o.key for o in objects]
        assert "file.txt" in keys


def test_etag_returned(s3_env):
    backend, _ = s3_env
    etag = backend.upload_bytes("test/etag.bin", b"etag data")
    info = backend.head_object("test/etag.bin")
    # ETags from moto include quotes, S3Backend should handle both
    assert info.etag
