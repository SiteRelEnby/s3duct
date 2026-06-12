"""Tests for s3duct.maintenance (gc + prune)."""

import hashlib

import boto3
import pytest
from moto import mock_aws

from s3duct.backends.s3 import S3Backend
from s3duct.integrity import DualHash, compute_chain
from s3duct.maintenance import run_gc, run_prune
from s3duct.manifest import ChunkRecord, Manifest

CHUNK_SIZE = 64


def _upload_stream(client, name, data, created=""):
    """Upload a valid stream (manifest + chunks) directly to S3."""
    manifest = Manifest.new(name, CHUNK_SIZE, False, None, None, "STANDARD")
    if created:
        manifest.created = created
    prev_chain = None
    offset, index = 0, 0
    while offset < len(data):
        chunk = data[offset:offset + CHUNK_SIZE]
        s3_key = f"{name}/chunk-{index:06d}"
        client.put_object(Bucket="test-bucket", Key=s3_key, Body=chunk)
        dh = DualHash(sha256=hashlib.sha256(chunk).hexdigest(),
                      sha3_256=hashlib.sha3_256(chunk).hexdigest())
        chain_hex = compute_chain(dh, prev_chain)
        prev_chain = bytes.fromhex(chain_hex)
        resp = client.head_object(Bucket="test-bucket", Key=s3_key)
        manifest.add_chunk(ChunkRecord(
            index=index, s3_key=s3_key, size=len(chunk),
            sha256=dh.sha256, sha3_256=dh.sha3_256, etag=resp["ETag"],
        ))
        offset += CHUNK_SIZE
        index += 1
    manifest.final_chain = prev_chain.hex() if prev_chain else ""
    client.put_object(Bucket="test-bucket", Key=Manifest.s3_key(name),
                      Body=manifest.to_json().encode())
    return manifest


@pytest.fixture
def maint_env():
    with mock_aws():
        client = boto3.client("s3", region_name="us-east-1")
        client.create_bucket(Bucket="test-bucket")
        backend = S3Backend(bucket="test-bucket", region="us-east-1")
        yield backend, client


def _keys(client):
    resp = client.list_objects_v2(Bucket="test-bucket")
    return {o["Key"] for o in resp.get("Contents", [])}


# --- gc ---


def test_gc_no_orphans(maint_env):
    backend, client = maint_env
    _upload_stream(client, "clean", b"x" * (CHUNK_SIZE * 2))
    deleted = run_gc(backend, older_than_days=0)
    assert deleted == []
    assert "clean/chunk-000000" in _keys(client)


def test_gc_deletes_unreferenced_chunk(maint_env):
    """Chunk beyond the manifest's list (clobber leftover) is collected."""
    backend, client = maint_env
    _upload_stream(client, "leftover", b"x" * (CHUNK_SIZE * 2))
    client.put_object(Bucket="test-bucket", Key="leftover/chunk-000009",
                      Body=b"orphan")
    deleted = run_gc(backend, older_than_days=0)
    assert deleted == ["leftover/chunk-000009"]
    keys = _keys(client)
    assert "leftover/chunk-000009" not in keys
    assert "leftover/chunk-000000" in keys


def test_gc_deletes_manifestless_stream(maint_env):
    """Chunks with no manifest at all (interrupted upload) are collected."""
    backend, client = maint_env
    client.put_object(Bucket="test-bucket", Key="dead/chunk-000000", Body=b"a")
    client.put_object(Bucket="test-bucket", Key="dead/chunk-000001", Body=b"b")
    deleted = run_gc(backend, older_than_days=0)
    assert sorted(deleted) == ["dead/chunk-000000", "dead/chunk-000001"]
    assert _keys(client) == set()


def test_gc_age_gate_protects_recent(maint_env):
    """Recent orphans (in-progress upload) are not collected."""
    backend, client = maint_env
    client.put_object(Bucket="test-bucket", Key="inflight/chunk-000000", Body=b"a")
    deleted = run_gc(backend, older_than_days=7)
    assert deleted == []
    assert "inflight/chunk-000000" in _keys(client)


def test_gc_dry_run(maint_env):
    backend, client = maint_env
    client.put_object(Bucket="test-bucket", Key="dead/chunk-000000", Body=b"a")
    would = run_gc(backend, older_than_days=0, dry_run=True)
    assert would == ["dead/chunk-000000"]
    assert "dead/chunk-000000" in _keys(client)


def test_gc_skips_unreadable_manifest(maint_env):
    """Encrypted manifest without a key: stream untouched."""
    backend, client = maint_env
    client.put_object(Bucket="test-bucket", Key="enc/.manifest.json",
                      Body=b"\x00\x01\x02 not json")
    client.put_object(Bucket="test-bucket", Key="enc/chunk-000000", Body=b"a")
    deleted = run_gc(backend, older_than_days=0)
    assert deleted == []
    assert "enc/chunk-000000" in _keys(client)


def test_gc_ignores_foreign_keys(maint_env):
    """Keys that aren't manifests or chunks are never touched."""
    backend, client = maint_env
    client.put_object(Bucket="test-bucket", Key="random/file.txt", Body=b"hi")
    deleted = run_gc(backend, older_than_days=0)
    assert deleted == []
    assert "random/file.txt" in _keys(client)


# --- prune ---


def test_prune_keeps_newest(maint_env):
    backend, client = maint_env
    for day in ("01", "02", "03", "04"):
        _upload_stream(client, f"daily/2026-06-{day}", b"d" * CHUNK_SIZE,
                       created=f"2026-06-{day}T00:00:00+00:00")
    deleted = run_prune(backend, keep=2, stream_prefix="daily/")
    assert sorted(deleted) == ["daily/2026-06-01", "daily/2026-06-02"]
    keys = _keys(client)
    assert "daily/2026-06-03/.manifest.json" in keys
    assert "daily/2026-06-04/.manifest.json" in keys
    assert "daily/2026-06-01/.manifest.json" not in keys
    assert "daily/2026-06-01/chunk-000000" not in keys


def test_prune_nothing_to_do(maint_env):
    backend, client = maint_env
    _upload_stream(client, "only", b"x" * CHUNK_SIZE,
                   created="2026-01-01T00:00:00+00:00")
    assert run_prune(backend, keep=3) == []
    assert "only/.manifest.json" in _keys(client)


def test_prune_respects_stream_prefix(maint_env):
    backend, client = maint_env
    _upload_stream(client, "daily/a", b"x" * CHUNK_SIZE,
                   created="2026-01-01T00:00:00+00:00")
    _upload_stream(client, "daily/b", b"x" * CHUNK_SIZE,
                   created="2026-01-02T00:00:00+00:00")
    _upload_stream(client, "weekly/old", b"x" * CHUNK_SIZE,
                   created="2020-01-01T00:00:00+00:00")
    deleted = run_prune(backend, keep=1, stream_prefix="daily/")
    assert deleted == ["daily/a"]
    assert "weekly/old/.manifest.json" in _keys(client)


def test_prune_dry_run(maint_env):
    backend, client = maint_env
    _upload_stream(client, "p/a", b"x" * CHUNK_SIZE, created="2026-01-01T00:00:00+00:00")
    _upload_stream(client, "p/b", b"x" * CHUNK_SIZE, created="2026-01-02T00:00:00+00:00")
    would = run_prune(backend, keep=1, dry_run=True)
    assert would == ["p/a"]
    assert "p/a/.manifest.json" in _keys(client)


def test_prune_skips_unreadable_manifest(maint_env):
    backend, client = maint_env
    _upload_stream(client, "q/a", b"x" * CHUNK_SIZE, created="2026-01-01T00:00:00+00:00")
    _upload_stream(client, "q/b", b"x" * CHUNK_SIZE, created="2026-01-02T00:00:00+00:00")
    client.put_object(Bucket="test-bucket", Key="q/enc/.manifest.json",
                      Body=b"\x00\x01 not json")
    deleted = run_prune(backend, keep=1)
    # unreadable stream not counted, not deleted
    assert deleted == ["q/a"]
    assert "q/enc/.manifest.json" in _keys(client)
