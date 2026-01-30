"""Tests for s3duct.manifest."""

from s3duct.manifest import ChunkRecord, Manifest


def test_chunk_record_creation():
    cr = ChunkRecord(index=0, s3_key="test/chunk-000000", size=100,
                     sha256="abc", sha3_256="def", etag="etag1")
    assert cr.index == 0
    assert cr.size == 100
    assert cr.s3_key == "test/chunk-000000"


def test_manifest_add_chunk():
    m = Manifest(name="test")
    m.add_chunk(ChunkRecord(0, "k0", 100, "a", "b", "e0"))
    m.add_chunk(ChunkRecord(1, "k1", 200, "c", "d", "e1"))
    assert m.chunk_count == 2
    assert m.total_bytes == 300


def test_manifest_serialization_roundtrip():
    m = Manifest.new("mystream", 1024, True, "age", "age1xyz", "STANDARD")
    m.add_chunk(ChunkRecord(0, "mystream/chunk-000000", 1024, "sha", "sha3", "et"))
    m.add_chunk(ChunkRecord(1, "mystream/chunk-000001", 512, "sha2", "sha32", "et2"))
    m.final_chain = "chainval"
    m.stream_sha256 = "stream_sha"
    m.stream_sha3_256 = "stream_sha3"

    raw = m.to_json()
    m2 = Manifest.from_json(raw)

    assert m2.name == "mystream"
    assert m2.chunk_size == 1024
    assert m2.encrypted is True
    assert m2.encryption_recipient == "age1xyz"
    assert m2.chunk_count == 2
    assert m2.total_bytes == 1536
    assert m2.final_chain == "chainval"
    assert m2.stream_sha256 == "stream_sha"
    assert len(m2.chunks) == 2
    assert m2.chunks[0].s3_key == "mystream/chunk-000000"
    assert m2.chunks[1].size == 512


def test_manifest_from_json_empty_chunks():
    m = Manifest.new("empty", 512, False, None, None, None)
    raw = m.to_json()
    m2 = Manifest.from_json(raw)
    assert m2.chunks == []
    assert m2.chunk_count == 0


def test_manifest_s3_key():
    assert Manifest.s3_key("mystream") == "mystream/.manifest.json"
    assert Manifest.s3_key("a/b") == "a/b/.manifest.json"


def test_manifest_new_factory():
    m = Manifest.new("test", 1024, False, None, None, "GLACIER")
    assert m.name == "test"
    assert m.chunk_size == 1024
    assert m.encrypted is False
    assert m.storage_class == "GLACIER"
    assert m.created  # should be non-empty ISO timestamp
    assert m.version == 1


def test_manifest_tool_version():
    from s3duct import __version__
    m = Manifest.new("ver", 512, False, None, None, None)
    assert m.tool_version == __version__

    raw = m.to_json()
    m2 = Manifest.from_json(raw)
    assert m2.tool_version == __version__


def test_manifest_tags_roundtrip():
    m = Manifest.new("tagged", 512, False, None, None, None,
                     tags={"project": "backups", "env": "prod"})
    assert m.tags == {"project": "backups", "env": "prod"}

    raw = m.to_json()
    m2 = Manifest.from_json(raw)
    assert m2.tags == {"project": "backups", "env": "prod"}


def test_manifest_tags_default_empty():
    m = Manifest.new("notags", 512, False, None, None, None)
    assert m.tags == {}


def test_manifest_from_json_ignores_unknown_fields():
    """Old manifests or future fields don't crash from_json."""
    import json
    data = {"version": 1, "name": "test", "chunks": [], "unknown_future_field": True}
    m = Manifest.from_json(json.dumps(data))
    assert m.name == "test"


def test_manifest_from_json_missing_new_fields():
    """Old manifests without tool_version/tags still parse."""
    import json
    data = {"version": 1, "name": "old", "chunks": []}
    m = Manifest.from_json(json.dumps(data))
    assert m.tool_version == ""
    assert m.tags == {}


def test_manifest_encrypted_manifest_field():
    m = Manifest.new("enc", 512, True, "aes-256-gcm", None, None,
                     encrypted_manifest=True)
    assert m.encrypted_manifest is True
    raw = m.to_json()
    m2 = Manifest.from_json(raw)
    assert m2.encrypted_manifest is True
