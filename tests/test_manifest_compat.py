"""Manifest backward/forward compatibility tests.

Ensures that manifests from older versions can be loaded by current code,
and that manifests with unknown future fields are handled gracefully.
"""

import hashlib
import json
from pathlib import Path

import pytest

from s3duct.manifest import ChunkRecord, Manifest

FIXTURES_DIR = Path(__file__).parent / "fixtures" / "compat"


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _verify_chunk_hashes(fixture_dir: Path, manifest: Manifest) -> None:
    """Verify that chunk files match the hashes recorded in the manifest."""
    for chunk_rec in manifest.chunks:
        chunk_file = fixture_dir / "chunks" / Path(chunk_rec.s3_key).name
        data = chunk_file.read_bytes()
        assert len(data) == chunk_rec.size, (
            f"Chunk {chunk_rec.index}: expected {chunk_rec.size} bytes, got {len(data)}"
        )
        assert hashlib.sha256(data).hexdigest() == chunk_rec.sha256, (
            f"Chunk {chunk_rec.index}: SHA-256 mismatch"
        )
        assert hashlib.sha3_256(data).hexdigest() == chunk_rec.sha3_256, (
            f"Chunk {chunk_rec.index}: SHA3-256 mismatch"
        )


def _verify_stream_hashes(fixture_dir: Path, manifest: Manifest) -> None:
    """Verify whole-stream hashes by concatenating all chunks."""
    stream = b""
    for chunk_rec in manifest.chunks:
        chunk_file = fixture_dir / "chunks" / Path(chunk_rec.s3_key).name
        stream += chunk_file.read_bytes()
    assert len(stream) == manifest.total_bytes
    assert hashlib.sha256(stream).hexdigest() == manifest.stream_sha256
    assert hashlib.sha3_256(stream).hexdigest() == manifest.stream_sha3_256


# ---------------------------------------------------------------------------
# v0.1.0 - original format (no description field)
# ---------------------------------------------------------------------------

class TestV010Compat:
    """Test loading a v0.1.0 manifest (pre-description field)."""

    FIXTURE = FIXTURES_DIR / "v0.1.0"

    def test_manifest_loads(self):
        raw = (self.FIXTURE / "manifest.json").read_text()
        m = Manifest.from_json(raw)
        assert m.version == 1
        assert m.name == "compat-v010"
        assert m.tool_version == "0.1.0"
        assert m.chunk_count == 3
        assert m.total_bytes == 168

    def test_missing_description_defaults_empty(self):
        raw = (self.FIXTURE / "manifest.json").read_text()
        m = Manifest.from_json(raw)
        assert m.description == ""

    def test_chunk_integrity(self):
        raw = (self.FIXTURE / "manifest.json").read_text()
        m = Manifest.from_json(raw)
        _verify_chunk_hashes(self.FIXTURE, m)

    def test_stream_integrity(self):
        raw = (self.FIXTURE / "manifest.json").read_text()
        m = Manifest.from_json(raw)
        _verify_stream_hashes(self.FIXTURE, m)

    def test_roundtrip_preserves_data(self):
        """Load v0.1.0 manifest, serialize to JSON, reload - fields survive."""
        raw = (self.FIXTURE / "manifest.json").read_text()
        m = Manifest.from_json(raw)
        reloaded = Manifest.from_json(m.to_json())
        assert reloaded.name == m.name
        assert reloaded.chunk_count == m.chunk_count
        assert reloaded.total_bytes == m.total_bytes
        assert reloaded.final_chain == m.final_chain
        assert reloaded.stream_sha256 == m.stream_sha256
        assert len(reloaded.chunks) == len(m.chunks)
        # Roundtrip adds description="" (new default) - that's fine
        assert reloaded.description == ""


# ---------------------------------------------------------------------------
# v0.4.0 - adds description field
# ---------------------------------------------------------------------------

class TestV040Compat:
    """Test loading a v0.4.0 manifest (with description field)."""

    FIXTURE = FIXTURES_DIR / "v0.4.0"

    def test_manifest_loads(self):
        raw = (self.FIXTURE / "manifest.json").read_text()
        m = Manifest.from_json(raw)
        assert m.version == 1
        assert m.name == "compat-v040"
        assert m.tool_version == "0.4.0a1"
        assert m.chunk_count == 3
        assert m.total_bytes == 147

    def test_description_field_present(self):
        raw = (self.FIXTURE / "manifest.json").read_text()
        m = Manifest.from_json(raw)
        assert m.description == "Compat fixture with description field"

    def test_chunk_integrity(self):
        raw = (self.FIXTURE / "manifest.json").read_text()
        m = Manifest.from_json(raw)
        _verify_chunk_hashes(self.FIXTURE, m)

    def test_stream_integrity(self):
        raw = (self.FIXTURE / "manifest.json").read_text()
        m = Manifest.from_json(raw)
        _verify_stream_hashes(self.FIXTURE, m)

    def test_tags_preserved(self):
        raw = (self.FIXTURE / "manifest.json").read_text()
        m = Manifest.from_json(raw)
        assert m.tags == {"fixture": "v0.4.0"}


# ---------------------------------------------------------------------------
# Forward compatibility - unknown fields from future versions
# ---------------------------------------------------------------------------

class TestForwardCompat:
    """Ensure manifests with unknown fields load without error."""

    def test_unknown_top_level_fields_ignored(self):
        """A future version might add fields we don't know about yet."""
        data = {
            "version": 1,
            "name": "future-stream",
            "description": "from the future",
            "created": "2027-01-01T00:00:00+00:00",
            "tool_version": "9.9.9",
            "chunk_count": 1,
            "chunk_size": 64,
            "total_bytes": 10,
            "encrypted": False,
            "encrypted_manifest": False,
            "encryption_method": None,
            "encryption_recipient": None,
            "storage_class": None,
            "tags": {},
            "chunks": [{
                "index": 0, "s3_key": "future-stream/chunk-000000",
                "size": 10, "sha256": "abc", "sha3_256": "def", "etag": "\"e\""
            }],
            "final_chain": "aaa",
            "stream_sha256": "bbb",
            "stream_sha3_256": "ccc",
            # Unknown future fields:
            "compression": "zstd",
            "multipart_threshold": 8388608,
            "backend_type": "gcs",
        }
        m = Manifest.from_json(json.dumps(data))
        assert m.name == "future-stream"
        assert m.description == "from the future"
        assert m.chunk_count == 1
        # Unknown fields silently dropped
        assert not hasattr(m, "compression")
        assert not hasattr(m, "backend_type")

    def test_missing_optional_fields_use_defaults(self):
        """Minimal manifest with only required-ish fields."""
        data = {
            "version": 1,
            "name": "minimal",
            "chunks": [{
                "index": 0, "s3_key": "minimal/chunk-000000",
                "size": 5, "sha256": "a", "sha3_256": "b", "etag": "\"c\""
            }],
        }
        m = Manifest.from_json(json.dumps(data))
        assert m.name == "minimal"
        assert m.description == ""
        assert m.encrypted is False
        assert m.encrypted_manifest is False
        assert m.tags == {}
        assert m.final_chain == ""
        assert m.stream_sha256 == ""

    def test_version_field_survives_roundtrip(self):
        m = Manifest.new(
            name="test", chunk_size=64, encrypted=False,
            encryption_method=None, encryption_recipient=None,
            storage_class=None, description="test desc",
        )
        reloaded = Manifest.from_json(m.to_json())
        assert reloaded.version == 1
        assert reloaded.description == "test desc"
