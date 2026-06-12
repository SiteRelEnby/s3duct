"""Manifest for a completed upload session."""

import json
from dataclasses import dataclass, field, fields, asdict
from datetime import datetime, timezone
from pathlib import Path

from s3duct.config import MANIFEST_FILENAME

SUPPORTED_MANIFEST_VERSION = 1


class UnsupportedManifestVersion(ValueError):
    """Manifest was written by a newer s3duct than this one can read."""


@dataclass
class ChunkRecord:
    index: int
    s3_key: str
    size: int
    sha256: str
    sha3_256: str
    etag: str
    # Size of the encrypted object as uploaded (None for unencrypted chunks).
    # Lets raw-mode downloads verify and re-split the encrypted stream.
    encrypted_size: int | None = None


@dataclass
class Manifest:
    version: int = 1
    name: str = ""
    description: str = ""
    created: str = ""
    tool_version: str = ""
    chunk_count: int = 0
    chunk_size: int = 0
    total_bytes: int = 0
    encrypted: bool = False
    encrypted_manifest: bool = False
    encryption_method: str | None = None  # "aes-256-gcm" or "age"
    encryption_recipient: str | None = None
    storage_class: str | None = None
    tags: dict[str, str] = field(default_factory=dict)
    chunks: list[ChunkRecord] = field(default_factory=list)
    final_chain: str = ""
    stream_sha256: str = ""
    stream_sha3_256: str = ""

    def add_chunk(self, record: ChunkRecord) -> None:
        self.chunks.append(record)
        self.chunk_count = len(self.chunks)
        self.total_bytes = sum(c.size for c in self.chunks)

    def to_json(self) -> str:
        data = asdict(self)
        # Omit null encrypted_size so manifests for unencrypted streams stay
        # byte-compatible with readers that predate the field
        for c in data["chunks"]:
            if c.get("encrypted_size") is None:
                del c["encrypted_size"]
        return json.dumps(data, indent=2)

    @classmethod
    def from_json(cls, raw: str | bytes) -> "Manifest":
        data = json.loads(raw)
        version = data.get("version", 1)
        if version > SUPPORTED_MANIFEST_VERSION:
            raise UnsupportedManifestVersion(
                f"Manifest version {version} is newer than this s3duct "
                f"supports (version {SUPPORTED_MANIFEST_VERSION}). "
                "Upgrade s3duct to read this stream."
            )
        chunk_known = {f.name for f in fields(ChunkRecord)}
        chunks = [
            ChunkRecord(**{k: v for k, v in c.items() if k in chunk_known})
            for c in data.pop("chunks", [])
        ]
        known = {f.name for f in fields(cls)}
        filtered = {k: v for k, v in data.items() if k in known}
        m = cls(**filtered)
        m.chunks = chunks
        return m

    @staticmethod
    def s3_key(name: str) -> str:
        return f"{name}/{MANIFEST_FILENAME}"

    @staticmethod
    def new(name: str, chunk_size: int, encrypted: bool,
            encryption_method: str | None, encryption_recipient: str | None,
            storage_class: str | None,
            tags: dict[str, str] | None = None,
            encrypted_manifest: bool = False,
            description: str = "") -> "Manifest":
        from s3duct import __version__
        return Manifest(
            name=name,
            description=description,
            created=datetime.now(timezone.utc).isoformat(),
            tool_version=__version__,
            chunk_size=chunk_size,
            encrypted=encrypted,
            encrypted_manifest=encrypted_manifest,
            encryption_method=encryption_method,
            encryption_recipient=encryption_recipient,
            storage_class=storage_class,
            tags=tags or {},
        )
