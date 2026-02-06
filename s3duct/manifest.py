"""Manifest for a completed upload session."""

import json
from dataclasses import dataclass, field, fields, asdict
from datetime import datetime, timezone
from pathlib import Path

from s3duct.config import MANIFEST_FILENAME


@dataclass
class ChunkRecord:
    index: int
    s3_key: str
    size: int
    sha256: str
    sha3_256: str
    etag: str


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
        return json.dumps(data, indent=2)

    @classmethod
    def from_json(cls, raw: str | bytes) -> "Manifest":
        data = json.loads(raw)
        chunks = [ChunkRecord(**c) for c in data.pop("chunks", [])]
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
