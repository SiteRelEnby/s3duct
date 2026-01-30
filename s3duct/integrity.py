"""Dual-hash integrity and signature chain computation."""

import hashlib
import hmac
from dataclasses import dataclass
from pathlib import Path

from s3duct.config import GENESIS_KEY, READ_BUFFER_SIZE


@dataclass(frozen=True)
class DualHash:
    sha256: str
    sha3_256: str

    def combined(self) -> bytes:
        """Concatenated hash bytes for chain input."""
        return bytes.fromhex(self.sha256) + bytes.fromhex(self.sha3_256)


@dataclass(frozen=True)
class ChunkIntegrity:
    """Full integrity record for a single chunk."""
    dual_hash: DualHash
    chain: str
    size: int


class IntegrityHasher:
    """Computes SHA-256 + SHA3-256 in a single pass over data."""

    def __init__(self) -> None:
        self._sha256 = hashlib.sha256()
        self._sha3_256 = hashlib.sha3_256()
        self._size = 0

    def update(self, data: bytes) -> None:
        self._sha256.update(data)
        self._sha3_256.update(data)
        self._size += len(data)

    def finalize(self) -> DualHash:
        return DualHash(
            sha256=self._sha256.hexdigest(),
            sha3_256=self._sha3_256.hexdigest(),
        )

    @property
    def size(self) -> int:
        return self._size


class StreamHasher:
    """Tracks a running hash of the entire stream across all chunks."""

    def __init__(self) -> None:
        self._sha256 = hashlib.sha256()
        self._sha3_256 = hashlib.sha3_256()

    def update(self, data: bytes) -> None:
        self._sha256.update(data)
        self._sha3_256.update(data)

    def finalize(self) -> DualHash:
        return DualHash(
            sha256=self._sha256.hexdigest(),
            sha3_256=self._sha3_256.hexdigest(),
        )


def compute_chain(dual_hash: DualHash, prev_chain: bytes | None) -> str:
    """Compute the next chain value via HMAC-SHA256."""
    key = prev_chain if prev_chain is not None else GENESIS_KEY
    return hmac.new(key, dual_hash.combined(), hashlib.sha256).hexdigest()


def hash_file(path: Path) -> tuple[DualHash, int]:
    """Hash a file on disk, returning dual hash and byte count."""
    hasher = IntegrityHasher()
    with open(path, "rb") as f:
        while True:
            data = f.read(READ_BUFFER_SIZE)
            if not data:
                break
            hasher.update(data)
    return hasher.finalize(), hasher.size
