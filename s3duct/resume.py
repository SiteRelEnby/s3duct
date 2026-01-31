"""Resume log management with signature chain verification."""

import json
import sys
from dataclasses import dataclass, asdict
from pathlib import Path

from s3duct.config import SESSION_DIR, RESUME_LOG_FILENAME
from s3duct.integrity import DualHash, compute_chain


@dataclass
class ResumeEntry:
    chunk: int
    size: int
    sha256: str
    sha3_256: str
    chain: str
    s3_key: str
    etag: str
    ts: str

    @property
    def dual_hash(self) -> DualHash:
        return DualHash(sha256=self.sha256, sha3_256=self.sha3_256)

    def to_json(self) -> str:
        return json.dumps(asdict(self), separators=(",", ":"))

    @classmethod
    def from_json(cls, line: str) -> "ResumeEntry":
        return cls(**json.loads(line))


class ResumeLog:
    """Append-only JSONL resume log with chain verification."""

    def __init__(self, name: str) -> None:
        safe_name = name.replace("/", "__")
        self._path = SESSION_DIR / f"{safe_name}.jsonl"
        self._entries: list[ResumeEntry] = []
        self._load()

    def _load(self) -> None:
        if not self._path.exists():
            return
        with open(self._path, "r") as f:
            for line in f:
                line = line.strip()
                if line:
                    self._entries.append(ResumeEntry.from_json(line))

    @property
    def entries(self) -> list[ResumeEntry]:
        return list(self._entries)

    @property
    def last_chunk_index(self) -> int:
        """Index of the last chunk in the contiguous prefix, or -1 if none."""
        return self.contiguous_prefix()

    def contiguous_prefix(self) -> int:
        """Find highest N where chunks 0..N are all present.

        Returns -1 if there are no entries or chunk 0 is missing.
        Emits a warning to stderr if gaps or ordering issues are detected.
        """
        if not self._entries:
            return -1
        present = {e.chunk for e in self._entries}
        if 0 not in present:
            return -1
        # Check for out-of-order entries
        indices = [e.chunk for e in self._entries]
        if indices != sorted(indices):
            print(
                "WARNING: Resume log entries are not in order. "
                "Log may be corrupt; chain verification will fail.",
                file=sys.stderr,
            )
        n = 0
        while (n + 1) in present:
            n += 1
        max_chunk = max(present)
        if n < max_chunk:
            missing = sorted(set(range(n + 1, max_chunk + 1)) - present)
            print(
                f"WARNING: Resume log has gaps (missing chunks: "
                f"{', '.join(str(m) for m in missing)}). "
                f"Chain verification will fail; log will be discarded.",
                file=sys.stderr,
            )
        return n

    def verify_chain(self) -> bool:
        """Verify the full signature chain of all entries.

        Entries must be in strict sequential order (chunk 0, 1, 2, ...).
        Out-of-order or gapped logs will fail verification.
        """
        prev_chain: bytes | None = None
        for i, entry in enumerate(self._entries):
            if entry.chunk != i:
                return False
            expected = compute_chain(entry.dual_hash, prev_chain)
            if expected != entry.chain:
                return False
            prev_chain = bytes.fromhex(entry.chain)
        return True

    def verify_entry(self, index: int, dual_hash: DualHash, size: int) -> bool:
        """Verify a single entry matches expected hash and size during fast-forward."""
        if index >= len(self._entries):
            return False
        entry = self._entries[index]
        return (
            entry.chunk == index
            and entry.size == size
            and entry.sha256 == dual_hash.sha256
            and entry.sha3_256 == dual_hash.sha3_256
        )

    def get_last_chain_bytes(self) -> bytes | None:
        """Get chain bytes from the last entry for continuing the chain."""
        if not self._entries:
            return None
        return bytes.fromhex(self._entries[-1].chain)

    def append(self, entry: ResumeEntry) -> None:
        """Append a new entry and flush to disk."""
        self._path.parent.mkdir(parents=True, exist_ok=True)
        with open(self._path, "a") as f:
            f.write(entry.to_json() + "\n")
        self._entries.append(entry)

    def clear(self) -> None:
        """Delete the resume log and reset state."""
        if self._path.exists():
            self._path.unlink()
        self._entries.clear()

    @property
    def path(self) -> Path:
        return self._path

    @property
    def s3_key(self) -> str:
        return RESUME_LOG_FILENAME
