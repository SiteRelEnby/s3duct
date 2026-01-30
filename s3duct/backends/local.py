"""Local filesystem storage backend for development and testing."""

import hashlib
import shutil
from pathlib import Path

from s3duct.backends.base import ObjectInfo, StorageBackend


class LocalBackend(StorageBackend):
    """Stores objects as files in a local directory tree."""

    def __init__(self, root: Path, prefix: str = "") -> None:
        self._root = root
        self._prefix = prefix.rstrip("/") + "/" if prefix else ""
        self._root.mkdir(parents=True, exist_ok=True)

    def _full_path(self, key: str) -> Path:
        return self._root / f"{self._prefix}{key}"

    @staticmethod
    def _etag(data: bytes) -> str:
        return hashlib.md5(data).hexdigest()

    def upload(self, key: str, file_path: Path, storage_class: str | None = None) -> str:
        dest = self._full_path(key)
        dest.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(file_path, dest)
        return self._etag(dest.read_bytes())

    def upload_bytes(self, key: str, data: bytes, storage_class: str | None = None) -> str:
        dest = self._full_path(key)
        dest.parent.mkdir(parents=True, exist_ok=True)
        dest.write_bytes(data)
        return self._etag(data)

    def download(self, key: str, dest_path: Path) -> None:
        src = self._full_path(key)
        shutil.copy2(src, dest_path)

    def download_bytes(self, key: str) -> bytes:
        return self._full_path(key).read_bytes()

    def list_objects(self, prefix: str) -> list[ObjectInfo]:
        full_prefix = f"{self._prefix}{prefix}"
        results = []
        for p in self._root.rglob("*"):
            if not p.is_file():
                continue
            rel = str(p.relative_to(self._root))
            if not rel.startswith(full_prefix):
                continue
            data = p.read_bytes()
            key = rel[len(self._prefix):] if self._prefix and rel.startswith(self._prefix) else rel
            results.append(ObjectInfo(
                key=key, size=len(data), etag=self._etag(data),
            ))
        return results

    def head_object(self, key: str) -> ObjectInfo:
        p = self._full_path(key)
        data = p.read_bytes()
        return ObjectInfo(key=key, size=len(data), etag=self._etag(data))

    def delete_object(self, key: str) -> None:
        self._full_path(key).unlink(missing_ok=True)

    def initiate_restore(self, key: str, days: int, tier: str) -> None:
        pass  # no-op for local storage

    def is_restore_complete(self, key: str) -> bool:
        return True  # always available locally
