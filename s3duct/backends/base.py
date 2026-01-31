"""Abstract storage backend interface."""

from abc import ABC, abstractmethod
from dataclasses import dataclass
from pathlib import Path


@dataclass
class ObjectInfo:
    key: str
    size: int
    etag: str
    storage_class: str | None = None
    restore_status: str | None = None


class StorageBackend(ABC):

    @abstractmethod
    def upload(self, key: str, file_path: Path, storage_class: str | None = None) -> str:
        """Upload a file. Returns ETag."""
        ...

    @abstractmethod
    def upload_bytes(self, key: str, data: bytes, storage_class: str | None = None) -> str:
        """Upload raw bytes. Returns ETag."""
        ...

    @abstractmethod
    def download(self, key: str, dest_path: Path) -> None:
        """Download an object to a local file."""
        ...

    @abstractmethod
    def download_bytes(self, key: str) -> bytes:
        """Download an object as bytes."""
        ...

    @abstractmethod
    def list_objects(self, prefix: str) -> list[ObjectInfo]:
        """List objects with a given prefix."""
        ...

    @abstractmethod
    def head_object(self, key: str) -> ObjectInfo:
        """Get metadata for a single object."""
        ...

    @abstractmethod
    def delete_object(self, key: str) -> None:
        """Delete a single object."""
        ...

    @abstractmethod
    def preflight_check(self) -> None:
        """Verify credentials and bucket access. Raises on failure."""
        ...

    @abstractmethod
    def initiate_restore(self, key: str, days: int, tier: str) -> None:
        """Request restore of a Glacier/GDA object."""
        ...

    @abstractmethod
    def is_restore_complete(self, key: str) -> bool:
        """Check if a Glacier restore is complete."""
        ...
