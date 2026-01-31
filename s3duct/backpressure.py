"""Disk space backpressure for chunked uploads."""

import shutil
import time
from dataclasses import dataclass
from pathlib import Path

# Minimum safety margin of free disk to preserve (100 MB)
_DISK_SAFETY_MARGIN = 100 * 1024 * 1024


@dataclass
class BackpressureConfig:
    chunk_size: int
    scratch_dir: Path
    max_buffer_chunks: int | None = None  # None = auto
    diskspace_limit: int | None = None    # explicit byte limit, or None
    min_buffer_chunks: int = 2            # floor for parallel uploads

    def __post_init__(self) -> None:
        if self.diskspace_limit is not None and self.diskspace_limit < self.chunk_size:
            raise ValueError(
                f"--diskspace-limit ({self.diskspace_limit:,} bytes) must be "
                f">= chunk size ({self.chunk_size:,} bytes)"
            )


def compute_adaptive_buffer(chunk_size: int, scratch_dir: Path) -> int:
    """Determine buffer chunk count based on available disk space.

    Uses 80% of free space, clamped to [2, 10] chunks.
    """
    free = shutil.disk_usage(scratch_dir).free
    max_by_disk = int((free * 0.8) / chunk_size) if chunk_size > 0 else 10
    return max(2, min(max_by_disk, 10))


class BackpressureMonitor:
    """Monitors scratch disk usage and gates chunk writes."""

    def __init__(self, config: BackpressureConfig) -> None:
        self._config = config
        self._effective_limit = self._compute_limit()

    def _compute_limit(self) -> int:
        if self._config.diskspace_limit is not None:
            return self._config.diskspace_limit

        buf = self._config.max_buffer_chunks
        if buf is None:
            buf = compute_adaptive_buffer(
                self._config.chunk_size, self._config.scratch_dir
            )
        buf = max(buf, self._config.min_buffer_chunks)
        return buf * self._config.chunk_size

    @property
    def effective_limit(self) -> int:
        return self._effective_limit

    def scratch_usage(self) -> int:
        """Current bytes used in scratch dir."""
        return sum(
            f.stat().st_size
            for f in self._config.scratch_dir.iterdir()
            if f.is_file()
        )

    def free_disk_space(self) -> int:
        """Free space on the filesystem containing scratch_dir."""
        return shutil.disk_usage(self._config.scratch_dir).free

    def can_write_chunk(self) -> bool:
        """Check if there is room to write another chunk."""
        usage = self.scratch_usage()
        if usage + self._config.chunk_size > self._effective_limit:
            return False
        safety = max(_DISK_SAFETY_MARGIN, int(1.5 * self._config.chunk_size))
        if self.free_disk_space() < self._config.chunk_size + safety:
            return False
        return True

    def wait_for_space(self, poll_interval: float = 0.5) -> None:
        """Block until space is available for the next chunk."""
        while not self.can_write_chunk():
            time.sleep(poll_interval)
