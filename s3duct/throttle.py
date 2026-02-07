"""AIMD-style adaptive concurrency control for parallel transfers.

Shared by both uploader and downloader pipelines. Adjusts the number of
active worker slots based on transfer-vs-local-I/O throughput ratio, S3
throttle signals, and post-scale-up throughput evaluation.

Scaling rules:
- Additive increase (+1) when transfer-bound: avg transfer time > 2x local I/O
- Additive decrease (-1) when I/O-bound: avg transfer time < 0.5x local I/O
- Multiplicative decrease on S3 throttle: halve excess workers above minimum
- Post-scale-up evaluation: revert if aggregate throughput dropped after +1
- Cooldown: suppress scale-up attempts after throttle or failed scale-up
"""

import threading
import time
from statistics import mean

from s3duct.progress import ProgressTracker


# Defaults (can be overridden per-instance)
DEFAULT_MAX_POOL = 16
DEFAULT_INITIAL = 4
DEFAULT_MIN = 2
ADJUST_INTERVAL = 3     # re-evaluate every N completed transfers
WINDOW_SIZE = 8          # rolling window for timing samples
EVAL_WINDOW = 3          # completions after scale-up before evaluating
COOLDOWN_INTERVALS = 3   # adjustment intervals to wait after backoff


class AdaptiveThrottle:
    """Semaphore-based adaptive concurrency with throughput monitoring."""

    def __init__(self, initial: int = DEFAULT_INITIAL,
                 min_workers: int = DEFAULT_MIN,
                 max_workers: int = DEFAULT_MAX_POOL,
                 tracker: ProgressTracker | None = None) -> None:
        self._min = max(1, min_workers)
        self._max = max(self._min, max_workers)
        self._current = min(max(initial, self._min), self._max)
        self._semaphore = threading.Semaphore(self._current)
        self._lock = threading.Lock()
        self._tracker = tracker

        # Rolling timing windows
        self._transfer_times: list[float] = []
        self._transfer_sizes: list[int] = []
        self._io_times: list[float] = []

        # Completion counter (drives adjustment schedule)
        self._completions = 0

        # Throughput tracking for post-scale-up evaluation
        self._total_bytes = 0
        self._throughput_snapshots: list[tuple[float, int]] = []  # (monotonic, bytes)
        self._pre_scaleup_throughput: float | None = None
        self._eval_at: int | None = None          # completion count to evaluate at

        # Cooldown: don't scale up until completions >= this
        self._cooldown_until = 0

    @property
    def current_workers(self) -> int:
        return self._current

    def acquire(self) -> None:
        """Acquire a slot before submitting work."""
        self._semaphore.acquire()

    def release(self) -> None:
        """Release a slot after work completes."""
        self._semaphore.release()

    # ------------------------------------------------------------------
    # Recording
    # ------------------------------------------------------------------

    def record_transfer_time(self, elapsed: float, size: int = 0) -> None:
        """Record a completed transfer's wall-clock time and size.

        Called from worker threads. Triggers adjustment check every
        ADJUST_INTERVAL completions.
        """
        with self._lock:
            self._transfer_times.append(elapsed)
            self._transfer_sizes.append(size)
            if len(self._transfer_times) > WINDOW_SIZE:
                self._transfer_times.pop(0)
                self._transfer_sizes.pop(0)

            self._total_bytes += size
            self._completions += 1

            # Evaluate previous scale-up if due
            if (self._eval_at is not None
                    and self._completions >= self._eval_at):
                self._evaluate_scaleup()

            if self._completions % ADJUST_INTERVAL == 0:
                self._adjust()

    def record_io_time(self, elapsed: float) -> None:
        """Record time spent on local I/O (read for upload, drain for download)."""
        with self._lock:
            self._io_times.append(elapsed)
            if len(self._io_times) > WINDOW_SIZE:
                self._io_times.pop(0)

    def record_throttle(self) -> None:
        """AIMD multiplicative decrease on S3 throttle/SlowDown.

        Halves the excess workers above minimum (at least -1).
        Sets a cooldown to prevent immediate re-scaling.
        """
        with self._lock:
            old = self._current
            excess = self._current - self._min
            if excess <= 0:
                return

            reduction = max(1, excess // 2)
            reduced = 0
            for _ in range(reduction):
                try:
                    self._semaphore.acquire(blocking=False)
                    self._current -= 1
                    reduced += 1
                except Exception:
                    break

            if reduced > 0:
                self._cooldown_until = self._completions + ADJUST_INTERVAL * COOLDOWN_INTERVALS
                # Cancel any pending scale-up evaluation
                self._eval_at = None
                self._pre_scaleup_throughput = None
                if self._tracker:
                    self._tracker.update_workers(
                        old, self._current,
                        f"S3 throttle: -{reduced} (AIMD)",
                    )

    # ------------------------------------------------------------------
    # Internal
    # ------------------------------------------------------------------

    def _current_throughput(self) -> float:
        """Estimate aggregate throughput (bytes/sec) from recent samples.

        Uses the throughput snapshot history if available, otherwise
        falls back to per-transfer averages scaled by worker count.
        """
        now = time.monotonic()
        current_bytes = self._total_bytes

        # Use snapshots if we have a recent baseline
        if self._throughput_snapshots:
            ts, bs = self._throughput_snapshots[-1]
            dt = now - ts
            if dt > 0.1:
                return (current_bytes - bs) / dt

        # Fallback: per-transfer rate * parallelism
        if self._transfer_times and self._transfer_sizes:
            total_sz = sum(self._transfer_sizes)
            total_t = sum(self._transfer_times)
            if total_t > 0:
                return (total_sz / total_t) * self._current

        return 0.0

    def _take_snapshot(self) -> None:
        """Record a throughput snapshot for later comparison."""
        self._throughput_snapshots.append(
            (time.monotonic(), self._total_bytes)
        )
        # Keep only the last few
        if len(self._throughput_snapshots) > 4:
            self._throughput_snapshots.pop(0)

    def _evaluate_scaleup(self) -> None:
        """Check if the last scale-up actually improved throughput."""
        current_tp = self._current_throughput()
        self._eval_at = None

        if (self._pre_scaleup_throughput is not None
                and self._pre_scaleup_throughput > 0
                and current_tp < self._pre_scaleup_throughput * 0.9):
            # Throughput dropped >10% after scale-up — revert
            old = self._current
            if self._current > self._min:
                try:
                    self._semaphore.acquire(blocking=False)
                    self._current -= 1
                    self._cooldown_until = (
                        self._completions + ADJUST_INTERVAL * COOLDOWN_INTERVALS
                    )
                    if self._tracker:
                        self._tracker.update_workers(
                            old, self._current,
                            f"scale-up reverted: throughput "
                            f"{self._pre_scaleup_throughput / 1e6:.1f} -> "
                            f"{current_tp / 1e6:.1f} MB/s",
                        )
                except Exception:
                    pass

        self._pre_scaleup_throughput = None

    def _scale_up(self, reason: str) -> None:
        """Add one worker slot, with throughput snapshot for later evaluation."""
        old = self._current
        self._pre_scaleup_throughput = self._current_throughput()
        self._take_snapshot()
        self._semaphore.release()
        self._current += 1
        self._eval_at = self._completions + EVAL_WINDOW
        if self._tracker:
            self._tracker.update_workers(old, self._current, reason)

    def _scale_down(self, reason: str) -> None:
        """Remove one worker slot."""
        old = self._current
        try:
            self._semaphore.acquire(blocking=False)
            self._current -= 1
            if self._tracker:
                self._tracker.update_workers(old, self._current, reason)
        except Exception:
            pass

    def _adjust(self) -> None:
        """Periodic adjustment based on transfer vs local-I/O ratio."""
        if len(self._transfer_times) < 2 or len(self._io_times) < 2:
            return

        avg_transfer = mean(self._transfer_times)
        avg_io = mean(self._io_times)
        if avg_io <= 0:
            return

        in_cooldown = self._completions < self._cooldown_until

        if (avg_transfer > 2 * avg_io
                and self._current < self._max
                and not in_cooldown):
            self._scale_up(
                f"transfer-bound +1, avg transfer {avg_transfer:.1f}s "
                f"vs I/O {avg_io:.1f}s"
            )
        elif avg_transfer < 0.5 * avg_io and self._current > self._min:
            self._scale_down(
                f"I/O-bound -1, avg transfer {avg_transfer:.1f}s "
                f"vs I/O {avg_io:.1f}s"
            )
