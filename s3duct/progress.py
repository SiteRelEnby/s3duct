"""Progress tracking for s3duct operations."""

import sys
import time
from abc import ABC, abstractmethod

import click


def _format_bytes(n: int) -> str:
    """Format bytes as human-readable string."""
    for unit in ("B", "KB", "MB", "GB", "TB"):
        if abs(n) < 1024:
            return f"{n:.1f} {unit}"
        n /= 1024
    return f"{n:.1f} PB"


def _format_duration(seconds: float) -> str:
    """Format duration as MM:SS or HH:MM:SS."""
    s = int(seconds)
    if s < 3600:
        return f"{s // 60:02d}:{s % 60:02d}"
    return f"{s // 3600}:{(s % 3600) // 60:02d}:{s % 60:02d}"


class ProgressTracker(ABC):
    """Abstract progress tracker interface."""

    @abstractmethod
    def start(self, total_bytes: int | None, total_chunks: int, label: str,
              chunk_size: int | None = None) -> None:
        """Start tracking an operation.

        Args:
            total_bytes: Total expected bytes (for progress bar), or None if unknown.
            total_chunks: Total expected chunks, or 0 if unknown.
            label: Operation label (e.g., "Uploading", "Downloading").
            chunk_size: Chunk size in bytes (for estimating total chunks from total_bytes).
        """
        ...

    @abstractmethod
    def update_chunk(self, index: int, size: int, elapsed: float | None = None) -> None:
        """Record a completed chunk. elapsed is seconds for this chunk (verbose mode)."""
        ...

    @abstractmethod
    def chunk_staged(self, index: int, size: int) -> None:
        """Record a chunk that has been staged (on disk, ready or in-flight)."""
        ...

    @abstractmethod
    def update_workers(self, old: int, new: int, reason: str) -> None:
        """Record a worker count change."""
        ...

    @abstractmethod
    def set_workers(self, count: int) -> None:
        """Set current worker count for stats display."""
        ...

    @abstractmethod
    def log(self, message: str) -> None:
        """Print a log message (above progress bar if rich)."""
        ...

    @abstractmethod
    def finish(self, summary: str | None = None) -> None:
        """Stop tracking and optionally print a summary."""
        ...


class RichProgress(ProgressTracker):
    """Rich progress bar tracker for TTY output."""

    def __init__(self) -> None:
        from rich.console import Console
        from rich.progress import (
            BarColumn,
            DownloadColumn,
            Progress,
            SpinnerColumn,
            TextColumn,
            TimeElapsedColumn,
            TimeRemainingColumn,
            TransferSpeedColumn,
        )

        self._console = Console(stderr=True)
        self._progress = Progress(
            SpinnerColumn(),
            TextColumn("[bold blue]{task.description}"),
            BarColumn(),
            DownloadColumn(),
            TransferSpeedColumn(),
            TimeRemainingColumn(),
            TimeElapsedColumn(),
            console=self._console,
            transient=False,
        )
        self._task_id = None
        self._started = False
        self._workers = 0

    def start(self, total_bytes: int | None, total_chunks: int, label: str,
              chunk_size: int | None = None) -> None:
        self._progress.start()
        self._started = True
        self._task_id = self._progress.add_task(
            label,
            total=total_bytes if total_bytes else None,
        )

    def update_chunk(self, index: int, size: int, elapsed: float | None = None) -> None:
        if self._task_id is not None:
            self._progress.update(self._task_id, advance=size)

    def chunk_staged(self, index: int, size: int) -> None:
        pass  # Not shown in simple progress bar

    def update_workers(self, old: int, new: int, reason: str) -> None:
        self._workers = new
        self._progress.console.print(
            f"  [dim][auto] workers: {old} → {new} ({reason})[/dim]"
        )

    def set_workers(self, count: int) -> None:
        self._workers = count

    def log(self, message: str) -> None:
        if self._started:
            self._progress.console.print(f"[dim]{message}[/dim]")
        else:
            self._console.print(f"[dim]{message}[/dim]")

    def finish(self, summary: str | None = None) -> None:
        if self._started:
            if self._task_id is not None:
                self._progress.update(self._task_id, description="[bold green]Done")
            self._progress.stop()
            self._started = False
        if summary:
            self._console.print(summary)


class PlainProgress(ProgressTracker):
    """Plain text progress for non-TTY output (piped, cron, etc.)."""

    def start(self, total_bytes: int | None, total_chunks: int, label: str,
              chunk_size: int | None = None) -> None:
        size_str = f", {total_bytes:,} bytes" if total_bytes else ""
        click.echo(f"{label} ({total_chunks} chunks{size_str})...", err=True)

    def update_chunk(self, index: int, size: int, elapsed: float | None = None) -> None:
        click.echo(f"  Chunk {index} ({size:,} bytes)", err=True)

    def chunk_staged(self, index: int, size: int) -> None:
        pass  # Not shown in plain mode

    def update_workers(self, old: int, new: int, reason: str) -> None:
        click.echo(f"  [auto] workers: {old} → {new} ({reason})", err=True)

    def set_workers(self, count: int) -> None:
        pass  # Not displayed in plain mode

    def log(self, message: str) -> None:
        click.echo(message, err=True)

    def finish(self, summary: str | None = None) -> None:
        if summary:
            click.echo(summary, err=True)


class NullProgress(ProgressTracker):
    """Silent tracker for --progress none."""

    def start(self, total_bytes: int | None, total_chunks: int, label: str,
              chunk_size: int | None = None) -> None:
        pass

    def update_chunk(self, index: int, size: int, elapsed: float | None = None) -> None:
        pass

    def chunk_staged(self, index: int, size: int) -> None:
        pass

    def update_workers(self, old: int, new: int, reason: str) -> None:
        pass

    def set_workers(self, count: int) -> None:
        pass

    def log(self, message: str) -> None:
        pass

    def finish(self, summary: str | None = None) -> None:
        pass


class RichVerboseProgress(ProgressTracker):
    """Rich progress with scrolling event log and pinned stats line."""

    def __init__(self) -> None:
        from rich.console import Console
        from rich.live import Live
        from rich.table import Table

        self._console = Console(stderr=True)
        self._start_time: float = 0
        self._bytes_completed: int = 0
        self._chunks_completed: int = 0
        self._chunks_staged: int = 0
        self._total_bytes: int | None = None
        self._total_chunks: int = 0  # Known total, or 0 if unknown
        self._chunk_size: int | None = None  # For estimating total
        self._workers: int = 0
        self._label: str = ""
        self._live: Live | None = None
        self._log_lines: list[str] = []
        self._max_log_lines = 10
        self._in_flight: set[int] = set()  # Chunk indices currently being uploaded

    def _elapsed(self) -> float:
        return time.time() - self._start_time if self._start_time else 0

    def _estimated_total_chunks(self) -> int | None:
        """Return known total, or estimate from total_bytes/chunk_size."""
        if self._total_chunks > 0:
            return self._total_chunks
        if self._total_bytes and self._chunk_size:
            return (self._total_bytes + self._chunk_size - 1) // self._chunk_size
        return None

    def _render_stats(self) -> "Table":
        from rich.table import Table

        table = Table.grid(padding=(0, 2))
        table.add_column()

        elapsed = self._elapsed()
        throughput = self._bytes_completed / elapsed if elapsed > 0 else 0

        parts = []
        if self._workers:
            parts.append(f"[cyan][{self._workers} workers][/cyan]")

        # Chunk progress: "2 uploaded, 4 staged of ~50" or "2 uploaded, 4 staged"
        in_flight = self._chunks_staged - self._chunks_completed
        chunk_parts = [f"{self._chunks_completed} uploaded"]
        if in_flight > 0:
            chunk_parts.append(f"{in_flight} in-flight")
        chunk_parts.append(f"{self._chunks_staged} staged")

        estimated = self._estimated_total_chunks()
        if estimated:
            prefix = "" if self._total_chunks > 0 else "~"
            parts.append(f"{', '.join(chunk_parts)} of {prefix}{estimated}")
        else:
            parts.append(", ".join(chunk_parts))

        if self._total_bytes:
            parts.append(f"{_format_bytes(self._bytes_completed)} / {_format_bytes(self._total_bytes)}")
        else:
            parts.append(_format_bytes(self._bytes_completed))

        if throughput > 0:
            parts.append(f"{_format_bytes(int(throughput))}/s")
        elif self._in_flight:
            parts.append("[dim]uploading...[/dim]")
        else:
            parts.append("0.0 B/s")
        parts.append(f"{_format_duration(elapsed)} elapsed")

        if self._total_bytes and throughput > 0:
            remaining = (self._total_bytes - self._bytes_completed) / throughput
            parts.append(f"~{_format_duration(remaining)} remaining")

        table.add_row(" | ".join(parts))
        return table

    def _render(self) -> "Table":
        from rich.table import Table

        table = Table.grid()
        table.add_column()

        # Log lines
        for line in self._log_lines[-self._max_log_lines:]:
            table.add_row(f"[dim]{line}[/dim]")

        # In-flight chunks line (if any)
        if self._in_flight:
            chunks = sorted(self._in_flight)
            if len(chunks) <= 8:
                in_flight_str = ", ".join(str(c) for c in chunks)
            else:
                in_flight_str = ", ".join(str(c) for c in chunks[:6]) + f", ... +{len(chunks) - 6} more"
            table.add_row(f"[yellow]In-flight:[/yellow] {in_flight_str}")

        # Stats line
        table.add_row(self._render_stats())
        return table

    def _refresh(self) -> None:
        if self._live:
            self._live.update(self._render())

    def start(self, total_bytes: int | None, total_chunks: int, label: str,
              chunk_size: int | None = None) -> None:
        from rich.live import Live

        self._start_time = time.time()
        self._total_bytes = total_bytes
        self._total_chunks = total_chunks
        self._chunk_size = chunk_size
        self._label = label
        self._live = Live(self._render(), console=self._console, refresh_per_second=4)
        self._live.start()

    def update_chunk(self, index: int, size: int, elapsed: float | None = None) -> None:
        self._bytes_completed += size
        self._chunks_completed += 1
        self._in_flight.discard(index)

        if elapsed is not None:
            throughput = size / elapsed if elapsed > 0 else 0
            ts = _format_duration(self._elapsed())
            self._log_lines.append(
                f"[{ts}] Chunk {index} ({_format_bytes(size)} in {elapsed:.2f}s, {_format_bytes(int(throughput))}/s)"
            )
        self._refresh()

    def chunk_staged(self, index: int, size: int) -> None:
        self._chunks_staged += 1
        self._in_flight.add(index)
        self._refresh()

    def update_workers(self, old: int, new: int, reason: str) -> None:
        self._workers = new
        ts = _format_duration(self._elapsed())
        self._log_lines.append(f"[{ts}] Workers: {old} → {new} ({reason})")
        self._refresh()

    def set_workers(self, count: int) -> None:
        self._workers = count
        self._refresh()

    def log(self, message: str) -> None:
        ts = _format_duration(self._elapsed()) if self._start_time else "00:00"
        self._log_lines.append(f"[{ts}] {message}")
        self._refresh()

    def finish(self, summary: str | None = None) -> None:
        if self._live:
            self._live.stop()
            self._live = None
        if summary:
            self._console.print(summary)


class PlainVerboseProgress(ProgressTracker):
    """Plain text verbose progress with timestamps."""

    def __init__(self) -> None:
        self._start_time: float = 0
        self._workers: int = 0

    def _elapsed(self) -> float:
        return time.time() - self._start_time if self._start_time else 0

    def start(self, total_bytes: int | None, total_chunks: int, label: str,
              chunk_size: int | None = None) -> None:
        self._start_time = time.time()
        size_str = f", {_format_bytes(total_bytes)}" if total_bytes else ""
        chunks_str = f"{total_chunks} chunks" if total_chunks else "streaming"
        click.echo(f"[00:00] {label} ({chunks_str}{size_str})...", err=True)

    def update_chunk(self, index: int, size: int, elapsed: float | None = None) -> None:
        ts = _format_duration(self._elapsed())
        if elapsed is not None:
            throughput = size / elapsed if elapsed > 0 else 0
            click.echo(
                f"[{ts}] Chunk {index} uploaded ({_format_bytes(size)} in {elapsed:.2f}s, {_format_bytes(int(throughput))}/s)",
                err=True,
            )
        else:
            click.echo(f"[{ts}] Chunk {index} uploaded ({_format_bytes(size)})", err=True)

    def chunk_staged(self, index: int, size: int) -> None:
        ts = _format_duration(self._elapsed())
        click.echo(f"[{ts}] Chunk {index} staged ({_format_bytes(size)})", err=True)

    def update_workers(self, old: int, new: int, reason: str) -> None:
        self._workers = new
        ts = _format_duration(self._elapsed())
        click.echo(f"[{ts}] Workers: {old} → {new} ({reason})", err=True)

    def set_workers(self, count: int) -> None:
        self._workers = count

    def log(self, message: str) -> None:
        ts = _format_duration(self._elapsed()) if self._start_time else "00:00"
        click.echo(f"[{ts}] {message}", err=True)

    def finish(self, summary: str | None = None) -> None:
        if summary:
            ts = _format_duration(self._elapsed())
            click.echo(f"[{ts}] {summary}", err=True)


def get_tracker(mode: str = "auto", verbose: bool = False) -> ProgressTracker:
    """Get a progress tracker based on mode and TTY detection."""
    if mode == "none":
        return NullProgress()
    if mode == "rich" or (mode == "auto" and sys.stderr.isatty()):
        return RichVerboseProgress() if verbose else RichProgress()
    return PlainVerboseProgress() if verbose else PlainProgress()
