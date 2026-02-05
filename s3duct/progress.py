"""Progress tracking for s3duct operations."""

import sys
from abc import ABC, abstractmethod

import click


class ProgressTracker(ABC):
    """Abstract progress tracker interface."""

    @abstractmethod
    def start(self, total_bytes: int | None, total_chunks: int, label: str) -> None:
        """Start tracking an operation."""
        ...

    @abstractmethod
    def update_chunk(self, index: int, size: int) -> None:
        """Record a completed chunk."""
        ...

    @abstractmethod
    def update_workers(self, old: int, new: int, reason: str) -> None:
        """Record a worker count change."""
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

    def start(self, total_bytes: int | None, total_chunks: int, label: str) -> None:
        self._progress.start()
        self._started = True
        self._task_id = self._progress.add_task(
            label,
            total=total_bytes if total_bytes else None,
        )

    def update_chunk(self, index: int, size: int) -> None:
        if self._task_id is not None:
            self._progress.update(self._task_id, advance=size)

    def update_workers(self, old: int, new: int, reason: str) -> None:
        self._progress.console.print(
            f"  [dim][auto] workers: {old} → {new} ({reason})[/dim]"
        )

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

    def start(self, total_bytes: int | None, total_chunks: int, label: str) -> None:
        size_str = f", {total_bytes:,} bytes" if total_bytes else ""
        click.echo(f"{label} ({total_chunks} chunks{size_str})...", err=True)

    def update_chunk(self, index: int, size: int) -> None:
        click.echo(f"  Chunk {index} ({size:,} bytes)", err=True)

    def update_workers(self, old: int, new: int, reason: str) -> None:
        click.echo(f"  [auto] workers: {old} → {new} ({reason})", err=True)

    def log(self, message: str) -> None:
        click.echo(message, err=True)

    def finish(self, summary: str | None = None) -> None:
        if summary:
            click.echo(summary, err=True)


class NullProgress(ProgressTracker):
    """Silent tracker for --progress none."""

    def start(self, total_bytes: int | None, total_chunks: int, label: str) -> None:
        pass

    def update_chunk(self, index: int, size: int) -> None:
        pass

    def update_workers(self, old: int, new: int, reason: str) -> None:
        pass

    def log(self, message: str) -> None:
        pass

    def finish(self, summary: str | None = None) -> None:
        pass


def get_tracker(mode: str = "auto") -> ProgressTracker:
    """Get a progress tracker based on mode and TTY detection."""
    if mode == "none":
        return NullProgress()
    if mode == "rich" or (mode == "auto" and sys.stderr.isatty()):
        return RichProgress()
    return PlainProgress()
