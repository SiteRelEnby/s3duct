"""Download pipeline: S3 -> decrypt -> verify -> stdout."""

import json
import sys
import threading
import time
from concurrent.futures import Future, ThreadPoolExecutor
from dataclasses import dataclass
from pathlib import Path
from statistics import mean

import click
from botocore.exceptions import ClientError

from s3duct import __version__
from s3duct.backends.base import StorageBackend
from s3duct.config import SCRATCH_DIR
from s3duct.encryption import aes_decrypt_file, age_decrypt_file
from s3duct.integrity import hash_file, compute_chain, DualHash
from s3duct.manifest import ChunkRecord, Manifest
from s3duct.progress import ProgressTracker, PlainProgress

# Adaptive worker scaling constants (same defaults as uploader)
_DL_MAX_POOL_SIZE = 16
_DL_INITIAL_WORKERS = 4
_DL_MIN_WORKERS = 2
_DL_ADJUST_INTERVAL = 3


@dataclass
class _DownloadJob:
    """A chunk to be downloaded."""
    index: int
    chunk_rec: ChunkRecord
    dest_path: Path


@dataclass
class _DownloadResult:
    """Result of a completed download + optional decryption."""
    job: _DownloadJob
    final_path: Path  # may differ from dest_path after decryption
    elapsed: float


class _DownloadThrottle:
    """Adaptive concurrency control for parallel downloads.

    Adjusts worker count based on download time vs drain time
    (verify + stdout write). If downloads are slow relative to drain,
    we're download-bound and should add workers. If drain is slow
    (stdout bottleneck), reduce workers.
    """

    def __init__(self, initial: int = _DL_INITIAL_WORKERS,
                 min_workers: int = _DL_MIN_WORKERS,
                 max_workers: int = _DL_MAX_POOL_SIZE,
                 tracker: ProgressTracker | None = None) -> None:
        self._min = max(1, min_workers)
        self._max = max(self._min, max_workers)
        self._current = min(max(initial, self._min), self._max)
        self._semaphore = threading.Semaphore(self._current)
        self._lock = threading.Lock()
        self._download_times: list[float] = []
        self._drain_times: list[float] = []
        self._completions = 0
        self._tracker = tracker

    @property
    def current_workers(self) -> int:
        return self._current

    def acquire(self) -> None:
        self._semaphore.acquire()

    def release(self) -> None:
        self._semaphore.release()

    def record_download_time(self, elapsed: float) -> None:
        with self._lock:
            self._download_times.append(elapsed)
            if len(self._download_times) > 6:
                self._download_times.pop(0)
            self._completions += 1
            if self._completions % _DL_ADJUST_INTERVAL == 0:
                self._adjust()

    def record_drain_time(self, elapsed: float) -> None:
        with self._lock:
            self._drain_times.append(elapsed)
            if len(self._drain_times) > 6:
                self._drain_times.pop(0)

    def record_throttle(self) -> None:
        """Called when S3 returns a throttle/SlowDown error."""
        with self._lock:
            if self._current > self._min:
                try:
                    self._semaphore.acquire(blocking=False)
                    old = self._current + 1
                    self._current -= 1
                    if self._tracker:
                        self._tracker.update_workers(old, self._current, "S3 throttle detected")
                except Exception:
                    pass

    def _adjust(self) -> None:
        if len(self._download_times) < 2 or len(self._drain_times) < 2:
            return
        avg_download = mean(self._download_times)
        avg_drain = mean(self._drain_times)
        if avg_drain <= 0:
            return

        old = self._current
        if avg_download > 2 * avg_drain and self._current < self._max:
            self._semaphore.release()
            self._current += 1
            if self._tracker:
                self._tracker.update_workers(
                    old, self._current,
                    f"download-bound, avg download {avg_download:.1f}s vs drain {avg_drain:.1f}s",
                )
        elif avg_download < 0.5 * avg_drain and self._current > self._min:
            try:
                self._semaphore.acquire(blocking=False)
                self._current -= 1
                if self._tracker:
                    self._tracker.update_workers(
                        old, self._current,
                        f"drain-bound, avg download {avg_download:.1f}s vs drain {avg_drain:.1f}s",
                    )
            except Exception:
                pass


def _download_one(
    backend: StorageBackend,
    job: _DownloadJob,
    decrypt: bool,
    encrypted: bool,
    encryption_method: str | None,
    aes_key: bytes | None,
    age_identity: str | None,
    throttle: "_DownloadThrottle | None",
) -> _DownloadResult:
    """Download and optionally decrypt a single chunk. Runs in a worker thread."""
    t0 = time.monotonic()
    try:
        backend.download(job.chunk_rec.s3_key, job.dest_path)

        final_path = job.dest_path
        if decrypt and encrypted:
            method = encryption_method or "age"
            dec_path = job.dest_path.with_suffix(".dec")
            if method == "aes-256-gcm":
                aes_decrypt_file(job.dest_path, dec_path, aes_key)
            else:
                age_decrypt_file(job.dest_path, dec_path, age_identity)
            job.dest_path.unlink()
            final_path = dec_path

        elapsed = time.monotonic() - t0
        if throttle:
            throttle.record_download_time(elapsed)
        return _DownloadResult(job=job, final_path=final_path, elapsed=elapsed)
    finally:
        if throttle:
            throttle.release()


def _decrypt_manifest(
    raw: bytes,
    aes_key: bytes | None = None,
    age_identity: str | None = None,
) -> Manifest:
    """Try to parse manifest, decrypting if necessary.

    Tries JSON first, then AES decryption, then age decryption.
    Raises click.ClickException on failure.
    """
    try:
        return Manifest.from_json(raw)
    except (json.JSONDecodeError, UnicodeDecodeError):
        pass

    if aes_key:
        from s3duct.encryption import aes_decrypt_manifest
        try:
            decrypted = aes_decrypt_manifest(raw, aes_key)
            manifest = Manifest.from_json(decrypted)
            click.echo("Manifest decrypted successfully.", err=True)
            return manifest
        except Exception:
            pass

    if age_identity:
        from s3duct.encryption import age_decrypt_manifest
        try:
            decrypted = age_decrypt_manifest(raw, age_identity)
            manifest = Manifest.from_json(decrypted)
            click.echo("Manifest decrypted successfully.", err=True)
            return manifest
        except Exception:
            pass

    if aes_key or age_identity:
        raise click.ClickException(
            "Manifest appears encrypted but could not be decrypted. "
            "Check your key/identity."
        )
    raise click.ClickException(
        "Manifest is not valid JSON — it may be encrypted. "
        "Provide --key or --age-identity to decrypt."
    )


def _drain_oldest(
    window: list[tuple[_DownloadJob, Future]],
    prev_chain: bytes | None,
    decrypt: bool,
    encrypted: bool,
    name: str,
    throttle: _DownloadThrottle | None,
    tracker: ProgressTracker | None = None,
) -> bytes | None:
    """Pop oldest future, verify, write to stdout, cleanup. Returns updated prev_chain."""
    job, future = window.pop(0)
    t0 = time.monotonic()
    try:
        result: _DownloadResult = future.result()
    except ClientError as e:
        code = e.response["Error"].get("Code", "")
        if code == "InvalidObjectState":
            raise click.ClickException(
                f"Chunk {job.index} is archived in Glacier/Deep Archive "
                f"and not available for download.\n"
                f"Run 's3duct restore --bucket <bucket> --name {name}' to "
                f"initiate thaw, then retry."
            )
        raise
    except Exception:
        job.dest_path.unlink(missing_ok=True)
        job.dest_path.with_suffix(".dec").unlink(missing_ok=True)
        raise

    chunk_path = result.final_path
    chunk_rec = job.chunk_rec

    # Verify integrity (skip in raw/no-decrypt mode)
    skip_integrity = encrypted and not decrypt
    if not skip_integrity:
        dual_hash, size = hash_file(chunk_path)
        expected = DualHash(sha256=chunk_rec.sha256, sha3_256=chunk_rec.sha3_256)

        if dual_hash != expected:
            chunk_path.unlink(missing_ok=True)
            raise click.ClickException(
                f"Integrity check failed for chunk {chunk_rec.index}. "
                "Data may be corrupt."
            )

        if size != chunk_rec.size:
            chunk_path.unlink(missing_ok=True)
            raise click.ClickException(
                f"Size mismatch for chunk {chunk_rec.index}: "
                f"expected {chunk_rec.size}, got {size}"
            )

        chain_hex = compute_chain(dual_hash, prev_chain)
        prev_chain = bytes.fromhex(chain_hex)

    # Write to stdout
    with open(chunk_path, "rb") as f:
        while True:
            data = f.read(8 * 1024 * 1024)
            if not data:
                break
            sys.stdout.buffer.write(data)
    sys.stdout.buffer.flush()

    # Cleanup
    chunk_path.unlink(missing_ok=True)

    if tracker:
        tracker.update_chunk(job.chunk_rec.index, job.chunk_rec.size)

    if throttle:
        throttle.record_drain_time(time.monotonic() - t0)

    return prev_chain


def run_get(
    backend: StorageBackend,
    name: str,
    decrypt: bool = False,
    encryption_method: str | None = None,
    aes_key: bytes | None = None,
    age_identity: str | None = None,
    scratch_dir: Path | None = None,
    summary: str = "text",  # "text", "json", or "none"
    download_workers: int | str = 1,
    min_download_workers: int | None = None,
    max_download_workers: int | None = None,
    tracker: ProgressTracker | None = None,
) -> None:
    """Execute the full get pipeline."""
    if tracker is None:
        tracker = PlainProgress()

    backend.preflight_check()

    if scratch_dir is None:
        scratch_dir = SCRATCH_DIR
    scratch_dir.mkdir(parents=True, exist_ok=True)

    # Download manifest
    manifest_key = Manifest.s3_key(name)
    tracker.log("Downloading manifest...")
    raw = backend.download_bytes(manifest_key)

    # Decrypt manifest if needed (validates key/identity early)
    manifest = _decrypt_manifest(raw, aes_key=aes_key, age_identity=age_identity)

    if manifest.encrypted and not decrypt:
        method = manifest.encryption_method or "age"
        if method == "aes-256-gcm":
            tracker.log("Warning: stream was encrypted with AES-256-GCM. Use --key to decrypt.")
        else:
            tracker.log("Warning: stream was encrypted with age. Use --age-identity to decrypt.")

    # Auto-detect encryption method from manifest
    if decrypt and manifest.encrypted:
        method = encryption_method or manifest.encryption_method or "age"
        if method == "aes-256-gcm" and not aes_key:
            raise click.ClickException("--key required to decrypt AES-256-GCM encrypted stream")
        if method == "age" and not age_identity:
            raise click.ClickException("--age-identity required to decrypt age encrypted stream")

    tracker.start(manifest.total_bytes, manifest.chunk_count, "Downloading")

    # Resolve worker count
    adaptive = download_workers == "auto"
    if adaptive:
        min_w = min_download_workers or _DL_MIN_WORKERS
        max_w = max_download_workers or _DL_MAX_POOL_SIZE
        effective_workers = min(max(_DL_INITIAL_WORKERS, min_w), max_w)
    else:
        effective_workers = int(download_workers)
        min_w = effective_workers
        max_w = effective_workers

    prev_chain: bytes | None = None

    if effective_workers > 1 or adaptive:
        # --- Parallel download path ---
        throttle = _DownloadThrottle(
            initial=effective_workers, min_workers=min_w, max_workers=max_w,
            tracker=tracker,
        ) if adaptive else None

        pool_size = max_w if adaptive else effective_workers
        window: list[tuple[_DownloadJob, Future]] = []
        deferred_cleanup: list[Path] = []

        if adaptive:
            tracker.log(f"  (workers: auto ({effective_workers}), range {min_w}-{max_w})")

        def _abort_window() -> None:
            for remaining_job, remaining_future in window:
                remaining_future.cancel()
                deferred_cleanup.append(remaining_job.dest_path)
                deferred_cleanup.append(remaining_job.dest_path.with_suffix(".dec"))
            window.clear()

        try:
            with ThreadPoolExecutor(max_workers=pool_size) as pool:
                for chunk_rec in manifest.chunks:
                    chunk_path = scratch_dir / f"chunk-{chunk_rec.index:06d}"
                    job = _DownloadJob(
                        index=chunk_rec.index,
                        chunk_rec=chunk_rec,
                        dest_path=chunk_path,
                    )

                    if throttle:
                        throttle.acquire()

                    future = pool.submit(
                        _download_one, backend, job, decrypt,
                        manifest.encrypted,
                        encryption_method or manifest.encryption_method,
                        aes_key, age_identity, throttle,
                    )
                    window.append((job, future))

                    # Drain when window is full (backpressure)
                    max_window = throttle.current_workers if throttle else effective_workers
                    while len(window) >= max_window:
                        try:
                            prev_chain = _drain_oldest(
                                window, prev_chain, decrypt,
                                manifest.encrypted, name, throttle,
                                tracker=tracker,
                            )
                        except Exception:
                            _abort_window()
                            raise

                # Drain remaining
                while window:
                    try:
                        prev_chain = _drain_oldest(
                            window, prev_chain, decrypt,
                            manifest.encrypted, name, throttle,
                            tracker=tracker,
                        )
                    except Exception:
                        _abort_window()
                        raise
        finally:
            for p in deferred_cleanup:
                p.unlink(missing_ok=True)
    else:
        # --- Sequential download path (workers=1) ---
        for chunk_rec in manifest.chunks:
            chunk_path = scratch_dir / f"chunk-{chunk_rec.index:06d}"

            try:
                backend.download(chunk_rec.s3_key, chunk_path)
            except ClientError as e:
                code = e.response["Error"].get("Code", "")
                if code == "InvalidObjectState":
                    raise click.ClickException(
                        f"Chunk {chunk_rec.index} is archived in Glacier/Deep Archive "
                        f"and not available for download.\n"
                        f"Run 's3duct restore --bucket <bucket> --name {name}' to "
                        f"initiate thaw, then retry."
                    )
                raise

            if decrypt and manifest.encrypted:
                method = encryption_method or manifest.encryption_method or "age"
                if method == "aes-256-gcm":
                    dec_path = chunk_path.with_suffix(".dec")
                    aes_decrypt_file(chunk_path, dec_path, aes_key)
                else:
                    dec_path = chunk_path.with_suffix(".dec")
                    age_decrypt_file(chunk_path, dec_path, age_identity)
                chunk_path.unlink()
                chunk_path = dec_path

            skip_integrity = manifest.encrypted and not decrypt
            if not skip_integrity:
                dual_hash, size = hash_file(chunk_path)
                expected = DualHash(sha256=chunk_rec.sha256, sha3_256=chunk_rec.sha3_256)

                if dual_hash != expected:
                    chunk_path.unlink(missing_ok=True)
                    raise click.ClickException(
                        f"Integrity check failed for chunk {chunk_rec.index}. "
                        "Data may be corrupt."
                    )

                if size != chunk_rec.size:
                    chunk_path.unlink(missing_ok=True)
                    raise click.ClickException(
                        f"Size mismatch for chunk {chunk_rec.index}: "
                        f"expected {chunk_rec.size}, got {size}"
                    )

                chain_hex = compute_chain(dual_hash, prev_chain)
                prev_chain = bytes.fromhex(chain_hex)

            with open(chunk_path, "rb") as f:
                while True:
                    data = f.read(8 * 1024 * 1024)
                    if not data:
                        break
                    sys.stdout.buffer.write(data)
            sys.stdout.buffer.flush()

            chunk_path.unlink(missing_ok=True)
            tracker.update_chunk(chunk_rec.index, chunk_rec.size)

    # Verify final chain (skip in raw/no-decrypt mode)
    raw_mode = manifest.encrypted and not decrypt
    if not raw_mode and prev_chain and manifest.final_chain:
        if prev_chain.hex() != manifest.final_chain:
            raise click.ClickException("Final chain mismatch. Stream may be incomplete or tampered.")

    workers_label = f"auto ({throttle.current_workers})" if adaptive and throttle else str(effective_workers)
    if summary == "json":
        report = {
            "version": __version__,
            "status": "complete",
            "stream": name,
            "chunks_downloaded": manifest.chunk_count,
            "total_bytes": manifest.total_bytes,
            "stream_sha256": manifest.stream_sha256,
            "stream_sha3_256": manifest.stream_sha3_256,
            "chain_verified": not raw_mode,
            "raw_mode": raw_mode,
            "encrypted": manifest.encrypted,
            "encryption_method": manifest.encryption_method,
        }
        click.echo(json.dumps(report), err=True)
        tracker.finish()
    elif summary == "text":
        if effective_workers > 1 or adaptive:
            tracker.finish(f"Restore complete. (workers: {workers_label})")
        else:
            tracker.finish("Restore complete.")
    else:
        tracker.finish()


def run_list(backend: StorageBackend, prefix: str = "") -> None:
    """List stored streams."""
    # List all manifest files
    objects = backend.list_objects(prefix)
    manifests = [o for o in objects if o.key.endswith("/.manifest.json")]

    if not manifests:
        click.echo("No streams found.", err=True)
        return

    for obj in sorted(manifests, key=lambda o: o.key):
        # Stream name is everything before /.manifest.json
        stream_name = obj.key.rsplit("/.manifest.json", 1)[0]
        try:
            raw = backend.download_bytes(obj.key)
            m = Manifest.from_json(raw)
            encrypted = " [encrypted]" if m.encrypted else ""
            sc = f" [{m.storage_class}]" if m.storage_class and m.storage_class != "STANDARD" else ""
            ver = f"  v{m.tool_version}" if m.tool_version else ""
            click.echo(
                f"{stream_name}  "
                f"{m.chunk_count} chunks  "
                f"{m.total_bytes:,} bytes  "
                f"{m.created}"
                f"{encrypted}"
                f"{sc}"
                f"{ver}"
            )
        except (json.JSONDecodeError, UnicodeDecodeError):
            click.echo(f"{stream_name}  (manifest encrypted)")
        except Exception:
            click.echo(f"{stream_name}  (manifest unreadable)")


def run_delete(
    backend: StorageBackend,
    name: str,
    dry_run: bool = False,
    aes_key: bytes | None = None,
    age_identity: str | None = None,
    tracker: ProgressTracker | None = None,
) -> None:
    """Delete a stream (all chunks + manifest)."""
    if tracker is None:
        tracker = PlainProgress()

    backend.preflight_check()

    manifest_key = Manifest.s3_key(name)
    tracker.log("Downloading manifest...")
    raw = backend.download_bytes(manifest_key)
    manifest = _decrypt_manifest(raw, aes_key=aes_key, age_identity=age_identity)

    # Collect all keys to delete
    keys_to_delete = [c.s3_key for c in manifest.chunks]
    keys_to_delete.append(manifest_key)

    if dry_run:
        tracker.log(f"Would delete {len(keys_to_delete)} objects:")
        for key in keys_to_delete:
            tracker.log(f"  {key}")
        return

    tracker.start(None, len(keys_to_delete), "Deleting")
    deleted = 0
    for i, key in enumerate(keys_to_delete):
        try:
            backend.delete_object(key)
            deleted += 1
            tracker.update_chunk(i, 0)
        except Exception as e:
            tracker.log(f"  Failed to delete {key}: {e}")

    tracker.finish(f"Done. Deleted {deleted}/{len(keys_to_delete)} objects.")


def run_verify(
    backend: StorageBackend,
    name: str,
    aes_key: bytes | None = None,
    age_identity: str | None = None,
    summary: str = "text",  # "text", "json", or "none"
    tracker: ProgressTracker | None = None,
) -> None:
    """Verify integrity of a stored stream without downloading chunk data."""
    if tracker is None:
        tracker = PlainProgress()

    backend.preflight_check()

    manifest_key = Manifest.s3_key(name)
    raw = backend.download_bytes(manifest_key)

    manifest = _decrypt_manifest(raw, aes_key=aes_key, age_identity=age_identity)

    tracker.start(None, manifest.chunk_count, "Verifying")
    errors = 0
    mismatches = []
    missing = []

    for chunk_rec in manifest.chunks:
        try:
            info = backend.head_object(chunk_rec.s3_key)
            if info.etag != chunk_rec.etag:
                tracker.log(f"  MISMATCH chunk {chunk_rec.index}: ETag differs")
                mismatches.append(chunk_rec.index)
                errors += 1
            else:
                tracker.update_chunk(chunk_rec.index, chunk_rec.size)
        except Exception as e:
            tracker.log(f"  MISSING chunk {chunk_rec.index}: {e}")
            missing.append(chunk_rec.index)
            errors += 1

    if summary == "json":
        click.echo(json.dumps({
            "version": __version__,
            "status": "fail" if errors else "ok",
            "stream": name,
            "chunks_total": manifest.chunk_count,
            "chunks_ok": manifest.chunk_count - errors,
            "chunks_mismatched": mismatches,
            "chunks_missing": missing,
            "errors": errors,
        }), err=True)
        tracker.finish()
    elif summary == "text":
        if errors:
            tracker.finish(f"Verification failed: {errors} error(s).")
        else:
            tracker.finish("All chunks verified.")
    else:
        tracker.finish()

    if errors:
        raise SystemExit(1)
