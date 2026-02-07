"""Upload pipeline: chunker -> integrity -> encryption -> S3 -> resume log."""

import json as _json
import os
import stat
import sys
import threading
import time
from concurrent.futures import Future, ThreadPoolExecutor
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path

import click
from botocore.exceptions import ClientError

from s3duct import __version__

from s3duct.backends.base import StorageBackend
from s3duct.backpressure import BackpressureConfig, BackpressureMonitor
from s3duct.chunker import ChunkInfo, chunk_stream, fast_forward_stream
from s3duct.config import DEFAULT_CHUNK_SIZE, SCRATCH_DIR
from s3duct.encryption import (
    aes_encrypt_file,
    age_encrypt_file,
    get_recipient_from_identity,
)
from s3duct.integrity import DualHash, StreamHasher, compute_chain
from s3duct.manifest import ChunkRecord, Manifest
from s3duct.progress import ProgressTracker, PlainProgress
from s3duct.resume import ResumeEntry, ResumeLog
from s3duct.throttle import AdaptiveThrottle

# Adaptive worker scaling constants
MAX_POOL_SIZE = 16
INITIAL_WORKERS = 4
MIN_WORKERS = 2


@dataclass
class UploadJob:
    """A chunk ready for upload."""
    index: int
    upload_path: Path
    s3_key: str
    size: int
    dual_hash: DualHash
    chain_hex: str


@dataclass
class UploadResult:
    """Result of a completed upload."""
    job: UploadJob
    etag: str
    elapsed: float = 0.0


def _upload_one(backend: StorageBackend, job: UploadJob,
                storage_class: str | None) -> UploadResult:
    """Upload a single chunk to S3. Runs in a worker thread."""
    etag = backend.upload(job.s3_key, job.upload_path, storage_class)
    return UploadResult(job=job, etag=etag)


def _encrypt_chunk(chunk_info: ChunkInfo, encryption_method: str | None,
                   aes_key: bytes | None, recipient: str | None) -> Path:
    """Encrypt a chunk file and return the encrypted path. Deletes the plaintext."""
    if encryption_method == "aes-256-gcm":
        enc_path = chunk_info.path.with_suffix(".enc")
        aes_encrypt_file(chunk_info.path, enc_path, aes_key)
    elif encryption_method == "age":
        enc_path = chunk_info.path.with_suffix(".age")
        age_encrypt_file(chunk_info.path, enc_path, recipient)
    else:
        raise ValueError(f"Unknown encryption method: {encryption_method}")
    chunk_info.path.unlink()
    return enc_path


def _drain_one(window: list[tuple["UploadJob", Future]],
               resume_log: ResumeLog, manifest: Manifest,
               tracker: "ProgressTracker | None" = None) -> None:
    """Pop the oldest future from the window, wait for it, and record the result."""
    job, future = window.pop(0)
    try:
        result: UploadResult = future.result()
    except Exception:
        # Clean up the failed job's file before re-raising
        job.upload_path.unlink(missing_ok=True)
        raise

    entry = ResumeEntry(
        chunk=result.job.index,
        size=result.job.size,
        sha256=result.job.dual_hash.sha256,
        sha3_256=result.job.dual_hash.sha3_256,
        chain=result.job.chain_hex,
        s3_key=result.job.s3_key,
        etag=result.etag,
        ts=datetime.now(timezone.utc).isoformat(),
    )
    resume_log.append(entry)

    manifest.add_chunk(ChunkRecord(
        index=result.job.index,
        s3_key=result.job.s3_key,
        size=result.job.size,
        sha256=result.job.dual_hash.sha256,
        sha3_256=result.job.dual_hash.sha3_256,
        etag=result.etag,
    ))

    # Update progress tracker with elapsed time
    if tracker:
        tracker.update_chunk(result.job.index, result.job.size, result.elapsed)

    # Clean up uploaded file from disk
    result.job.upload_path.unlink(missing_ok=True)


_CHUNK_INDEX_WARNING_EMITTED = False


def _chunk_s3_key(name: str, index: int) -> str:
    global _CHUNK_INDEX_WARNING_EMITTED
    if index >= 1_000_000 and not _CHUNK_INDEX_WARNING_EMITTED:
        click.echo(
            f"WARNING: Chunk index {index} exceeds 6-digit zero-padding. "
            f"S3 keys will still work but will be wider than chunk-NNNNNN format.",
            err=True,
        )
        _CHUNK_INDEX_WARNING_EMITTED = True
    return f"{name}/chunk-{index:06d}"


def _stdin_expected_size() -> int | None:
    """If stdin is a regular file, return its size. Otherwise None."""
    try:
        st = os.fstat(sys.stdin.buffer.fileno())
        if stat.S_ISREG(st.st_mode):
            return st.st_size
    except (OSError, AttributeError):
        pass
    return None


def run_put(
    backend: StorageBackend,
    name: str,
    chunk_size: int = DEFAULT_CHUNK_SIZE,
    encrypt: bool = False,
    encrypt_manifest: bool = False,
    encryption_method: str | None = None,  # "aes-256-gcm" or "age"
    aes_key: bytes | None = None,
    age_identity: str | None = None,
    storage_class: str | None = None,
    scratch_dir: Path | None = None,
    diskspace_limit: int | None = None,
    buffer_chunks: int | None = None,
    tags: dict[str, str] | None = None,
    description: str = "",
    strict_resume: bool = True,
    summary: str = "text",  # "text", "json", or "none"
    upload_workers: int | str = "auto",  # int for fixed, "auto" for adaptive
    min_upload_workers: int | None = None,
    max_upload_workers: int | None = None,
    expected_size: int | None = None,
    tracker: ProgressTracker | None = None,
    clobber: bool = False,
) -> None:
    """Execute the full put pipeline."""
    if tracker is None:
        tracker = PlainProgress()

    # Verify credentials and bucket access before reading any stdin
    backend.preflight_check()

    if scratch_dir is None:
        scratch_dir = SCRATCH_DIR
    scratch_dir.mkdir(parents=True, exist_ok=True)

    warnings: list[str] = []

    # Resolve worker count
    adaptive = False
    user_min = min_upload_workers if min_upload_workers is not None else MIN_WORKERS
    user_max = max_upload_workers if max_upload_workers is not None else MAX_POOL_SIZE
    if isinstance(upload_workers, str) and upload_workers == "auto":
        adaptive = True
        effective_workers = min(max(INITIAL_WORKERS, user_min), user_max)
    else:
        effective_workers = int(upload_workers)
        if effective_workers < 1:
            effective_workers = 1

    # Check expected stdin size for regular files
    expected_stdin_size = _stdin_expected_size()

    # Set up backpressure — need at least (workers+1) buffer chunks
    min_buf = effective_workers + 1 if not adaptive else user_max + 1
    bp_config = BackpressureConfig(
        chunk_size=chunk_size,
        scratch_dir=scratch_dir,
        max_buffer_chunks=buffer_chunks,
        diskspace_limit=diskspace_limit,
        min_buffer_chunks=min_buf,
    )
    bp_monitor = BackpressureMonitor(bp_config)

    # Resolve encryption
    recipient = None
    if encrypt and encryption_method == "age":
        if not age_identity:
            raise click.ClickException("--age-identity required for age encryption")
        recipient = get_recipient_from_identity(age_identity)
    elif encrypt and encryption_method == "aes-256-gcm":
        if not aes_key:
            raise click.ClickException("--key required for AES encryption")

    resume_log = ResumeLog(name)
    stream_hasher = StreamHasher()
    prev_chain: bytes | None = None

    # Check if stream already exists (manifest present = completed upload)
    old_chunk_keys: set[str] | None = None
    _manifest_key = Manifest.s3_key(name)
    try:
        backend.head_object(_manifest_key)
        # Manifest exists — a previous upload completed with this name
        if not clobber:
            raise click.ClickException(
                f"Stream {name!r} already exists. "
                "Use --clobber to replace it, or choose a different --name."
            )
        # --clobber: read old manifest so we can clean up orphan chunks later
        tracker.log(f"Stream {name!r} exists, --clobber specified. Reading old manifest...")
        raw = backend.download_bytes(_manifest_key)
        from s3duct.downloader import _decrypt_manifest
        try:
            old_manifest = _decrypt_manifest(raw, aes_key=aes_key, age_identity=age_identity)
            old_chunk_keys = {c.s3_key for c in old_manifest.chunks}
        except click.ClickException:
            raise click.ClickException(
                f"Stream {name!r} exists but its manifest could not be read "
                "(encrypted or corrupt). Provide the correct --key/--age-identity "
                "with --clobber, or delete the stream first with 's3duct delete'."
            )
        # Clear stale resume log from any previous attempt
        resume_log.clear()
    except ClientError as e:
        if e.response["Error"].get("Code") not in ("404", "NoSuchKey"):
            raise
        # Manifest doesn't exist — OK to proceed (fresh or resume)

    # Check for resume
    resume_from = -1
    if resume_log.last_chunk_index >= 0:
        if not resume_log.verify_chain():
            tracker.log("Resume log chain verification failed. Starting fresh.")
            resume_log.clear()
        else:
            tracker.log(
                f"Found resume log with {resume_log.last_chunk_index + 1} completed chunks. "
                "Verifying stream...",
            )
            # Fast-forward through already-uploaded chunks
            ff_count = resume_log.last_chunk_index + 1
            ff_verified = 0
            for idx, dual_hash, size in fast_forward_stream(
                sys.stdin.buffer, chunk_size, ff_count, stream_hasher
            ):
                if not resume_log.verify_entry(idx, dual_hash, size):
                    tracker.log(
                        f"Stream mismatch at chunk {idx}. "
                        "This is a different stream. Starting fresh.",
                    )
                    resume_log.clear()
                    # Can't rewind stdin - must abort
                    raise click.ClickException(
                        "Cannot resume: stream does not match resume log. "
                        "Please re-run with the original stream from the beginning."
                    )
                tracker.log(f"  Verified chunk {idx}")
                ff_verified += 1

            if ff_verified < ff_count:
                msg = (
                    f"resume log contains {ff_count} chunks but "
                    f"stdin ended after verifying {ff_verified}. "
                    f"The remaining {ff_count - ff_verified} chunk(s) were "
                    f"not re-verified against input."
                )
                if strict_resume:
                    raise click.ClickException(
                        f"WARNING: {msg} Aborting (--strict-resume is enabled)."
                    )
                warnings.append(msg)
                tracker.log(f"WARNING: {msg}")

            resume_from = resume_log.last_chunk_index
            prev_chain = resume_log.get_last_chain_bytes()
            tracker.log(f"Resuming from chunk {resume_from + 1}")

    manifest = Manifest.new(
        name=name,
        chunk_size=chunk_size,
        encrypted=encrypt,
        encryption_method=encryption_method,
        encryption_recipient=recipient,
        storage_class=storage_class,
        tags=tags,
        encrypted_manifest=encrypt_manifest,
        description=description,
    )

    # Add already-uploaded chunks to manifest
    for entry in resume_log.entries:
        manifest.add_chunk(ChunkRecord(
            index=entry.chunk,
            s3_key=entry.s3_key,
            size=entry.size,
            sha256=entry.sha256,
            sha3_256=entry.sha3_256,
            etag=entry.etag,
        ))

    chunks_uploaded = resume_log.last_chunk_index + 1

    # Resolve total size for progress bar
    total_size = expected_size or expected_stdin_size

    # Start progress tracking
    tracker.start(total_size, 0, "Uploading", chunk_size=chunk_size)
    tracker.set_workers(effective_workers)

    # Set up adaptive throttle or fixed pool
    throttle = AdaptiveThrottle(effective_workers, user_min, user_max, tracker=tracker) if adaptive else None
    pool_size = user_max if adaptive else effective_workers
    window: list[tuple[UploadJob, Future]] = []
    # Files to clean up after pool shutdown (deferred to avoid deleting files
    # while worker threads are still trying to upload them)
    _deferred_cleanup: list[Path] = []

    def _bp_hook() -> None:
        """Backpressure hook that drains completed uploads to free disk space."""
        while not bp_monitor.can_write_chunk():
            if window and window[0][1].done():
                _drain_one(window, resume_log, manifest)
            elif window:
                # Block on oldest upload to free space
                _drain_one(window, resume_log, manifest)
            else:
                time.sleep(0.5)

    def _abort_window() -> None:
        """Cancel remaining futures and defer file cleanup."""
        for remaining_job, remaining_future in window:
            remaining_future.cancel()
            _deferred_cleanup.append(remaining_job.upload_path)
        window.clear()

    try:
        with ThreadPoolExecutor(max_workers=pool_size) as pool:
            read_start = time.monotonic()

            for chunk_info in chunk_stream(
                sys.stdin.buffer, chunk_size, scratch_dir, stream_hasher,
                pre_chunk_hook=_bp_hook,
            ):
                read_elapsed = time.monotonic() - read_start
                if throttle:
                    throttle.record_io_time(read_elapsed)

                real_index = chunks_uploaded
                chunk_info_index = real_index

                # Sequential: compute chain
                chain_hex = compute_chain(chunk_info.dual_hash, prev_chain)
                prev_chain = bytes.fromhex(chain_hex)

                s3_key = _chunk_s3_key(name, chunk_info_index)

                # Sequential: encrypt if needed
                upload_path = chunk_info.path
                if encrypt:
                    upload_path = _encrypt_chunk(
                        chunk_info, encryption_method, aes_key, recipient
                    )

                job = UploadJob(
                    index=chunk_info_index,
                    upload_path=upload_path,
                    s3_key=s3_key,
                    size=chunk_info.size,
                    dual_hash=chunk_info.dual_hash,
                    chain_hex=chain_hex,
                )

                # Track chunk as staged (on disk, ready for upload)
                tracker.chunk_staged(chunk_info_index, chunk_info.size)

                # Acquire throttle slot if adaptive
                if throttle:
                    throttle.acquire()

                def _do_upload(b, j, sc, t, upload_start):
                    """Wrapper that measures upload time and releases throttle."""
                    try:
                        result = _upload_one(b, j, sc)
                        elapsed = time.monotonic() - upload_start
                        result.elapsed = elapsed
                        if t:
                            t.record_transfer_time(elapsed, j.size)
                        return result
                    finally:
                        if t:
                            t.release()

                upload_start = time.monotonic()
                future = pool.submit(
                    _do_upload, backend, job, storage_class,
                    throttle, upload_start,
                )
                window.append((job, future))

                # Drain when window full
                max_window = throttle.current_workers if throttle else effective_workers
                while len(window) >= max_window:
                    try:
                        _drain_one(window, resume_log, manifest, tracker)
                    except Exception:
                        _abort_window()
                        raise

                chunks_uploaded += 1
                read_start = time.monotonic()

            # Drain remaining after stdin exhausted
            while window:
                try:
                    _drain_one(window, resume_log, manifest, tracker)
                except Exception:
                    _abort_window()
                    raise
    finally:
        # Clean up deferred files after pool threads have stopped
        for p in _deferred_cleanup:
            p.unlink(missing_ok=True)

    if chunks_uploaded == 0:
        tracker.finish("No data received on stdin.")
        return

    # Finalize manifest
    stream_hash = stream_hasher.finalize()
    manifest.final_chain = prev_chain.hex() if prev_chain else ""
    manifest.stream_sha256 = stream_hash.sha256
    manifest.stream_sha3_256 = stream_hash.sha3_256

    # Upload manifest (always STANDARD class so it's immediately accessible)
    manifest_key = Manifest.s3_key(name)
    manifest_bytes = manifest.to_json().encode()
    if encrypt_manifest:
        if encryption_method == "aes-256-gcm" and aes_key:
            from s3duct.encryption import aes_encrypt_manifest
            manifest_bytes = aes_encrypt_manifest(manifest_bytes, aes_key)
        elif encryption_method == "age" and recipient:
            from s3duct.encryption import age_encrypt_manifest
            manifest_bytes = age_encrypt_manifest(manifest_bytes, recipient)
    backend.upload_bytes(manifest_key, manifest_bytes, "STANDARD")
    tracker.log(f"Manifest uploaded to {manifest_key}")

    # Clean up orphaned chunks from previous stream (--clobber)
    if old_chunk_keys is not None:
        new_chunk_keys = {c.s3_key for c in manifest.chunks}
        orphans = old_chunk_keys - new_chunk_keys
        if orphans:
            tracker.log(f"Cleaning up {len(orphans)} orphaned chunk(s)...")
            for key in orphans:
                try:
                    backend.delete_object(key)
                except Exception:
                    tracker.log(f"  Warning: failed to delete orphan {key}")

    # Clean up local resume log
    resume_log.clear()

    # Sanity check: if stdin was a regular file, verify we read the expected size
    if expected_stdin_size is not None and manifest.total_bytes != expected_stdin_size:
        msg = (
            f"stdin is a regular file ({expected_stdin_size:,} bytes) "
            f"but {manifest.total_bytes:,} bytes were uploaded. "
            f"The file may have been modified during upload."
        )
        warnings.append(msg)
        tracker.log(f"WARNING: {msg}")

    # Sanity check: if --expected-size was provided, warn if stream was shorter
    if expected_size is not None and manifest.total_bytes < expected_size:
        shortfall = expected_size - manifest.total_bytes
        msg = (
            f"--expected-size was {expected_size:,} bytes "
            f"but stream ended at {manifest.total_bytes:,} bytes "
            f"(short by {shortfall:,} bytes). Stream may be truncated."
        )
        warnings.append(msg)
        tracker.log(f"WARNING: {msg}")

    chunks_resumed = max(0, resume_from + 1) if resume_from >= 0 else 0

    if summary == "json":
        click.echo(_json.dumps({
            "version": __version__,
            "status": "complete",
            "stream": name,
            "chunks_uploaded": chunks_uploaded,
            "chunks_resumed": chunks_resumed,
            "chunks_new": chunks_uploaded - chunks_resumed,
            "total_bytes": manifest.total_bytes,
            "chunk_size": chunk_size,
            "stream_sha256": manifest.stream_sha256,
            "stream_sha3_256": manifest.stream_sha3_256,
            "final_chain": manifest.final_chain,
            "encrypted": encrypt,
            "encryption_method": encryption_method,
            "manifest_key": Manifest.s3_key(name),
            "upload_workers": (
                f"auto ({throttle.current_workers})" if throttle
                else str(effective_workers)
            ),
            "warnings": warnings,
        }), err=True)
    elif summary == "text":
        worker_info = (
            f"auto ({throttle.current_workers})" if throttle
            else str(effective_workers)
        )
        tracker.finish(
            f"Done. {chunks_uploaded} chunks, {manifest.total_bytes:,} bytes total. "
            f"(workers: {worker_info})",
        )
    else:
        tracker.finish()
