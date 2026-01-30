"""Upload pipeline: chunker -> integrity -> encryption -> S3 -> resume log."""

import json as _json
import os
import stat
import sys
from datetime import datetime, timezone
from pathlib import Path

import click

from s3duct.backends.base import StorageBackend
from s3duct.backpressure import BackpressureConfig, BackpressureMonitor
from s3duct.chunker import chunk_stream, fast_forward_stream
from s3duct.config import DEFAULT_CHUNK_SIZE, SCRATCH_DIR
from s3duct.encryption import (
    aes_encrypt_file,
    age_encrypt_file,
    get_recipient_from_identity,
)
from s3duct.integrity import StreamHasher, compute_chain
from s3duct.manifest import ChunkRecord, Manifest
from s3duct.resume import ResumeEntry, ResumeLog


def _chunk_s3_key(name: str, index: int) -> str:
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
    strict_resume: bool = True,
    summary: str = "text",  # "text", "json", or "none"
) -> None:
    """Execute the full put pipeline."""
    if scratch_dir is None:
        scratch_dir = SCRATCH_DIR
    scratch_dir.mkdir(parents=True, exist_ok=True)

    warnings: list[str] = []

    # Check expected stdin size for regular files
    expected_stdin_size = _stdin_expected_size()

    # Set up backpressure
    bp_config = BackpressureConfig(
        chunk_size=chunk_size,
        scratch_dir=scratch_dir,
        max_buffer_chunks=buffer_chunks,
        diskspace_limit=diskspace_limit,
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

    # Check for resume
    resume_from = -1
    if resume_log.last_chunk_index >= 0:
        if not resume_log.verify_chain():
            click.echo("Resume log chain verification failed. Starting fresh.", err=True)
            resume_log.clear()
        else:
            click.echo(
                f"Found resume log with {resume_log.last_chunk_index + 1} completed chunks. "
                "Verifying stream...",
                err=True,
            )
            # Fast-forward through already-uploaded chunks
            ff_count = resume_log.last_chunk_index + 1
            ff_verified = 0
            for idx, dual_hash, size in fast_forward_stream(
                sys.stdin.buffer, chunk_size, ff_count, stream_hasher
            ):
                if not resume_log.verify_entry(idx, dual_hash, size):
                    click.echo(
                        f"Stream mismatch at chunk {idx}. "
                        "This is a different stream. Starting fresh.",
                        err=True,
                    )
                    resume_log.clear()
                    # Can't rewind stdin - must abort
                    raise click.ClickException(
                        "Cannot resume: stream does not match resume log. "
                        "Please re-run with the original stream from the beginning."
                    )
                click.echo(f"  Verified chunk {idx}", err=True)
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
                click.echo(f"WARNING: {msg}", err=True)

            resume_from = resume_log.last_chunk_index
            prev_chain = resume_log.get_last_chain_bytes()
            click.echo(f"Resuming from chunk {resume_from + 1}", err=True)

    manifest = Manifest.new(
        name=name,
        chunk_size=chunk_size,
        encrypted=encrypt,
        encryption_method=encryption_method,
        encryption_recipient=recipient,
        storage_class=storage_class,
        tags=tags,
        encrypted_manifest=encrypt_manifest,
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

    for chunk_info in chunk_stream(
        sys.stdin.buffer, chunk_size, scratch_dir, stream_hasher,
        pre_chunk_hook=bp_monitor.wait_for_space,
    ):
        real_index = chunks_uploaded
        chunk_info_index = real_index

        # Compute chain
        chain_hex = compute_chain(chunk_info.dual_hash, prev_chain)
        prev_chain = bytes.fromhex(chain_hex)

        s3_key = _chunk_s3_key(name, chunk_info_index)

        # Encrypt if needed
        upload_path = chunk_info.path
        if encrypt:
            enc_path = chunk_info.path.with_suffix(".enc")
            if encryption_method == "aes-256-gcm":
                aes_encrypt_file(chunk_info.path, enc_path, aes_key)
            elif encryption_method == "age":
                enc_path = chunk_info.path.with_suffix(".age")
                age_encrypt_file(chunk_info.path, enc_path, recipient)
            chunk_info.path.unlink()
            upload_path = enc_path

        # Upload
        click.echo(
            f"Uploading chunk {chunk_info_index} ({chunk_info.size:,} bytes)...",
            err=True,
        )
        etag = backend.upload(s3_key, upload_path, storage_class)

        # Log
        entry = ResumeEntry(
            chunk=chunk_info_index,
            size=chunk_info.size,
            sha256=chunk_info.dual_hash.sha256,
            sha3_256=chunk_info.dual_hash.sha3_256,
            chain=chain_hex,
            s3_key=s3_key,
            etag=etag,
            ts=datetime.now(timezone.utc).isoformat(),
        )
        resume_log.append(entry)

        # Add to manifest
        manifest.add_chunk(ChunkRecord(
            index=chunk_info_index,
            s3_key=s3_key,
            size=chunk_info.size,
            sha256=chunk_info.dual_hash.sha256,
            sha3_256=chunk_info.dual_hash.sha3_256,
            etag=etag,
        ))

        # Cleanup
        upload_path.unlink(missing_ok=True)
        chunks_uploaded += 1

    if chunks_uploaded == 0:
        click.echo("No data received on stdin.", err=True)
        return

    # Finalize manifest
    stream_hash = stream_hasher.finalize()
    manifest.final_chain = prev_chain.hex() if prev_chain else ""
    manifest.stream_sha256 = stream_hash.sha256
    manifest.stream_sha3_256 = stream_hash.sha3_256

    # Upload manifest (always STANDARD class so it's immediately accessible)
    manifest_key = Manifest.s3_key(name)
    manifest_bytes = manifest.to_json().encode()
    if encrypt_manifest and encryption_method == "aes-256-gcm" and aes_key:
        from s3duct.encryption import aes_encrypt_manifest
        manifest_bytes = aes_encrypt_manifest(manifest_bytes, aes_key)
    backend.upload_bytes(manifest_key, manifest_bytes, "STANDARD")
    click.echo(f"Manifest uploaded to {manifest_key}", err=True)

    # Upload resume log as checkpoint
    if resume_log.path.exists():
        log_key = f"{name}/{resume_log.s3_key}"
        backend.upload(log_key, resume_log.path, "STANDARD")

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
        click.echo(f"WARNING: {msg}", err=True)

    chunks_resumed = max(0, resume_from + 1) if resume_from >= 0 else 0

    if summary == "json":
        click.echo(_json.dumps({
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
            "warnings": warnings,
        }), err=True)
    elif summary == "text":
        click.echo(
            f"Done. {chunks_uploaded} chunks, {manifest.total_bytes:,} bytes total.",
            err=True,
        )
