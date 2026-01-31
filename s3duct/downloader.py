"""Download pipeline: S3 -> decrypt -> verify -> stdout."""

import json
import sys
from pathlib import Path

import click

from s3duct import __version__
from s3duct.backends.base import StorageBackend
from s3duct.config import SCRATCH_DIR
from s3duct.encryption import aes_decrypt_file, age_decrypt_file
from s3duct.integrity import hash_file, compute_chain, DualHash
from s3duct.manifest import Manifest


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


def run_get(
    backend: StorageBackend,
    name: str,
    decrypt: bool = False,
    encryption_method: str | None = None,
    aes_key: bytes | None = None,
    age_identity: str | None = None,
    scratch_dir: Path | None = None,
    summary: str = "text",  # "text", "json", or "none"
) -> None:
    """Execute the full get pipeline."""
    if scratch_dir is None:
        scratch_dir = SCRATCH_DIR
    scratch_dir.mkdir(parents=True, exist_ok=True)

    # Download manifest
    manifest_key = Manifest.s3_key(name)
    click.echo(f"Downloading manifest...", err=True)
    raw = backend.download_bytes(manifest_key)

    # Decrypt manifest if needed (validates key/identity early)
    manifest = _decrypt_manifest(raw, aes_key=aes_key, age_identity=age_identity)

    if manifest.encrypted and not decrypt:
        method = manifest.encryption_method or "age"
        if method == "aes-256-gcm":
            click.echo(
                "Warning: stream was encrypted with AES-256-GCM. Use --key to decrypt.",
                err=True,
            )
        else:
            click.echo(
                "Warning: stream was encrypted with age. Use --age-identity to decrypt.",
                err=True,
            )

    # Auto-detect encryption method from manifest
    if decrypt and manifest.encrypted:
        method = encryption_method or manifest.encryption_method or "age"
        if method == "aes-256-gcm" and not aes_key:
            raise click.ClickException("--key required to decrypt AES-256-GCM encrypted stream")
        if method == "age" and not age_identity:
            raise click.ClickException("--age-identity required to decrypt age encrypted stream")

    click.echo(
        f"Restoring {manifest.chunk_count} chunks, {manifest.total_bytes:,} bytes...",
        err=True,
    )

    prev_chain: bytes | None = None

    for chunk_rec in manifest.chunks:
        chunk_path = scratch_dir / f"chunk-{chunk_rec.index:06d}"

        # Download
        click.echo(f"  Downloading chunk {chunk_rec.index}...", err=True)
        backend.download(chunk_rec.s3_key, chunk_path)

        # Decrypt if needed
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

        # Verify integrity (against plaintext hashes) -- skip if raw mode
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

            # Verify chain
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

    # Verify final chain (skip in raw/no-decrypt mode)
    raw_mode = manifest.encrypted and not decrypt
    if not raw_mode and prev_chain and manifest.final_chain:
        if prev_chain.hex() != manifest.final_chain:
            raise click.ClickException("Final chain mismatch. Stream may be incomplete or tampered.")

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
    elif summary == "text":
        click.echo("Restore complete.", err=True)


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
            ver = f"  v{m.tool_version}" if m.tool_version else ""
            click.echo(
                f"{stream_name}  "
                f"{m.chunk_count} chunks  "
                f"{m.total_bytes:,} bytes  "
                f"{m.created}"
                f"{encrypted}"
                f"{ver}"
            )
        except (json.JSONDecodeError, UnicodeDecodeError):
            click.echo(f"{stream_name}  (manifest encrypted)")
        except Exception:
            click.echo(f"{stream_name}  (manifest unreadable)")


def run_verify(
    backend: StorageBackend,
    name: str,
    aes_key: bytes | None = None,
    age_identity: str | None = None,
    summary: str = "text",  # "text", "json", or "none"
) -> None:
    """Verify integrity of a stored stream without downloading chunk data."""
    manifest_key = Manifest.s3_key(name)
    raw = backend.download_bytes(manifest_key)

    manifest = _decrypt_manifest(raw, aes_key=aes_key, age_identity=age_identity)

    click.echo(f"Verifying {manifest.chunk_count} chunks...", err=True)
    errors = 0
    mismatches = []
    missing = []

    for chunk_rec in manifest.chunks:
        try:
            info = backend.head_object(chunk_rec.s3_key)
            if info.etag != chunk_rec.etag:
                click.echo(f"  MISMATCH chunk {chunk_rec.index}: ETag differs", err=True)
                mismatches.append(chunk_rec.index)
                errors += 1
            else:
                click.echo(f"  OK chunk {chunk_rec.index}", err=True)
        except Exception as e:
            click.echo(f"  MISSING chunk {chunk_rec.index}: {e}", err=True)
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
    elif summary == "text":
        if errors:
            click.echo(f"Verification failed: {errors} error(s).", err=True)
        else:
            click.echo("All chunks verified.", err=True)

    if errors:
        raise SystemExit(1)
