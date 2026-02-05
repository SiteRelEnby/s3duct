"""Glacier/Deep Archive thaw management."""

import time

import click
from botocore.exceptions import ClientError

from s3duct.backends.base import StorageBackend
from s3duct.downloader import _decrypt_manifest
from s3duct.manifest import Manifest
from s3duct.progress import ProgressTracker, PlainProgress

# Storage classes that require restore before download
_GLACIER_CLASSES = frozenset({"GLACIER", "DEEP_ARCHIVE", "GLACIER_IR"})


def run_restore(
    backend: StorageBackend,
    name: str,
    days: int = 7,
    tier: str = "Standard",
    wait: bool = False,
    poll_interval: int = 60,
    aes_key: bytes | None = None,
    age_identity: str | None = None,
    tracker: ProgressTracker | None = None,
) -> None:
    """Initiate Glacier restore for all chunks in a stream."""
    if tracker is None:
        tracker = PlainProgress()

    backend.preflight_check()

    # Download and parse manifest
    manifest_key = Manifest.s3_key(name)
    tracker.log("Downloading manifest...")
    raw = backend.download_bytes(manifest_key)
    manifest = _decrypt_manifest(raw, aes_key=aes_key, age_identity=age_identity)

    total = manifest.chunk_count
    sc = manifest.storage_class or "unknown"
    tracker.log(f"Stream has {total} chunks (storage class: {sc}).")

    if sc not in _GLACIER_CLASSES:
        tracker.log(
            f"Storage class {sc!r} does not require restore. "
            "Chunks should be immediately downloadable.",
        )
        return

    # Check status and initiate restore for each chunk
    already_available = 0
    initiated = 0
    in_progress = 0

    tracker.start(None, total, "Restoring")

    for chunk_rec in manifest.chunks:
        info = backend.head_object(chunk_rec.s3_key)

        # Already in a non-Glacier class (e.g. lifecycle transitioned back)
        if info.storage_class and info.storage_class not in _GLACIER_CLASSES:
            already_available += 1
            tracker.update_chunk(chunk_rec.index, 0)
            continue

        # Already restored
        if info.restore_status and 'ongoing-request="false"' in info.restore_status:
            already_available += 1
            tracker.update_chunk(chunk_rec.index, 0)
            continue

        # Restore in progress
        if info.restore_status and 'ongoing-request="true"' in info.restore_status:
            in_progress += 1
            tracker.update_chunk(chunk_rec.index, 0)
            continue

        # Initiate restore
        try:
            backend.initiate_restore(chunk_rec.s3_key, days, tier)
            initiated += 1
            tracker.update_chunk(chunk_rec.index, 0)
        except ClientError as e:
            code = e.response["Error"].get("Code", "")
            if code == "RestoreAlreadyInProgress":
                in_progress += 1
                tracker.update_chunk(chunk_rec.index, 0)
            else:
                raise

    pending = initiated + in_progress
    tracker.finish(
        f"Restore summary: {initiated} initiated, {in_progress} already in progress, "
        f"{already_available} already available ({total} total).",
    )

    if pending == 0:
        tracker.log("All chunks are available. You can run 's3duct get' now.")
        return

    if not wait:
        tracker.log(
            "Run with --wait to block until all chunks are restored, "
            "or re-run later to check status.",
        )
        return

    # Poll until all chunks are restored
    tracker.log(f"Waiting for restore to complete (polling every {poll_interval}s)...")
    while True:
        time.sleep(poll_interval)
        restored = sum(
            1 for c in manifest.chunks
            if backend.is_restore_complete(c.s3_key)
        )
        tracker.log(f"  {restored}/{total} chunks restored")
        if restored >= total:
            break

    tracker.log("All chunks restored. You can now run 's3duct get' to download.")
