"""Glacier/Deep Archive thaw management."""

import time

from botocore.exceptions import ClientError

from s3duct.backends.base import StorageBackend
from s3duct.downloader import _decrypt_manifest, _fetch_manifest_bytes
from s3duct.manifest import Manifest
from s3duct.progress import PlainProgress, ProgressTracker

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
    raw = _fetch_manifest_bytes(backend, name, manifest_key)
    manifest = _decrypt_manifest(raw, aes_key=aes_key, age_identity=age_identity)

    total = manifest.chunk_count
    sc = manifest.storage_class or "unknown"
    tracker.log(f"Stream has {total} chunks (storage class: {sc}).")

    if sc not in _GLACIER_CLASSES:
        # The manifest records the class at upload time; chunks may have been
        # lifecycle-transitioned to Glacier since, so check the actual objects.
        tracker.log(
            f"Manifest storage class {sc!r} does not normally require restore; "
            "checking actual chunk status (lifecycle rules may have "
            "transitioned chunks).",
        )

    # Check status and initiate restore for each chunk
    already_available = 0
    initiated = 0
    in_progress = 0
    pending_keys: list[str] = []

    tracker.start(None, total, "Restoring")

    for chunk_rec in manifest.chunks:
        info = backend.head_object(chunk_rec.s3_key)

        # In a non-Glacier class (S3 reports no storage class for STANDARD)
        if info.storage_class not in _GLACIER_CLASSES:
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
            pending_keys.append(chunk_rec.s3_key)
            tracker.update_chunk(chunk_rec.index, 0)
            continue

        # Initiate restore
        try:
            backend.initiate_restore(chunk_rec.s3_key, days, tier)
            initiated += 1
            pending_keys.append(chunk_rec.s3_key)
            tracker.update_chunk(chunk_rec.index, 0)
        except ClientError as e:
            code = e.response["Error"].get("Code", "")
            if code == "RestoreAlreadyInProgress":
                in_progress += 1
                pending_keys.append(chunk_rec.s3_key)
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

    # Poll until all pending chunks are restored. Only the chunks that
    # actually needed a restore are polled — already-available ones never
    # report a completed restore and would make this loop spin forever.
    tracker.log(f"Waiting for restore to complete (polling every {poll_interval}s)...")
    while True:
        time.sleep(poll_interval)
        restored = sum(
            1 for key in pending_keys
            if backend.is_restore_complete(key)
        )
        tracker.log(f"  {restored}/{len(pending_keys)} pending chunks restored")
        if restored >= len(pending_keys):
            break

    tracker.log("All chunks restored. You can now run 's3duct get' to download.")
