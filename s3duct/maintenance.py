"""Bucket maintenance: orphaned-chunk garbage collection and stream retention."""

from datetime import datetime, timedelta, timezone

import click

from s3duct.backends.base import ObjectInfo, StorageBackend
from s3duct.config import MANIFEST_FILENAME
from s3duct.downloader import _decrypt_manifest, run_delete
from s3duct.progress import PlainProgress, ProgressTracker

_MANIFEST_SUFFIX = f"/{MANIFEST_FILENAME}"


def _group_objects(objects: list[ObjectInfo]) -> tuple[dict[str, ObjectInfo],
                                                       dict[str, list[ObjectInfo]]]:
    """Group listed objects into manifests and chunks, keyed by stream name.

    Objects that are neither a manifest nor a chunk key are ignored — gc
    never touches keys it didn't create.
    """
    manifests: dict[str, ObjectInfo] = {}
    chunks: dict[str, list[ObjectInfo]] = {}
    for obj in objects:
        if obj.key.endswith(_MANIFEST_SUFFIX):
            manifests[obj.key[: -len(_MANIFEST_SUFFIX)]] = obj
        elif "/chunk-" in obj.key:
            stream = obj.key.rsplit("/chunk-", 1)[0]
            chunks.setdefault(stream, []).append(obj)
    return manifests, chunks


def run_gc(
    backend: StorageBackend,
    older_than_days: float = 7,
    dry_run: bool = False,
    aes_key: bytes | None = None,
    age_identity: str | None = None,
    tracker: ProgressTracker | None = None,
) -> list[str]:
    """Delete chunks not referenced by any manifest.

    Two kinds of orphans:
      - chunks in a stream whose manifest doesn't reference them
        (e.g. leftovers from a --clobber re-upload with fewer chunks)
      - chunks in a stream with no manifest at all (interrupted upload
        that was never resumed, or a manually deleted manifest)

    Only objects older than older_than_days are deleted, so an in-progress
    upload (which writes chunks before its manifest) is never collected.
    Streams whose manifest cannot be decrypted are skipped entirely.

    Returns the list of deleted (or, in dry-run, would-be-deleted) keys.
    """
    if tracker is None:
        tracker = PlainProgress()

    backend.preflight_check()

    tracker.log("Listing objects...")
    objects = backend.list_objects("")
    manifests, chunks_by_stream = _group_objects(objects)

    cutoff = datetime.now(timezone.utc) - timedelta(days=older_than_days)
    orphans: list[ObjectInfo] = []
    skipped_recent = 0
    skipped_streams: list[str] = []

    for stream, stream_chunks in sorted(chunks_by_stream.items()):
        if stream in manifests:
            raw = backend.download_bytes(manifests[stream].key)
            try:
                manifest = _decrypt_manifest(raw, aes_key=aes_key,
                                             age_identity=age_identity)
            except click.ClickException:
                tracker.log(
                    f"  Skipping {stream!r}: manifest could not be read "
                    "(provide --key/--age-identity to gc its orphans)."
                )
                skipped_streams.append(stream)
                continue
            referenced = {c.s3_key for c in manifest.chunks}
            candidates = [o for o in stream_chunks if o.key not in referenced]
        else:
            tracker.log(
                f"  Stream {stream!r} has {len(stream_chunks)} chunk(s) "
                "but no manifest (interrupted upload?)."
            )
            candidates = list(stream_chunks)

        for obj in candidates:
            if obj.last_modified is not None and obj.last_modified > cutoff:
                skipped_recent += 1
                continue
            orphans.append(obj)

    if skipped_recent:
        tracker.log(
            f"  Skipped {skipped_recent} orphan(s) newer than "
            f"{older_than_days} day(s) (may belong to an in-progress upload)."
        )

    if not orphans:
        tracker.finish("No orphaned chunks to delete.")
        return []

    total_bytes = sum(o.size for o in orphans)
    if dry_run:
        tracker.log(f"Would delete {len(orphans)} orphaned chunk(s) "
                    f"({total_bytes:,} bytes):")
        for obj in orphans:
            tracker.log(f"  {obj.key}")
        tracker.finish("Dry run: nothing deleted.")
        return [o.key for o in orphans]

    deleted: list[str] = []
    tracker.start(None, len(orphans), "Deleting orphans")
    for i, obj in enumerate(orphans):
        try:
            backend.delete_object(obj.key)
            deleted.append(obj.key)
            tracker.update_chunk(i, 0)
        except Exception as e:
            tracker.log(f"  Failed to delete {obj.key}: {e}")
    tracker.finish(
        f"Deleted {len(deleted)}/{len(orphans)} orphaned chunk(s) "
        f"({total_bytes:,} bytes)."
    )
    return deleted


def run_prune(
    backend: StorageBackend,
    keep: int,
    stream_prefix: str = "",
    dry_run: bool = False,
    aes_key: bytes | None = None,
    age_identity: str | None = None,
    tracker: ProgressTracker | None = None,
) -> list[str]:
    """Keep the newest `keep` streams matching stream_prefix; delete the rest.

    Streams are ordered by the manifest's created timestamp. Streams whose
    manifest cannot be read (encrypted without a key) are skipped, never
    deleted — their age can't be determined.

    Returns the list of deleted (or would-be-deleted) stream names.
    """
    if tracker is None:
        tracker = PlainProgress()
    if keep < 1:
        raise click.ClickException("--keep must be >= 1")

    backend.preflight_check()

    tracker.log("Listing streams...")
    objects = backend.list_objects(stream_prefix)
    manifests = [o for o in objects if o.key.endswith(_MANIFEST_SUFFIX)]

    dated: list[tuple[str, str]] = []  # (created, stream)
    for obj in manifests:
        stream = obj.key[: -len(_MANIFEST_SUFFIX)]
        raw = backend.download_bytes(obj.key)
        try:
            manifest = _decrypt_manifest(raw, aes_key=aes_key,
                                         age_identity=age_identity)
        except click.ClickException:
            tracker.log(f"  Skipping {stream!r}: manifest could not be read.")
            continue
        dated.append((manifest.created, stream))

    if len(dated) <= keep:
        tracker.finish(
            f"{len(dated)} stream(s) found, keep={keep}: nothing to prune."
        )
        return []

    # ISO-8601 timestamps sort lexicographically; newest last
    dated.sort()
    to_delete = [stream for _, stream in dated[:-keep]]
    kept = [stream for _, stream in dated[-keep:]]
    tracker.log(f"Keeping {len(kept)} newest: {', '.join(kept)}")

    if dry_run:
        tracker.log(f"Would delete {len(to_delete)} stream(s):")
        for stream in to_delete:
            tracker.log(f"  {stream}")
        tracker.finish("Dry run: nothing deleted.")
        return to_delete

    for stream in to_delete:
        tracker.log(f"Deleting stream {stream!r}...")
        run_delete(backend, stream, aes_key=aes_key,
                   age_identity=age_identity, tracker=tracker)
    tracker.finish(f"Pruned {len(to_delete)} stream(s), kept {len(kept)}.")
    return to_delete
