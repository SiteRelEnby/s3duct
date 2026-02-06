## Future Enhancements

### Realtime intra-chunk progress
Show bytes uploaded in realtime within each chunk, not just when chunks complete.

Implementation approach:
- Use boto3's `Callback=` parameter on `upload_file`/`upload_fileobj`
- Each worker thread reports partial progress to a shared counter
- Use `threading.Lock` or `queue.Queue` for thread-safe aggregation
- Update progress bar more frequently (but throttle to avoid terminal spam)

Complexity: Medium-high (concurrency + callback plumbing)

### Non-S3 backends (GCS, Azure Blob, etc.)
Add native support for cloud storage providers beyond S3-compatible APIs.

Potential backends:
- Google Cloud Storage (GCS)
- Azure Blob Storage
- Backblaze B2 (native API, not S3-compat)
- Local filesystem (for testing/archiving)

Implementation approach:
- Abstract storage backend already exists (`StorageBackend` base class)
- Add new backend classes implementing the same interface
- CLI would accept `--backend gcs` / `--backend azure` etc.
- Each backend has its own credential handling

Complexity: Medium (per backend)
