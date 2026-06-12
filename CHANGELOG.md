# Changelog

All notable changes to s3duct are documented here.

## [0.4.1] - 2026-06-12

- Version bump because we made a mistake with the 0.4.0 build, pushed
some refs to wrong remote.

## [0.4.0] - 2026-06-12

### Added
- `verify --deep`: download every chunk and verify content hashes —
  ciphertext SHA-256 without a key, plaintext dual-hash plus the full
  integrity chain with one (default verify remains the fast ETag check)
- `gc` command: delete orphaned chunks (interrupted uploads, `--clobber`
  leftovers), age-gated by `--older-than` to protect in-progress uploads
- `prune` command: backup rotation — keep the newest N streams matching
  `--stream-prefix`, delete the rest
- Encrypted uploads record the ciphertext SHA-256 per chunk;
  `get --no-decrypt` (raw mode) is now fully integrity-verified without
  the decryption key
- SHA-256 upload checksums, verified server-side by S3 at upload time
  (default on for AWS, off for custom endpoints; `--upload-checksums`
  to override)
- Intra-chunk transfer progress: rich progress modes now advance during
  a chunk's upload/download instead of once per completed chunk
- Chunk encryption runs in upload workers (parallel) instead of the main
  read loop
- ruff + mypy in CI

### Fixed
- Adaptive throttle scale-down no longer drifts from the real semaphore
  permit count (`acquire(blocking=False)` returns `False`, it doesn't raise)
- AIMD multiplicative decrease is now actually wired up: S3 SlowDown/
  Throttling responses feed back into the adaptive worker count via a new
  `StorageBackend.on_throttle` hook (previously dead code)
- Throttle timing signals no longer include queue wait (upload), network
  wait (download drain), or backpressure wait (read)
- Chunks drained under disk backpressure now update the progress display
- `restore` checks the actual storage class of each chunk instead of the
  manifest's record, so lifecycle-transitioned streams can be thawed;
  `--wait` polls only pending chunks (mixed streams no longer hang)
- `--retries 0` could make downloads silently no-op; retries must now be >= 1
- Invalid `--chunk-size`/`--diskspace-limit`/etc. values report a clean CLI
  error instead of a traceback; two-letter suffixes (`MB`, `GB`) accepted
- Stream names may no longer contain `..` segments
- LocalBackend gained the missing `preflight_check` (it previously could
  not be instantiated)

### Changed
- Each run stages chunks in its own scratch subdirectory: concurrent
  uploads/downloads no longer collide, and stale files from crashed runs
  can't wedge backpressure
- Resume logs record the destination (bucket/prefix/endpoint) and chunk
  size; resuming with different parameters fails with a clear error
- Missing streams report "Stream not found" instead of a boto traceback
  (backends now raise `FileNotFoundError` for missing objects)
- AES-256-GCM file encryption/decryption streams in 8 MB buffers instead
  of loading whole chunks into memory (on-disk format unchanged)
- Encrypted uploads record each chunk's stored size in the manifest;
  `get --no-decrypt` verifies it (raw mode previously had no integrity
  checking at all) and the sizes make raw output re-splittable
- Manifests from a newer format version are rejected with an upgrade
  message instead of being misparsed

## [0.3.1] - 2026-01-31

- Add `--version` flag to CLI
- Add program version to JSON summary output (`put`, `get`, `verify`)
- Add PyPI, Python version, and license badges to README
- Add automated release workflow (tag-triggered CI/CD to PyPI)
- Update README for v0.3.1 and v0.3.0 features (oops)

## [0.3.0] - 2026-01-31

- Parallel upload pipeline with sliding-window ThreadPoolExecutor
- Adaptive worker scaling (`--upload-workers auto`, default) adjusts
  concurrency based on upload-vs-read throughput ratio
- `--upload-workers`, `--min-upload-workers`, `--max-upload-workers` options
- Manifest encryption on by default when encryption is active;
  `--no-encrypt-manifest` to opt out
- Age manifest encryption support (`age_encrypt_manifest` /
  `age_decrypt_manifest` via stdin/stdout piping)
- `--age-identity` on `verify` command for age-encrypted manifests
- Downloader `_decrypt_manifest()` helper tries JSON, then AES, then age
- Strict resume log: `verify_chain()` rejects gaps and out-of-order entries
- Dynamic backpressure safety margin scales with chunk size
- Backpressure drain hook prevents deadlock with parallel window
- End-to-end age encryption tests
- 173 tests

## [0.2.1] - 2026-01-31

- Warn on unencrypted upload (10s pause on TTY, silent with `--no-encrypt`)
- Fix JSON summary: report `chain_verified=false` and `raw_mode=true` when
  downloading encrypted stream with `--no-decrypt`
- Add `--key` to `verify` command for encrypted manifest support
- `verify` now gives helpful error on encrypted manifest instead of crashing
- 150 tests

## [0.2.0] - 2026-01-31

- Bump retry defaults to absorb multi-minute outages (10 retries, 120s max
  delay, ~8 min total window)
- Fix `--no-decrypt` bug: skip integrity/chain checks in raw download mode
  (manifest hashes are for plaintext, not ciphertext)
- Add stream name validation (reject empty, leading `/.`, double `//`)
- Improve age CLI error messages (catch `FileNotFoundError`, suggest install
  URL)
- Remove dead `encrypt_file` / `decrypt_file` aliases
- Add pyproject.toml metadata: author, classifiers, keywords, URLs
- 145 tests

## [0.1.0] - 2026-01-31

Initial release.

- Chunked streaming upload/download via stdin/stdout
- AES-256-GCM symmetric encryption and age asymmetric encryption
- Dual-hash integrity (SHA-256 + SHA3-256) with HMAC signature chain
- Resumable uploads with chain-verified fast-forward
- Disk-aware backpressure with adaptive buffering
- Structured summary output (`--summary text/json/none`)
- `--strict-resume` for truncated input detection
- Regular file stdin size sanity check
- S3-compatible (AWS, R2, MinIO, B2, Wasabi)
- 139 tests
- Licensed under Elastic License 2.0
