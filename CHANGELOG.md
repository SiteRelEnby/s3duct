# Changelog

All notable changes to s3duct are documented here.

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
