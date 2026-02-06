# s3duct

[![PyPI](https://img.shields.io/pypi/v/s3duct)](https://pypi.org/project/s3duct/)
[![Python](https://img.shields.io/pypi/pyversions/s3duct)](https://pypi.org/project/s3duct/)
[![License](https://img.shields.io/pypi/l/s3duct)](LICENSE)

Chunked, resumable, encrypted pipe to S3-compatible object storage.

Stream data from stdin directly to S3 in fixed-size chunks with integrity
verification, optional encryption, cache management, and automatic resume on failure.

## Features

- **Chunked streaming** - pipes stdin to S3 without loading the full file into memory
- **Resumable uploads** - interrupted uploads pick up where they left off using a
  signature-chained resume log
- **Dual-hash integrity** - every chunk is verified with both SHA-256 and SHA3-256
- **HMAC signature chain** - cryptographic chain across all chunks prevents tampering
  or reordering
- **Encryption** - optional client-side encryption: AES-256-GCM (symmetric) or
  [age](https://age-encryption.org/) (asymmetric, with optional post-quantum keys)
- **Parallel uploads** - adaptive multi-threaded upload pipeline with automatic
  concurrency scaling
- **Backpressure** - disk-aware flow control prevents scratch directory from filling up
- **S3-compatible** - works with AWS S3, Cloudflare R2, MinIO, Backblaze B2, Wasabi,
  and any S3-compatible endpoint via `--endpoint-url`

## Installation

```bash
pip install s3duct
```

### Shell Completion

Enable tab completion for commands and options:

```bash
# Bash (~/.bashrc)
s3duct completion bash >> ~/.bashrc

# Zsh (~/.zshrc)
s3duct completion zsh >> ~/.zshrc

# Fish (~/.config/fish/completions/s3duct.fish)
s3duct completion fish > ~/.config/fish/completions/s3duct.fish
```

Then restart your shell or source the file.

### Development

```bash
git clone https://github.com/SiteRelEnby/s3duct
cd s3duct
python3 -m venv .venv
source .venv/bin/activate
pip install -e ".[dev]"

# run tests
pytest
```

## AWS Credentials

s3duct uses [boto3's standard credential chain](https://boto3.amazonaws.com/v1/documentation/api/latest/guide/credentials.html).
It does not handle authentication itself. Credentials are resolved in this
order:

1. **Environment variables** — `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`,
   `AWS_SESSION_TOKEN`
2. **Shared credentials file** — `~/.aws/credentials` (set up via `aws configure`)
3. **Config file** — `~/.aws/config` (profiles, SSO)
4. **IAM instance role** — automatic on EC2, ECS, Lambda
5. **SSO / credential process** — configured in `~/.aws/config`

For non-AWS S3-compatible services, set the appropriate credentials for that
provider and use `--endpoint-url`.

## Quick Start

### Upload

```bash
# No encryption
tar czf - /my/data | s3duct put --bucket mybucket --name mybackup

# AES-256-GCM encryption (recommended for most users)
tar czf - /my/data | s3duct put --bucket mybucket --name mybackup \
  --key hex:$(openssl rand -hex 32)

# age encryption (asymmetric — see Encryption section below)
tar czf - /my/data | s3duct put --bucket mybucket --name mybackup \
  --age-identity ~/.age/key.txt

# Custom chunk size, storage class, and tags
pg_dump mydb | s3duct put --bucket mybucket --name db-backup \
  --chunk-size 256M --storage-class STANDARD_IA --tag env=prod
```

### Download

```bash
# Unencrypted
s3duct get --bucket mybucket --name mybackup > restored.tar.gz

# AES-256-GCM
s3duct get --bucket mybucket --name mybackup --key hex:... | zpool import

# age
s3duct get --bucket mybucket --name mybackup \
  --age-identity ~/.age/key.txt | tar xzf -
```

### List stored streams

```bash
s3duct list --bucket mybucket
```

### Verify integrity

```bash
s3duct verify --bucket mybucket --name mybackup
```

## Encryption

s3duct supports two encryption methods. Both encrypt each chunk client-side
before upload. The manifest records which method was used, so `get`
auto-detects the decryption path.

The manifest is encrypted by default when encryption is active (both AES-256-GCM
and age are supported). Use `--no-encrypt-manifest` to keep the manifest as
readable JSON if needed.

If the manifest is unencrypted, it exposes metadata only (not plaintext): stream
name/prefix, chunk count, per-chunk sizes, hashes, ETags, timestamps, storage
class, tags, and the encryption method/recipient. With read access to the bucket,
many of these (object keys, sizes, storage class, timestamps, and ETags) are
already inferable from object listings and HEADs, but the manifest makes it
trivial to enumerate and correlate.

### AES-256-GCM (symmetric)

Simple, fast, quantum-resistant. You manage one 32-byte key.

```bash
# Generate a key (save this — you need it to decrypt)
openssl rand -hex 32
# e.g., a1b2c3d4...64 hex chars

# Upload
tar czf - /data | s3duct put --bucket b --name n --key hex:a1b2c3...

# Download
s3duct get --bucket b --name n --key hex:a1b2c3... > data.tar.gz
```

Key formats:
- `hex:AABBCC...` — 64 hex characters (32 bytes)
- `file:/path/to/keyfile` — raw 32-byte file
- `env:VAR_NAME` — environment variable containing hex key

The manifest is encrypted by default with AES. To keep it as readable JSON:
```bash
s3duct put --bucket b --name n --key hex:... --no-encrypt-manifest
```

Each chunk is encrypted with a random 96-bit nonce. A nonce collision
under the same key (including cross-session key reuse) becomes probable
around 2^48 chunks (the [birthday bound](https://en.wikipedia.org/wiki/Birthday_bound)), which at the default 512MB chunk
size is ~128 exabytes. Use a different key per stream to reset the bound.
If you anticipate your use case exceeding the birthday bound within a
single stream and reasonable block size, [age](#age-asymmetric) does not suffer from birthday
bound issues. If symmetric is a hard requirement, please [open an issue](https://github.com/SiteRelEnby/s3duct/issues) and
make a donation to [Trans Lifeline](https://translifeline.org/).
Plus we'd just be interested to hear about how well it hyperscales.

### age (asymmetric)

Uses [age](https://age-encryption.org/) for public-key encryption.
Useful when the encryptor shouldn't hold the decryption key (e.g.,
automated backups encrypting to an offline recovery key).

age uses X25519 (classic) or ML-KEM-768+X25519 (PQ hybrid) to wrap a per-file
key, with HKDF-SHA256 and ChaCha20-Poly1305. Payload encryption also uses
HKDF-SHA256 and ChaCha20-Poly1305, with an HMAC-SHA256 header MAC.

```bash
# Install age
# macOS: brew install age
# Linux: xbps-install age / apt install age / apk add age / pacman -S age / etc. - see https://age-encryption.org/

# Generate a keypair
age-keygen -pq -o ~/.age/key.txt
# Prints the public key (recipient): age1...

# Upload (uses the keypair — extracts public key automatically)
tar czf - /data | s3duct put --bucket b --name n --age-identity ~/.age/key.txt

# Download (needs the private key)
s3duct get --bucket b --name n --age-identity ~/.age/key.txt > data.tar.gz
```

#### Post-quantum keys

age supports hybrid post-quantum keys (X25519 + ML-KEM-768) via the
`-pq` flag. These protect against both classical and future quantum
attacks. This is recommended if long-term confidentiality matters
(e.g., encrypted archives that may sit in storage for years), and is
likely to become the default in a future release (note that -pq was
already specified in the previous section - the only downside to
using post-quantum keys is an overhead of ~2KB/block).

### Comparison

| | AES-256-GCM | age |
|---|---|---|
| Quantum resistant | Yes | Yes, with `-pq` |
| Key model | Symmetric | Asymmetric |
| External dependency | None | age CLI |
| Best for | Simple backups | Multi-party / offline keys |

### Storage Classes

```bash
# Standard (default)
tar czf - /data | s3duct put --bucket mybucket --name backup --no-encrypt

# Infrequent Access
tar czf - /data | s3duct put --bucket mybucket --name backup --no-encrypt \
  --storage-class STANDARD_IA

# Glacier (retrieval requires thaw — see below)
tar czf - /data | s3duct put --bucket mybucket --name archive --no-encrypt \
  --storage-class GLACIER

# Glacier Deep Archive (cheapest, slowest retrieval)
tar czf - /data | s3duct put --bucket mybucket --name deep-archive --no-encrypt \
  --storage-class DEEP_ARCHIVE
```

Supported values: `STANDARD`, `REDUCED_REDUNDANCY`, `STANDARD_IA`, `ONEZONE_IA`,
`INTELLIGENT_TIERING`, `GLACIER`, `DEEP_ARCHIVE`, `GLACIER_IR`.

Note: the manifest is always uploaded as `STANDARD` so it remains immediately
accessible regardless of the chunk storage class.

**Glacier/Deep Archive retrieval:** chunks stored in Glacier or Deep Archive
cannot be downloaded directly. Use `s3duct restore` to initiate the thaw:

```bash
# Start restore (Standard tier, 7 days)
s3duct restore --bucket mybucket --name archive

# Wait for completion (polls every 60s)
s3duct restore --bucket mybucket --name archive --wait

# Faster restore (higher cost)
s3duct restore --bucket mybucket --name archive --tier Expedited

# Then download once restored
s3duct get --bucket mybucket --name archive > data.tar.gz
```

### S3-compatible endpoints

```bash
# Cloudflare R2
s3duct put --bucket mybucket --name backup --no-encrypt \
  --endpoint-url https://<account-id>.r2.cloudflarestorage.com

# MinIO
s3duct put --bucket mybucket --name backup --no-encrypt \
  --endpoint-url http://localhost:9000
```

## Options

### `s3duct put`

| Option | Default | Description |
|--------|---------|-------------|
| `--bucket` | (required) | S3 bucket name |
| `--name` | (required) | Stream name (used as S3 key prefix) |
| `--chunk-size` | `512M` | Chunk size (e.g., `256M`, `1G`) |
| `--key` | | AES-256-GCM key (`hex:...`, `file:...`, or `env:...`) |
| `--age-identity` | | Path to age identity file (mutually exclusive with `--key`) |
| `--no-encrypt` | | Disable encryption even if key/identity provided |
| `--encrypt-manifest/--no-encrypt-manifest` | on when encrypted | Encrypt manifest with same key/identity as chunks |
| `--clobber` | | Overwrite existing stream (fails by default if stream exists) |
| `--tag` | | Custom metadata tag (`key=value`, repeatable) |
| `--storage-class` | `STANDARD` | S3 storage class |
| `--region` | | AWS region |
| `--prefix` | | S3 key prefix |
| `--endpoint-url` | | Custom S3 endpoint URL |
| `--expected-size` | | Expected stream size (warns if actual is shorter) |
| `--diskspace-limit` | auto | Max scratch disk usage (e.g., `2G`) |
| `--buffer-chunks` | auto | Max buffered chunks before backpressure |
| `--strict-resume/--no-strict-resume` | on | Fail if stdin ends before all resume-log chunks are re-verified |
| `--retries` | `10` | Max retry attempts per S3 operation |
| `--upload-workers` | `auto` | Parallel upload threads (`auto` adapts based on throughput, or an integer for fixed concurrency) |
| `--min-upload-workers` | `2` | Minimum workers for auto mode |
| `--max-upload-workers` | `16` | Maximum workers for auto mode |
| `--progress` | `auto` | Progress display: `auto`, `rich`, `plain`, or `none` |
| `--verbose` | | Show detailed progress events and timing |
| `--summary` | `text` | Summary output format: `text`, `json`, or `none` |

### `s3duct get`

| Option | Default | Description |
|--------|---------|-------------|
| `--bucket` | (required) | S3 bucket name |
| `--name` | (required) | Stream name to restore |
| `--key` | | AES-256-GCM key (`hex:...`, `file:...`, or `env:...`) |
| `--age-identity` | | Path to age identity file (mutually exclusive with `--key`) |
| `--no-decrypt` | | Skip decryption (download raw encrypted chunks) |
| `--region` | | AWS region |
| `--prefix` | | S3 key prefix |
| `--endpoint-url` | | Custom S3 endpoint URL |
| `--retries` | `10` | Max retry attempts per S3 operation |
| `--download-workers` | `1` | Parallel download threads (`auto` or integer) |
| `--min-download-workers` | `2` | Minimum workers for auto mode |
| `--max-download-workers` | `16` | Maximum workers for auto mode |
| `--progress` | `auto` | Progress display: `auto`, `rich`, `plain`, or `none` |
| `--verbose` | | Show detailed progress events and timing |
| `--summary` | `text` | Summary output format: `text`, `json`, or `none` |

### `s3duct list`

| Option | Default | Description |
|--------|---------|-------------|
| `--bucket` | (required) | S3 bucket name |
| `--prefix` | | Filter by prefix |
| `--region` | | AWS region |
| `--endpoint-url` | | Custom S3 endpoint URL |

### `s3duct verify`

| Option | Default | Description |
|--------|---------|-------------|
| `--bucket` | (required) | S3 bucket name |
| `--name` | (required) | Stream name to verify |
| `--key` | | AES-256-GCM key (required if manifest is encrypted with AES) |
| `--age-identity` | | Path to age identity file (required if manifest is encrypted with age) |
| `--region` | | AWS region |
| `--prefix` | | S3 key prefix |
| `--endpoint-url` | | Custom S3 endpoint URL |
| `--retries` | `10` | Max retry attempts per S3 operation |
| `--progress` | `auto` | Progress display: `auto`, `rich`, `plain`, or `none` |
| `--verbose` | | Show detailed progress events and timing |
| `--summary` | `text` | Summary output format: `text`, `json`, or `none` |

### `s3duct delete`

| Option | Default | Description |
|--------|---------|-------------|
| `--bucket` | (required) | S3 bucket name |
| `--name` | (required) | Stream name to delete |
| `--key` | | AES-256-GCM key (required if manifest is encrypted with AES) |
| `--age-identity` | | Path to age identity file (required if manifest is encrypted with age) |
| `--dry-run` | | Show what would be deleted without deleting |
| `--force` | | Skip confirmation prompt |
| `--region` | | AWS region |
| `--prefix` | | S3 key prefix |
| `--endpoint-url` | | Custom S3 endpoint URL |
| `--progress` | `auto` | Progress display: `auto`, `rich`, `plain`, or `none` |
| `--verbose` | | Show detailed progress events and timing |

### `s3duct restore`

Initiate Glacier/Deep Archive restore for archived streams.

| Option | Default | Description |
|--------|---------|-------------|
| `--bucket` | (required) | S3 bucket name |
| `--name` | (required) | Stream name to restore |
| `--key` | | AES-256-GCM key (required if manifest is encrypted with AES) |
| `--age-identity` | | Path to age identity file (required if manifest is encrypted with age) |
| `--tier` | `Standard` | Restore tier: `Expedited`, `Standard`, or `Bulk` |
| `--days` | `7` | Days to keep restored copies available |
| `--wait` | | Wait for restore to complete (polls every 60s) |
| `--region` | | AWS region |
| `--prefix` | | S3 key prefix |
| `--endpoint-url` | | Custom S3 endpoint URL |
| `--progress` | `auto` | Progress display: `auto`, `rich`, `plain`, or `none` |
| `--verbose` | | Show detailed progress events and timing |

## How It Works

### Upload Pipeline

```
Main thread                         Thread pool (auto-scaled)
──────────                          ────────────────────────
stdin → chunk (512MB default)
     → SHA-256 + SHA3-256 dual hash
     → HMAC-SHA256 signature chain
     → optional encryption ──────→  S3 upload with retry (parallel)
     → when window full: drain  ←─  result + ETag
       oldest upload
     → append to resume log
     → repeat until EOF
     → drain remaining uploads
     → upload manifest (encrypted by default)
```

Uploads run in parallel using a sliding window. The main thread reads,
hashes, and encrypts sequentially (these depend on ordering), then
submits uploads to a thread pool. Completed uploads are drained
oldest-first to maintain strict resume log ordering. Worker count
auto-scales based on upload-vs-read throughput ratio (override with
`--upload-workers N`).

### Integrity Chain

Each chunk's signature is computed as:

```
chain[0] = HMAC-SHA256(genesis_key, sha256(chunk) || sha3_256(chunk))
chain[n] = HMAC-SHA256(chain[n-1], sha256(chunk) || sha3_256(chunk))
```

This creates a cryptographic chain where any modification, deletion, or
reordering of chunks is detectable.

### Resume

If an upload is interrupted, the resume log (stored locally and in S3)
records which chunks were successfully uploaded along with their chain
signatures. On restart, s3duct fast-forwards through stdin, verifying
each chunk's hash matches the log, then continues uploading from where
it left off.

**Edge case — truncated resume input:** if stdin ends before all
previously-uploaded chunks are re-verified (e.g., the source process
crashed or you piped a shorter stream), s3duct will by default return
an error. `--no-strict-resume` is available to disable this safety check.

### Shell pipefail

s3duct reads stdin until EOF. It cannot detect whether the upstream
process exited successfully or crashed — both produce the same EOF.
Use `set -o pipefail` in bash (or `setopt PIPE_FAIL` in zsh) so your
shell reports upstream failures:

```bash
set -o pipefail
pg_dump mydb | s3duct put --bucket b --name db-backup || echo "upload or dump failed"
```

Without `pipefail`, a crash in `pg_dump` would go unnoticed — s3duct
would upload whatever partial data it received, and the pipeline would
exit 0.

If stdin is a regular file (not a pipe), s3duct checks the file size
against bytes read and warns on mismatch, as this may indicate the
file was modified during upload.

### Backpressure

s3duct monitors available disk space in its scratch directory and pauses
reading from stdin when approaching the limit. By default it auto-tunes
based on available disk (2-10 chunks buffered). Use `--diskspace-limit`
or `--buffer-chunks` for explicit control.

## License

[Elastic License 2.0](LICENSE)
