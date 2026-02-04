"""CLI entry point for s3duct."""

import sys

import click

from s3duct import __version__
from s3duct.backends.s3 import S3Backend
from s3duct.config import DEFAULT_CHUNK_SIZE, DEFAULT_STORAGE_CLASS, MAX_RETRY_ATTEMPTS


def parse_size(value: str) -> int:
    """Parse a human-readable size string (e.g., '512M', '1G') to bytes."""
    value = value.strip().upper()
    multipliers = {"B": 1, "K": 1024, "M": 1024**2, "G": 1024**3, "T": 1024**4}
    for suffix, mult in multipliers.items():
        if value.endswith(suffix):
            return int(float(value[:-1]) * mult)
    return int(value)


@click.group()
@click.version_option(version=__version__, prog_name="s3duct")
def main() -> None:
    """s3duct - Chunked, resumable, encrypted pipe to object storage."""
    pass


def validate_name(name: str) -> None:
    """Validate stream name is safe for use as an S3 key prefix."""
    if not name or not name.strip():
        raise click.BadParameter("Stream name cannot be empty.")
    if name.startswith("/") or name.startswith("."):
        raise click.BadParameter(f"Stream name should not start with '/' or '.': {name!r}")
    if "//" in name:
        raise click.BadParameter(f"Stream name should not contain '//': {name!r}")


def parse_tag(value: str) -> tuple[str, str]:
    """Parse a 'key=value' tag string."""
    if "=" not in value:
        raise click.BadParameter(f"Tag must be key=value, got: {value!r}")
    k, v = value.split("=", 1)
    if not k:
        raise click.BadParameter(f"Tag key cannot be empty: {value!r}")
    return k, v


@main.command()
@click.option("--bucket", required=True, help="S3 bucket name.")
@click.option("--name", required=True, help="Stream name (used as S3 prefix).")
@click.option("--chunk-size", default="512M", help="Chunk size (e.g., 512M, 1G). Default: 512M.")
@click.option("--key", default=None, help="AES-256-GCM key (hex:..., file:..., or env:...).")
@click.option("--age-identity", type=click.Path(exists=True), help="Path to age identity file.")
@click.option("--no-encrypt", is_flag=True, default=False, help="Disable encryption even if --key or --age-identity set.")
@click.option("--encrypt-manifest/--no-encrypt-manifest", default=None, help="Encrypt manifest (default: on when encryption is active). Use --no-encrypt-manifest to keep manifest as readable JSON.")
@click.option("--tag", multiple=True, help="Custom metadata tag (key=value, repeatable).")
@click.option("--storage-class", default=DEFAULT_STORAGE_CLASS, help="S3 storage class. Default: STANDARD.")
@click.option("--region", default=None, help="AWS region.")
@click.option("--prefix", default="", help="S3 key prefix.")
@click.option("--endpoint-url", default=None, help="Custom S3 endpoint (for R2, MinIO, etc.).")
@click.option("--diskspace-limit", default=None, help="Max scratch disk usage (e.g., 2G). Must be >= chunk-size.")
@click.option("--buffer-chunks", default=None, type=int, help="Max buffered chunks in scratch (default: auto).")
@click.option("--strict-resume/--no-strict-resume", default=True, help="Fail if stdin ends before all resume-log chunks are re-verified (default: on).")
@click.option("--retries", default=MAX_RETRY_ATTEMPTS, type=int, help=f"Max retry attempts per S3 operation (default: {MAX_RETRY_ATTEMPTS}).")
@click.option("--upload-workers", default="auto", help="Parallel upload threads. 'auto' adapts based on throughput (default). Use an integer for fixed concurrency.")
@click.option("--min-upload-workers", default=None, type=int, help="Minimum workers for auto mode (default: 2).")
@click.option("--max-upload-workers", default=None, type=int, help="Maximum workers for auto mode (default: 16).")
@click.option("--summary", type=click.Choice(["text", "json", "none"]), default="text", help="Summary output format (default: text).")
def put(bucket, name, chunk_size, key, age_identity, no_encrypt, encrypt_manifest,
        tag, storage_class, region, prefix, endpoint_url, diskspace_limit, buffer_chunks,
        strict_resume, retries, upload_workers, min_upload_workers, max_upload_workers, summary):
    """Upload a stream from stdin to S3."""
    from s3duct.encryption import parse_key
    from s3duct.uploader import run_put

    validate_name(name)

    if key and age_identity:
        raise click.ClickException("--key and --age-identity are mutually exclusive.")

    # Determine encryption
    encrypt = False
    encryption_method = None
    aes_key = None

    if not no_encrypt:
        if key:
            encrypt = True
            encryption_method = "aes-256-gcm"
            aes_key = parse_key(key)
        elif age_identity:
            encrypt = True
            encryption_method = "age"

    # Warn on unencrypted upload
    if not encrypt and not no_encrypt:
        click.echo(
            "WARNING: No encryption configured. Data will be uploaded in plaintext.\n"
            "         Use --key or --age-identity to encrypt, or --no-encrypt to silence this warning.",
            err=True,
        )
        if sys.stderr.isatty():
            click.echo("         Proceeding in 10 seconds... (Ctrl+C to abort)", err=True)
            import time
            time.sleep(10)

    # Resolve encrypt_manifest default: on when encryption is active
    if encrypt_manifest is None:
        encrypt_manifest = encrypt
    if encrypt_manifest and not encrypt:
        raise click.ClickException(
            "--encrypt-manifest requires encryption (--key or --age-identity)."
        )

    # Parse tags
    tags = {}
    for t in tag:
        k, v = parse_tag(t)
        tags[k] = v

    parsed_chunk_size = parse_size(chunk_size)
    parsed_limit = parse_size(diskspace_limit) if diskspace_limit else None

    if parsed_limit is not None and parsed_limit < parsed_chunk_size:
        raise click.ClickException(
            f"--diskspace-limit ({diskspace_limit}) must be >= --chunk-size ({chunk_size})"
        )

    backend = S3Backend(bucket=bucket, region=region, prefix=prefix,
                        endpoint_url=endpoint_url, max_retries=retries)
    # Parse upload_workers: "auto" or an integer
    parsed_workers: int | str = upload_workers
    if upload_workers != "auto":
        try:
            parsed_workers = int(upload_workers)
            if parsed_workers < 1:
                raise click.ClickException("--upload-workers must be >= 1")
        except ValueError:
            raise click.ClickException(
                f"--upload-workers must be 'auto' or a positive integer, got: {upload_workers!r}"
            )

    if min_upload_workers is not None and upload_workers != "auto":
        raise click.ClickException("--min-upload-workers only applies when --upload-workers is 'auto'")
    if max_upload_workers is not None and upload_workers != "auto":
        raise click.ClickException("--max-upload-workers only applies when --upload-workers is 'auto'")
    if (min_upload_workers is not None and max_upload_workers is not None
            and min_upload_workers > max_upload_workers):
        raise click.ClickException("--min-upload-workers must be <= --max-upload-workers")

    run_put(
        backend=backend,
        name=name,
        chunk_size=parsed_chunk_size,
        encrypt=encrypt,
        encrypt_manifest=encrypt_manifest,
        encryption_method=encryption_method,
        aes_key=aes_key,
        age_identity=age_identity,
        storage_class=storage_class,
        diskspace_limit=parsed_limit,
        buffer_chunks=buffer_chunks,
        tags=tags or None,
        strict_resume=strict_resume,
        summary=summary,
        upload_workers=parsed_workers,
        min_upload_workers=min_upload_workers,
        max_upload_workers=max_upload_workers,
    )


@main.command()
@click.option("--bucket", required=True, help="S3 bucket name.")
@click.option("--name", required=True, help="Stream name to restore.")
@click.option("--key", default=None, help="AES-256-GCM key (hex:..., file:..., or env:...).")
@click.option("--age-identity", type=click.Path(exists=True), help="Path to age identity file.")
@click.option("--no-decrypt", is_flag=True, default=False, help="Skip decryption (download raw encrypted chunks).")
@click.option("--region", default=None, help="AWS region.")
@click.option("--prefix", default="", help="S3 key prefix.")
@click.option("--endpoint-url", default=None, help="Custom S3 endpoint (for R2, MinIO, etc.).")
@click.option("--retries", default=MAX_RETRY_ATTEMPTS, type=int, help=f"Max retry attempts per S3 operation (default: {MAX_RETRY_ATTEMPTS}).")
@click.option("--summary", type=click.Choice(["text", "json", "none"]), default="text", help="Summary output format (default: text).")
def get(bucket, name, key, age_identity, no_decrypt, region, prefix, endpoint_url, retries, summary):
    """Download a stream from S3 to stdout."""
    from s3duct.encryption import parse_key
    from s3duct.downloader import run_get

    validate_name(name)

    if key and age_identity:
        raise click.ClickException("--key and --age-identity are mutually exclusive.")

    decrypt = not no_decrypt
    encryption_method = None
    aes_key = None

    if key:
        encryption_method = "aes-256-gcm"
        aes_key = parse_key(key)
    elif age_identity:
        encryption_method = "age"

    backend = S3Backend(bucket=bucket, region=region, prefix=prefix,
                        endpoint_url=endpoint_url, max_retries=retries)
    run_get(
        backend=backend,
        name=name,
        decrypt=decrypt,
        encryption_method=encryption_method,
        aes_key=aes_key,
        age_identity=age_identity,
        summary=summary,
    )


@main.command("list")
@click.option("--bucket", required=True, help="S3 bucket name.")
@click.option("--prefix", default="", help="Filter by prefix.")
@click.option("--region", default=None, help="AWS region.")
@click.option("--endpoint-url", default=None, help="Custom S3 endpoint (for R2, MinIO, etc.).")
def list_cmd(bucket, prefix, region, endpoint_url):
    """List stored streams."""
    from s3duct.downloader import run_list

    backend = S3Backend(bucket=bucket, region=region, prefix=prefix, endpoint_url=endpoint_url)
    run_list(backend)


@main.command()
@click.option("--bucket", required=True, help="S3 bucket name.")
@click.option("--name", required=True, help="Stream name to verify.")
@click.option("--key", default=None, help="AES-256-GCM key (hex:..., file:..., or env:...). Required if manifest is encrypted with AES.")
@click.option("--age-identity", type=click.Path(exists=True), help="Path to age identity file. Required if manifest is encrypted with age.")
@click.option("--region", default=None, help="AWS region.")
@click.option("--prefix", default="", help="S3 key prefix.")
@click.option("--endpoint-url", default=None, help="Custom S3 endpoint (for R2, MinIO, etc.).")
@click.option("--retries", default=MAX_RETRY_ATTEMPTS, type=int, help=f"Max retry attempts per S3 operation (default: {MAX_RETRY_ATTEMPTS}).")
@click.option("--summary", type=click.Choice(["text", "json", "none"]), default="text", help="Summary output format (default: text).")
def verify(bucket, name, key, age_identity, region, prefix, endpoint_url, retries, summary):
    """Verify integrity of a stored stream."""
    from s3duct.encryption import parse_key
    from s3duct.downloader import run_verify

    validate_name(name)

    if key and age_identity:
        raise click.ClickException("--key and --age-identity are mutually exclusive.")

    aes_key = parse_key(key) if key else None

    backend = S3Backend(bucket=bucket, region=region, prefix=prefix,
                        endpoint_url=endpoint_url, max_retries=retries)
    run_verify(backend, name, aes_key=aes_key, age_identity=age_identity, summary=summary)


@main.command()
@click.option("--bucket", required=True, help="S3 bucket name.")
@click.option("--name", required=True, help="Stream name to delete.")
@click.option("--dry-run", is_flag=True, default=False, help="List what would be deleted without deleting.")
@click.option("--force", is_flag=True, default=False, help="Skip confirmation prompt.")
@click.option("--key", default=None, help="AES-256-GCM key (hex:..., file:..., or env:...). Required if manifest is encrypted with AES.")
@click.option("--age-identity", type=click.Path(exists=True), help="Path to age identity file. Required if manifest is encrypted with age.")
@click.option("--region", default=None, help="AWS region.")
@click.option("--prefix", default="", help="S3 key prefix.")
@click.option("--endpoint-url", default=None, help="Custom S3 endpoint (for R2, MinIO, etc.).")
@click.option("--retries", default=MAX_RETRY_ATTEMPTS, type=int, help=f"Max retry attempts per S3 operation (default: {MAX_RETRY_ATTEMPTS}).")
def delete(bucket, name, dry_run, force, key, age_identity, region, prefix, endpoint_url, retries):
    """Delete a stream and all its chunks from S3."""
    from s3duct.encryption import parse_key
    from s3duct.downloader import run_delete

    validate_name(name)

    if key and age_identity:
        raise click.ClickException("--key and --age-identity are mutually exclusive.")

    aes_key = parse_key(key) if key else None

    if not dry_run and not force:
        click.confirm(
            f"Delete stream '{name}' and all its chunks from s3://{bucket}?",
            abort=True,
            err=True,
        )

    backend = S3Backend(bucket=bucket, region=region, prefix=prefix,
                        endpoint_url=endpoint_url, max_retries=retries)
    run_delete(backend, name, dry_run=dry_run, aes_key=aes_key, age_identity=age_identity)


@main.command()
@click.option("--bucket", required=True, help="S3 bucket name.")
@click.option("--name", required=True, help="Stream name to restore from Glacier.")
@click.option("--days", default=7, type=int, help="Days to keep restored copies (default: 7).")
@click.option("--tier", default="Standard",
              type=click.Choice(["Expedited", "Standard", "Bulk"], case_sensitive=True),
              help="Restore tier (default: Standard).")
@click.option("--wait", is_flag=True, default=False, help="Poll until all chunks are restored.")
@click.option("--poll-interval", default=60, type=int, help="Seconds between status checks (default: 60).")
@click.option("--key", default=None, help="AES-256-GCM key (hex:..., file:..., or env:...).")
@click.option("--age-identity", type=click.Path(exists=True), help="Path to age identity file.")
@click.option("--region", default=None, help="AWS region.")
@click.option("--prefix", default="", help="S3 key prefix.")
@click.option("--endpoint-url", default=None, help="Custom S3 endpoint (for R2, MinIO, etc.).")
@click.option("--retries", default=MAX_RETRY_ATTEMPTS, type=int, help=f"Max retry attempts per S3 operation (default: {MAX_RETRY_ATTEMPTS}).")
def restore(bucket, name, days, tier, wait, poll_interval, key, age_identity,
            region, prefix, endpoint_url, retries):
    """Initiate Glacier/Deep Archive restore for a stream's chunks."""
    from s3duct.encryption import parse_key
    from s3duct.thaw import run_restore

    validate_name(name)

    if key and age_identity:
        raise click.ClickException("--key and --age-identity are mutually exclusive.")

    aes_key = parse_key(key) if key else None

    backend = S3Backend(bucket=bucket, region=region, prefix=prefix,
                        endpoint_url=endpoint_url, max_retries=retries)
    run_restore(
        backend=backend,
        name=name,
        days=days,
        tier=tier,
        wait=wait,
        poll_interval=poll_interval,
        aes_key=aes_key,
        age_identity=age_identity,
    )


if __name__ == "__main__":
    main()
