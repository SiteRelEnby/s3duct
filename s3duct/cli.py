"""CLI entry point for s3duct."""

import click

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
@click.option("--encrypt-manifest", is_flag=True, default=False, help="Encrypt manifest with same key as chunks.")
@click.option("--tag", multiple=True, help="Custom metadata tag (key=value, repeatable).")
@click.option("--storage-class", default=DEFAULT_STORAGE_CLASS, help="S3 storage class. Default: STANDARD.")
@click.option("--region", default=None, help="AWS region.")
@click.option("--prefix", default="", help="S3 key prefix.")
@click.option("--endpoint-url", default=None, help="Custom S3 endpoint (for R2, MinIO, etc.).")
@click.option("--diskspace-limit", default=None, help="Max scratch disk usage (e.g., 2G). Must be >= chunk-size.")
@click.option("--buffer-chunks", default=None, type=int, help="Max buffered chunks in scratch (default: auto).")
@click.option("--strict-resume/--no-strict-resume", default=True, help="Fail if stdin ends before all resume-log chunks are re-verified (default: on).")
@click.option("--retries", default=MAX_RETRY_ATTEMPTS, type=int, help=f"Max retry attempts per S3 operation (default: {MAX_RETRY_ATTEMPTS}).")
@click.option("--summary", type=click.Choice(["text", "json", "none"]), default="text", help="Summary output format (default: text).")
def put(bucket, name, chunk_size, key, age_identity, no_encrypt, encrypt_manifest,
        tag, storage_class, region, prefix, endpoint_url, diskspace_limit, buffer_chunks,
        strict_resume, retries, summary):
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

    if encrypt_manifest:
        if not (encrypt and encryption_method == "aes-256-gcm"):
            raise click.ClickException("--encrypt-manifest requires AES encryption (--key).")

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
@click.option("--region", default=None, help="AWS region.")
@click.option("--prefix", default="", help="S3 key prefix.")
@click.option("--endpoint-url", default=None, help="Custom S3 endpoint (for R2, MinIO, etc.).")
@click.option("--retries", default=MAX_RETRY_ATTEMPTS, type=int, help=f"Max retry attempts per S3 operation (default: {MAX_RETRY_ATTEMPTS}).")
@click.option("--summary", type=click.Choice(["text", "json", "none"]), default="text", help="Summary output format (default: text).")
def verify(bucket, name, region, prefix, endpoint_url, retries, summary):
    """Verify integrity of a stored stream."""
    from s3duct.downloader import run_verify

    validate_name(name)

    backend = S3Backend(bucket=bucket, region=region, prefix=prefix,
                        endpoint_url=endpoint_url, max_retries=retries)
    run_verify(backend, name, summary=summary)


if __name__ == "__main__":
    main()
