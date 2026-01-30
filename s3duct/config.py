"""Default configuration and constants."""

from pathlib import Path

DEFAULT_CHUNK_SIZE = 512 * 1024 * 1024  # 512 MB
DEFAULT_STORAGE_CLASS = "STANDARD"
DEFAULT_REGION = "us-east-1"

SESSION_DIR = Path.home() / ".s3duct" / "sessions"
SCRATCH_DIR = Path.home() / ".s3duct" / "scratch"

GENESIS_KEY = b"s3duct-genesis"
READ_BUFFER_SIZE = 8 * 1024 * 1024  # 8 MB read buffer

MAX_RETRY_ATTEMPTS = 5
RETRY_BASE_DELAY = 1.0  # seconds
RETRY_MAX_DELAY = 60.0  # seconds

RESUME_LOG_FILENAME = ".resume.jsonl"
MANIFEST_FILENAME = ".manifest.json"
