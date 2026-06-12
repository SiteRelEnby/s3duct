"""s3duct - Chunked, resumable, encrypted pipe to object storage."""

import sys

if sys.version_info < (3, 10):  # noqa: UP036 — friendly error for old interpreters
    raise SystemExit("s3duct requires Python 3.10 or later.")

__version__ = "0.4.0"
