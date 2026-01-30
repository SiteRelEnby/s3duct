"""Stream chunking from stdin to disk files."""

import sys
from dataclasses import dataclass
from pathlib import Path
from collections.abc import Callable
from typing import BinaryIO, Generator

from s3duct.config import DEFAULT_CHUNK_SIZE, READ_BUFFER_SIZE, SCRATCH_DIR
from s3duct.integrity import IntegrityHasher, StreamHasher, DualHash


@dataclass
class ChunkInfo:
    """Metadata about a written chunk."""
    index: int
    path: Path
    size: int
    dual_hash: DualHash


def chunk_stream(
    stream: BinaryIO,
    chunk_size: int = DEFAULT_CHUNK_SIZE,
    scratch_dir: Path | None = None,
    stream_hasher: StreamHasher | None = None,
    pre_chunk_hook: Callable[[], None] | None = None,
) -> Generator[ChunkInfo, None, None]:
    """Read from a stream and yield chunk files on disk.

    Each chunk is written to scratch_dir and yielded. The caller is
    responsible for deleting chunk files after use.

    Args:
        stream: Input byte stream (typically sys.stdin.buffer).
        chunk_size: Target size per chunk in bytes.
        scratch_dir: Directory for temporary chunk files.
        stream_hasher: Optional hasher to track the full stream hash.
        pre_chunk_hook: Optional callable invoked before reading each chunk.
            Used for backpressure (blocks until disk space is available).

    Yields:
        ChunkInfo for each completed chunk.
    """
    if scratch_dir is None:
        scratch_dir = SCRATCH_DIR
    scratch_dir.mkdir(parents=True, exist_ok=True)

    chunk_index = 0
    eof = False

    while not eof:
        if pre_chunk_hook is not None:
            pre_chunk_hook()
        chunk_path = scratch_dir / f"chunk-{chunk_index:06d}"
        hasher = IntegrityHasher()
        bytes_written = 0

        with open(chunk_path, "wb") as f:
            while bytes_written < chunk_size:
                to_read = min(READ_BUFFER_SIZE, chunk_size - bytes_written)
                data = stream.read(to_read)
                if not data:
                    eof = True
                    break
                f.write(data)
                hasher.update(data)
                if stream_hasher:
                    stream_hasher.update(data)
                bytes_written += len(data)

        if bytes_written == 0:
            chunk_path.unlink(missing_ok=True)
            break

        yield ChunkInfo(
            index=chunk_index,
            path=chunk_path,
            size=bytes_written,
            dual_hash=hasher.finalize(),
        )
        chunk_index += 1


def fast_forward_stream(
    stream: BinaryIO,
    chunk_size: int,
    count: int,
    stream_hasher: StreamHasher | None = None,
) -> Generator[tuple[int, DualHash, int], None, None]:
    """Read and hash chunks from stream without writing to disk.

    Used during resume to verify the stream matches the resume log.

    Yields:
        (chunk_index, dual_hash, size) for each chunk read.
    """
    for i in range(count):
        hasher = IntegrityHasher()
        bytes_read = 0

        while bytes_read < chunk_size:
            to_read = min(READ_BUFFER_SIZE, chunk_size - bytes_read)
            data = stream.read(to_read)
            if not data:
                if bytes_read > 0:
                    yield i, hasher.finalize(), bytes_read
                return
            hasher.update(data)
            if stream_hasher:
                stream_hasher.update(data)
            bytes_read += len(data)

        yield i, hasher.finalize(), bytes_read
