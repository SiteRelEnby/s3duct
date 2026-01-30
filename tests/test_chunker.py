"""Tests for s3duct.chunker."""

import hashlib
import io

from s3duct.chunker import chunk_stream, fast_forward_stream
from s3duct.integrity import StreamHasher


CHUNK_SIZE = 64  # small chunks for testing


def test_chunk_stream_single_chunk(scratch_dir):
    data = b"x" * 32
    stream = io.BytesIO(data)
    chunks = list(chunk_stream(stream, CHUNK_SIZE, scratch_dir))
    assert len(chunks) == 1
    assert chunks[0].size == 32
    assert chunks[0].index == 0
    assert chunks[0].path.read_bytes() == data


def test_chunk_stream_exact_multiple(scratch_dir):
    data = b"a" * CHUNK_SIZE * 3
    stream = io.BytesIO(data)
    chunks = list(chunk_stream(stream, CHUNK_SIZE, scratch_dir))
    assert len(chunks) == 3
    for i, c in enumerate(chunks):
        assert c.index == i
        assert c.size == CHUNK_SIZE


def test_chunk_stream_last_smaller(scratch_dir):
    data = b"b" * (CHUNK_SIZE * 2 + 30)
    stream = io.BytesIO(data)
    chunks = list(chunk_stream(stream, CHUNK_SIZE, scratch_dir))
    assert len(chunks) == 3
    assert chunks[0].size == CHUNK_SIZE
    assert chunks[1].size == CHUNK_SIZE
    assert chunks[2].size == 30


def test_chunk_stream_empty(scratch_dir):
    stream = io.BytesIO(b"")
    chunks = list(chunk_stream(stream, CHUNK_SIZE, scratch_dir))
    assert len(chunks) == 0


def test_chunk_stream_files_written(scratch_dir):
    data = b"c" * (CHUNK_SIZE + 10)
    stream = io.BytesIO(data)
    chunks = list(chunk_stream(stream, CHUNK_SIZE, scratch_dir))
    assert len(chunks) == 2
    assert chunks[0].path.read_bytes() == data[:CHUNK_SIZE]
    assert chunks[1].path.read_bytes() == data[CHUNK_SIZE:]


def test_chunk_stream_hashes_correct(scratch_dir):
    data = b"d" * CHUNK_SIZE
    stream = io.BytesIO(data)
    chunks = list(chunk_stream(stream, CHUNK_SIZE, scratch_dir))
    assert chunks[0].dual_hash.sha256 == hashlib.sha256(data).hexdigest()
    assert chunks[0].dual_hash.sha3_256 == hashlib.sha3_256(data).hexdigest()


def test_chunk_stream_with_stream_hasher(scratch_dir):
    data = b"e" * (CHUNK_SIZE * 2 + 10)
    stream = io.BytesIO(data)
    sh = StreamHasher()
    list(chunk_stream(stream, CHUNK_SIZE, scratch_dir, stream_hasher=sh))
    result = sh.finalize()
    assert result.sha256 == hashlib.sha256(data).hexdigest()


def test_fast_forward_stream_basic():
    data = b"f" * (CHUNK_SIZE * 3)
    stream = io.BytesIO(data)
    results = list(fast_forward_stream(stream, CHUNK_SIZE, 3))
    assert len(results) == 3
    for i, (idx, dh, size) in enumerate(results):
        assert idx == i
        assert size == CHUNK_SIZE
        chunk_data = data[i * CHUNK_SIZE:(i + 1) * CHUNK_SIZE]
        assert dh.sha256 == hashlib.sha256(chunk_data).hexdigest()


def test_fast_forward_stream_partial_eof():
    data = b"g" * (CHUNK_SIZE + 20)
    stream = io.BytesIO(data)
    results = list(fast_forward_stream(stream, CHUNK_SIZE, 5))
    assert len(results) == 2
    assert results[0][2] == CHUNK_SIZE
    assert results[1][2] == 20


def test_fast_forward_stream_with_stream_hasher():
    data = b"h" * (CHUNK_SIZE * 2)
    stream = io.BytesIO(data)
    sh = StreamHasher()
    list(fast_forward_stream(stream, CHUNK_SIZE, 2, stream_hasher=sh))
    result = sh.finalize()
    assert result.sha256 == hashlib.sha256(data).hexdigest()
