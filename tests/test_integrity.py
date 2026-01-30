"""Tests for s3duct.integrity."""

import hashlib
import hmac

from s3duct.config import GENESIS_KEY
from s3duct.integrity import (
    DualHash,
    IntegrityHasher,
    StreamHasher,
    compute_chain,
    hash_file,
)


def test_dual_hash_combined():
    sha256_hex = hashlib.sha256(b"test").hexdigest()
    sha3_hex = hashlib.sha3_256(b"test").hexdigest()
    dh = DualHash(sha256=sha256_hex, sha3_256=sha3_hex)
    expected = bytes.fromhex(sha256_hex) + bytes.fromhex(sha3_hex)
    assert dh.combined() == expected


def test_integrity_hasher_known_data():
    data = b"hello world"
    hasher = IntegrityHasher()
    hasher.update(data)
    result = hasher.finalize()
    assert result.sha256 == hashlib.sha256(data).hexdigest()
    assert result.sha3_256 == hashlib.sha3_256(data).hexdigest()


def test_integrity_hasher_incremental():
    data = b"hello world"
    h1 = IntegrityHasher()
    h1.update(data)
    r1 = h1.finalize()

    h2 = IntegrityHasher()
    h2.update(b"hello ")
    h2.update(b"world")
    r2 = h2.finalize()

    assert r1 == r2


def test_integrity_hasher_size():
    hasher = IntegrityHasher()
    hasher.update(b"abc")
    hasher.update(b"de")
    assert hasher.size == 5


def test_stream_hasher():
    data = b"stream of data across chunks"
    sh = StreamHasher()
    sh.update(data[:10])
    sh.update(data[10:])
    result = sh.finalize()
    assert result.sha256 == hashlib.sha256(data).hexdigest()
    assert result.sha3_256 == hashlib.sha3_256(data).hexdigest()


def test_compute_chain_genesis():
    data = b"chunk data"
    dh = DualHash(
        sha256=hashlib.sha256(data).hexdigest(),
        sha3_256=hashlib.sha3_256(data).hexdigest(),
    )
    result = compute_chain(dh, None)
    expected = hmac.new(GENESIS_KEY, dh.combined(), hashlib.sha256).hexdigest()
    assert result == expected


def test_compute_chain_continuation():
    data = b"chunk data"
    dh = DualHash(
        sha256=hashlib.sha256(data).hexdigest(),
        sha3_256=hashlib.sha3_256(data).hexdigest(),
    )
    prev_chain = b"some_previous_chain_value_here!"
    result = compute_chain(dh, prev_chain)
    expected = hmac.new(prev_chain, dh.combined(), hashlib.sha256).hexdigest()
    assert result == expected


def test_compute_chain_deterministic():
    dh = DualHash(
        sha256=hashlib.sha256(b"x").hexdigest(),
        sha3_256=hashlib.sha3_256(b"x").hexdigest(),
    )
    assert compute_chain(dh, None) == compute_chain(dh, None)


def test_hash_file(tmp_path):
    data = b"file content for hashing"
    f = tmp_path / "testfile"
    f.write_bytes(data)
    dh, size = hash_file(f)
    assert size == len(data)
    assert dh.sha256 == hashlib.sha256(data).hexdigest()
    assert dh.sha3_256 == hashlib.sha3_256(data).hexdigest()


def test_hash_file_empty(tmp_path):
    f = tmp_path / "empty"
    f.write_bytes(b"")
    dh, size = hash_file(f)
    assert size == 0
    assert dh.sha256 == hashlib.sha256(b"").hexdigest()
    assert dh.sha3_256 == hashlib.sha3_256(b"").hexdigest()
