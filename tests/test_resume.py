"""Tests for s3duct.resume."""

import hashlib

from s3duct.integrity import DualHash, compute_chain
from s3duct.resume import ResumeEntry, ResumeLog


def _make_entry(index: int, data: bytes, prev_chain: bytes | None, s3_key: str = "k") -> ResumeEntry:
    """Helper to create a valid ResumeEntry with correct chain."""
    dh = DualHash(
        sha256=hashlib.sha256(data).hexdigest(),
        sha3_256=hashlib.sha3_256(data).hexdigest(),
    )
    chain = compute_chain(dh, prev_chain)
    return ResumeEntry(
        chunk=index, size=len(data),
        sha256=dh.sha256, sha3_256=dh.sha3_256,
        chain=chain, s3_key=s3_key, etag="etag",
        ts="2025-01-01T00:00:00Z",
    )


def test_resume_entry_roundtrip():
    e = ResumeEntry(chunk=0, size=100, sha256="aa", sha3_256="bb",
                    chain="cc", s3_key="test/chunk-000000", etag="et", ts="ts")
    raw = e.to_json()
    e2 = ResumeEntry.from_json(raw)
    assert e2.chunk == 0
    assert e2.size == 100
    assert e2.sha256 == "aa"
    assert e2.s3_key == "test/chunk-000000"


def test_resume_log_empty(session_dir):
    log = ResumeLog("test-stream")
    assert log.last_chunk_index == -1
    assert log.entries == []


def test_resume_log_append_and_load(session_dir):
    e0 = _make_entry(0, b"chunk0", None)
    e1 = _make_entry(1, b"chunk1", bytes.fromhex(e0.chain))

    log1 = ResumeLog("test-stream")
    log1.append(e0)
    log1.append(e1)
    assert log1.last_chunk_index == 1

    # New log instance should load from disk
    log2 = ResumeLog("test-stream")
    assert log2.last_chunk_index == 1
    assert len(log2.entries) == 2
    assert log2.entries[0].sha256 == e0.sha256
    assert log2.entries[1].sha256 == e1.sha256


def test_resume_log_verify_chain_valid(session_dir):
    e0 = _make_entry(0, b"data0", None)
    e1 = _make_entry(1, b"data1", bytes.fromhex(e0.chain))

    log = ResumeLog("test-chain")
    log.append(e0)
    log.append(e1)
    assert log.verify_chain() is True


def test_resume_log_verify_chain_tampered(session_dir):
    e0 = _make_entry(0, b"data0", None)
    # Tamper with the chain value
    e1_bad = ResumeEntry(
        chunk=1, size=5, sha256=hashlib.sha256(b"data1").hexdigest(),
        sha3_256=hashlib.sha3_256(b"data1").hexdigest(),
        chain="0000000000000000000000000000000000000000000000000000000000000000",
        s3_key="k", etag="et", ts="ts",
    )

    log = ResumeLog("test-tamper")
    log.append(e0)
    log.append(e1_bad)
    assert log.verify_chain() is False


def test_resume_log_verify_entry(session_dir):
    e0 = _make_entry(0, b"data0", None)
    log = ResumeLog("test-verify")
    log.append(e0)

    dh = DualHash(
        sha256=hashlib.sha256(b"data0").hexdigest(),
        sha3_256=hashlib.sha3_256(b"data0").hexdigest(),
    )
    assert log.verify_entry(0, dh, 5) is True

    # Wrong hash
    dh_bad = DualHash(sha256="wrong", sha3_256="wrong")
    assert log.verify_entry(0, dh_bad, 5) is False

    # Wrong size
    assert log.verify_entry(0, dh, 999) is False

    # Out of range
    assert log.verify_entry(5, dh, 5) is False


def test_resume_log_clear(session_dir):
    e0 = _make_entry(0, b"data", None)
    log = ResumeLog("test-clear")
    log.append(e0)
    assert log.path.exists()

    log.clear()
    assert not log.path.exists()
    assert log.entries == []
    assert log.last_chunk_index == -1


def test_resume_log_get_last_chain_bytes(session_dir):
    log = ResumeLog("test-chain-bytes")
    assert log.get_last_chain_bytes() is None

    e0 = _make_entry(0, b"data", None)
    log.append(e0)
    chain_bytes = log.get_last_chain_bytes()
    assert chain_bytes == bytes.fromhex(e0.chain)


def test_resume_log_slash_in_name(session_dir):
    """Stream names with slashes should be safe for filenames."""
    log = ResumeLog("path/to/stream")
    e0 = _make_entry(0, b"data", None)
    log.append(e0)
    assert "__" in log.path.name
    assert log.last_chunk_index == 0


def test_resume_log_out_of_order_entries(session_dir):
    """Log with entries 0,1,2,4,5 — contiguous_prefix returns 2."""
    e0 = _make_entry(0, b"d0", None)
    e1 = _make_entry(1, b"d1", bytes.fromhex(e0.chain))
    e2 = _make_entry(2, b"d2", bytes.fromhex(e1.chain))
    # Skip chunk 3
    e3 = _make_entry(3, b"d3", bytes.fromhex(e2.chain))
    e4 = _make_entry(4, b"d4", bytes.fromhex(e3.chain))

    log = ResumeLog("test-gap")
    log.append(e0)
    log.append(e1)
    log.append(e2)
    # Write entries 4 and 5 (with chunk indices 4 and 5, skipping 3)
    # We need to manually write entries with gap
    log._entries.append(ResumeEntry(
        chunk=4, size=2, sha256=e4.sha256, sha3_256=e4.sha3_256,
        chain=e4.chain, s3_key="k", etag="et", ts="ts",
    ))
    log._entries.append(ResumeEntry(
        chunk=5, size=2,
        sha256=hashlib.sha256(b"d5").hexdigest(),
        sha3_256=hashlib.sha3_256(b"d5").hexdigest(),
        chain="fakechainvalue",
        s3_key="k", etag="et", ts="ts",
    ))

    assert log.contiguous_prefix() == 2


def test_resume_log_gap_warning(session_dir, capsys):
    """Verify warning emitted when gap detected in resume log."""
    e0 = _make_entry(0, b"d0", None)
    e1 = _make_entry(1, b"d1", bytes.fromhex(e0.chain))

    log = ResumeLog("test-gap-warn")
    log.append(e0)
    log.append(e1)
    # Add entry with chunk=3, skipping chunk=2
    log._entries.append(ResumeEntry(
        chunk=3, size=2,
        sha256=hashlib.sha256(b"d3").hexdigest(),
        sha3_256=hashlib.sha3_256(b"d3").hexdigest(),
        chain="fakechainvalue",
        s3_key="k", etag="et", ts="ts",
    ))

    result = log.contiguous_prefix()
    assert result == 1

    captured = capsys.readouterr()
    assert "WARNING" in captured.err
    assert "gaps" in captured.err
    assert "missing chunks: 2" in captured.err
    assert "discarded" in captured.err


def test_resume_log_verify_chain_rejects_gap(session_dir):
    """verify_chain() fails on a gapped log (chunk index != position)."""
    e0 = _make_entry(0, b"d0", None)
    e1 = _make_entry(1, b"d1", bytes.fromhex(e0.chain))

    log = ResumeLog("test-chain-gap")
    log.append(e0)
    log.append(e1)
    # Inject entry with chunk=3, skipping chunk=2
    log._entries.append(ResumeEntry(
        chunk=3, size=2,
        sha256=hashlib.sha256(b"d3").hexdigest(),
        sha3_256=hashlib.sha3_256(b"d3").hexdigest(),
        chain="fakechainvalue",
        s3_key="k", etag="et", ts="ts",
    ))

    assert log.verify_chain() is False


def test_resume_log_verify_chain_rejects_out_of_order(session_dir):
    """verify_chain() fails if entries are not in sequential order."""
    e0 = _make_entry(0, b"d0", None)
    e1 = _make_entry(1, b"d1", bytes.fromhex(e0.chain))
    e2 = _make_entry(2, b"d2", bytes.fromhex(e1.chain))

    log = ResumeLog("test-chain-ooo")
    log.append(e0)
    log.append(e2)  # skip e1, append e2 — out of order
    log.append(e1)

    assert log.verify_chain() is False
