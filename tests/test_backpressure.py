"""Tests for s3duct.backpressure."""

import threading
import time

import pytest

from s3duct.backpressure import (
    BackpressureConfig,
    BackpressureMonitor,
    compute_adaptive_buffer,
)


def test_config_rejects_limit_below_chunk_size(tmp_path):
    with pytest.raises(ValueError, match="must be >= chunk size"):
        BackpressureConfig(
            chunk_size=1024,
            scratch_dir=tmp_path,
            diskspace_limit=512,
        )


def test_config_accepts_valid_limit(tmp_path):
    cfg = BackpressureConfig(
        chunk_size=1024,
        scratch_dir=tmp_path,
        diskspace_limit=2048,
    )
    assert cfg.diskspace_limit == 2048


def test_compute_adaptive_buffer(tmp_path):
    # With typical free space and small chunks, should get a reasonable number
    result = compute_adaptive_buffer(64, tmp_path)
    assert 2 <= result <= 10


def test_compute_adaptive_buffer_huge_chunks(tmp_path):
    # Chunks larger than available disk should clamp to 2
    result = compute_adaptive_buffer(1024 ** 4, tmp_path)  # 1 TB chunks
    assert result == 2


def test_scratch_usage_empty(tmp_path):
    cfg = BackpressureConfig(chunk_size=64, scratch_dir=tmp_path)
    mon = BackpressureMonitor(cfg)
    assert mon.scratch_usage() == 0


def test_scratch_usage_with_files(tmp_path):
    (tmp_path / "chunk-000000").write_bytes(b"x" * 100)
    (tmp_path / "chunk-000001").write_bytes(b"y" * 200)
    cfg = BackpressureConfig(chunk_size=64, scratch_dir=tmp_path)
    mon = BackpressureMonitor(cfg)
    assert mon.scratch_usage() == 300


def test_can_write_chunk_under_limit(tmp_path):
    cfg = BackpressureConfig(chunk_size=64, scratch_dir=tmp_path, max_buffer_chunks=5)
    mon = BackpressureMonitor(cfg)
    assert mon.can_write_chunk() is True


def test_can_write_chunk_over_limit(tmp_path):
    # Fill scratch with data that exceeds 2-chunk limit
    (tmp_path / "chunk-000000").write_bytes(b"x" * 64)
    (tmp_path / "chunk-000001").write_bytes(b"y" * 64)
    cfg = BackpressureConfig(chunk_size=64, scratch_dir=tmp_path, max_buffer_chunks=2)
    mon = BackpressureMonitor(cfg)
    assert mon.can_write_chunk() is False


def test_effective_limit_explicit(tmp_path):
    cfg = BackpressureConfig(chunk_size=64, scratch_dir=tmp_path, diskspace_limit=1024)
    mon = BackpressureMonitor(cfg)
    assert mon.effective_limit == 1024


def test_effective_limit_buffer_chunks(tmp_path):
    cfg = BackpressureConfig(chunk_size=64, scratch_dir=tmp_path, max_buffer_chunks=5)
    mon = BackpressureMonitor(cfg)
    assert mon.effective_limit == 320  # 5 * 64


def test_wait_for_space_unblocks(tmp_path):
    """wait_for_space should unblock when a file is deleted."""
    blocker = tmp_path / "chunk-000000"
    blocker.write_bytes(b"x" * 64)
    cfg = BackpressureConfig(chunk_size=64, scratch_dir=tmp_path, max_buffer_chunks=1,
                             min_buffer_chunks=1)
    mon = BackpressureMonitor(cfg)

    unblocked = threading.Event()

    def waiter():
        mon.wait_for_space(poll_interval=0.05)
        unblocked.set()

    t = threading.Thread(target=waiter)
    t.start()

    # Should still be blocked
    time.sleep(0.1)
    assert not unblocked.is_set()

    # Free space
    blocker.unlink()
    t.join(timeout=2.0)
    assert unblocked.is_set()


def test_cli_diskspace_limit_validation():
    """CLI should reject --diskspace-limit below --chunk-size."""
    from click.testing import CliRunner
    from s3duct.cli import main

    runner = CliRunner()
    result = runner.invoke(main, [
        "put", "--bucket", "b", "--name", "n", "--no-encrypt",
        "--chunk-size", "1M", "--diskspace-limit", "512K",
    ], input="")
    assert result.exit_code != 0
    assert "diskspace-limit" in result.output.lower() or "diskspace-limit" in str(result.exception or "").lower()
