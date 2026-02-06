"""Tests for s3duct.progress module."""

from unittest.mock import patch

from s3duct.progress import (
    NullProgress,
    PlainProgress,
    PlainVerboseProgress,
    RichProgress,
    RichVerboseProgress,
    _format_bytes,
    _format_duration,
    get_tracker,
)


def test_get_tracker_none():
    tracker = get_tracker("none")
    assert isinstance(tracker, NullProgress)


def test_get_tracker_rich():
    tracker = get_tracker("rich")
    assert isinstance(tracker, RichProgress)


def test_get_tracker_plain():
    tracker = get_tracker("plain")
    assert isinstance(tracker, PlainProgress)


def test_get_tracker_auto_tty():
    with patch("sys.stderr") as mock_stderr:
        mock_stderr.isatty.return_value = True
        tracker = get_tracker("auto")
        assert isinstance(tracker, RichProgress)


def test_get_tracker_auto_no_tty():
    with patch("sys.stderr") as mock_stderr:
        mock_stderr.isatty.return_value = False
        tracker = get_tracker("auto")
        assert isinstance(tracker, PlainProgress)


def test_plain_progress_start(capsys):
    tracker = PlainProgress()
    tracker.start(1024, 5, "Testing")
    captured = capsys.readouterr()
    assert "Testing" in captured.err
    assert "5 chunks" in captured.err
    assert "1,024 bytes" in captured.err


def test_plain_progress_start_no_total(capsys):
    tracker = PlainProgress()
    tracker.start(None, 3, "Verifying")
    captured = capsys.readouterr()
    assert "Verifying" in captured.err
    assert "3 chunks" in captured.err
    assert "bytes" not in captured.err


def test_plain_progress_update_chunk(capsys):
    tracker = PlainProgress()
    tracker.update_chunk(2, 512)
    captured = capsys.readouterr()
    assert "Chunk 2" in captured.err
    assert "512" in captured.err


def test_plain_progress_update_workers(capsys):
    tracker = PlainProgress()
    tracker.update_workers(4, 6, "upload-bound")
    captured = capsys.readouterr()
    assert "4" in captured.err
    assert "6" in captured.err
    assert "upload-bound" in captured.err


def test_plain_progress_log(capsys):
    tracker = PlainProgress()
    tracker.log("hello world")
    captured = capsys.readouterr()
    assert "hello world" in captured.err


def test_plain_progress_finish(capsys):
    tracker = PlainProgress()
    tracker.finish("Done. 5 chunks uploaded.")
    captured = capsys.readouterr()
    assert "Done. 5 chunks uploaded." in captured.err


def test_plain_progress_finish_no_summary(capsys):
    tracker = PlainProgress()
    tracker.finish()
    captured = capsys.readouterr()
    assert captured.err == ""


def test_null_progress_all_methods(capsys):
    """NullProgress should produce no output."""
    tracker = NullProgress()
    tracker.start(1024, 5, "Testing")
    tracker.update_chunk(0, 512)
    tracker.update_workers(2, 4, "reason")
    tracker.log("message")
    tracker.finish("summary")
    captured = capsys.readouterr()
    assert captured.err == ""
    assert captured.out == ""


def test_rich_progress_lifecycle():
    """RichProgress should not crash through a full lifecycle."""
    tracker = RichProgress()
    tracker.start(1024, 5, "Testing")
    tracker.update_chunk(0, 256)
    tracker.update_chunk(1, 256)
    tracker.update_workers(2, 3, "test reason")
    tracker.log("info message")
    tracker.finish("Done.")


def test_rich_progress_no_total():
    """RichProgress should handle None total (indeterminate)."""
    tracker = RichProgress()
    tracker.start(None, 3, "Verifying")
    tracker.update_chunk(0, 100)
    tracker.finish()


# --- Helper function tests ---


def test_format_bytes_small():
    assert _format_bytes(512) == "512.0 B"
    assert _format_bytes(0) == "0.0 B"


def test_format_bytes_kilobytes():
    assert _format_bytes(1024) == "1.0 KB"
    assert _format_bytes(2048) == "2.0 KB"


def test_format_bytes_megabytes():
    assert _format_bytes(1024 * 1024) == "1.0 MB"
    assert _format_bytes(int(1.5 * 1024 * 1024)) == "1.5 MB"


def test_format_bytes_gigabytes():
    assert _format_bytes(1024**3) == "1.0 GB"


def test_format_bytes_terabytes():
    assert _format_bytes(1024**4) == "1.0 TB"


def test_format_duration_seconds():
    assert _format_duration(0) == "00:00"
    assert _format_duration(59) == "00:59"


def test_format_duration_minutes():
    assert _format_duration(60) == "01:00"
    assert _format_duration(90) == "01:30"
    assert _format_duration(3599) == "59:59"


def test_format_duration_hours():
    assert _format_duration(3600) == "1:00:00"
    assert _format_duration(3661) == "1:01:01"
    assert _format_duration(7200) == "2:00:00"


# --- Verbose mode tests ---


def test_get_tracker_verbose_tty():
    with patch("sys.stderr") as mock_stderr:
        mock_stderr.isatty.return_value = True
        tracker = get_tracker("auto", verbose=True)
        assert isinstance(tracker, RichVerboseProgress)


def test_get_tracker_verbose_no_tty():
    with patch("sys.stderr") as mock_stderr:
        mock_stderr.isatty.return_value = False
        tracker = get_tracker("auto", verbose=True)
        assert isinstance(tracker, PlainVerboseProgress)


def test_get_tracker_rich_verbose():
    tracker = get_tracker("rich", verbose=True)
    assert isinstance(tracker, RichVerboseProgress)


def test_get_tracker_plain_verbose():
    tracker = get_tracker("plain", verbose=True)
    assert isinstance(tracker, PlainVerboseProgress)


def test_get_tracker_none_ignores_verbose():
    tracker = get_tracker("none", verbose=True)
    assert isinstance(tracker, NullProgress)


def test_plain_verbose_progress_start(capsys):
    tracker = PlainVerboseProgress()
    tracker.start(1024, 5, "Testing")
    captured = capsys.readouterr()
    assert "[00:00]" in captured.err
    assert "Testing" in captured.err
    assert "5 chunks" in captured.err


def test_plain_verbose_progress_update_chunk(capsys):
    tracker = PlainVerboseProgress()
    tracker.start(1024, 5, "Testing")
    capsys.readouterr()  # clear start output
    tracker.update_chunk(2, 512 * 1024, elapsed=0.5)
    captured = capsys.readouterr()
    assert "Chunk 2" in captured.err
    assert "512.0 KB" in captured.err
    assert "0.50s" in captured.err


def test_plain_verbose_progress_update_workers(capsys):
    tracker = PlainVerboseProgress()
    tracker.start(1024, 5, "Testing")
    capsys.readouterr()  # clear start output
    tracker.update_workers(4, 6, "upload-bound")
    captured = capsys.readouterr()
    assert "Workers" in captured.err
    assert "4" in captured.err
    assert "6" in captured.err
    assert "upload-bound" in captured.err


def test_plain_verbose_progress_log(capsys):
    tracker = PlainVerboseProgress()
    tracker.start(1024, 5, "Testing")
    capsys.readouterr()  # clear
    tracker.log("custom message")
    captured = capsys.readouterr()
    assert "custom message" in captured.err


def test_plain_verbose_progress_finish(capsys):
    tracker = PlainVerboseProgress()
    tracker.start(1024, 5, "Testing")
    capsys.readouterr()  # clear
    tracker.finish("Done.")
    captured = capsys.readouterr()
    assert "Done." in captured.err


def test_rich_verbose_progress_lifecycle():
    """RichVerboseProgress should not crash through a full lifecycle."""
    tracker = RichVerboseProgress()
    tracker.start(1024 * 1024, 5, "Testing")
    tracker.set_workers(4)
    tracker.update_chunk(0, 256 * 1024, elapsed=0.3)
    tracker.update_chunk(1, 256 * 1024, elapsed=0.4)
    tracker.update_workers(4, 6, "upload-bound")
    tracker.log("info message")
    tracker.finish("Done.")


def test_rich_verbose_progress_chunk_staged():
    """RichVerboseProgress should track staged chunks."""
    tracker = RichVerboseProgress()
    tracker.start(1024 * 1024, 0, "Uploading", chunk_size=256 * 1024)
    tracker.set_workers(4)
    # Stage 3 chunks
    tracker.chunk_staged(0, 256 * 1024)
    tracker.chunk_staged(1, 256 * 1024)
    tracker.chunk_staged(2, 256 * 1024)
    # Complete 1
    tracker.update_chunk(0, 256 * 1024, elapsed=0.5)
    # Internal state should reflect 1 completed, 3 staged
    assert tracker._chunks_completed == 1
    assert tracker._chunks_staged == 3
    tracker.finish()


def test_rich_verbose_progress_estimated_total():
    """RichVerboseProgress should estimate total chunks from total_bytes/chunk_size."""
    tracker = RichVerboseProgress()
    # 1 MB total, 256 KB chunks = ~4 chunks
    tracker.start(1024 * 1024, 0, "Uploading", chunk_size=256 * 1024)
    assert tracker._estimated_total_chunks() == 4
    tracker.finish()


def test_rich_verbose_progress_known_total():
    """RichVerboseProgress should prefer known total over estimated."""
    tracker = RichVerboseProgress()
    # Known total of 10 chunks
    tracker.start(1024 * 1024, 10, "Downloading", chunk_size=256 * 1024)
    assert tracker._estimated_total_chunks() == 10
    tracker.finish()


def test_plain_verbose_progress_chunk_staged(capsys):
    """PlainVerboseProgress should log staged chunks."""
    tracker = PlainVerboseProgress()
    tracker.start(1024, 0, "Uploading", chunk_size=256)
    capsys.readouterr()  # clear
    tracker.chunk_staged(0, 256)
    captured = capsys.readouterr()
    assert "Chunk 0 staged" in captured.err
    assert "256.0 B" in captured.err


def test_rich_verbose_progress_in_flight_tracking():
    """RichVerboseProgress should track in-flight chunks."""
    tracker = RichVerboseProgress()
    tracker.start(1024 * 1024, 0, "Uploading", chunk_size=256 * 1024)

    # Stage 3 chunks - they should be in-flight
    tracker.chunk_staged(0, 256 * 1024)
    tracker.chunk_staged(1, 256 * 1024)
    tracker.chunk_staged(2, 256 * 1024)
    assert tracker._in_flight == {0, 1, 2}

    # Complete chunk 0 - should be removed from in-flight
    tracker.update_chunk(0, 256 * 1024, elapsed=0.5)
    assert tracker._in_flight == {1, 2}

    # Complete chunk 1
    tracker.update_chunk(1, 256 * 1024, elapsed=0.4)
    assert tracker._in_flight == {2}

    tracker.finish()
