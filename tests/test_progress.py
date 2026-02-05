"""Tests for s3duct.progress module."""

from unittest.mock import patch

from s3duct.progress import (
    NullProgress,
    PlainProgress,
    RichProgress,
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
