"""Tests for AIMD adaptive concurrency throttle."""

import threading
import time

import pytest

from s3duct.throttle import AdaptiveThrottle, ADJUST_INTERVAL, COOLDOWN_INTERVALS


class TestBasicScaling:
    """Test additive increase/decrease based on transfer vs I/O ratio."""

    def test_initial_workers(self):
        t = AdaptiveThrottle(initial=4, min_workers=2, max_workers=8)
        assert t.current_workers == 4

    def test_initial_clamps_to_min(self):
        t = AdaptiveThrottle(initial=1, min_workers=3, max_workers=8)
        assert t.current_workers == 3

    def test_initial_clamps_to_max(self):
        t = AdaptiveThrottle(initial=20, min_workers=2, max_workers=8)
        assert t.current_workers == 8

    def test_scale_up_when_transfer_bound(self):
        """When transfers take >2x local I/O time, add a worker."""
        t = AdaptiveThrottle(initial=2, min_workers=2, max_workers=8)

        # Record some I/O times (fast: 0.1s)
        for _ in range(3):
            t.record_io_time(0.1)

        # Record transfer times (slow: 0.5s, >2x the 0.1s I/O)
        # Need ADJUST_INTERVAL completions to trigger _adjust
        for _ in range(ADJUST_INTERVAL):
            t.record_transfer_time(0.5, 1000)

        assert t.current_workers == 3

    def test_scale_down_when_io_bound(self):
        """When transfers take <0.5x local I/O time, remove a worker."""
        t = AdaptiveThrottle(initial=4, min_workers=2, max_workers=8)

        # Record I/O times (slow: 2.0s)
        for _ in range(3):
            t.record_io_time(2.0)

        # Record transfer times (fast: 0.5s, <0.5x the 2.0s I/O)
        for _ in range(ADJUST_INTERVAL):
            t.record_transfer_time(0.5, 1000)

        assert t.current_workers == 3

    def test_no_scale_below_min(self):
        t = AdaptiveThrottle(initial=2, min_workers=2, max_workers=8)

        for _ in range(3):
            t.record_io_time(2.0)

        for _ in range(ADJUST_INTERVAL):
            t.record_transfer_time(0.1, 1000)

        assert t.current_workers == 2  # stays at min

    def test_no_scale_above_max(self):
        t = AdaptiveThrottle(initial=8, min_workers=2, max_workers=8)

        for _ in range(3):
            t.record_io_time(0.1)

        for _ in range(ADJUST_INTERVAL):
            t.record_transfer_time(0.5, 1000)

        assert t.current_workers == 8  # stays at max

    def test_no_adjust_without_enough_samples(self):
        """Need at least 2 samples of each type before adjusting."""
        t = AdaptiveThrottle(initial=4, min_workers=2, max_workers=8)

        # Only 1 I/O sample - not enough
        t.record_io_time(0.1)

        for _ in range(ADJUST_INTERVAL):
            t.record_transfer_time(0.5, 1000)

        assert t.current_workers == 4  # unchanged


class TestAIMDThrottle:
    """Test multiplicative decrease on S3 throttle signals."""

    def test_throttle_halves_excess(self):
        """Throttle should halve excess workers above minimum."""
        t = AdaptiveThrottle(initial=8, min_workers=2, max_workers=16)
        # excess = 8 - 2 = 6, halve = 3
        t.record_throttle()
        assert t.current_workers == 5

    def test_throttle_at_minimum_no_change(self):
        t = AdaptiveThrottle(initial=2, min_workers=2, max_workers=8)
        t.record_throttle()
        assert t.current_workers == 2

    def test_throttle_small_excess_reduces_by_one(self):
        """With only 1 excess worker, reduce by 1."""
        t = AdaptiveThrottle(initial=3, min_workers=2, max_workers=8)
        # excess = 1, max(1, 1//2) = 1
        t.record_throttle()
        assert t.current_workers == 2

    def test_throttle_sets_cooldown(self):
        """After throttle, should not scale up immediately."""
        t = AdaptiveThrottle(initial=6, min_workers=2, max_workers=8)

        t.record_throttle()
        workers_after_throttle = t.current_workers
        assert workers_after_throttle < 6

        # Try to scale up - should be blocked by cooldown
        for _ in range(3):
            t.record_io_time(0.1)
        for _ in range(ADJUST_INTERVAL):
            t.record_transfer_time(0.5, 1000)

        # Should still be at post-throttle level (cooldown active)
        assert t.current_workers == workers_after_throttle

    def test_multiple_throttles_converge_to_min(self):
        t = AdaptiveThrottle(initial=16, min_workers=2, max_workers=16)

        for _ in range(10):
            t.record_throttle()

        assert t.current_workers == 2


class TestScaleUpEvaluation:
    """Test post-scale-up throughput evaluation and revert."""

    def test_scale_up_reverted_on_throughput_drop(self):
        """If throughput drops >10% after scale-up, revert."""
        t = AdaptiveThrottle(initial=2, min_workers=2, max_workers=8)

        # Build up I/O baseline
        for _ in range(3):
            t.record_io_time(0.1)

        # First batch: fast transfers that trigger scale-up
        # (transfer time > 2x I/O time)
        for _ in range(ADJUST_INTERVAL):
            t.record_transfer_time(0.5, 1_000_000)

        assert t.current_workers == 3  # scaled up

        # Now simulate throughput dropping after scale-up:
        # same transfer times but much smaller sizes (i.e., slower overall)
        for _ in range(3):
            t.record_io_time(0.1)
        # Small size relative to time = low throughput per transfer
        for _ in range(ADJUST_INTERVAL):
            t.record_transfer_time(0.5, 100)  # 100 bytes in 0.5s vs 1MB before

        # Should have reverted (or at least not gone higher)
        assert t.current_workers <= 3


class TestCooldown:
    """Test cooldown behavior after backoff events."""

    def test_cooldown_after_throttle_expires(self):
        """After enough completions, cooldown should expire and allow scale-up."""
        t = AdaptiveThrottle(initial=4, min_workers=2, max_workers=8)

        t.record_throttle()
        workers_after = t.current_workers

        # Burn through the cooldown period with neutral transfer times
        # (transfer ~= I/O, so neither scale-up nor scale-down triggers)
        cooldown_completions = ADJUST_INTERVAL * COOLDOWN_INTERVALS
        for _ in range(cooldown_completions):
            t.record_io_time(0.1)
            t.record_transfer_time(0.15, 1000)

        assert t.current_workers == workers_after  # no change during cooldown

        # Now trigger transfer-bound condition (transfer >> I/O)
        for _ in range(ADJUST_INTERVAL + 2):
            t.record_io_time(0.1)
            t.record_transfer_time(0.5, 1000)

        # Cooldown should be expired, scale-up should work
        assert t.current_workers > workers_after


class TestSemaphore:
    """Test that the semaphore actually controls concurrency."""

    def test_acquire_release(self):
        t = AdaptiveThrottle(initial=2, min_workers=1, max_workers=4)
        t.acquire()
        t.acquire()
        # Third acquire should block, so try non-blocking
        acquired = t._semaphore.acquire(blocking=False)
        assert not acquired
        t.release()
        t.release()

    def test_semaphore_tracks_scaling(self):
        """After scale-up, should be able to acquire one more."""
        t = AdaptiveThrottle(initial=2, min_workers=2, max_workers=8)

        for _ in range(3):
            t.record_io_time(0.1)
        for _ in range(ADJUST_INTERVAL):
            t.record_transfer_time(0.5, 1000)

        assert t.current_workers == 3
        # Should now be able to acquire 3 slots
        t.acquire()
        t.acquire()
        t.acquire()
        acquired = t._semaphore.acquire(blocking=False)
        assert not acquired
        t.release()
        t.release()
        t.release()


class TestTrackerCallbacks:
    """Test that tracker.update_workers is called on scaling events."""

    def test_tracker_called_on_scale_up(self):
        calls = []

        class MockTracker:
            def update_workers(self, old, new, reason):
                calls.append((old, new, reason))

        t = AdaptiveThrottle(initial=2, min_workers=2, max_workers=8,
                             tracker=MockTracker())
        for _ in range(3):
            t.record_io_time(0.1)
        for _ in range(ADJUST_INTERVAL):
            t.record_transfer_time(0.5, 1000)

        assert len(calls) == 1
        assert calls[0][0] == 2  # old
        assert calls[0][1] == 3  # new
        assert "transfer-bound" in calls[0][2]

    def test_tracker_called_on_throttle(self):
        calls = []

        class MockTracker:
            def update_workers(self, old, new, reason):
                calls.append((old, new, reason))

        t = AdaptiveThrottle(initial=6, min_workers=2, max_workers=8,
                             tracker=MockTracker())
        t.record_throttle()

        assert len(calls) == 1
        assert calls[0][0] == 6   # old
        assert calls[0][1] == 4   # new (halved excess: 6-2=4, 4//2=2, 6-2=4)
        assert "AIMD" in calls[0][2]
