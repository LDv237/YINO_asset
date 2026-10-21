"""Unit tests for the live backlog/health metrics."""

from __future__ import annotations

from collections import deque

from tickforge.runtime.engine import LiveEngine


def test_oldest_pending_lag_tracks_head_of_line_stall() -> None:
    engine = LiveEngine.__new__(LiveEngine)
    base = 1_000_000_000
    engine._pending_recv_mono_ns = deque([base, base + 1_000_000])

    # The head has waited 10 s even though the next event is only 1 ms newer.
    assert engine._oldest_pending_ms(now_ns=base + 10_000_000_000) == 10_000.0
    engine._pending_recv_mono_ns.clear()
    assert engine._oldest_pending_ms(now_ns=base + 10_000_000_000) == 0.0

    # Health must use the larger of the arrival delta and the head wait.
    engine._processing_lag_ms = lambda: 1.0
    engine._oldest_pending_ms = lambda now_ns=None: 10_000.0
    assert engine._backlog_lag_ms() == 10_000.0
