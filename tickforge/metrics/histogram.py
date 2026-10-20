"""Fixed log-scale latency histogram for nanosecond observations.

The histogram uses a compile-time fixed set of bucket upper bounds spanning
1 microsecond to 60 seconds.  Fixed bounds keep memory bounded and make the
serialized snapshot independent of the observation order, which matters for
reproducible status output.

Quantiles are reported as the *upper bound of the bucket* that contains the
requested rank.  This is deliberately conservative: ``p99_ns`` is never lower
than the true 99th percentile of the recorded samples.  The bucket ratio is
``2 ** 0.25`` (~18.9%), so the reported value is at most about 19% above the
true quantile inside the covered range.

All methods are thread safe.  The WAL writer thread and the event-loop thread
may record into the same histogram concurrently.
"""

from __future__ import annotations

import bisect
import math
import threading
from typing import Any, Sequence

__all__ = [
    "BUCKET_RATIO",
    "MIN_BUCKET_NS",
    "MAX_BUCKET_NS",
    "BUCKET_BOUNDS_NS",
    "HISTOGRAM_QUANTILES",
    "Histogram",
    "quantile_ns",
]

#: Multiplicative width of one bucket.
BUCKET_RATIO: float = 2.0 ** 0.25

#: Smallest bucket upper bound: 1 microsecond.
MIN_BUCKET_NS: int = 1_000

#: Largest bucket upper bound: 60 seconds.  Larger samples are counted in it.
MAX_BUCKET_NS: int = 60_000_000_000

#: Quantiles exposed in snapshots and Prometheus summaries, in order.
HISTOGRAM_QUANTILES: tuple[float, ...] = (0.5, 0.9, 0.95, 0.99, 0.999)


def _build_bucket_bounds() -> tuple[int, ...]:
    """Build the immutable, logarithmically spaced bucket upper bounds."""

    bounds: list[int] = []
    value = float(MIN_BUCKET_NS)
    while value < MAX_BUCKET_NS:
        candidate = int(round(value))
        if not bounds or candidate > bounds[-1]:
            bounds.append(candidate)
        value *= BUCKET_RATIO
    if not bounds or bounds[-1] != MAX_BUCKET_NS:
        bounds.append(MAX_BUCKET_NS)
    # The first bound is always exactly 1us by construction; guard against a
    # future constant change via the assertion-free clamp below.
    if bounds[0] != MIN_BUCKET_NS:
        bounds[0] = MIN_BUCKET_NS
    return tuple(bounds)


#: Sorted bucket upper bounds in nanoseconds, from 1us through 60s.
BUCKET_BOUNDS_NS: tuple[int, ...] = _build_bucket_bounds()


def quantile_ns(counts: Sequence[int], total: int, q: float) -> int:
    """Return a conservative quantile from cumulative bucket counts.

    Args:
        counts: Per-bucket sample counts (not cumulative) aligned with
            :data:`BUCKET_BOUNDS_NS`.
        total: Total number of samples.  Must equal ``sum(counts)``.
        q: Quantile in ``(0, 1]``.

    Returns:
        The upper bound of the bucket containing rank ``ceil(q * total)``,
        or ``0`` when no samples were recorded.
    """

    if not 0.0 < q <= 1.0:
        raise ValueError(f"quantile must be in (0, 1], got {q!r}")
    if total <= 0:
        return 0
    rank = max(1, math.ceil(q * total))
    cumulative = 0
    for bound, count in zip(BUCKET_BOUNDS_NS, counts):
        cumulative += count
        if cumulative >= rank:
            return int(bound)
    # Overflow is folded into the final bucket, so this is unreachable unless
    # ``counts`` and ``total`` disagree.
    return int(BUCKET_BOUNDS_NS[-1])


class Histogram:
    """Thread-safe nanosecond histogram with fixed log-scale buckets.

    The internal state is intentionally small: one integer counter per bucket
    plus ``count``, ``sum``, ``min`` and ``max`` aggregates.  Recording is
    O(log(number_of_buckets)) and never allocates per sample.
    """

    __slots__ = ("_lock", "_counts", "_count", "_sum", "_min", "_max")

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._counts: list[int] = [0] * len(BUCKET_BOUNDS_NS)
        self._count: int = 0
        self._sum: int = 0
        self._min: int | None = None
        self._max: int | None = None

    def observe(self, value_ns: int) -> None:
        """Record one observation in nanoseconds.

        Negative values are clamped to zero (they fall in the first bucket).
        Values above 60s share the final overflow bucket while ``sum``,
        ``min`` and ``max`` retain the exact numbers.
        """

        value = int(value_ns)
        if value < 0:
            value = 0
        index = bisect.bisect_left(BUCKET_BOUNDS_NS, value)
        if index >= len(BUCKET_BOUNDS_NS):
            index = len(BUCKET_BOUNDS_NS) - 1
        with self._lock:
            self._counts[index] += 1
            self._count += 1
            self._sum += value
            if self._min is None or value < self._min:
                self._min = value
            if self._max is None or value > self._max:
                self._max = value

    @property
    def count(self) -> int:
        """Number of recorded observations."""

        with self._lock:
            return self._count

    @property
    def sum_ns(self) -> int:
        """Exact sum of recorded observations in nanoseconds."""

        with self._lock:
            return self._sum

    def quantile(self, q: float) -> int:
        """Return the conservative upper-bound quantile in nanoseconds."""

        with self._lock:
            counts = list(self._counts)
            total = self._count
        return quantile_ns(counts, total, q)

    def snapshot(self) -> dict[str, Any]:
        """Return a consistent, JSON-serializable view of the histogram.

        The ``buckets`` entry is a list of ``{"le": upper_ns, "count": n}``
        objects where ``count`` is cumulative, matching Prometheus histogram
        conventions.  The final bucket also absorbs observations above 60s.
        """

        with self._lock:
            counts = list(self._counts)
            total = self._count
            total_sum = self._sum
            minimum = 0 if self._min is None else self._min
            maximum = 0 if self._max is None else self._max
            cumulative = 0
            buckets: list[dict[str, int]] = []
            for bound, count in zip(BUCKET_BOUNDS_NS, counts):
                cumulative += count
                buckets.append({"le": int(bound), "count": cumulative})

        p50, p90, p95, p99, p999 = (
            quantile_ns(counts, total, q) for q in HISTOGRAM_QUANTILES
        )
        mean = (total_sum / total) if total else 0.0
        return {
            "count": total,
            "min_ns": minimum,
            "max_ns": maximum,
            "sum_ns": total_sum,
            "mean_ns": mean,
            "p50_ns": p50,
            "p90_ns": p90,
            "p95_ns": p95,
            "p99_ns": p99,
            "p999_ns": p999,
            "buckets": buckets,
        }
