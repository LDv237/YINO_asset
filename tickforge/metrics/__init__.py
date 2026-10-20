"""Metrics primitives used across TickForge.

The public surface is small:

* :class:`MetricsRegistry` - thread-safe counters, gauges and ns histograms
  with a Prometheus text renderer.
* :class:`NullMetrics` - a no-op implementation of the same interface.
* :class:`Histogram` and its fixed bucket bounds, for direct use in tests and
  benchmarks.

Example::

    from tickforge.metrics import MetricsRegistry

    metrics = MetricsRegistry()
    metrics.inc("tickforge_ws_messages_total")
    metrics.observe_ns("tickforge_latency_parse_ns", 12_345)
    print(metrics.render_prometheus())
"""

from __future__ import annotations

from .histogram import (
    BUCKET_BOUNDS_NS,
    BUCKET_RATIO,
    HISTOGRAM_QUANTILES,
    MAX_BUCKET_NS,
    MIN_BUCKET_NS,
    Histogram,
    quantile_ns,
)
from .registry import (
    MetricsRegistry,
    NullMetrics,
    escape_label_value,
    sanitize_label_name,
    sanitize_metric_name,
)

__all__ = [
    "MetricsRegistry",
    "NullMetrics",
    "Histogram",
    "quantile_ns",
    "BUCKET_BOUNDS_NS",
    "BUCKET_RATIO",
    "HISTOGRAM_QUANTILES",
    "MIN_BUCKET_NS",
    "MAX_BUCKET_NS",
    "escape_label_value",
    "sanitize_label_name",
    "sanitize_metric_name",
]
