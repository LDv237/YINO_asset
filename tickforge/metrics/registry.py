"""Thread-safe metrics registry with Prometheus text rendering.

Metric identity is ``name`` plus a canonical, sorted label set.  The canonical
key format matches the frozen interface: ``name{k="v",k2="v2"}`` with label
keys sorted lexicographically.  Label values are escaped (``\\``, ``"`` and
newline) inside the key so distinct label sets can never collide.

The registry is intentionally dependency-free and cheap on the hot path: a
single short-lived lock protects the registry dictionary, while each metric
object owns a small lock for its value.  The event-loop thread, the WAL writer
thread and the observability server may therefore share one registry safely.

Latency histograms are exported as Prometheus summaries with quantiles
0.5/0.9/0.99/0.999 plus ``_count`` and ``_sum``.  See
:mod:`tickforge.metrics.histogram` for the conservative quantile semantics.
"""

from __future__ import annotations

import math
import re
import threading
from collections.abc import Mapping
from dataclasses import dataclass
from typing import Any

from .histogram import HISTOGRAM_QUANTILES, Histogram

__all__ = [
    "MetricsRegistry",
    "NullMetrics",
    "sanitize_metric_name",
    "sanitize_label_name",
    "escape_label_value",
]

_COUNTER = "counter"
_GAUGE = "gauge"
_SUMMARY = "summary"

_METRIC_INVALID_CHARS = re.compile(r"[^a-zA-Z0-9_:]")
_LABEL_INVALID_CHARS = re.compile(r"[^a-zA-Z0-9_]")

# (quantile, snapshot field) for the fixed histogram quantiles, in order.
_QUANTILE_FIELDS: tuple[tuple[float, str], ...] = (
    (0.5, "p50_ns"),
    (0.9, "p90_ns"),
    (0.99, "p99_ns"),
    (0.999, "p999_ns"),
)
assert tuple(q for q, _ in _QUANTILE_FIELDS) == HISTOGRAM_QUANTILES


def escape_label_value(value: str) -> str:
    """Escape a label value for the Prometheus text exposition format.

    Per the text format specification only backslash, double quote and line
    feed have defined escape sequences.
    """

    return value.replace("\\", "\\\\").replace("\n", "\\n").replace('"', '\\"')


def sanitize_metric_name(name: str) -> str:
    """Coerce ``name`` into a valid Prometheus metric name.

    Invalid characters become underscores; a leading digit is prefixed with
    an underscore.  Colons are retained because they are legal in metric names.
    """

    cleaned = _METRIC_INVALID_CHARS.sub("_", str(name))
    if not cleaned:
        return "_"
    if cleaned[0].isdigit():
        return "_" + cleaned
    return cleaned


def sanitize_label_name(name: str) -> str:
    """Coerce ``name`` into a valid Prometheus label name (no colons)."""

    cleaned = _LABEL_INVALID_CHARS.sub("_", str(name))
    if not cleaned:
        return "_"
    if cleaned[0].isdigit():
        return "_" + cleaned
    return cleaned


def _validate_metric_name(name: str) -> None:
    if not isinstance(name, str):
        raise TypeError(f"metric name must be str, got {type(name).__name__}")
    if not name:
        raise ValueError("metric name must not be empty")


def _normalize_labels(labels: Mapping[str, str] | None) -> tuple[tuple[str, str], ...]:
    """Return a sorted, immutable copy of ``labels`` with string values."""

    if labels is None:
        return ()
    if not isinstance(labels, Mapping):
        raise TypeError(f"labels must be a mapping or None, got {type(labels).__name__}")
    pairs: list[tuple[str, str]] = []
    for key, value in labels.items():
        if not isinstance(key, str):
            raise TypeError(f"label name must be str, got {type(key).__name__}")
        if not key:
            raise ValueError("label name must not be empty")
        pairs.append((key, str(value)))
    pairs.sort(key=lambda item: item[0])
    return tuple(pairs)


def _canonical_key(name: str, labels: tuple[tuple[str, str], ...]) -> str:
    """Build the canonical identity key for a metric and label set."""

    if not labels:
        return name
    inner = ",".join(
        f'{key}="{escape_label_value(value)}"' for key, value in labels
    )
    return f"{name}{{{inner}}}"


def _format_value(value: float) -> str:
    """Format a metric value for the Prometheus text format."""

    number = float(value)
    if math.isnan(number):
        return "NaN"
    if math.isinf(number):
        return "+Inf" if number > 0 else "-Inf"
    return repr(number)


class _Counter:
    """A monotonically increasing float protected by its own lock."""

    __slots__ = ("_lock", "_value")

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._value = 0.0

    def add(self, value: float) -> None:
        with self._lock:
            self._value += float(value)

    def get(self) -> float:
        with self._lock:
            return self._value


class _Gauge:
    """A last-write-wins float protected by its own lock."""

    __slots__ = ("_lock", "_value")

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._value = 0.0

    def set(self, value: float) -> None:
        with self._lock:
            self._value = float(value)

    def get(self) -> float:
        with self._lock:
            return self._value


@dataclass(frozen=True, slots=True)
class _Entry:
    """One registered metric instance (one base name plus one label set)."""

    kind: str
    name: str
    labels: tuple[tuple[str, str], ...]
    metric: _Counter | _Gauge | Histogram


def _new_metric(kind: str) -> _Counter | _Gauge | Histogram:
    if kind == _COUNTER:
        return _Counter()
    if kind == _GAUGE:
        return _Gauge()
    if kind == _SUMMARY:
        return Histogram()
    raise ValueError(f"unknown metric kind: {kind!r}")


class MetricsRegistry:
    """Thread-safe registry for counters, gauges and nanosecond histograms."""

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._entries: dict[str, _Entry] = {}
        # Metric type is fixed per base name, independent of the label set,
        # so Prometheus never sees conflicting TYPE declarations.
        self._kinds: dict[str, str] = {}

    # ------------------------------------------------------------------
    # Registration helpers
    # ------------------------------------------------------------------
    def _metric_for(
        self,
        name: str,
        kind: str,
        labels: Mapping[str, str] | None,
    ) -> _Counter | _Gauge | Histogram:
        _validate_metric_name(name)
        pairs = _normalize_labels(labels)
        key = _canonical_key(name, pairs)
        with self._lock:
            entry = self._entries.get(key)
            if entry is not None:
                if entry.kind != kind:
                    raise ValueError(
                        f"metric {name!r} is already registered as "
                        f"{entry.kind}, cannot use it as {kind}"
                    )
                return entry.metric
            declared = self._kinds.get(name)
            if declared is not None and declared != kind:
                raise ValueError(
                    f"metric {name!r} is already registered as {declared}, "
                    f"cannot register it as {kind}"
                )
            metric = _new_metric(kind)
            self._kinds.setdefault(name, kind)
            self._entries[key] = _Entry(kind, name, pairs, metric)
            return metric

    def _lookup(
        self, name: str, labels: Mapping[str, str] | None
    ) -> _Entry | None:
        _validate_metric_name(name)
        pairs = _normalize_labels(labels)
        key = _canonical_key(name, pairs)
        with self._lock:
            return self._entries.get(key)

    # ------------------------------------------------------------------
    # Public write API
    # ------------------------------------------------------------------
    def inc(
        self,
        name: str,
        labels: dict[str, str] | None = None,
        value: float = 1.0,
    ) -> None:
        """Increment counter ``name{labels}`` by ``value`` (default 1.0)."""

        metric = self._metric_for(name, _COUNTER, labels)
        metric.add(value)  # type: ignore[union-attr]

    def set_gauge(
        self,
        name: str,
        value: float,
        labels: dict[str, str] | None = None,
    ) -> None:
        """Set gauge ``name{labels}`` to ``value``."""

        metric = self._metric_for(name, _GAUGE, labels)
        metric.set(value)  # type: ignore[union-attr]

    def observe_ns(
        self,
        name: str,
        value_ns: int,
        labels: dict[str, str] | None = None,
    ) -> None:
        """Record one nanosecond observation into histogram ``name{labels}``."""

        metric = self._metric_for(name, _SUMMARY, labels)
        metric.observe(value_ns)  # type: ignore[union-attr]

    # ------------------------------------------------------------------
    # Public read API
    # ------------------------------------------------------------------
    def counter_value(
        self, name: str, labels: dict[str, str] | None = None
    ) -> float:
        """Return the counter value, or ``0.0`` if it has not been touched.

        Raises:
            ValueError: If the name is registered as a different metric type.
        """

        entry = self._lookup(name, labels)
        if entry is None:
            return 0.0
        if entry.kind != _COUNTER:
            raise ValueError(
                f"metric {name!r} is a {entry.kind}, not a counter"
            )
        return entry.metric.get()  # type: ignore[union-attr]

    def gauge_value(
        self, name: str, labels: dict[str, str] | None = None
    ) -> float:
        """Return the gauge value, or ``0.0`` if it has not been set.

        Raises:
            ValueError: If the name is registered as a different metric type.
        """

        entry = self._lookup(name, labels)
        if entry is None:
            return 0.0
        if entry.kind != _GAUGE:
            raise ValueError(f"metric {name!r} is a {entry.kind}, not a gauge")
        return entry.metric.get()  # type: ignore[union-attr]

    def snapshot(self) -> dict[str, Any]:
        """Return a consistent snapshot of all metrics.

        Shape::

            {
              "counters": {canonical_key: float, ...},
              "gauges": {canonical_key: float, ...},
              "histograms": {canonical_key: {...}, ...},
            }

        Keys are sorted so repeated snapshots of identical state compare equal.
        """

        with self._lock:
            items = sorted(self._entries.items(), key=lambda item: item[0])
        counters: dict[str, float] = {}
        gauges: dict[str, float] = {}
        histograms: dict[str, dict[str, Any]] = {}
        for key, entry in items:
            if entry.kind == _COUNTER:
                counters[key] = entry.metric.get()  # type: ignore[union-attr]
            elif entry.kind == _GAUGE:
                gauges[key] = entry.metric.get()  # type: ignore[union-attr]
            else:
                histograms[key] = entry.metric.snapshot()  # type: ignore[union-attr]
        return {
            "counters": counters,
            "gauges": gauges,
            "histograms": histograms,
        }

    # ------------------------------------------------------------------
    # Prometheus rendering
    # ------------------------------------------------------------------
    def render_prometheus(self) -> str:
        """Render all metrics in Prometheus text exposition format 0.0.4.

        Each metric family receives exactly one ``# TYPE`` line, regardless of
        how many label sets it has.  Names and label names are sanitized and
        label values are escaped.  Histograms become summaries with quantiles
        0.5/0.9/0.99/0.999 plus ``_count`` and ``_sum`` series.
        """

        with self._lock:
            entries = sorted(
                self._entries.values(),
                key=lambda entry: (
                    sanitize_metric_name(entry.name),
                    entry.labels,
                    entry.kind,
                ),
            )

        lines: list[str] = []
        declared: dict[str, str] = {}
        for entry in entries:
            name = sanitize_metric_name(entry.name)
            if name not in declared:
                declared[name] = entry.kind
                lines.append(f"# TYPE {name} {entry.kind}")

            if entry.kind == _COUNTER:
                value = entry.metric.get()  # type: ignore[union-attr]
                lines.append(_render_sample(name, entry.labels, value))
            elif entry.kind == _GAUGE:
                value = entry.metric.get()  # type: ignore[union-attr]
                lines.append(_render_sample(name, entry.labels, value))
            else:
                snap = entry.metric.snapshot()  # type: ignore[union-attr]
                for quantile, field in _QUANTILE_FIELDS:
                    lines.append(
                        _render_sample(
                            name,
                            entry.labels,
                            snap[field],
                            extra=(("quantile", repr(quantile)),),
                        )
                    )
                lines.append(
                    _render_sample(name + "_count", entry.labels, snap["count"])
                )
                lines.append(
                    _render_sample(name + "_sum", entry.labels, snap["sum_ns"])
                )
        if not lines:
            return ""
        return "\n".join(lines) + "\n"


def _render_sample(
    name: str,
    labels: tuple[tuple[str, str], ...],
    value: float,
    *,
    extra: tuple[tuple[str, str], ...] = (),
) -> str:
    """Render one ``name{labels} value`` line."""

    items = extra + labels
    if not items:
        return f"{name} {_format_value(value)}"
    rendered = ",".join(
        f'{sanitize_label_name(key)}="{escape_label_value(str(val))}"'
        for key, val in items
    )
    return f"{name}{{{rendered}}} {_format_value(value)}"


class NullMetrics:
    """No-op metrics sink exposing the same API as :class:`MetricsRegistry`.

    Useful for tests, benchmarks and replay runs where observability output is
    not needed.  Every method is intentionally side-effect free.
    """

    __slots__ = ()

    def inc(
        self,
        name: str,
        labels: dict[str, str] | None = None,
        value: float = 1.0,
    ) -> None:
        """Discard a counter increment."""

    def set_gauge(
        self,
        name: str,
        value: float,
        labels: dict[str, str] | None = None,
    ) -> None:
        """Discard a gauge update."""

    def observe_ns(
        self,
        name: str,
        value_ns: int,
        labels: dict[str, str] | None = None,
    ) -> None:
        """Discard a histogram observation."""

    def counter_value(
        self, name: str, labels: dict[str, str] | None = None
    ) -> float:
        """Return ``0.0``; nothing is ever recorded."""

        return 0.0

    def gauge_value(
        self, name: str, labels: dict[str, str] | None = None
    ) -> float:
        """Return ``0.0``; nothing is ever recorded."""

        return 0.0

    def snapshot(self) -> dict[str, Any]:
        """Return an empty snapshot with the registry's shape."""

        return {"counters": {}, "gauges": {}, "histograms": {}}

    def render_prometheus(self) -> str:
        """Return an empty exposition body."""

        return ""
