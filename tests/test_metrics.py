"""Unit and property-style tests for the metrics registry and histograms."""

from __future__ import annotations

import math
import random
import re
import threading
from concurrent.futures import ThreadPoolExecutor

import pytest

from tickforge.metrics import (
    BUCKET_BOUNDS_NS,
    HISTOGRAM_QUANTILES,
    MAX_BUCKET_NS,
    MIN_BUCKET_NS,
    Histogram,
    MetricsRegistry,
    NullMetrics,
    escape_label_value,
    quantile_ns,
    sanitize_label_name,
    sanitize_metric_name,
)
from tickforge.metrics.histogram import BUCKET_RATIO

# ---------------------------------------------------------------------------
# Exposition parser helpers
# ---------------------------------------------------------------------------

_SAMPLE_RE = re.compile(
    r"^(?P<name>[a-zA-Z_:][a-zA-Z0-9_:]*)"
    r"(?:\{(?P<labels>[^}]*)\})? (?P<value>\S+)$"
)
_LABEL_PAIR_RE = re.compile(
    r'([a-zA-Z_][a-zA-Z0-9_]*)="((?:\\.|[^"\\])*)"(?:,|$)'
)


def _unescape_label(raw: str) -> str:
    out: list[str] = []
    i = 0
    while i < len(raw):
        ch = raw[i]
        if ch == "\\" and i + 1 < len(raw):
            nxt = raw[i + 1]
            if nxt == "n":
                out.append("\n")
            elif nxt == "\\":
                out.append("\\")
            elif nxt == '"':
                out.append('"')
            else:  # pragma: no cover - not produced by our renderer
                out.append("\\" + nxt)
            i += 2
        else:
            out.append(ch)
            i += 1
    return "".join(out)


def parse_exposition(
    text: str,
) -> tuple[dict[str, str], list[tuple[str, dict[str, str], str]]]:
    """Minimal Prometheus 0.0.4 parser used to validate our renderer.

    Returns the TYPE families and the parsed samples.  Also asserts that
    comments are well formed and that no family declares its type twice.
    """

    families: dict[str, str] = {}
    samples: list[tuple[str, dict[str, str], str]] = []
    for line in text.splitlines():
        if not line:
            continue
        if line.startswith("# TYPE "):
            parts = line.split()
            assert len(parts) == 4, f"malformed TYPE line: {line!r}"
            name, kind = parts[2], parts[3]
            assert kind in {"counter", "gauge", "summary"}, line
            assert name not in families, f"duplicate TYPE line for {name}"
            families[name] = kind
            continue
        assert not line.startswith("#"), f"unexpected comment: {line!r}"
        match = _SAMPLE_RE.match(line)
        assert match is not None, f"unparseable sample line: {line!r}"
        raw_labels = match.group("labels")
        labels: dict[str, str] = {}
        if raw_labels:
            pos = 0
            while pos < len(raw_labels):
                pair = _LABEL_PAIR_RE.match(raw_labels, pos)
                assert pair is not None, f"bad label syntax: {raw_labels!r}"
                assert pair.group(1) not in labels, "duplicate label"
                labels[pair.group(1)] = _unescape_label(pair.group(2))
                pos = pair.end()
        samples.append((match.group("name"), labels, match.group("value")))
    return families, samples


# ---------------------------------------------------------------------------
# Bucket layout and histogram semantics
# ---------------------------------------------------------------------------


def test_bucket_bounds_are_fixed_log_scale() -> None:
    bounds = BUCKET_BOUNDS_NS
    assert isinstance(bounds, tuple)
    assert len(bounds) >= 50
    assert bounds[0] == MIN_BUCKET_NS == 1_000
    assert bounds[-1] == MAX_BUCKET_NS == 60_000_000_000
    assert all(lo < hi for lo, hi in zip(bounds, bounds[1:]))
    assert BUCKET_RATIO > 1.0
    # Consecutive bounds should not be more than one ratio step apart (apart
    # from integer rounding), i.e. this really is a log-scale bucket layout.
    for lo, hi in zip(bounds[:-1], bounds[1:]):
        # Integer rounding of the bounds may add up to a fraction of a
        # nanosecond; allow a 1% tolerance around the ideal ratio.
        assert hi / lo <= BUCKET_RATIO * 1.01
    # The full range 1us .. 60s is covered.
    assert bounds[0] <= 1_000
    assert bounds[-1] >= 60_000_000_000


def test_empty_histogram_snapshot() -> None:
    snap = Histogram().snapshot()
    assert set(snap) == {
        "count",
        "min_ns",
        "max_ns",
        "sum_ns",
        "mean_ns",
        "p50_ns",
        "p90_ns",
        "p95_ns",
        "p99_ns",
        "p999_ns",
        "buckets",
    }
    assert snap["count"] == 0
    assert snap["min_ns"] == 0
    assert snap["max_ns"] == 0
    assert snap["sum_ns"] == 0
    assert snap["mean_ns"] == 0.0
    for key in ("p50_ns", "p90_ns", "p95_ns", "p99_ns", "p999_ns"):
        assert snap[key] == 0
    assert len(snap["buckets"]) == len(BUCKET_BOUNDS_NS)
    assert all(bucket["count"] == 0 for bucket in snap["buckets"])
    assert [bucket["le"] for bucket in snap["buckets"]] == list(BUCKET_BOUNDS_NS)


def test_histogram_aggregates_and_percentile_ordering() -> None:
    hist = Histogram()
    values = [1_000, 1_500, 2_000, 5_000, 20_000, 1_000_000, 59_000_000_000]
    for value in values:
        hist.observe(value)

    snap = hist.snapshot()
    assert snap["count"] == len(values)
    assert snap["min_ns"] == min(values)
    assert snap["max_ns"] == max(values)
    assert snap["sum_ns"] == sum(values)
    assert snap["mean_ns"] == pytest.approx(sum(values) / len(values))

    quantiles = [
        snap["p50_ns"],
        snap["p90_ns"],
        snap["p95_ns"],
        snap["p99_ns"],
        snap["p999_ns"],
    ]
    assert quantiles == sorted(quantiles)
    for reported in quantiles:
        assert snap["min_ns"] <= reported <= MAX_BUCKET_NS
    # Conservative: each reported quantile is at least the true nearest-rank
    # sample it represents.
    ordered = sorted(values)
    for q, reported in zip(HISTOGRAM_QUANTILES, quantiles):
        rank = max(1, math.ceil(q * len(ordered)))
        assert reported >= ordered[rank - 1]

    buckets = snap["buckets"]
    assert all(
        left["count"] <= right["count"]
        for left, right in zip(buckets, buckets[1:])
    )
    assert buckets[-1]["count"] == snap["count"]


def test_histogram_overflow_and_negative_observations() -> None:
    hist = Histogram()
    huge = 10**13  # 10,000 seconds, far above the 60s ceiling
    hist.observe(huge)
    hist.observe(-42)
    snap = hist.snapshot()
    assert snap["count"] == 2
    assert snap["min_ns"] == 0  # negatives are clamped
    assert snap["max_ns"] == huge
    assert snap["sum_ns"] == huge
    assert snap["buckets"][-1]["count"] == 2
    assert snap["p50_ns"] <= max(MAX_BUCKET_NS, huge)
    assert snap["p999_ns"] == MAX_BUCKET_NS


def test_histogram_quantile_property_randomized() -> None:
    rng = random.Random(20240607)
    hist = Histogram()
    values: list[int] = []
    for _ in range(3_000):
        value = int(10 ** rng.uniform(3, 10.7))  # ~1us .. ~50s
        values.append(value)
        hist.observe(value)

    snap = hist.snapshot()
    assert snap["count"] == len(values)
    assert snap["min_ns"] == min(values)
    assert snap["max_ns"] == max(values)
    assert snap["sum_ns"] == sum(values)
    ordered = sorted(values)
    reported = [
        snap[k] for k in ("p50_ns", "p90_ns", "p95_ns", "p99_ns", "p999_ns")
    ]
    assert reported == sorted(reported)
    for q, value in zip(HISTOGRAM_QUANTILES, reported):
        rank = max(1, math.ceil(q * len(ordered)))
        assert value >= ordered[rank - 1]
        # Not absurdly conservative: within roughly one bucket ratio.
        assert value <= max(BUCKET_BOUNDS_NS[0], ordered[rank - 1] * BUCKET_RATIO * 1.05)
    assert snap["buckets"][-1]["count"] == snap["count"]


def test_quantile_ns_validation_and_empty() -> None:
    empty_counts = [0] * len(BUCKET_BOUNDS_NS)
    assert quantile_ns(empty_counts, 0, 0.5) == 0
    with pytest.raises(ValueError):
        quantile_ns(empty_counts, 0, 0.0)
    with pytest.raises(ValueError):
        quantile_ns(empty_counts, 0, 1.01)
    assert quantile_ns([1] + [0] * (len(BUCKET_BOUNDS_NS) - 1), 1, 1.0) == (
        BUCKET_BOUNDS_NS[0]
    )


def test_histogram_is_thread_safe() -> None:
    registry = MetricsRegistry()
    threads = 8
    per_thread = 500
    barrier = threading.Barrier(threads)

    def worker(worker_id: int) -> None:
        barrier.wait()
        for index in range(per_thread):
            registry.observe_ns("tickforge_latency_test_ns", (worker_id * 1_000 + index) * 10)

    with ThreadPoolExecutor(max_workers=threads) as pool:
        futures = [pool.submit(worker, worker_id) for worker_id in range(threads)]
        for future in futures:
            future.result()

    expected_count = threads * per_thread
    expected_sum = sum(
        (worker_id * 1_000 + index) * 10
        for worker_id in range(threads)
        for index in range(per_thread)
    )
    snap = registry.snapshot()["histograms"]["tickforge_latency_test_ns"]
    assert snap["count"] == expected_count
    assert snap["sum_ns"] == expected_sum
    assert snap["mean_ns"] == pytest.approx(expected_sum / expected_count)


# ---------------------------------------------------------------------------
# Registry semantics
# ---------------------------------------------------------------------------


def test_label_key_canonicalization() -> None:
    registry = MetricsRegistry()
    registry.inc("requests_total", {"b": "2", "a": "1"})
    registry.inc("requests_total", {"a": "1", "b": "2"}, value=2.5)
    registry.inc("requests_total", {"a": "1", "b": "2"}, 0.5)

    assert registry.counter_value("requests_total", {"b": "2", "a": "1"}) == 4.0
    assert registry.snapshot()["counters"] == {
        'requests_total{a="1",b="2"}': 4.0
    }

    # An empty mapping is identical to no labels.
    registry.inc("plain_total")
    registry.inc("plain_total", {})
    assert registry.counter_value("plain_total") == 2.0
    assert registry.counter_value("plain_total", {}) == 2.0
    assert registry.snapshot()["counters"]["plain_total"] == 2.0


def test_label_values_are_escaped_in_keys() -> None:
    registry = MetricsRegistry()
    labels_a = {"k": 'a"b'}
    labels_b = {"k": "a\\b"}
    registry.inc("special_total", labels_a)
    registry.inc("special_total", labels_b)
    snapshot = registry.snapshot()["counters"]
    assert len(snapshot) == 2
    assert registry.counter_value("special_total", labels_a) == 1.0
    assert registry.counter_value("special_total", labels_b) == 1.0


def test_gauge_set_and_missing_values() -> None:
    registry = MetricsRegistry()
    assert registry.counter_value("does_not_exist") == 0.0
    assert registry.gauge_value("does_not_exist", {"a": "b"}) == 0.0

    registry.set_gauge("queue_depth", 3)
    registry.set_gauge("queue_depth", 7.5)
    assert registry.gauge_value("queue_depth") == 7.5
    registry.set_gauge("queue_depth", 1, {"symbol": "BTCUSDT"})
    assert registry.gauge_value("queue_depth", {"symbol": "BTCUSDT"}) == 1.0
    assert registry.gauge_value("queue_depth") == 7.5


def test_metric_type_is_stable_per_name() -> None:
    registry = MetricsRegistry()
    registry.inc("shared_name")
    with pytest.raises(ValueError):
        registry.set_gauge("shared_name", 1)
    with pytest.raises(ValueError):
        registry.set_gauge("shared_name", 1, {"symbol": "BTCUSDT"})
    with pytest.raises(ValueError):
        registry.observe_ns("shared_name", 1_000)
    with pytest.raises(ValueError):
        registry.gauge_value("shared_name")


def test_invalid_metric_and_label_names() -> None:
    registry = MetricsRegistry()
    with pytest.raises(ValueError):
        registry.inc("")
    with pytest.raises(TypeError):
        registry.inc(None)  # type: ignore[arg-type]
    with pytest.raises(ValueError):
        registry.inc("x_total", {"": "v"})
    with pytest.raises(TypeError):
        registry.inc("x_total", {1: "v"})  # type: ignore[dict-item]
    with pytest.raises(TypeError):
        registry.inc("x_total", ["not", "a", "mapping"])  # type: ignore[arg-type]


def test_concurrent_counter_increments_from_threads() -> None:
    registry = MetricsRegistry()
    threads = 8
    per_thread = 1_000
    barrier = threading.Barrier(threads)

    def worker(worker_id: int) -> None:
        barrier.wait()
        for _ in range(per_thread):
            registry.inc("tickforge_pipeline_events_total")
            registry.inc("tickforge_fills_total", {"symbol": f"S{worker_id % 2}"})

    with ThreadPoolExecutor(max_workers=threads) as pool:
        futures = [pool.submit(worker, worker_id) for worker_id in range(threads)]
        for future in futures:
            future.result()

    assert registry.counter_value("tickforge_pipeline_events_total") == threads * per_thread
    assert registry.counter_value(
        "tickforge_fills_total", {"symbol": "S0"}
    ) == (threads // 2) * per_thread
    assert registry.counter_value(
        "tickforge_fills_total", {"symbol": "S1"}
    ) == (threads // 2) * per_thread


def test_snapshot_is_isolated_and_deterministic() -> None:
    first = MetricsRegistry()
    second = MetricsRegistry()
    operations = [
        ("counter", "b_total", {"x": "1"}, 2.0),
        ("counter", "a_total", None, 1.0),
        ("gauge", "z_gauge", None, 9.0),
        ("hist", "y_latency_ns", {"symbol": "BTCUSDT"}, 1_234),
    ]
    for kind, name, labels, value in operations:
        if kind == "counter":
            first.inc(name, labels, value)
        elif kind == "gauge":
            first.set_gauge(name, value, labels)
        else:
            first.observe_ns(name, value, labels)
    for kind, name, labels, value in reversed(operations):
        if kind == "counter":
            second.inc(name, labels, value)
        elif kind == "gauge":
            second.set_gauge(name, value, labels)
        else:
            second.observe_ns(name, value, labels)

    snapshot = first.snapshot()
    assert list(snapshot["counters"]) == ["a_total", 'b_total{x="1"}']
    assert list(snapshot["gauges"]) == ["z_gauge"]
    assert list(snapshot["histograms"]) == ['y_latency_ns{symbol="BTCUSDT"}']
    assert snapshot == second.snapshot()

    # Mutating the returned snapshot must not affect registry state.
    snapshot["counters"]["a_total"] = 999.0
    snapshot["histograms"]['y_latency_ns{symbol="BTCUSDT"}']["count"] = 999
    assert first.counter_value("a_total") == 1.0
    assert first.snapshot()["histograms"]['y_latency_ns{symbol="BTCUSDT"}']["count"] == 1


# ---------------------------------------------------------------------------
# Prometheus rendering
# ---------------------------------------------------------------------------


def test_prometheus_output_is_parseable_and_deduplicates_types() -> None:
    registry = MetricsRegistry()
    registry.inc("tickforge_ws_messages_total", None, 5)
    registry.inc("tickforge_ws_messages_total", {"symbol": "BTCUSDT"}, 3)
    registry.inc("tickforge_ws_messages_total", {"symbol": "ETHUSDT"}, 2)
    registry.set_gauge("tickforge_wal_queue_depth", 7)
    registry.set_gauge("tickforge_book_buffered", 2, {"symbol": "BTCUSDT"})
    registry.observe_ns("tickforge_latency_parse_ns", 500_000, {"symbol": "BTCUSDT"})
    registry.observe_ns("tickforge_latency_parse_ns", 1_500_000, {"symbol": "BTCUSDT"})

    text = registry.render_prometheus()
    assert text.endswith("\n")
    families, samples = parse_exposition(text)

    assert families == {
        "tickforge_book_buffered": "gauge",
        "tickforge_latency_parse_ns": "summary",
        "tickforge_wal_queue_depth": "gauge",
        "tickforge_ws_messages_total": "counter",
    }
    assert text.count("# TYPE tickforge_ws_messages_total counter") == 1
    assert text.count("# TYPE tickforge_latency_parse_ns summary") == 1

    by_name: dict[tuple[str, tuple[tuple[str, str], ...]], float] = {}
    for name, labels, raw_value in samples:
        key = (name, tuple(sorted(labels.items())))
        assert key not in by_name, f"duplicate sample {key}"
        by_name[key] = float(raw_value)

    def sample(name: str, **labels: str) -> float:
        return by_name[(name, tuple(sorted(labels.items())))]

    assert sample("tickforge_ws_messages_total") == 5.0
    assert sample("tickforge_ws_messages_total", symbol="BTCUSDT") == 3.0
    assert sample("tickforge_ws_messages_total", symbol="ETHUSDT") == 2.0
    assert sample("tickforge_wal_queue_depth") == 7.0
    assert sample("tickforge_book_buffered", symbol="BTCUSDT") == 2.0

    for quantile in ("0.5", "0.9", "0.99", "0.999"):
        assert sample(
            "tickforge_latency_parse_ns", quantile=quantile, symbol="BTCUSDT"
        ) >= 0
    assert sample("tickforge_latency_parse_ns_count", symbol="BTCUSDT") == 2.0
    assert sample("tickforge_latency_parse_ns_sum", symbol="BTCUSDT") == 2_000_000.0


def test_prometheus_sanitizes_names_and_escapes_values() -> None:
    registry = MetricsRegistry()
    registry.inc("my.bad-name", {"label.bad": 'a"b\\c\nd'})
    registry.inc("9lives_total")

    text = registry.render_prometheus()
    assert 'my_bad_name{label_bad="a\\"b\\\\c\\nd"} 1.0' in text
    assert "# TYPE _9lives_total counter" in text
    families, samples = parse_exposition(text)
    assert "my_bad_name" in families
    assert "_9lives_total" in families
    parsed = {name: labels for name, labels, _ in samples}
    assert parsed["my_bad_name"]["label_bad"] == 'a"b\\c\nd'

    assert sanitize_metric_name("valid:name") == "valid:name"
    assert sanitize_metric_name("a-b.c d") == "a_b_c_d"
    assert sanitize_metric_name("1st") == "_1st"
    assert sanitize_metric_name("") == "_"
    assert sanitize_label_name("bad-label") == "bad_label"
    assert sanitize_label_name("9label") == "_9label"
    assert sanitize_label_name("what:ever") == "what_ever"
    assert escape_label_value('a"b\\c\nd') == 'a\\"b\\\\c\\nd'


def test_prometheus_histogram_quantiles_match_snapshot() -> None:
    registry = MetricsRegistry()
    for value in (1_000, 10_000, 100_000, 1_000_000):
        registry.observe_ns("lat_ns", value)
    snap = registry.snapshot()["histograms"]["lat_ns"]
    families, samples = parse_exposition(registry.render_prometheus())
    assert families["lat_ns"] == "summary"

    values: dict[str, float] = {}
    for name, labels, raw in samples:
        if name == "lat_ns":
            values[labels["quantile"]] = float(raw)
    assert values == {
        "0.5": float(snap["p50_ns"]),
        "0.9": float(snap["p90_ns"]),
        "0.95": float(snap["p95_ns"]),
        "0.99": float(snap["p99_ns"]),
        "0.999": float(snap["p999_ns"]),
    }
    assert float(
        next(raw for name, _, raw in samples if name == "lat_ns_count")
    ) == float(snap["count"])
    assert float(
        next(raw for name, _, raw in samples if name == "lat_ns_sum")
    ) == float(snap["sum_ns"])


def test_empty_registry_renders_empty_string() -> None:
    registry = MetricsRegistry()
    assert registry.render_prometheus() == ""
    assert registry.snapshot() == {"counters": {}, "gauges": {}, "histograms": {}}


def test_prometheus_handles_non_finite_values() -> None:
    registry = MetricsRegistry()
    registry.set_gauge("nan_gauge", float("nan"))
    registry.set_gauge("pos_inf_gauge", float("inf"))
    registry.set_gauge("neg_inf_gauge", float("-inf"))
    text = registry.render_prometheus()
    _, samples = parse_exposition(text)
    values = {name: raw for name, _, raw in samples}
    assert values["nan_gauge"] == "NaN"
    assert values["pos_inf_gauge"] == "+Inf"
    assert values["neg_inf_gauge"] == "-Inf"


# ---------------------------------------------------------------------------
# NullMetrics
# ---------------------------------------------------------------------------


def test_null_metrics_is_a_no_op_with_the_same_api() -> None:
    null = NullMetrics()
    null.inc("a_total")
    null.inc("a_total", {"x": "y"}, 5.0)
    null.set_gauge("depth", 42)
    null.set_gauge("depth", 1, {"symbol": "BTCUSDT"})
    null.observe_ns("lat_ns", 123_456)
    null.observe_ns("lat_ns", 999, {"symbol": "BTCUSDT"})

    assert null.counter_value("a_total") == 0.0
    assert null.counter_value("a_total", {"x": "y"}) == 0.0
    assert null.gauge_value("depth") == 0.0
    assert null.gauge_value("depth", {"symbol": "BTCUSDT"}) == 0.0
    assert null.snapshot() == {"counters": {}, "gauges": {}, "histograms": {}}
    assert null.render_prometheus() == ""
