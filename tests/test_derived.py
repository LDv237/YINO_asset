"""Tests for the deterministic JSONL sink used by derived outputs."""

from __future__ import annotations

import hashlib
import threading

import orjson
import pytest

import tickforge.store.derived as derived_module
from tickforge.events import FactorSnapshot, Fill, Signal
from tickforge.store.derived import JsonlSink, SinkResult

EMPTY_SHA256 = hashlib.sha256(b"").hexdigest()


def sample_snapshots(count: int = 50) -> list[dict]:
    """Build deterministic, varied derived records without wall-clock input."""

    out: list[dict] = []
    for i in range(count):
        out.append(
            {
                "symbol": "BTCUSDT" if i % 2 == 0 else "ETHUSDT",
                "ts_ms": 1_700_000_000_000 + i * 100,
                "seq": i + 1,
                "mid": 100.0 + i * 0.25,
                "microprice": 100.01 + i * 0.25,
                "spread_bps": 0.5 + (i % 7) * 0.125,
                "ofi_1s": -1.5 + (i % 5),
                "depth_imbalance_5": (i % 11) / 10.0 - 0.5,
                "trade_imbalance_1s": 0.0 if i % 3 else 0.75,
                "realized_vol_bps": i * 0.01,
                "momentum_5s": -0.002 * (i % 4),
                "best_bid": 99.99 + i * 0.25,
                "best_ask": 100.01 + i * 0.25,
                "bid_qty": 1.23456789,
                "ask_qty": 2.3456789,
                "note": "unicode-é中文",
                "nullable": None if i % 4 == 0 else i,
            }
        )
    return out


# ----------------------------------------------------------------------
# Basic behaviour
# ----------------------------------------------------------------------
def test_write_close_result_and_exact_bytes(tmp_path) -> None:
    path = tmp_path / "derived" / "factors.jsonl"
    sink = JsonlSink(path)
    objects = sample_snapshots(50)
    for obj in objects:
        sink.write(obj)
    result = sink.close()

    assert isinstance(result, SinkResult)
    assert result.path == str(path)
    assert result.records == 50

    raw = path.read_bytes()
    assert result.bytes == len(raw)
    assert result.bytes == sum(len(orjson.dumps(o)) + 1 for o in objects)
    assert result.sha256 == hashlib.sha256(raw).hexdigest()
    assert raw.endswith(b"\n")
    assert raw.count(b"\n") == 50

    lines = raw.splitlines()
    assert len(lines) == 50
    parsed = [orjson.loads(line) for line in lines]
    assert parsed == objects
    # No extra whitespace or pretty printing: each line is the canonical
    # orjson encoding followed by exactly one newline.
    assert lines[0] == orjson.dumps(objects[0])


def test_close_is_idempotent(tmp_path) -> None:
    path = tmp_path / "signals.jsonl"
    sink = JsonlSink(path)
    sink.write({"a": 1})
    first = sink.close()
    second = sink.close()
    assert second is first
    assert second == first
    assert path.read_bytes() == b'{"a":1}\n'

    with pytest.raises(ValueError, match="closed"):
        sink.write({"a": 2})
    # The file must not have been touched by the rejected write.
    assert path.read_bytes() == b'{"a":1}\n'


def test_empty_sink(tmp_path) -> None:
    path = tmp_path / "empty.jsonl"
    result = JsonlSink(path).close()
    assert result.records == 0
    assert result.bytes == 0
    assert result.sha256 == EMPTY_SHA256
    assert path.read_bytes() == b""


# ----------------------------------------------------------------------
# Determinism
# ----------------------------------------------------------------------
def test_identical_input_produces_identical_digest(tmp_path) -> None:
    objects = sample_snapshots(100)

    def write_to(name: str) -> SinkResult:
        sink = JsonlSink(tmp_path / name, flush_interval_s=0, fsync=False)
        for obj in objects:
            sink.write(obj)
        return sink.close()

    a = write_to("a.jsonl")
    b = write_to("b.jsonl")
    assert a.sha256 == b.sha256
    assert a.bytes == b.bytes
    assert a.records == b.records == 100
    # Paths differ but content is byte-for-byte identical.
    assert (tmp_path / "a.jsonl").read_bytes() == (tmp_path / "b.jsonl").read_bytes()


def test_key_order_is_preserved_and_significant(tmp_path) -> None:
    """The digest contract relies on the insertion order of ``to_dict``."""

    ordered = {"symbol": "BTCUSDT", "seq": 7, "mid": 1.5, "extra": True}
    reordered = {"seq": 7, "symbol": "BTCUSDT", "extra": True, "mid": 1.5}

    sink1 = JsonlSink(tmp_path / "ordered.jsonl")
    sink1.write(ordered)
    r1 = sink1.close()

    sink2 = JsonlSink(tmp_path / "reordered.jsonl")
    sink2.write(reordered)
    r2 = sink2.close()

    line1 = (tmp_path / "ordered.jsonl").read_bytes()
    line2 = (tmp_path / "reordered.jsonl").read_bytes()
    assert list(orjson.loads(line1).keys()) == list(ordered.keys())
    assert list(orjson.loads(line2).keys()) == list(reordered.keys())
    assert r1.sha256 != r2.sha256


def test_event_to_dict_digest_is_reproducible(tmp_path) -> None:
    snap = FactorSnapshot(
        symbol="BTCUSDT",
        ts_ms=1_700_000_000_000,
        seq=42,
        mid=100.0,
        microprice=100.01,
        spread_bps=0.5,
        ofi_1s=0.25,
        depth_imbalance_5=-0.1,
        trade_imbalance_1s=0.3,
        realized_vol_bps=1.25,
        momentum_5s=0.001,
        best_bid=99.99,
        best_ask=100.01,
        bid_qty=1.5,
        ask_qty=2.5,
    )
    signal = Signal(
        symbol="BTCUSDT",
        ts_ms=1_700_000_000_100,
        seq=43,
        fair_price=100.005,
        bid_price=99.995,
        bid_qty=5.0,
        ask_price=100.015,
        ask_qty=5.0,
        reason="mm",
    )
    fill = Fill(
        symbol="BTCUSDT",
        ts_ms=1_700_000_000_200,
        trade_id=123456,
        side="BUY",
        price=99.995,
        qty=1.0,
        fee=0.00099,
        position=1.0,
        cash=-99.996,
        realized_pnl=0.0,
        reason="maker",
    )

    def run(name: str) -> SinkResult:
        sink = JsonlSink(tmp_path / name)
        for event in (snap, signal, fill):
            sink.write(event.to_dict())
        return sink.close()

    first = run("run1.jsonl")
    second = run("run2.jsonl")
    assert first.sha256 == second.sha256
    lines = (tmp_path / "run1.jsonl").read_bytes().splitlines()
    assert [list(orjson.loads(line).keys()) for line in lines] == [
        list(snap.to_dict().keys()),
        list(signal.to_dict().keys()),
        list(fill.to_dict().keys()),
    ]


# ----------------------------------------------------------------------
# Flushing and fsync
# ----------------------------------------------------------------------
def test_flush_interval_controls_visibility(tmp_path) -> None:
    buffered_path = tmp_path / "buffered.jsonl"
    eager_path = tmp_path / "eager.jsonl"

    buffered = JsonlSink(buffered_path, flush_interval_s=3600.0)
    eager = JsonlSink(eager_path, flush_interval_s=0.0)
    obj = {"symbol": "SOLUSDT", "seq": 1}
    line_len = len(orjson.dumps(obj)) + 1

    buffered.write(obj)
    eager.write(obj)
    assert buffered_path.stat().st_size == 0  # still in the user-space buffer
    assert eager_path.stat().st_size == line_len

    assert buffered.close().bytes == line_len
    assert eager.close().bytes == line_len
    assert buffered_path.read_bytes() == eager_path.read_bytes()


def test_fsync_flag_controls_os_fsync(tmp_path, monkeypatch) -> None:
    calls: list[int] = []
    real_fsync = derived_module.os.fsync

    def counting_fsync(fd: int) -> None:
        calls.append(fd)
        real_fsync(fd)

    monkeypatch.setattr(derived_module.os, "fsync", counting_fsync)

    sink = JsonlSink(tmp_path / "synced.jsonl", flush_interval_s=0.0, fsync=True)
    for i in range(3):
        sink.write({"i": i})
    assert len(calls) == 3
    sink.close()
    assert len(calls) == 4  # close always fsyncs when configured

    unsynced = JsonlSink(tmp_path / "unsynced.jsonl", flush_interval_s=0.0, fsync=False)
    unsynced.write({"i": 0})
    unsynced.close()
    assert len(calls) == 4  # unchanged

    # A failing flush/fsync inside close() must propagate, not return a
    # SinkResult for a file whose durability is unknown.
    failing = JsonlSink(
        tmp_path / "failing.jsonl", flush_interval_s=3600.0, fsync=True
    )
    failing.write({"i": 0})

    def broken_fsync(fd: int) -> None:
        raise OSError("simulated fsync failure")

    monkeypatch.setattr(derived_module.os, "fsync", broken_fsync)
    with pytest.raises(OSError, match="simulated fsync failure"):
        failing.close()
    assert failing._result is None


def test_serialization_error_does_not_corrupt_stream(tmp_path) -> None:
    path = tmp_path / "partial.jsonl"
    sink = JsonlSink(path, flush_interval_s=0.0)
    with pytest.raises(TypeError):
        sink.write({"bad": object()})
    assert path.stat().st_size == 0
    sink.write({"good": 1})
    result = sink.close()
    assert result.records == 1
    assert path.read_bytes() == b'{"good":1}\n'
    assert result.sha256 == hashlib.sha256(b'{"good":1}\n').hexdigest()


def test_unicode_and_control_characters_roundtrip(tmp_path) -> None:
    path = tmp_path / "unicode.jsonl"
    obj = {"symbol": "中文", "text": "line\nbreak\tand\\slash", "emoji": "\U0001f600"}
    sink = JsonlSink(path)
    sink.write(obj)
    result = sink.close()
    raw = path.read_bytes()
    assert b"\n" not in raw[:-1]  # embedded newline is escaped
    assert orjson.loads(raw) == obj
    assert result.records == 1


# ----------------------------------------------------------------------
# Edge cases and property-style checks
# ----------------------------------------------------------------------
def test_parent_directories_are_created(tmp_path) -> None:
    path = tmp_path / "a" / "b" / "c.jsonl"
    sink = JsonlSink(path)
    sink.write({"x": 1})
    sink.close()
    assert path.is_file()


def test_invalid_flush_interval_rejected(tmp_path) -> None:
    with pytest.raises(ValueError):
        JsonlSink(tmp_path / "x.jsonl", flush_interval_s=-0.1)


def test_context_manager_closes(tmp_path) -> None:
    path = tmp_path / "ctx.jsonl"
    with JsonlSink(path) as sink:
        sink.write({"x": 1})
    assert path.read_bytes() == b'{"x":1}\n'


def test_property_roundtrip_arbitrary_nested_documents(tmp_path) -> None:
    """Many deterministic shapes must round-trip line by line."""

    docs: list[dict] = []
    for i in range(300):
        docs.append(
            {
                "seq": i,
                "scalar_int": (i * 37) % 1000 - 500,
                "scalar_float": (i - 150) / 8.0,
                "flag": i % 2 == 0,
                "nothing": None,
                "array": [i, i + 1, [i, {"deep": i % 5}]],
                "object": {"k": "v", "n": {"x": i, "y": -i}},
                "text": "row-%d-é" % i,
            }
        )
    sink = JsonlSink(tmp_path / "props.jsonl", flush_interval_s=0)
    for doc in docs:
        sink.write(doc)
    result = sink.close()

    raw = (tmp_path / "props.jsonl").read_bytes()
    assert result.sha256 == hashlib.sha256(raw).hexdigest()
    assert result.bytes == len(raw)
    assert result.records == 300
    assert [orjson.loads(line) for line in raw.splitlines()] == docs
