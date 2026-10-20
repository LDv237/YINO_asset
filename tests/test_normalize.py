from __future__ import annotations

import orjson
import pytest

from tickforge.events import BookTickerEvent, DepthEvent, SnapshotEvent, TradeEvent
from tickforge.normalize import (
    ParseError,
    decode_snapshot,
    encode_snapshot,
    parse_ws_message,
    str_to_ticks,
    ticks_to_str,
)


def test_tick_conversion_roundtrip() -> None:
    cases = {
        "0.00000001": 1,
        "1.00000000": 100_000_000,
        "81574.38000000": 8_157_438_000_000,
        "0.1": 10_000_000,
        "123": 12_300_000_000,
        "-0.00000002": -2,
    }
    for text, ticks in cases.items():
        assert str_to_ticks(text) == ticks
        if ticks >= 0:
            assert str_to_ticks(ticks_to_str(ticks)) == ticks


@pytest.mark.parametrize("bad", ["", "1.2.3", "abc", "1.", "0.123456789", " "])
def test_tick_conversion_rejects_invalid(bad: str) -> None:
    with pytest.raises(ParseError):
        str_to_ticks(bad)


def test_depth_parse_combined_frame() -> None:
    frame = {
        "stream": "btcusdt@depth@100ms",
        "data": {
            "e": "depthUpdate",
            "E": 1700000000123,
            "s": "BTCUSDT",
            "U": 100,
            "u": 105,
            "b": [["100.10", "1.5"], ["99.90", "0"]],
            "a": [["100.20", "2.0"]],
        },
    }
    ev = parse_ws_message(
        orjson.dumps(frame), seq=7, recv_mono_ns=10, recv_wall_ns=20
    )
    assert isinstance(ev, DepthEvent)
    assert ev.symbol == "BTCUSDT"
    assert ev.first_update_id == 100
    assert ev.final_update_id == 105
    assert ev.bids == ((10_010_000_000, 150_000_000), (9_990_000_000, 0))
    assert ev.asks == ((10_020_000_000, 200_000_000),)
    assert ev.seq == 7 and ev.recv_mono_ns == 10 and ev.recv_wall_ns == 20


def test_trade_parse_and_side() -> None:
    data = {
        "e": "aggTrade",
        "E": 1700000001000,
        "s": "BTCUSDT",
        "a": 42,
        "p": "100.25",
        "q": "0.5",
        "m": True,
    }
    frame = {"stream": "btcusdt@aggTrade", "data": data}
    ev = parse_ws_message(orjson.dumps(frame), seq=1, recv_mono_ns=2, recv_wall_ns=3)
    assert isinstance(ev, TradeEvent)
    assert ev.agg_trade_id == 42
    assert ev.price == 100.25
    assert ev.qty == 0.5
    assert ev.aggressor_side == "sell"


def test_book_ticker_parse() -> None:
    frame = {
        "stream": "btcusdt@bookTicker",
        "data": {
            "u": 900,
            "s": "BTCUSDT",
            "b": "100.00",
            "B": "2.0",
            "a": "100.10",
            "A": "3.0",
        },
    }
    ev = parse_ws_message(orjson.dumps(frame), seq=3, recv_mono_ns=4, recv_wall_ns=5)
    assert isinstance(ev, BookTickerEvent)
    assert ev.update_id == 900
    assert ev.bid_px_ticks == 10_000_000_000
    assert ev.ask_qty_ticks == 300_000_000


def test_parse_rejects_bad_json_and_shape() -> None:
    with pytest.raises(ParseError):
        parse_ws_message(b"not json", seq=1, recv_mono_ns=2, recv_wall_ns=3)
    with pytest.raises(ParseError):
        parse_ws_message(
            b'{"stream":"btcusdt@depth@100ms","data":{"e":"depthUpdate"}}',
            seq=1,
            recv_mono_ns=2,
            recv_wall_ns=3,
        )


def test_snapshot_roundtrip() -> None:
    payload = encode_snapshot(
        symbol="btcusdt",
        last_update_id=123,
        bids=[["100.00", "1.0"]],
        asks=[["100.10", "2.0"]],
        requested_mono_ns=1,
        recv_mono_ns=2,
        recv_wall_ns=3,
    )
    ev = decode_snapshot(payload, seq=9, recv_mono_ns=4, recv_wall_ns=5)
    assert isinstance(ev, SnapshotEvent)
    assert ev.symbol == "BTCUSDT"
    assert ev.last_update_id == 123
    assert ev.bids == ((10_000_000_000, 100_000_000),)
    assert ev.asks == ((10_010_000_000, 200_000_000),)
    assert ev.requested_mono_ns == 1
    assert ev.seq == 9


def test_decode_snapshot_rejects_empty_side() -> None:
    payload = encode_snapshot(
        symbol="BTCUSDT",
        last_update_id=1,
        bids=[],
        asks=[["1", "1"]],
        requested_mono_ns=0,
        recv_mono_ns=0,
        recv_wall_ns=0,
    )
    with pytest.raises(ParseError):
        decode_snapshot(payload, seq=1, recv_mono_ns=1, recv_wall_ns=1)
