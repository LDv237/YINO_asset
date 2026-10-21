#!/usr/bin/env python3
"""Trace one signal back to its factor snapshot and raw WAL event.

The signal stream stores ``(symbol, seq)``; the factor snapshot with the same
key is the state that produced the quote, and the WAL record with the same
``seq`` is the original exchange frame.  This tool prints all three, plus a
note on how fills link back via ``trade_id`` (Binance ``aggTrade.a``).

Usage::

    python tools/trace_signal.py --run artifacts/runs/<run_id> --index 0
    python tools/trace_signal.py --run artifacts/runs/<run_id> \
        --symbol BTCUSDT --seq 12345
"""

from __future__ import annotations

import argparse
import base64
import json
import sys
from pathlib import Path
from typing import Any

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

try:
    import orjson
except ImportError:  # pragma: no cover - orjson is a runtime dependency
    orjson = None

from tickforge.store.wal import KIND_SNAPSHOT, KIND_WS, WalReader


def _loads(payload: bytes) -> Any:
    if orjson is not None:
        try:
            return orjson.loads(payload)
        except orjson.JSONDecodeError:
            return {"payload_base64": base64.b64encode(payload).decode("ascii")}
    try:
        return json.loads(payload.decode("utf-8"))
    except (UnicodeDecodeError, ValueError):
        return {"payload_base64": base64.b64encode(payload).decode("ascii")}


def _iter_jsonl(path: Path):
    with path.open("r", encoding="utf-8") as fh:
        for line in fh:
            line = line.strip()
            if line:
                yield json.loads(line)


def _pick_signal(
    path: Path, *, symbol: str | None, seq: int | None, index: int
) -> tuple[int, dict]:
    if seq is not None:
        for i, row in enumerate(_iter_jsonl(path)):
            if row.get("seq") == seq and (symbol is None or row.get("symbol") == symbol):
                return i, row
        raise SystemExit(f"no signal with seq={seq} symbol={symbol!r} in {path}")

    if index < 0:
        raise SystemExit("--index must be >= 0")
    for i, row in enumerate(_iter_jsonl(path)):
        if symbol is not None and row.get("symbol") != symbol:
            continue
        if i == index:
            return i, row
    raise SystemExit(f"no signal at index {index} symbol={symbol!r} in {path}")


def _find_factor(path: Path, signal: dict) -> dict | None:
    target = (signal.get("symbol"), signal.get("seq"))
    for row in _iter_jsonl(path):
        if (row.get("symbol"), row.get("seq")) == target:
            return row
    return None


def _find_raw(raw_dir: Path, seq: int) -> dict | None:
    for record in WalReader(raw_dir, verify_crc=True, allow_truncated_tail=True):
        if record.seq != seq:
            continue
        payload = _loads(record.payload)
        kind = "ws" if record.kind == KIND_WS else "snapshot" if record.kind == KIND_SNAPSHOT else str(record.kind)
        stream = payload.get("stream") if isinstance(payload, dict) else None
        data = payload.get("data", payload) if isinstance(payload, dict) else payload
        if isinstance(data, dict):
            event_type = data.get("e")
            summary = {
                "event_type": event_type,
                "symbol": data.get("s"),
                "event_time_ms": data.get("E") or data.get("T"),
                "first_update_id": data.get("U"),
                "last_update_id": data.get("u"),
            }
            agg_id = data.get("a")
            if event_type in ("aggTrade", "trade") and isinstance(agg_id, (int, str)):
                summary["agg_trade_id"] = agg_id
            summary = {k: v for k, v in summary.items() if v is not None}
        else:
            summary = {}
        return {
            "seq": record.seq,
            "kind": kind,
            "stream": stream,
            "recv_wall_ns": record.recv_wall_ns,
            "recv_mono_ns": record.recv_mono_ns,
            "summary": summary,
            "data": data,
        }
    return None


def _next_fills(path: Path, signal: dict, limit: int = 3) -> list[dict]:
    """Show the first fills at/after the signal time; not a direct attribution."""

    symbol = signal.get("symbol")
    ts_ms = int(signal.get("ts_ms") or 0)
    out: list[dict] = []
    if not path.is_file():
        return out
    for row in _iter_jsonl(path):
        if row.get("symbol") != symbol:
            continue
        if int(row.get("ts_ms") or 0) < ts_ms:
            continue
        out.append(
            {
                "ts_ms": row.get("ts_ms"),
                "trade_id": row.get("trade_id"),
                "side": row.get("side"),
                "price": row.get("price"),
                "qty": row.get("qty"),
                "position": row.get("position"),
            }
        )
        if len(out) >= limit:
            break
    return out


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--run", required=True, help="run directory with raw/ and derived/")
    parser.add_argument("--symbol", default=None)
    parser.add_argument("--seq", type=int, default=None)
    parser.add_argument("--index", type=int, default=0, help="signal index when --seq is not given")
    parser.add_argument("--raw", action="store_true", help="include the full raw payload")
    args = parser.parse_args()

    run = Path(args.run)
    signals_path = run / "derived" / "signals.jsonl"
    factors_path = run / "derived" / "factors.jsonl"
    fills_path = run / "derived" / "fills.jsonl"
    if not signals_path.is_file():
        raise SystemExit(f"signals.jsonl not found: {signals_path}")

    index, signal = _pick_signal(
        signals_path, symbol=args.symbol, seq=args.seq, index=args.index
    )
    factor = _find_factor(factors_path, signal) if factors_path.is_file() else None
    raw = _find_raw(run / "raw", int(signal.get("seq") or -1)) if signal.get("seq") is not None else None

    result = {
        "run": str(run),
        "signal_index": index,
        "signal": signal,
        "factor_snapshot": factor,
        "raw_event": raw,
        "fills_note": "fills.jsonl.trade_id == Binance aggTrade.a; the listed fills are the first fills at/after signal ts_ms for the same symbol, not a direct signal-to-fill attribution",
        "next_fills": _next_fills(fills_path, signal),
    }
    if not args.raw and isinstance(result["raw_event"], dict):
        result["raw_event"].pop("data", None)
    print(json.dumps(result, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
