#!/usr/bin/env python3
"""Export a TickForge WAL to human-friendly CSV files.

Outputs (in --out-dir):
  agg_trades.csv    one row per exchange-aggregated trade
  book_ticker.csv   one row per best-bid/ask update
  depth_updates.csv one row per diff-depth event, with reconstructed best quote
  snapshots.csv     one row per REST depth snapshot

The WAL remains the canonical raw source.  These CSVs are views for reviewers.
"""

from __future__ import annotations

import argparse
import csv
import json
import sys
from datetime import datetime, timezone
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from tickforge.book import OrderBook
from tickforge.events import TICKS_PER_UNIT, BookTickerEvent, DepthEvent, TradeEvent
from tickforge.normalize import decode_snapshot, parse_ws_message
from tickforge.store.wal import KIND_SNAPSHOT, KIND_WS, WalReader


def _time_utc(ns: int | None) -> str:
    if not ns:
        return ""
    return datetime.fromtimestamp(ns / 1e9, tz=timezone.utc).isoformat(
        timespec="milliseconds"
    )


def _ticks(value: int) -> str:
    return f"{value / TICKS_PER_UNIT:.8f}"


def _write_header(writer: csv.writer, fields: list[str]) -> None:
    writer.writerow(fields)


def export(run_dir: Path, out_dir: Path, max_records: int | None = None) -> dict:
    raw_dir = run_dir / "raw"
    if not raw_dir.is_dir():
        raise FileNotFoundError(f"raw WAL directory not found: {raw_dir}")
    out_dir.mkdir(parents=True, exist_ok=True)

    trades_path = out_dir / "agg_trades.csv"
    tickers_path = out_dir / "book_ticker.csv"
    depths_path = out_dir / "depth_updates.csv"
    snapshots_path = out_dir / "snapshots.csv"

    books: dict[str, OrderBook] = {}
    counts = {"ws": 0, "snapshot": 0, "trades": 0, "book_ticker": 0, "depth": 0}
    errors = 0
    records = 0

    with (
        open(trades_path, "w", encoding="utf-8-sig", newline="") as tf,
        open(tickers_path, "w", encoding="utf-8-sig", newline="") as bf,
        open(depths_path, "w", encoding="utf-8-sig", newline="") as df,
        open(snapshots_path, "w", encoding="utf-8-sig", newline="") as sf,
    ):
        tw, bw, dw, sw = csv.writer(tf), csv.writer(bf), csv.writer(df), csv.writer(sf)
        _write_header(
            tw,
            [
                "seq",
                "recv_time_utc",
                "exchange_time_utc",
                "symbol",
                "agg_trade_id",
                "price",
                "qty",
                "aggressor_side",
                "is_buyer_maker",
                "first_trade_id",
                "last_trade_id",
                "underlying_trade_count",
            ],
        )
        _write_header(
            bw,
            [
                "seq",
                "recv_time_utc",
                "symbol",
                "update_id",
                "bid_price",
                "bid_qty",
                "ask_price",
                "ask_qty",
                "spread",
            ],
        )
        _write_header(
            dw,
            [
                "seq",
                "recv_time_utc",
                "exchange_time_utc",
                "symbol",
                "first_update_id",
                "final_update_id",
                "bid_changes",
                "ask_changes",
                "best_bid",
                "best_bid_qty",
                "best_ask",
                "best_ask_qty",
                "spread",
            ],
        )
        _write_header(
            sw,
            [
                "seq",
                "recv_time_utc",
                "symbol",
                "last_update_id",
                "bid_levels",
                "ask_levels",
            ],
        )

        reader = WalReader(raw_dir, verify_crc=True, allow_truncated_tail=True)
        for record in reader:
            if max_records is not None and records >= max_records:
                break
            records += 1
            try:
                if record.kind == KIND_SNAPSHOT:
                    counts["snapshot"] += 1
                    snap = decode_snapshot(
                        record.payload,
                        seq=record.seq,
                        recv_mono_ns=record.recv_mono_ns,
                        recv_wall_ns=record.recv_wall_ns,
                    )
                    book = books.setdefault(snap.symbol, OrderBook(snap.symbol))
                    book.apply_snapshot(snap)
                    sw.writerow(
                        [
                            record.seq,
                            _time_utc(record.recv_wall_ns),
                            snap.symbol,
                            snap.last_update_id,
                            len(snap.bids),
                            len(snap.asks),
                        ]
                    )
                    continue
                if record.kind != KIND_WS:
                    continue
                counts["ws"] += 1
                event = parse_ws_message(
                    record.payload,
                    seq=record.seq,
                    recv_mono_ns=record.recv_mono_ns,
                    recv_wall_ns=record.recv_wall_ns,
                )
                if event is None:
                    continue
                if isinstance(event, TradeEvent):
                    counts["trades"] += 1
                    # f/l are present in the original payload; parse them here.
                    raw = json.loads(record.payload)
                    data = raw.get("data", raw)
                    first_id = data.get("f", "")
                    last_id = data.get("l", "")
                    underlying = (
                        int(last_id) - int(first_id) + 1
                        if first_id != "" and last_id != ""
                        else ""
                    )
                    tw.writerow(
                        [
                            record.seq,
                            _time_utc(record.recv_wall_ns),
                            _time_utc(event.event_time_ms * 1_000_000),
                            event.symbol,
                            event.agg_trade_id,
                            _ticks(event.price_ticks),
                            _ticks(event.qty_ticks),
                            event.aggressor_side,
                            event.is_buyer_maker,
                            first_id,
                            last_id,
                            underlying,
                        ]
                    )
                elif isinstance(event, BookTickerEvent):
                    counts["book_ticker"] += 1
                    spread = (event.ask_px_ticks - event.bid_px_ticks) / TICKS_PER_UNIT
                    bw.writerow(
                        [
                            record.seq,
                            _time_utc(record.recv_wall_ns),
                            event.symbol,
                            event.update_id,
                            _ticks(event.bid_px_ticks),
                            _ticks(event.bid_qty_ticks),
                            _ticks(event.ask_px_ticks),
                            _ticks(event.ask_qty_ticks),
                            f"{spread:.8f}",
                        ]
                    )
                elif isinstance(event, DepthEvent):
                    counts["depth"] += 1
                    book = books.setdefault(event.symbol, OrderBook(event.symbol))
                    book.apply_depth(event)
                    best = book.best()
                    if best is None:
                        best_bid = best_bid_qty = best_ask = best_ask_qty = spread = ""
                    else:
                        best_bid = _ticks(best.bid_px)
                        best_bid_qty = _ticks(best.bid_qty)
                        best_ask = _ticks(best.ask_px)
                        best_ask_qty = _ticks(best.ask_qty)
                        spread = f"{(best.ask_px - best.bid_px) / TICKS_PER_UNIT:.8f}"
                    dw.writerow(
                        [
                            record.seq,
                            _time_utc(record.recv_wall_ns),
                            _time_utc(event.event_time_ms * 1_000_000),
                            event.symbol,
                            event.first_update_id,
                            event.final_update_id,
                            len(event.bids),
                            len(event.asks),
                            best_bid,
                            best_bid_qty,
                            best_ask,
                            best_ask_qty,
                            spread,
                        ]
                    )
            except Exception:
                errors += 1
                continue

    return {
        "run_dir": str(run_dir),
        "out_dir": str(out_dir),
        "records_scanned": records,
        "counts": counts,
        "errors": errors,
        "files": {
            "agg_trades": {"path": str(trades_path), "bytes": trades_path.stat().st_size},
            "book_ticker": {"path": str(tickers_path), "bytes": tickers_path.stat().st_size},
            "depth_updates": {"path": str(depths_path), "bytes": depths_path.stat().st_size},
            "snapshots": {"path": str(snapshots_path), "bytes": snapshots_path.stat().st_size},
        },
    }


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--run", required=True)
    parser.add_argument("--out-dir", required=True)
    parser.add_argument("--max-records", type=int, default=None)
    args = parser.parse_args()
    result = export(Path(args.run), Path(args.out_dir), args.max_records)
    print(json.dumps(result, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
