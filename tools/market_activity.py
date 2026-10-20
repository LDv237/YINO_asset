#!/usr/bin/env python3
"""Fetch Binance hourly klines and rank a run window's activity.

Crypto trades 24/7; "active period" is not a fixed session.  This tool uses
public 1h klines (quote volume and trade count) to rank complete hourly buckets
and reports where a target run window sits.

Usage:
  python tools/market_activity.py --target 2026-10-20T07:28:20Z \
      --out artifacts/evidence/market_activity.json
"""

from __future__ import annotations

import argparse
import json
import urllib.request
from datetime import datetime, timedelta, timezone
from pathlib import Path

CST = timezone(timedelta(hours=8))


def _iso(ms: int, tz=timezone.utc) -> str:
    return datetime.fromtimestamp(ms / 1000, tz).isoformat(timespec="seconds")


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--symbol", default="BTCUSDT")
    parser.add_argument("--hours", type=int, default=48)
    parser.add_argument("--target", default="2026-10-20T07:28:20Z")
    parser.add_argument("--out", default="artifacts/evidence/market_activity.json")
    args = parser.parse_args()

    url = (
        "https://data-api.binance.vision/api/v3/klines"
        f"?symbol={args.symbol}&interval=1h&limit={max(2, args.hours)}"
    )
    with urllib.request.urlopen(url, timeout=20) as response:
        rows = json.load(response)

    target = datetime.fromisoformat(args.target.replace("Z", "+00:00"))
    target_ms = int(target.timestamp() * 1000)
    now = datetime.now(timezone.utc)
    current_hour_start_ms = int(
        now.replace(minute=0, second=0, microsecond=0).timestamp() * 1000
    )

    hours = []
    for row in rows:
        open_ms = int(row[0])
        close_ms = int(row[6])
        if open_ms >= current_hour_start_ms:
            continue  # current incomplete hour is not comparable
        hours.append(
            {
                "open_utc": _iso(open_ms),
                "open_cst": _iso(open_ms, CST),
                "quote_volume": float(row[7]),
                "base_volume": float(row[5]),
                "trades": int(row[8]),
            }
        )
    if not hours:
        raise SystemExit("no complete hourly klines returned")

    volumes = sorted(h["quote_volume"] for h in hours)
    target_hour = next(
        (h for h in hours if int(datetime.fromisoformat(h["open_utc"]).timestamp() * 1000) <= target_ms < int(datetime.fromisoformat(h["open_utc"]).timestamp() * 1000) + 3_600_000),
        None,
    )
    if target_hour is None:
        raise SystemExit("target is outside the fetched complete-hour window")

    rank = 1 + sum(1 for h in hours if h["quote_volume"] > target_hour["quote_volume"])
    percentile = sum(1 for v in volumes if v <= target_hour["quote_volume"]) / len(volumes) * 100
    top = sorted(hours, key=lambda h: h["quote_volume"], reverse=True)[:8]
    result = {
        "symbol": args.symbol,
        "generated_at_utc": now.isoformat(timespec="seconds"),
        "complete_hours": len(hours),
        "target_run_utc": args.target,
        "target_bucket": target_hour,
        "target_rank_by_quote_volume": f"{rank}/{len(hours)}",
        "target_percentile": round(percentile, 2),
        "top_hours": top,
        "method": (
            "Binance public 1h klines, quote volume and trade count; "
            "the current incomplete hour is excluded; rank 1 = highest volume."
        ),
        "caveat": (
            "This is a liquidity/activity proxy for the chosen symbol, not a "
            "guarantee that every future 15-minute window is equally active."
        ),
    }
    out = Path(args.out)
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(json.dumps(result, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    print(json.dumps(result, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
