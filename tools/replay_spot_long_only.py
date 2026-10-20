#!/usr/bin/env python3
"""Replay a prior run's WAL with spot long-only execution semantics.

This is a controlled comparison: the market-data, factors and order book are
the same as the reference run; only execution changes (no naked sells, buys
limited by quote cash).  The reference run's derived digests are intentionally
not compared, because signals/fills must differ once the strategy cannot quote
an ask without inventory.
"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from tickforge.replay.runner import ReplayRunner, config_from_run


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--run", required=True, help="reference run directory")
    parser.add_argument(
        "--out",
        default=None,
        help="output directory (default: <run>/variant_spot_long_only)",
    )
    parser.add_argument("--initial-cash-usd", type=float, default=10_000.0)
    args = parser.parse_args()

    run_dir = Path(args.run)
    cfg = config_from_run(run_dir)
    cfg.strategy.allow_short = False
    cfg.strategy.initial_cash_usd = float(args.initial_cash_usd)
    cfg.validate()

    out_dir = Path(args.out) if args.out else run_dir / "variant_spot_long_only"
    runner = ReplayRunner(
        run_dir,
        out_dir=out_dir,
        write_derived=True,
        cfg=cfg,
        compare_reference=False,
    )
    result = runner.run()
    symbols = result["pipeline"]["simulator"]["symbols"]
    print(
        json.dumps(
            {
                "run": str(run_dir),
                "out": str(out_dir),
                "records": result["records"],
                "throughput_events_per_s": result["throughput_events_per_s"],
                "positions": {sym: row["position"] for sym, row in symbols.items()},
                "fills": {sym: row["fills"] for sym, row in symbols.items()},
                "negative_positions": sum(
                    1 for row in symbols.values() if row["position"] < 0
                ),
                "negative_cash": sum(1 for row in symbols.values() if row["cash"] < 0),
                "comparison": result["comparison"],
            },
            indent=2,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
