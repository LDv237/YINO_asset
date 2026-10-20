"""Command-line interface.

Commands::

    python -m tickforge live   --config config/default.toml --duration 900
    python -m tickforge replay --run artifacts/runs/<run_id> --verify
    python -m tickforge verify --run artifacts/runs/<run_id>
    python -m tickforge show   --run artifacts/runs/<run_id>
    python -m tickforge bench  --run artifacts/runs/<run_id>
"""

from __future__ import annotations

import argparse
import asyncio
import json
import logging
import signal
import sys
from pathlib import Path
from typing import Any

from tickforge import __version__
from tickforge.config import Config, load_config
from tickforge.logging_setup import configure_logging
from tickforge.replay.runner import ReplayRunner, verify_run
from tickforge.runtime.engine import LiveEngine, prepare_run_dir

log = logging.getLogger("tickforge.cli")


def _parse_symbols(value: str | None) -> list[str] | None:
    if not value:
        return None
    return [item.strip().upper() for item in value.split(",") if item.strip()]


def _install_signal_handlers(engine: LiveEngine) -> None:
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(sig, engine.request_stop, f"signal:{sig.name}")
        except (NotImplementedError, RuntimeError):
            try:
                signal.signal(
                    sig,
                    lambda s, _f, eng=engine: eng.request_stop(f"signal:{s.name}"),
                )
            except (ValueError, OSError):
                pass


async def _run_live(cfg: Config, run_dir: Path, run_id: str) -> dict:
    engine = LiveEngine(cfg, run_dir, run_id=run_id)
    _install_signal_handlers(engine)
    return await engine.run()


def cmd_live(args: argparse.Namespace) -> int:
    cfg = load_config(args.config)
    if args.duration is not None:
        cfg.run.duration_s = args.duration
    if args.symbols:
        cfg.feed.symbols = _parse_symbols(args.symbols)
    if args.depth_speed is not None:
        cfg.feed.depth_speed_ms = args.depth_speed
    if args.out:
        cfg.run.output_dir = args.out
    if args.http_host:
        cfg.http.host = args.http_host
    if args.http_port is not None:
        cfg.http.port = args.http_port
    if args.no_http:
        cfg.http.enabled = False
    if args.fsync_policy:
        cfg.wal.fsync_policy = args.fsync_policy
    if args.segment_mb is not None:
        cfg.wal.segment_bytes = int(args.segment_mb * 1024 * 1024)
    if args.log_level:
        cfg.run.log_level = args.log_level.upper()
    cfg.validate()

    run_dir, run_id = prepare_run_dir(cfg.run.output_dir, args.run_id)
    log_path = configure_logging(run_dir / "logs", level=cfg.run.log_level)
    logger = logging.getLogger("tickforge.cli")
    logger.info(
        "tickforge live starting",
        extra={
            "version": __version__,
            "run_dir": str(run_dir),
            "log_file": str(log_path),
        },
    )
    try:
        summary = asyncio.run(_run_live(cfg, run_dir, run_id))
    except KeyboardInterrupt:
        logger.warning("interrupted before the event loop installed handlers")
        return 130
    except Exception:
        logger.exception("live run failed")
        return 1

    counts = summary.get("counts", {})
    print()
    print(f"run_dir     : {run_dir}")
    print(f"summary     : {run_dir / 'summary.json'}")
    print(f"manifest    : {run_dir / 'manifest.json'}")
    print(f"clean       : {summary.get('clean_shutdown')}")
    print(f"window_s    : {summary.get('window_s')}")
    print(f"ws_frames   : {counts.get('ws_frames_received')}")
    print(f"processed   : {counts.get('processed_envelopes')}")
    print(f"signals     : {(counts.get('pipeline') or {}).get('signals')}")
    print(f"fills       : {(counts.get('pipeline') or {}).get('fills')}")
    print(f"replay      : python -m tickforge replay --run {run_dir} --verify")
    return 0 if summary.get("clean_shutdown") else 4


def cmd_replay(args: argparse.Namespace) -> int:
    out_dir = Path(args.out) if args.out else None
    log_dir = (out_dir or (Path(args.run) / "replay")) / "logs"
    configure_logging(log_dir, level=args.log_level.upper())
    runner = ReplayRunner(
        args.run,
        out_dir=out_dir,
        write_derived=not args.no_derived,
        verify=True,
    )
    try:
        result = runner.run()
    except Exception:
        log.exception("replay failed")
        return 1
    comparison = result.get("comparison", {})
    print(json.dumps(
        {
            "records": result["records"],
            "throughput_events_per_s": result["throughput_events_per_s"],
            "deterministic": comparison.get("deterministic"),
            "verified": comparison.get("verified"),
            "reasons": comparison.get("reasons"),
            "count_match": comparison.get("count_match"),
            "digest_match": comparison.get("digest_match"),
            "out": str(Path(args.out) if args.out else Path(args.run) / "replay"),
        },
        indent=2,
    ))
    if args.verify:
        return 0 if comparison.get("verified") else 3
    return 0


def cmd_verify(args: argparse.Namespace) -> int:
    try:
        result = verify_run(args.run)
    except Exception:
        log.exception("verification failed")
        return 1
    print(json.dumps(result, ensure_ascii=False, indent=2))
    return 0 if result.get("ok") else 1


def cmd_show(args: argparse.Namespace) -> int:
    path = Path(args.run) / "summary.json"
    if not path.is_file():
        print(f"summary not found: {path}", file=sys.stderr)
        return 1
    with path.open("r", encoding="utf-8") as fh:
        summary = json.load(fh)
    keys = (
        "run_id",
        "mode",
        "started_at",
        "ended_at",
        "window_s",
        "clean_shutdown",
        "rates",
        "reconciliation",
        "data_gaps",
    )
    print(json.dumps({k: summary.get(k) for k in keys}, ensure_ascii=False, indent=2))
    latency = summary.get("latency_ns") or {}
    e2e = latency.get("tickforge_latency_e2e_ns")
    if e2e:
        print("e2e latency:", json.dumps({
            "count": e2e.get("count"),
            "p50_ns": e2e.get("p50_ns"),
            "p99_ns": e2e.get("p99_ns"),
            "p999_ns": e2e.get("p999_ns"),
            "max_ns": e2e.get("max_ns"),
        }, indent=2))
    return 0


def cmd_bench(args: argparse.Namespace) -> int:
    configure_logging(Path(args.run) / "replay" / "logs", level="WARNING")
    results: list[dict[str, Any]] = []
    try:
        for i in range(max(1, args.repeat)):
            runner = ReplayRunner(
                args.run,
                out_dir=Path(args.run) / f"replay-bench-{i}",
                write_derived=False,
                verify=False,
            )
            results.append(runner.run())
    except Exception:
        log.exception("benchmark failed")
        return 1
    throughput = [r["throughput_events_per_s"] for r in results if r.get("throughput_events_per_s")]
    print(json.dumps(
        {
            "runs": len(results),
            "records_per_run": results[0]["records"] if results else 0,
            "throughput_events_per_s": throughput,
            "best_events_per_s": max(throughput) if throughput else None,
        },
        indent=2,
    ))
    return 0


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="tickforge",
        description="Real-time Binance market-data pipeline with WAL, replay and verification.",
    )
    parser.add_argument("--version", action="version", version=f"tickforge {__version__}")
    sub = parser.add_subparsers(dest="command", required=True)

    live = sub.add_parser("live", help="capture live data and run the pipeline")
    live.add_argument("--config", default=None, help="TOML config path")
    live.add_argument("--duration", type=float, default=None, help="measurement window in seconds (0 = until Ctrl-C)")
    live.add_argument("--symbols", default=None, help="comma separated symbols, e.g. BTCUSDT,ETHUSDT")
    live.add_argument("--depth-speed", type=int, choices=(100, 1000), default=None)
    live.add_argument("--out", default=None, help="base output directory (run goes to <out>/runs/<id>)")
    live.add_argument("--run-id", default=None)
    live.add_argument("--http-host", default=None)
    live.add_argument("--http-port", type=int, default=None)
    live.add_argument("--no-http", action="store_true")
    live.add_argument("--fsync-policy", choices=("interval", "always", "never"), default=None)
    live.add_argument("--segment-mb", type=float, default=None)
    live.add_argument("--log-level", default="INFO", choices=("DEBUG", "INFO", "WARNING", "ERROR"))
    live.set_defaults(func=cmd_live)

    replay = sub.add_parser("replay", help="replay a run's WAL and compare derived digests")
    replay.add_argument("--run", required=True)
    replay.add_argument("--out", default=None)
    replay.add_argument("--no-derived", action="store_true", help="do not rewrite derived JSONL")
    replay.add_argument("--verify", action="store_true", help="exit non-zero when digests/counts differ")
    replay.add_argument("--log-level", default="INFO", choices=("DEBUG", "INFO", "WARNING", "ERROR"))
    replay.set_defaults(func=cmd_replay)

    verify = sub.add_parser("verify", help="check WAL CRC/counts and manifest hashes")
    verify.add_argument("--run", required=True)
    verify.set_defaults(func=cmd_verify)

    show = sub.add_parser("show", help="print the key fields of a run summary")
    show.add_argument("--run", required=True)
    show.set_defaults(func=cmd_show)

    bench = sub.add_parser("bench", help="replay at full speed and report pipeline throughput")
    bench.add_argument("--run", required=True)
    bench.add_argument("--repeat", type=int, default=1)
    bench.set_defaults(func=cmd_bench)
    return parser


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    if args.command == "replay" and not args.verify:
        # Default to strict verification for the documented command; --verify
        # only controls the exit code, while comparison always runs.
        pass
    return int(args.func(args))


if __name__ == "__main__":
    raise SystemExit(main())
