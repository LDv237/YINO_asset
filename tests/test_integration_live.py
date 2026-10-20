"""End-to-end live-path test with a local WebSocket + REST exchange double.

This exercises the real BinanceFeed, SnapshotClient, WAL, pipeline, derived
sinks, graceful shutdown, WAL verification and deterministic replay.  No
public network access is required.
"""

from __future__ import annotations

import asyncio
import hashlib
import json
import logging
from pathlib import Path

import orjson
import pytest
from aiohttp import web
from websockets.asyncio.server import serve

from tickforge.config import Config
from tickforge.logging_setup import configure_logging
from tickforge.replay.runner import ReplayRunner, verify_run
from tickforge.runtime.engine import LiveEngine, prepare_run_dir

SNAPSHOT_ID = 1_000
SYMBOL = "BTCUSDT"


async def _rest_depth(request: web.Request) -> web.Response:
    payload = {
        "lastUpdateId": SNAPSHOT_ID,
        "bids": [
            ["100.00000000", "1.00000000"],
            ["99.99000000", "5.00000000"],
            ["99.98000000", "5.00000000"],
        ],
        "asks": [
            ["100.01000000", "1.00000000"],
            ["100.02000000", "5.00000000"],
            ["100.03000000", "5.00000000"],
        ],
    }
    return web.Response(body=orjson.dumps(payload), content_type="application/json")


async def _ws_handler(ws) -> None:
    seq = SNAPSHOT_ID
    n = 0
    try:
        while True:
            n += 1
            seq += 1
            bid_qty = "2.0" if n % 2 else "1.5"
            ask_qty = "1.7" if n % 2 else "2.2"
            depth = {
                "stream": f"{SYMBOL.lower()}@depth@100ms",
                "data": {
                    "e": "depthUpdate",
                    "E": int(asyncio.get_running_loop().time() * 1000) + 1_700_000_000_000,
                    "s": SYMBOL,
                    "U": seq,
                    "u": seq,
                    "b": [["100.00000000", bid_qty], ["99.99000000", "5.0"]],
                    "a": [["100.01000000", ask_qty], ["100.02000000", "5.0"]],
                },
            }
            await ws.send(orjson.dumps(depth).decode())
            if n % 2 == 0:
                trade = {
                    "stream": f"{SYMBOL.lower()}@aggTrade",
                    "data": {
                        "e": "aggTrade",
                        "E": int(asyncio.get_running_loop().time() * 1000)
                        + 1_700_000_000_000,
                        "s": SYMBOL,
                        "a": 50_000 + n,
                        "p": "99.98000000" if n % 4 == 0 else "100.03000000",
                        "q": "0.01000000",
                        "m": n % 4 != 0,
                    },
                }
                await ws.send(orjson.dumps(trade).decode())
            if n % 3 == 0:
                ticker = {
                    "stream": f"{SYMBOL.lower()}@bookTicker",
                    "data": {
                        "u": seq,
                        "s": SYMBOL,
                        "b": "100.00000000",
                        "B": bid_qty,
                        "a": "100.01000000",
                        "A": ask_qty,
                    },
                }
                await ws.send(orjson.dumps(ticker).decode())
            await asyncio.sleep(0.05)
    except Exception:
        return


def _config(ws_port: int, rest_port: int, base: Path) -> Config:
    cfg = Config()
    cfg.feed.symbols = [SYMBOL]
    cfg.feed.ws_url = f"ws://127.0.0.1:{ws_port}/stream"
    cfg.feed.rest_url = f"http://127.0.0.1:{rest_port}"
    cfg.feed.depth_speed_ms = 100
    cfg.feed.stale_timeout_ms = 2_000
    cfg.feed.open_timeout_s = 5.0
    cfg.feed.ping_interval_s = 5.0
    cfg.feed.ping_timeout_s = 5.0
    cfg.feed.snapshot_retries = 2
    cfg.feed.snapshot_retry_backoff_s = 0.05
    cfg.feed.snapshot_limit = 100
    cfg.wal.segment_bytes = 1_000_000
    cfg.wal.fsync_interval_ms = 50
    cfg.engine.factor_interval_ms = 100
    cfg.engine.stats_interval_s = 1.0
    cfg.engine.drain_timeout_s = 5.0
    cfg.run.duration_s = 2.0
    cfg.run.output_dir = str(base)
    cfg.http.enabled = False
    cfg.validate()
    return cfg


async def test_live_pipeline_verifies_and_replays(tmp_path: Path) -> None:
    rest_app = web.Application()
    rest_app.router.add_get("/api/v3/depth", _rest_depth)
    rest_runner = web.AppRunner(rest_app)
    await rest_runner.setup()
    rest_site = web.TCPSite(rest_runner, "127.0.0.1", 0)
    await rest_site.start()
    rest_port = rest_site._server.sockets[0].getsockname()[1]

    ws_server = await serve(_ws_handler, "127.0.0.1", 0)
    ws_port = ws_server.sockets[0].getsockname()[1]

    cfg = _config(ws_port, rest_port, tmp_path)
    run_dir, run_id = prepare_run_dir(tmp_path, "integration")
    configure_logging(run_dir / "logs", level="WARNING")
    try:
        engine = LiveEngine(cfg, run_dir, run_id=run_id)
        summary = await asyncio.wait_for(engine.run(), timeout=30)
    finally:
        ws_server.close()
        await ws_server.wait_closed()
        await rest_runner.cleanup()

    assert summary["clean_shutdown"] is True, json.dumps(
        summary.get("reconciliation"), indent=2
    )
    counts = summary["counts"]
    pipe = counts["pipeline"]
    assert counts["ws_frames_received"] > 20
    assert counts["processed_envelopes"] == counts["ws_frames_received"] + pipe["snapshots"]
    assert pipe["snapshots"] >= 1
    assert pipe["depth_events"] > 10
    assert pipe["factor_snapshots"] > 5
    assert pipe["signals"] > 5
    assert pipe["fills"] > 0
    assert count_synced(summary) is True
    assert summary["reconciliation"]["lost_raw_frames"] == 0
    assert summary["reconciliation"]["drained"] is True
    assert summary["health_before_shutdown"]["ok"] is True
    assert summary["health_final"]["ok"] is True
    assert summary["health_final_context"] == "after_intentional_shutdown"

    verified = verify_run(run_dir)
    assert verified["ok"] is True, verified

    replay_out = tmp_path / "replay"
    replay = ReplayRunner(run_dir, out_dir=replay_out, write_derived=True).run()
    comparison = replay["comparison"]
    assert comparison["verified"] is True, comparison
    assert comparison["deterministic"] is True
    assert comparison["digest_match"]["factors_sha256"] is True
    assert comparison["digest_match"]["signals_sha256"] is True
    assert comparison["digest_match"]["fills_sha256"] is True

    # Fail-closed: a non-empty default replay output must not be overwritten.
    default_out = run_dir.parent / f"{run_dir.name}-replay"
    default_out.mkdir()
    (default_out / "sentinel").write_text("keep", encoding="utf-8")
    with pytest.raises(FileExistsError):
        ReplayRunner(run_dir).run()

    # Fail-closed: replay must re-hash the live derived files.
    factors = run_dir / "derived" / "factors.jsonl"
    original_factors = factors.read_bytes()
    factors.write_bytes(original_factors + b"\n")
    tampered_replay = ReplayRunner(
        run_dir, out_dir=tmp_path / "replay_tampered", write_derived=True
    ).run()
    assert tampered_replay["comparison"]["verified"] is False
    assert "live_derived_digest_mismatch" in tampered_replay["comparison"]["reasons"]
    factors.write_bytes(original_factors)

    # Fail-closed: a non-clean shutdown must fail verify even if counts match.
    summary_path = run_dir / "summary.json"
    summary_doc = json.loads(summary_path.read_text(encoding="utf-8"))
    summary_doc["clean_shutdown"] = False
    summary_path.write_text(
        json.dumps(summary_doc, ensure_ascii=False, indent=2) + "\n",
        encoding="utf-8",
    )
    manifest_path = run_dir / "manifest.json"
    manifest_doc = json.loads(manifest_path.read_text(encoding="utf-8"))
    for entry in manifest_doc.get("files", []):
        if entry.get("path") == "summary.json":
            entry["bytes"] = summary_path.stat().st_size
            entry["sha256"] = hashlib.sha256(summary_path.read_bytes()).hexdigest()
    manifest_path.write_text(
        json.dumps(manifest_doc, ensure_ascii=False, indent=2) + "\n",
        encoding="utf-8",
    )
    non_clean = verify_run(run_dir)
    assert non_clean["ok"] is False
    assert any("clean_shutdown" in error for error in non_clean["errors"])

    # Fail-closed: tampering with a derived file must fail the manifest check.
    factors = run_dir / "derived" / "factors.jsonl"
    factors.write_bytes(factors.read_bytes() + b"\n")
    tampered = verify_run(run_dir)
    assert tampered["ok"] is False
    assert any("manifest file hash mismatch" in error for error in tampered["errors"])


def count_synced(summary: dict) -> bool:
    books = (summary.get("pipeline") or {}).get("books") or {}
    return bool(books) and all(item.get("synced") for item in books.values())
