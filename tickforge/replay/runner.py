"""Deterministic WAL replay and integrity verification.

Replay is the main correctness argument of the project: the live run stores
every raw input event (including order-book snapshots) in the WAL; replaying
those bytes through the *same* ``Pipeline`` must reproduce the same factor,
signal and fill streams, byte for byte, and must reproduce the same counts.
"""

from __future__ import annotations

import json
import logging
import time
from dataclasses import fields
from pathlib import Path
from typing import Any

from tickforge.config import (
    Config,
    EngineConfig,
    FeedConfig,
    HttpConfig,
    RunConfig,
    StrategyConfig,
    WalConfig,
)
from tickforge.metrics import MetricsRegistry
from tickforge.runtime.pipeline import Envelope, Pipeline
from tickforge.store.derived import JsonlSink
from tickforge.store.wal import (
    KIND_SNAPSHOT,
    KIND_WS,
    WalCorruptionError,
    WalReader,
    WalScanResult,
)

log = logging.getLogger(__name__)


def _load_json(path: Path) -> dict | None:
    try:
        with path.open("r", encoding="utf-8") as fh:
            return json.load(fh)
    except (OSError, ValueError):
        return None


def _build(cls, values: dict) -> Any:
    allowed = {f.name for f in fields(cls)}
    return cls(**{k: v for k, v in values.items() if k in allowed})


def config_from_run(run_dir: str | Path) -> Config:
    """Load the config snapshot written by the live run.

    Reproducibility requires using the *same* factor/strategy parameters as
    the original run, not whatever happens to be in the caller's environment.
    """

    doc = _load_json(Path(run_dir) / "config.json") or {}
    raw = doc.get("config") or {}
    cfg = Config(
        feed=_build(FeedConfig, raw.get("feed", {})),
        wal=_build(WalConfig, raw.get("wal", {})),
        engine=_build(EngineConfig, raw.get("engine", {})),
        strategy=_build(StrategyConfig, raw.get("strategy", {})),
        http=_build(HttpConfig, raw.get("http", {})),
        run=_build(RunConfig, raw.get("run", {})),
    )
    cfg.validate()
    return cfg


class ReplayRunner:
    def __init__(
        self,
        run_dir: str | Path,
        *,
        out_dir: str | Path | None = None,
        write_derived: bool = True,
        verify: bool = True,
        cfg: Config | None = None,
        compare_reference: bool = True,
        logger: logging.Logger | None = None,
    ) -> None:
        self.run_dir = Path(run_dir)
        self.raw_dir = self.run_dir / "raw"
        self.out_dir = Path(out_dir) if out_dir else self.run_dir / "replay"
        self.write_derived = write_derived
        self.verify = verify
        self.log = logger or log
        self.cfg = cfg or config_from_run(self.run_dir)
        self.compare_reference = compare_reference

    def run(self) -> dict:
        if not self.raw_dir.is_dir():
            raise FileNotFoundError(f"raw WAL directory not found: {self.raw_dir}")
        self.out_dir.mkdir(parents=True, exist_ok=True)
        derived_dir = self.out_dir / "derived"
        derived_dir.mkdir(exist_ok=True)

        factor_sink = JsonlSink(derived_dir / "factors.jsonl") if self.write_derived else None
        signal_sink = JsonlSink(derived_dir / "signals.jsonl") if self.write_derived else None
        fill_sink = JsonlSink(derived_dir / "fills.jsonl") if self.write_derived else None
        metrics = MetricsRegistry()
        pipeline = Pipeline(
            self.cfg,
            metrics,
            factor_sink=factor_sink,
            signal_sink=signal_sink,
            fill_sink=fill_sink,
            request_snapshot=None,
            observe_latency=False,
            logger=self.log,
        )

        reader = WalReader(self.raw_dir, verify_crc=True, allow_truncated_tail=True)
        started = time.perf_counter()
        records = 0
        events = {"ws": 0, "snapshot": 0}
        for record in reader:
            records += 1
            if record.kind == KIND_WS:
                events["ws"] += 1
            elif record.kind == KIND_SNAPSHOT:
                events["snapshot"] += 1
            pipeline.process(
                Envelope(
                    kind=record.kind,
                    seq=record.seq,
                    recv_wall_ns=record.recv_wall_ns,
                    recv_mono_ns=record.recv_mono_ns,
                    payload=record.payload,
                )
            )
            if records % 100_000 == 0:
                self.log.info("replay progress", extra={"records": records})
        elapsed = time.perf_counter() - started

        digests: dict[str, str] = {}
        sink_info: dict[str, Any] = {}
        for name, sink in (
            ("factors", factor_sink),
            ("signals", signal_sink),
            ("fills", fill_sink),
        ):
            if sink is not None:
                result = sink.close()
                digests[f"{name}_sha256"] = result.sha256
                sink_info[name] = {
                    "path": result.path,
                    "records": result.records,
                    "bytes": result.bytes,
                    "sha256": result.sha256,
                }

        scan = reader.scan()
        if self.compare_reference:
            live_summary = _load_json(self.run_dir / "summary.json") or {}
            comparison = self._compare(scan, pipeline, digests, live_summary)
        else:
            comparison = {
                "verified": not scan.errors and not scan.truncated_tail_bytes,
                "deterministic": None,
                "reference_compared": False,
                "reasons": [],
                "note": (
                    "variant execution semantics; derived output intentionally "
                    "differs from the reference run"
                ),
                "replay_counts": {
                    key: pipeline.counts.get(key)
                    for key in ("ws_frames", "snapshots", "factor_snapshots", "signals", "fills")
                },
            }
        result = {
            "run_id": self.run_dir.name,
            "replayed_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
            "elapsed_s": round(elapsed, 3),
            "records": records,
            "events": events,
            "throughput_events_per_s": round(records / elapsed, 1) if elapsed > 0 else None,
            "scan": {
                "records": scan.records,
                "records_by_kind": {str(k): v for k, v in scan.records_by_kind.items()},
                "first_seq": scan.first_seq,
                "last_seq": scan.last_seq,
                "truncated_tail_bytes": scan.truncated_tail_bytes,
                "errors": scan.errors,
            },
            "pipeline": pipeline.stats(),
            "digests": digests,
            "derived": sink_info,
            "comparison": comparison,
        }
        result_path = self.out_dir / "replay_summary.json"
        result_path.write_text(
            json.dumps(result, ensure_ascii=False, indent=2, default=str) + "\n",
            encoding="utf-8",
        )
        self.log.info(
            "replay complete",
            extra={
                "records": records,
                "elapsed_s": round(elapsed, 3),
                "deterministic": comparison.get("deterministic"),
                "verified": comparison.get("verified"),
            },
        )
        return result

    def _compare(
        self,
        scan: WalScanResult,
        pipeline: Pipeline,
        digests: dict[str, str],
        live: dict,
    ) -> dict:
        reasons: list[str] = []
        if not live:
            # A standalone sample has no reference run to compare against;
            # replay still proves decodability, but not determinism.
            reasons.append("no_live_summary")
        live_counts = (live.get("counts") or {}).get("pipeline") or {}
        replay_counts = pipeline.counts
        count_keys = ("ws_frames", "snapshots", "factor_snapshots", "signals", "fills")
        count_match = {key: live_counts.get(key) == replay_counts.get(key) for key in count_keys}
        if live_counts and not all(count_match.values()):
            reasons.append("count_mismatch")

        live_digests = live.get("digests") or {}
        digest_match = {
            key: (live_digests.get(key) == digests.get(key))
            for key in ("factors_sha256", "signals_sha256", "fills_sha256")
            if key in live_digests or key in digests
        }
        deterministic = bool(live_digests) and all(digest_match.values())
        if live_digests and not deterministic:
            reasons.append("digest_mismatch")

        live_wal = live.get("wal") or {}
        wal_match = {
            "records": live_wal.get("records_total") == scan.records,
            "ws_records": live_wal.get("records_ws") == scan.records_by_kind.get(KIND_WS, 0),
            "snapshot_records": live_wal.get("records_snapshot")
            == scan.records_by_kind.get(KIND_SNAPSHOT, 0),
        }
        if live_wal and not all(wal_match.values()):
            reasons.append("wal_count_mismatch")

        if scan.truncated_tail_bytes:
            reasons.append("truncated_wal_tail")
        if scan.errors:
            reasons.append("wal_scan_errors")
        verified = not reasons
        return {
            "verified": verified,
            "deterministic": deterministic,
            "reasons": reasons,
            "count_match": count_match,
            "digest_match": digest_match,
            "wal_match": wal_match,
            "live_counts": {key: live_counts.get(key) for key in count_keys},
            "replay_counts": {key: replay_counts.get(key) for key in count_keys},
            "live_summary_present": bool(live),
        }


def verify_run(run_dir: str | Path) -> dict:
    """Check WAL integrity and reconcile it with the run summary/manifest."""

    run_dir = Path(run_dir)
    raw_dir = run_dir / "raw"
    if not raw_dir.is_dir():
        raise FileNotFoundError(f"raw WAL directory not found: {raw_dir}")
    summary = _load_json(run_dir / "summary.json") or {}
    manifest = _load_json(run_dir / "manifest.json") or {}
    checks: dict[str, Any] = {}
    errors: list[str] = []
    try:
        reader = WalReader(raw_dir)
        scan = reader.scan()
    except WalCorruptionError as exc:
        return {
            "run_id": run_dir.name,
            "ok": False,
            "errors": [f"wal corruption: {exc}"],
            "scan": None,
        }

    checks["wal_scan_ok"] = not scan.errors
    if scan.errors:
        errors.extend(scan.errors)
    live_wal = summary.get("wal") or {}
    if live_wal:
        checks["records_match_summary"] = (
            live_wal.get("records_total") == scan.records
            and live_wal.get("records_ws") == scan.records_by_kind.get(KIND_WS, 0)
            and live_wal.get("records_snapshot")
            == scan.records_by_kind.get(KIND_SNAPSHOT, 0)
        )
        if not checks["records_match_summary"]:
            errors.append("WAL record counts do not match summary.json")
        reconciliation = summary.get("reconciliation") or {}
        checks["no_lost_raw_frames"] = int(reconciliation.get("lost_raw_frames", 0)) == 0
        if not checks["no_lost_raw_frames"]:
            errors.append("summary reports lost raw frames")
    else:
        checks["records_match_summary"] = None

    # Verify manifest segment hashes when present.
    manifest_segments = (manifest.get("wal") or {}).get("segments") or []
    hash_results = []
    for seg in manifest_segments:
        path = raw_dir / seg.get("name", "")
        expected = seg.get("sha256")
        if not path.is_file() or not expected:
            hash_results.append({"name": seg.get("name"), "ok": False, "reason": "missing"})
            errors.append(f"manifest segment missing: {seg.get('name')}")
            continue
        import hashlib

        h = hashlib.sha256()
        with path.open("rb") as fh:
            for chunk in iter(lambda: fh.read(1024 * 1024), b""):
                h.update(chunk)
        ok = h.hexdigest() == expected
        hash_results.append({"name": seg.get("name"), "ok": ok})
        if not ok:
            errors.append(f"segment hash mismatch: {seg.get('name')}")
    checks["segment_hashes"] = hash_results

    return {
        "run_id": run_dir.name,
        "ok": not errors,
        "errors": errors,
        "checks": checks,
        "scan": {
            "records": scan.records,
            "records_by_kind": {str(k): v for k, v in scan.records_by_kind.items()},
            "bytes": scan.bytes,
            "segments": scan.segments,
            "first_seq": scan.first_seq,
            "last_seq": scan.last_seq,
            "truncated_tail_bytes": scan.truncated_tail_bytes,
        },
        "summary_clean_shutdown": summary.get("clean_shutdown"),
        "summary_digests": summary.get("digests"),
    }


__all__ = ["ReplayRunner", "config_from_run", "verify_run"]
