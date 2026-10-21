"""Deterministic WAL replay and integrity verification.

Replay is the main correctness argument of the project: the live run stores
every raw input event (including order-book snapshots) in the WAL; replaying
those bytes through the *same* ``Pipeline`` must reproduce the same factor,
signal and fill streams, byte for byte, and must reproduce the same counts.
"""

from __future__ import annotations

import hashlib
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


def _read_required_json(path: Path, label: str, errors: list[str]) -> dict:
    """Read a required JSON object, recording a failure instead of hiding it."""

    if not path.is_file():
        errors.append(f"{label} missing: {path.name}")
        return {}
    try:
        with path.open("r", encoding="utf-8") as fh:
            data = json.load(fh)
    except (OSError, ValueError) as exc:
        errors.append(f"{label} is not valid JSON: {exc}")
        return {}
    if not isinstance(data, dict):
        errors.append(f"{label} must be a JSON object")
        return {}
    return data


def _sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as fh:
        for chunk in iter(lambda: fh.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _build(cls, values: dict) -> Any:
    allowed = {f.name for f in fields(cls)}
    return cls(**{k: v for k, v in values.items() if k in allowed})


def config_from_run(run_dir: str | Path) -> Config:
    """Load the config snapshot written by the live run.

    Reproducibility requires using the *same* factor/strategy parameters as
    the original run, not whatever happens to be in the caller's environment.
    A missing/corrupt ``config.json`` falls back to ``summary.json``'s config
    (written by the same run); if neither exists, fail instead of silently
    using defaults.
    """

    run_dir = Path(run_dir)
    doc = _load_json(run_dir / "config.json")
    if doc is not None:
        raw = doc.get("config")
        if not raw:
            raise ValueError(f"config.json has no config section: {run_dir}")
    else:
        summary = _load_json(run_dir / "summary.json") or {}
        raw = summary.get("config")
        if not raw:
            raise FileNotFoundError(
                f"config.json is missing and summary.json has no config: {run_dir}"
            )
        log.warning("config.json missing; using summary.json config for %s", run_dir)

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
        cfg: Config | None = None,
        compare_reference: bool = True,
        logger: logging.Logger | None = None,
    ) -> None:
        self.run_dir = Path(run_dir)
        self.raw_dir = self.run_dir / "raw"
        self.out_dir = (
            Path(out_dir)
            if out_dir
            else self.run_dir.parent / f"{self.run_dir.name}-replay"
        )
        self.write_derived = write_derived
        self.log = logger or log
        self.cfg = cfg or config_from_run(self.run_dir)
        self.compare_reference = compare_reference
        self._out_prepared = False

    def prepare_out_dir(self) -> None:
        """Create the replay output directory, refusing to overwrite evidence."""

        if self.out_dir.exists() and (
            not self.out_dir.is_dir() or any(self.out_dir.iterdir())
        ):
            raise FileExistsError(
                f"replay output directory is not empty: {self.out_dir}; "
                "choose --out or remove the old output"
            )
        self.out_dir.mkdir(parents=True, exist_ok=True)
        self._out_prepared = True

    def run(self) -> dict:
        if not self.raw_dir.is_dir():
            raise FileNotFoundError(f"raw WAL directory not found: {self.raw_dir}")
        if not self._out_prepared:
            self.prepare_out_dir()
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

        reader = WalReader(
            self.raw_dir,
            verify_crc=True,
            allow_truncated_tail=True,
            record_max_bytes=self.cfg.wal.record_max_bytes,
        )
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

        scan = WalReader(
            self.raw_dir, record_max_bytes=self.cfg.wal.record_max_bytes
        ).scan()
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
        self._out_prepared = False
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
        if not live_counts:
            reasons.append("live_counts_missing")
        elif not all(count_match.values()):
            reasons.append("count_mismatch")

        live_digests = live.get("digests") or {}
        digest_match = {
            key: (live_digests.get(key) == digests.get(key))
            for key in ("factors_sha256", "signals_sha256", "fills_sha256")
            if key in live_digests or key in digests
        }
        deterministic = bool(live_digests) and all(digest_match.values())
        if not live_digests:
            reasons.append("live_digests_missing")
        elif not deterministic:
            reasons.append("digest_mismatch")

        # Re-hash the live derived files as well.  A tampered live file must
        # fail replay verification even when summary.json still has the old
        # digest.  Samples have no derived/ directory and no manifest.json, so
        # they keep comparing against the reference summary.
        live_derived_dir = self.run_dir / "derived"
        live_manifest_present = (self.run_dir / "manifest.json").is_file()
        live_file_digests: dict[str, str] = {}
        missing_derived: list[str] = []
        if live_derived_dir.is_dir():
            for name in ("factors", "signals", "fills"):
                path = live_derived_dir / f"{name}.jsonl"
                if path.is_file():
                    live_file_digests[f"{name}_sha256"] = _sha256_file(path)
                else:
                    missing_derived.append(name)
        elif live_manifest_present:
            missing_derived = ["factors", "signals", "fills"]
        if missing_derived:
            reasons.append("live_derived_missing")
        live_derived_match: dict[str, bool] = {}
        if live_file_digests:
            live_derived_match = {
                key: (live_file_digests.get(key) == digests.get(key))
                for key in ("factors_sha256", "signals_sha256", "fills_sha256")
            }
            if not all(live_derived_match.values()):
                reasons.append("live_derived_digest_mismatch")

        live_wal = live.get("wal") or {}
        wal_match = {
            "records": live_wal.get("records_total") == scan.records,
            "ws_records": live_wal.get("records_ws") == scan.records_by_kind.get(KIND_WS, 0),
            "snapshot_records": live_wal.get("records_snapshot")
            == scan.records_by_kind.get(KIND_SNAPSHOT, 0),
        }
        if not live_wal:
            reasons.append("live_wal_missing")
        elif not all(wal_match.values()):
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
            "live_derived_digest_match": live_derived_match,
            "wal_match": wal_match,
            "live_counts": {key: live_counts.get(key) for key in count_keys},
            "replay_counts": {key: replay_counts.get(key) for key in count_keys},
            "live_summary_present": bool(live),
        }


def verify_run(run_dir: str | Path) -> dict:
    """Check WAL integrity and reconcile it with the run summary/manifest.

    The check is fail-closed: a missing or corrupt summary/manifest, an empty
    WAL, a non-clean shutdown, or a manifest file hash mismatch is a failure,
    not a warning.
    """

    run_dir = Path(run_dir)
    checks: dict[str, Any] = {}
    errors: list[str] = []
    if not run_dir.is_dir():
        return {
            "run_id": run_dir.name,
            "ok": False,
            "errors": [f"run directory not found: {run_dir}"],
        }

    summary = _read_required_json(run_dir / "summary.json", "summary.json", errors)
    manifest = _read_required_json(run_dir / "manifest.json", "manifest.json", errors)

    raw_dir = run_dir / "raw"
    if not raw_dir.is_dir():
        errors.append(f"raw WAL directory not found: {raw_dir}")
        return {
            "run_id": run_dir.name,
            "ok": False,
            "errors": errors,
            "checks": checks,
        }

    record_max_bytes = None
    try:
        record_max_bytes = config_from_run(run_dir).wal.record_max_bytes
    except Exception:
        pass
    try:
        reader = WalReader(raw_dir, record_max_bytes=record_max_bytes)
        scan = reader.scan()
    except WalCorruptionError as exc:
        return {
            "run_id": run_dir.name,
            "ok": False,
            "errors": errors + [f"wal corruption: {exc}"],
            "scan": None,
        }

    checks["wal_scan_ok"] = not scan.errors
    if scan.errors:
        errors.extend(scan.errors)
    if scan.records == 0:
        errors.append("WAL has no records")

    live_wal = summary.get("wal") or {}
    if not live_wal:
        checks["records_match_summary"] = None
        errors.append("summary.json is missing wal counts")
    else:
        checks["records_match_summary"] = (
            live_wal.get("records_total") == scan.records
            and live_wal.get("records_ws") == scan.records_by_kind.get(KIND_WS, 0)
            and live_wal.get("records_snapshot")
            == scan.records_by_kind.get(KIND_SNAPSHOT, 0)
        )
        if not checks["records_match_summary"]:
            errors.append("WAL record counts do not match summary.json")

    reconciliation = summary.get("reconciliation")
    if not isinstance(reconciliation, dict):
        checks["no_lost_raw_frames"] = None
        errors.append("summary.json is missing reconciliation")
    else:
        checks["no_lost_raw_frames"] = int(reconciliation.get("lost_raw_frames", 0)) == 0
        if not checks["no_lost_raw_frames"]:
            errors.append("summary reports lost raw frames")

    checks["clean_shutdown"] = summary.get("clean_shutdown") is True
    if not checks["clean_shutdown"]:
        errors.append("summary.clean_shutdown is not true")

    # Older summaries predate these fields; an explicit failure is conclusive.
    if "derived_complete" in summary and summary["derived_complete"] is not True:
        checks["derived_complete"] = False
        errors.append("summary.derived_complete is not true")
    else:
        checks["derived_complete"] = True
    derived_errors = summary.get("derived_errors")
    if derived_errors:
        checks["derived_errors"] = False
        errors.append(f"summary reports derived close errors: {derived_errors}")
    else:
        checks["derived_errors"] = True

    # Fail-closed on incomplete derived output: all three files must have been
    # flushed, closed and hashed.  Older summaries predate derived_errors but
    # still carry the three digests, so this check stays backward compatible.
    digests = summary.get("digests")
    if not isinstance(digests, dict):
        checks["derived_digests_complete"] = False
        errors.append("summary.json is missing digests")
    else:
        missing = []
        for name in ("factors", "signals", "fills"):
            key = f"{name}_sha256"
            value = digests.get(key)
            if not isinstance(value, str) or not value:
                missing.append(key)
        checks["derived_digests_complete"] = not missing
        if missing:
            errors.append(
                "summary.json is missing derived digests: " + ", ".join(missing)
            )

    manifest_segments = (manifest.get("wal") or {}).get("segments") or []
    if not manifest_segments:
        errors.append("manifest.json is missing wal.segments")
    hash_results = []
    for seg in manifest_segments:
        name = seg.get("name", "")
        expected = seg.get("sha256")
        path = raw_dir / name
        if not name or not path.is_file() or not expected:
            hash_results.append({"name": name, "ok": False, "reason": "missing"})
            errors.append(f"manifest segment missing: {name}")
            continue
        ok = _sha256_file(path) == expected
        hash_results.append({"name": name, "ok": ok})
        if not ok:
            errors.append(f"segment hash mismatch: {name}")
    checks["segment_hashes"] = hash_results

    manifest_files = manifest.get("files") or []
    if not manifest_files:
        errors.append("manifest.json is missing files")
    file_results = []
    for item in manifest_files:
        rel = item.get("path")
        expected = item.get("sha256")
        if not rel or not expected:
            file_results.append({"path": rel, "ok": False, "reason": "missing hash"})
            errors.append(f"manifest file entry missing path/sha256: {rel}")
            continue
        path = run_dir / rel
        if not path.is_file():
            file_results.append({"path": rel, "ok": False, "reason": "missing"})
            errors.append(f"manifest file missing: {rel}")
            continue
        ok = _sha256_file(path) == expected
        file_results.append({"path": rel, "ok": ok})
        if not ok:
            errors.append(f"manifest file hash mismatch: {rel}")
    checks["manifest_files"] = file_results

    # The digest-completeness check above only proves the keys exist.  Re-hash
    # the three derived files so a summary with fabricated digests or a run
    # directory whose derived output vanished cannot verify ok.
    manifest_hashes = {
        item.get("path"): item.get("sha256")
        for item in manifest_files
        if isinstance(item, dict) and item.get("path")
    }
    derived_results = []
    for name in ("factors", "signals", "fills"):
        rel = f"derived/{name}.jsonl"
        path = run_dir / rel
        expected = digests.get(f"{name}_sha256") if isinstance(digests, dict) else None
        entry: dict[str, Any] = {"path": rel, "ok": False}
        if not path.is_file():
            entry["reason"] = "missing"
            errors.append(f"derived file missing: {rel}")
        else:
            actual = _sha256_file(path)
            entry["sha256"] = actual
            if not isinstance(expected, str) or not expected:
                entry["reason"] = "missing digest"
            elif actual != expected:
                entry["reason"] = "digest mismatch"
                errors.append(f"derived file hash mismatch: {rel}")
            elif manifest_hashes.get(rel) != actual:
                entry["reason"] = "manifest mismatch"
                errors.append(f"manifest.json has no matching hash for: {rel}")
            else:
                entry["ok"] = True
        derived_results.append(entry)
    checks["derived_files"] = derived_results

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
