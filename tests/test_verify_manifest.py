"""Regression tests for manifest accounting and replay failure-state checks."""

from __future__ import annotations

import hashlib
import json
import shutil
from pathlib import Path

from tickforge.replay.runner import ReplayRunner, verify_run
from tickforge.store.wal import WalReader

SAMPLE = Path(__file__).resolve().parents[1] / "samples" / "demo_run"


def _sha(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def _build_verifiable_run(tmp_path: Path) -> Path:
    """Copy the offline sample and add the summary/manifest a live run has."""

    run = tmp_path / "run"
    shutil.copytree(SAMPLE, run)
    scan = WalReader(run / "raw").scan()

    derived = run / "derived"
    derived.mkdir()
    digests: dict[str, str] = {}
    for name in ("factors", "signals", "fills"):
        path = derived / f"{name}.jsonl"
        path.write_bytes((json.dumps({"name": name}) + "\n").encode("utf-8"))
        digests[f"{name}_sha256"] = _sha(path)

    summary = json.loads((run / "summary.json").read_text(encoding="utf-8"))
    summary.update(
        {
            "mode": "live",
            "clean_shutdown": True,
            "derived_complete": True,
            "derived_errors": {},
            "digests": digests,
            "wal": {
                "records_total": scan.records,
                "records_ws": scan.records_by_kind.get(1, 0),
                "records_snapshot": scan.records_by_kind.get(2, 0),
            },
            "reconciliation": {"lost_raw_frames": 0, "drained": True},
        }
    )
    (run / "summary.json").write_text(
        json.dumps(summary, ensure_ascii=False, indent=2) + "\n", encoding="utf-8"
    )

    segments = []
    for name in scan.segments:
        path = run / "raw" / name
        segments.append(
            {
                "name": name,
                "records": scan.records,
                "bytes": path.stat().st_size,
                "sha256": _sha(path),
            }
        )
    files = [
        {"path": "summary.json", "sha256": _sha(run / "summary.json")},
        *[
            {"path": f"derived/{name}.jsonl", "sha256": digests[f"{name}_sha256"]}
            for name in ("factors", "signals", "fills")
        ],
    ]
    manifest = {
        "wal": {
            "segments": segments,
            "records": scan.records,
            "bytes": sum(seg["bytes"] for seg in segments),
            "first_seq": scan.first_seq,
            "last_seq": scan.last_seq,
            "durable_seq": scan.last_seq,
        },
        "files": files,
    }
    (run / "manifest.json").write_text(
        json.dumps(manifest, ensure_ascii=False, indent=2) + "\n", encoding="utf-8"
    )
    return run


def test_verify_rejects_manifest_aggregate_tampering(tmp_path: Path) -> None:
    run = _build_verifiable_run(tmp_path)
    assert verify_run(run)["ok"] is True

    manifest_path = run / "manifest.json"
    original = json.loads(manifest_path.read_text(encoding="utf-8"))
    for key, value in (
        ("records", 999),
        ("first_seq", 999),
        ("last_seq", 999999),
        ("durable_seq", 888888),
    ):
        tampered = json.loads(json.dumps(original))
        tampered["wal"][key] = value
        manifest_path.write_text(
            json.dumps(tampered, ensure_ascii=False, indent=2) + "\n",
            encoding="utf-8",
        )
        result = verify_run(run)
        assert result["ok"] is False
        assert any(f"manifest.wal.{key}" in error for error in result["errors"])

    manifest_path.write_text(
        json.dumps(original, ensure_ascii=False, indent=2) + "\n", encoding="utf-8"
    )


def test_verify_rejects_unlisted_wal_segment(tmp_path: Path) -> None:
    run = _build_verifiable_run(tmp_path)
    # An extra empty segment is structurally valid but not listed in the
    # manifest; the coverage check must catch it without relying on a
    # duplicate-sequence scan error.
    (run / "raw" / "raw-999999.wal").write_bytes(b"")

    result = verify_run(run)

    assert result["ok"] is False
    assert any(
        "manifest is missing WAL segments" in error for error in result["errors"]
    )


def test_replay_rejects_live_failure_state(tmp_path: Path) -> None:
    run = tmp_path / "live_fail"
    shutil.copytree(SAMPLE, run)
    summary_path = run / "summary.json"
    summary = json.loads(summary_path.read_text(encoding="utf-8"))
    summary.update(
        {
            "mode": "live",
            "clean_shutdown": False,
            "derived_complete": False,
            "derived_errors": {"factors": "simulated close failure"},
        }
    )
    summary_path.write_text(
        json.dumps(summary, ensure_ascii=False, indent=2) + "\n", encoding="utf-8"
    )

    comparison = ReplayRunner(
        run, out_dir=tmp_path / "replay_fail", write_derived=True
    ).run()["comparison"]

    assert comparison["verified"] is False
    for reason in (
        "live_clean_shutdown_failed",
        "live_derived_complete_failed",
        "live_derived_errors_present",
    ):
        assert reason in comparison["reasons"]


def test_replay_keeps_offline_sample_compatible(tmp_path: Path) -> None:
    comparison = ReplayRunner(
        SAMPLE, out_dir=tmp_path / "sample_replay", write_derived=True
    ).run()["comparison"]

    assert comparison["verified"] is True
    assert comparison["deterministic"] is True
