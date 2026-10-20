"""Fail-closed tests for the delivery verification tool."""

from __future__ import annotations

from pathlib import Path

from tickforge.replay.runner import verify_run


def test_verify_run_rejects_missing_directory(tmp_path: Path) -> None:
    result = verify_run(tmp_path / "does-not-exist")
    assert result["ok"] is False
    assert any("run directory not found" in error for error in result["errors"])


def test_verify_run_rejects_missing_summary_and_manifest(tmp_path: Path) -> None:
    (tmp_path / "raw").mkdir()
    result = verify_run(tmp_path)
    assert result["ok"] is False
    errors = " ".join(result["errors"])
    assert "summary.json missing" in errors
    assert "manifest.json missing" in errors
    assert "WAL has no records" in errors


def test_verify_run_rejects_corrupt_summary(tmp_path: Path) -> None:
    (tmp_path / "raw").mkdir()
    (tmp_path / "summary.json").write_text("{not json", encoding="utf-8")
    (tmp_path / "manifest.json").write_text("{}", encoding="utf-8")
    result = verify_run(tmp_path)
    assert result["ok"] is False
    assert any("summary.json is not valid JSON" in error for error in result["errors"])
