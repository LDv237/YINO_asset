"""Replay config loading must not silently fall back to defaults."""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from tickforge.config import Config
from tickforge.replay.runner import config_from_run


def test_config_from_run_uses_summary_when_config_missing(tmp_path: Path) -> None:
    summary = {"config": Config().to_dict()}
    (tmp_path / "summary.json").write_text(
        json.dumps(summary, ensure_ascii=False), encoding="utf-8"
    )

    cfg = config_from_run(tmp_path)

    assert cfg.feed.symbols == Config().feed.symbols
    assert cfg.wal.record_max_bytes == Config().wal.record_max_bytes


def test_config_from_run_requires_config_or_summary(tmp_path: Path) -> None:
    with pytest.raises(FileNotFoundError):
        config_from_run(tmp_path)
