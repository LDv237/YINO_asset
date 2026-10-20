from __future__ import annotations

from pathlib import Path

import pytest

from tickforge.config import Config, build_streams, load_config


def test_defaults_validate_and_build_streams() -> None:
    cfg = load_config(None, apply_env=False)
    assert cfg.feed.symbols[0] == "BTCUSDT"
    streams = cfg.feed.streams()
    assert len(streams) == len(cfg.feed.symbols) * 3
    assert "btcusdt@depth@100ms" in streams
    assert "btcusdt@aggTrade" in streams
    url = cfg.feed.combined_ws_url()
    assert url.startswith("wss://")
    assert "streams=btcusdt@depth@100ms/" in url


def test_toml_override_and_unknown_key(tmp_path: Path) -> None:
    path = tmp_path / "c.toml"
    path.write_text(
        """
[feed]
symbols = ["BTCUSDT"]
depth_speed_ms = 1000

[wal]
segment_bytes = 2000000
fsync_policy = "always"

[run]
duration_s = 5.0
""",
        encoding="utf-8",
    )
    cfg = load_config(path, apply_env=False)
    assert cfg.feed.symbols == ["BTCUSDT"]
    assert cfg.feed.depth_speed_ms == 1000
    assert cfg.wal.fsync_policy == "always"
    assert cfg.run.duration_s == 5.0

    path.write_text("[feed]\nunknown = 1\n", encoding="utf-8")
    with pytest.raises(ValueError, match="unknown config keys"):
        load_config(path, apply_env=False)


def test_env_overrides(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("TICKFORGE_SYMBOLS", "BTCUSDT,ETHUSDT")
    monkeypatch.setenv("TICKFORGE_HTTP_PORT", "9200")
    monkeypatch.setenv("TICKFORGE_DURATION_S", "12.5")
    cfg = load_config(None)
    assert cfg.feed.symbols == ["BTCUSDT", "ETHUSDT"]
    assert cfg.http.port == 9200
    assert cfg.run.duration_s == 12.5


@pytest.mark.parametrize(
    "mutate",
    [
        lambda c: setattr(c.feed, "symbols", []),
        lambda c: setattr(c.feed, "symbols", ["BTCUSDT", "BTCUSDT"]),
        lambda c: setattr(c.feed, "symbols", ["BTC"]),
        lambda c: setattr(c.feed, "depth_speed_ms", 250),
        lambda c: setattr(c.feed, "snapshot_limit", 7),
        lambda c: setattr(c.wal, "fsync_policy", "sometimes"),
        lambda c: setattr(c.wal, "segment_bytes", 100),
        lambda c: setattr(c.engine, "factor_interval_ms", 1),
        lambda c: setattr(c.http, "port", 70000),
        lambda c: setattr(c.run, "duration_s", -1),
    ],
)
def test_validation_rejects_bad_values(mutate) -> None:
    cfg = Config()
    mutate(cfg)
    with pytest.raises(ValueError):
        cfg.validate()


def test_build_streams_exact_order() -> None:
    streams = build_streams(["BTCUSDT"], 100)
    assert streams == [
        "btcusdt@depth@100ms",
        "btcusdt@aggTrade",
        "btcusdt@bookTicker",
    ]
