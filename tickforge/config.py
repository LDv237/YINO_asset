"""Typed configuration with TOML loading and conservative validation.

All tunables have defaults so the program can be started without a config
file.  ``config/default.toml`` documents the same values for operators.
"""

from __future__ import annotations

import math
import os
import tomllib
from dataclasses import asdict, dataclass, field, replace
from pathlib import Path
from typing import Any

DEFAULT_SYMBOLS = (
    "BTCUSDT",
    "ETHUSDT",
    "SOLUSDT",
    "BNBUSDT",
    "XRPUSDT",
    "DOGEUSDT",
)


@dataclass(slots=True)
class FeedConfig:
    ws_url: str = "wss://data-stream.binance.vision/stream"
    rest_url: str = "https://data-api.binance.vision"
    symbols: list[str] = field(default_factory=lambda: list(DEFAULT_SYMBOLS))
    depth_speed_ms: int = 100
    stale_timeout_ms: int = 5_000
    open_timeout_s: float = 10.0
    close_timeout_s: float = 2.0
    ping_interval_s: float = 20.0
    ping_timeout_s: float = 20.0
    reconnect_initial_s: float = 0.5
    reconnect_max_s: float = 30.0
    message_max_bytes: int = 16 * 1024 * 1024
    snapshot_limit: int = 5_000
    snapshot_timeout_s: float = 8.0
    snapshot_retries: int = 5
    snapshot_retry_backoff_s: float = 0.5

    def streams(self) -> list[str]:
        return build_streams(self.symbols, self.depth_speed_ms)

    def combined_ws_url(self) -> str:
        streams = "/".join(self.streams())
        sep = "&" if "?" in self.ws_url else "?"
        return f"{self.ws_url}{sep}streams={streams}"


@dataclass(slots=True)
class WalConfig:
    segment_bytes: int = 64 * 1024 * 1024
    fsync_interval_ms: int = 250  # "interval" policy
    fsync_policy: str = "interval"  # interval | always | never
    queue_max: int = 500_000
    record_max_bytes: int = 8 * 1024 * 1024


@dataclass(slots=True)
class EngineConfig:
    proc_queue_max: int = 200_000
    book_buffer_max: int = 50_000
    factor_interval_ms: int = 100
    stats_interval_s: float = 10.0
    health_interval_s: float = 1.0
    drain_timeout_s: float = 30.0
    max_processing_lag_ms: int = 2_000


@dataclass(slots=True)
class StrategyConfig:
    quote_notional_usd: float = 500.0
    max_position_notional_usd: float = 5_000.0
    inventory_skew: float = 0.25
    ofi_coeff_bps: float = 0.8
    momentum_coeff: float = 0.35
    min_half_spread_bps: float = 0.8
    vol_half_spread_coeff: float = 0.6
    max_half_spread_bps: float = 25.0
    fee_bps: float = 1.0
    quote_cooldown_ms: int = 100
    enabled: bool = True
    # Execution semantics.  The default preserves the original abstract
    # two-sided ledger (negative positions allowed).  Set allow_short=false for
    # a spot long-only variant; initial_cash_usd is then the per-symbol quote
    # balance used to fund buys.
    allow_short: bool = True
    initial_cash_usd: float = 0.0
    # Quote prices are rounded to the exchange tick size when enabled.  Kept
    # off by default so historical run configs (which predate the field)
    # replay byte-for-byte; new configs enable it explicitly.
    round_to_tick: bool = False


@dataclass(slots=True)
class HttpConfig:
    host: str = "127.0.0.1"
    port: int = 9100
    enabled: bool = True


@dataclass(slots=True)
class RunConfig:
    duration_s: float = 900.0
    output_dir: str = "artifacts"
    run_id: str | None = None
    log_level: str = "INFO"
    seed: int = 7  # reserved for synthetic tools; live path is deterministic


@dataclass(slots=True)
class Config:
    feed: FeedConfig = field(default_factory=FeedConfig)
    wal: WalConfig = field(default_factory=WalConfig)
    engine: EngineConfig = field(default_factory=EngineConfig)
    strategy: StrategyConfig = field(default_factory=StrategyConfig)
    http: HttpConfig = field(default_factory=HttpConfig)
    run: RunConfig = field(default_factory=RunConfig)

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)

    def validate(self) -> None:
        f = self.feed
        symbols = [s.strip().upper() for s in f.symbols if s.strip()]
        if not symbols:
            raise ValueError("at least one symbol is required")
        if len(set(symbols)) != len(symbols):
            raise ValueError("duplicate symbols in configuration")
        for sym in symbols:
            if not sym.isalnum() or not sym.endswith("USDT"):
                raise ValueError(f"unsupported symbol (demo expects *USDT): {sym}")
        f.symbols = symbols
        if not f.ws_url.startswith(("ws://", "wss://")):
            raise ValueError(f"invalid ws_url: {f.ws_url}")
        if not f.rest_url.startswith(("http://", "https://")):
            raise ValueError(f"invalid rest_url: {f.rest_url}")
        if f.depth_speed_ms not in (100, 1000):
            raise ValueError("depth_speed_ms must be 100 or 1000 for Binance spot")
        if f.snapshot_limit not in (5, 10, 20, 50, 100, 500, 1000, 5000):
            raise ValueError("snapshot_limit must be a Binance-supported depth limit")
        if f.snapshot_retries < 1:
            raise ValueError("snapshot_retries must be >= 1")

        w = self.wal
        if w.fsync_policy not in ("interval", "always", "never"):
            raise ValueError("fsync_policy must be interval|always|never")
        if w.segment_bytes < 1_000_000:
            raise ValueError("segment_bytes must be >= 1MB")
        if w.queue_max < 1_000:
            raise ValueError("wal.queue_max must be >= 1000")

        e = self.engine
        if not 10 <= e.factor_interval_ms <= 60_000:
            raise ValueError("factor_interval_ms must be in [10, 60000]")
        if e.proc_queue_max < 1_000:
            raise ValueError("proc_queue_max must be >= 1000")

        s = self.strategy
        if s.quote_notional_usd <= 0 or s.max_position_notional_usd <= 0:
            raise ValueError("quote and position notionals must be positive")
        if s.initial_cash_usd < 0 or not math.isfinite(s.initial_cash_usd):
            raise ValueError("initial_cash_usd must be a finite non-negative number")
        if not s.allow_short and s.initial_cash_usd <= 0:
            raise ValueError("spot long-only mode requires initial_cash_usd > 0")
        if s.max_position_notional_usd < s.quote_notional_usd:
            raise ValueError("max_position_notional_usd must be >= quote_notional_usd")

        if self.http.enabled and not 0 <= self.http.port <= 65_535:
            raise ValueError("http.port out of range")
        if self.run.duration_s < 0:
            raise ValueError("duration_s must be >= 0")


def build_streams(symbols: list[str], depth_speed_ms: int = 100) -> list[str]:
    out: list[str] = []
    for symbol in symbols:
        s = symbol.lower()
        out.append(f"{s}@depth@{depth_speed_ms}ms")
        out.append(f"{s}@aggTrade")
        out.append(f"{s}@bookTicker")
    return out


def _filtered(dc_type: type, values: dict[str, Any]) -> dict[str, Any]:
    allowed = set(dc_type.__dataclass_fields__)
    unknown = set(values) - allowed
    if unknown:
        raise ValueError(f"unknown config keys for {dc_type.__name__}: {sorted(unknown)}")
    return values


def _apply_env(cfg: Config) -> Config:
    """Small set of environment overrides useful in Docker/CI.

    Parsing is intentionally strict: a malformed override should fail at
    startup rather than silently run with a default.
    """

    if v := os.environ.get("TICKFORGE_WS_URL"):
        cfg.feed.ws_url = v
    if v := os.environ.get("TICKFORGE_REST_URL"):
        cfg.feed.rest_url = v
    if v := os.environ.get("TICKFORGE_SYMBOLS"):
        cfg.feed.symbols = [s.strip() for s in v.split(",") if s.strip()]
    if v := os.environ.get("TICKFORGE_HTTP_HOST"):
        cfg.http.host = v
    if v := os.environ.get("TICKFORGE_HTTP_PORT"):
        cfg.http.port = int(v)
    if v := os.environ.get("TICKFORGE_OUTPUT_DIR"):
        cfg.run.output_dir = v
    if v := os.environ.get("TICKFORGE_DURATION_S"):
        cfg.run.duration_s = float(v)
    if v := os.environ.get("TICKFORGE_FSYNC_POLICY"):
        cfg.wal.fsync_policy = v
    if v := os.environ.get("TICKFORGE_LOG_LEVEL"):
        cfg.run.log_level = v.upper()
    return cfg


def load_config(path: str | Path | None = None, *, apply_env: bool = True) -> Config:
    cfg = Config()
    if path is not None:
        p = Path(path)
        with p.open("rb") as fh:
            raw = tomllib.load(fh)
        unknown_sections = set(raw) - {"feed", "wal", "engine", "strategy", "http", "run"}
        if unknown_sections:
            raise ValueError(f"unknown config sections: {sorted(unknown_sections)}")
        if "feed" in raw:
            cfg.feed = FeedConfig(**_filtered(FeedConfig, raw["feed"]))
        if "wal" in raw:
            cfg.wal = WalConfig(**_filtered(WalConfig, raw["wal"]))
        if "engine" in raw:
            cfg.engine = EngineConfig(**_filtered(EngineConfig, raw["engine"]))
        if "strategy" in raw:
            cfg.strategy = StrategyConfig(**_filtered(StrategyConfig, raw["strategy"]))
        if "http" in raw:
            cfg.http = HttpConfig(**_filtered(HttpConfig, raw["http"]))
        if "run" in raw:
            cfg.run = RunConfig(**_filtered(RunConfig, raw["run"]))
    if apply_env:
        cfg = _apply_env(cfg)
    cfg.validate()
    return cfg


def with_overrides(cfg: Config, **changes: Any) -> Config:
    """Return a copy with top-level nested section overrides.

    Example: ``with_overrides(cfg, run=replace(cfg.run, duration_s=60))``.
    """

    return replace(cfg, **changes)


__all__ = [
    "DEFAULT_SYMBOLS",
    "FeedConfig",
    "WalConfig",
    "EngineConfig",
    "StrategyConfig",
    "HttpConfig",
    "RunConfig",
    "Config",
    "build_streams",
    "load_config",
    "with_overrides",
]
