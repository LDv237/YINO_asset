"""Persistent storage primitives: the binary WAL and derived JSONL sinks.

The WAL persists *raw* exchange payloads (WebSocket frames and REST snapshot
JSON) with receive timestamps and a global sequence number, so a replay can
rebuild the pipeline byte-for-byte.  Derived outputs (factor snapshots,
signals, fills) are written as deterministic JSON lines whose SHA-256 is the
replay equivalence check.
"""

from __future__ import annotations

from .derived import JsonlSink, SinkResult
from .wal import (
    DEFAULT_RECORD_MAX_BYTES,
    HEADER,
    HEADER_SIZE,
    KIND_SNAPSHOT,
    KIND_WS,
    MAGIC,
    WalClosed,
    WalCorruptionError,
    WalError,
    WalManifest,
    WalQueueFull,
    WalReader,
    WalRecord,
    WalScanResult,
    WalWriter,
)

__all__ = [
    "DEFAULT_RECORD_MAX_BYTES",
    "HEADER",
    "HEADER_SIZE",
    "KIND_SNAPSHOT",
    "KIND_WS",
    "MAGIC",
    "WalError",
    "WalCorruptionError",
    "WalQueueFull",
    "WalClosed",
    "WalRecord",
    "WalManifest",
    "WalScanResult",
    "WalWriter",
    "WalReader",
    "JsonlSink",
    "SinkResult",
]
