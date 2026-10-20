"""Binance market-data connectivity.

This package contains the two edge-facing components of TickForge:

* :class:`~tickforge.feed.binance.BinanceFeed` -- a resilient combined-stream
  WebSocket reader that timestamps every frame on arrival and hands the raw
  bytes to the ingress pipeline.
* :class:`~tickforge.feed.rest.SnapshotClient` -- a small aiohttp client for
  the REST depth snapshot needed to synchronise the order book.

Both components are independent of the rest of the pipeline and are exercised
against local mock servers in the test suite.
"""

from __future__ import annotations

from .binance import BinanceFeed, FeedError, FeedStale
from .rest import SnapshotClient, SnapshotData, SnapshotError

__all__ = [
    "BinanceFeed",
    "FeedError",
    "FeedStale",
    "SnapshotClient",
    "SnapshotData",
    "SnapshotError",
]
