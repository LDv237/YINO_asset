"""JSONL sinks for derived records (factor snapshots, signals, fills).

The sinks are deliberately simple: they run on the asyncio event-loop thread
and perform no locking.  Serialisation uses ``orjson`` with the dictionary's
insertion order untouched, because ``FactorSnapshot.to_dict`` /
``Signal.to_dict`` / ``Fill.to_dict`` guarantee a stable key order and the
resulting byte stream (and therefore its SHA-256) is the replay determinism
check.  Reordering keys or injecting timestamps here would break that
contract.
"""

from __future__ import annotations

import hashlib
import os
import time
from dataclasses import dataclass
from pathlib import Path

import orjson

__all__ = ["SinkResult", "JsonlSink"]


@dataclass(slots=True)
class SinkResult:
    """Final accounting for a :class:`JsonlSink`.

    ``bytes`` is the exact size of the file and ``sha256`` is its hex digest;
    together with ``records`` they make the output reproducible and easy to
    compare between a live run and a replay.
    """

    path: str
    records: int
    bytes: int
    sha256: str


def _sha256_file(path: Path) -> str:
    """Return the hex SHA-256 digest of *path*, reading in bounded chunks."""

    digest = hashlib.sha256()
    with path.open("rb") as fh:
        for chunk in iter(lambda: fh.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


class JsonlSink:
    """Append-only newline-delimited JSON writer.

    One UTF-8 JSON document is written per line with ``orjson``.  Data is
    buffered by the file object; ``flush_interval_s`` controls how often the
    buffer is pushed to the operating system (``0`` flushes after every
    record), and ``fsync=True`` also issues ``os.fsync`` at each flush and at
    close.  Flush timing never changes the bytes written.

    The sink is not thread-safe by design: it must be used from the event-loop
    thread.  ``close()`` is idempotent and returns the same
    :class:`SinkResult` on every call.
    """

    def __init__(
        self,
        path: str | Path,
        *,
        flush_interval_s: float = 1.0,
        fsync: bool = False,
    ) -> None:
        if flush_interval_s < 0:
            raise ValueError("flush_interval_s must be >= 0")
        self._path = Path(path)
        self._path.parent.mkdir(parents=True, exist_ok=True)
        self._fh = self._path.open("wb")
        self._flush_interval_ns = int(flush_interval_s * 1_000_000_000)
        self._fsync = bool(fsync)
        self._last_flush_ns = time.monotonic_ns()
        self._records = 0
        self._bytes = 0
        self._result: SinkResult | None = None

    def write(self, obj: dict) -> None:
        """Serialize *obj* as one JSON line and buffer it.

        Raises ``ValueError`` if the sink is already closed and propagates
        ``orjson`` serialisation errors unchanged.
        """

        if self._result is not None:
            raise ValueError(f"JSONL sink is closed: {self._path}")
        line = orjson.dumps(obj) + b"\n"
        written = self._fh.write(line)
        if written != len(line):
            raise OSError(
                f"short write to {self._path}: {written} of {len(line)} bytes"
            )
        self._records += 1
        self._bytes += len(line)
        if (
            self._flush_interval_ns == 0
            or time.monotonic_ns() - self._last_flush_ns >= self._flush_interval_ns
        ):
            self._flush()

    def _flush(self) -> None:
        self._fh.flush()
        if self._fsync:
            os.fsync(self._fh.fileno())
        self._last_flush_ns = time.monotonic_ns()

    def close(self) -> SinkResult:
        """Flush, close, hash the file and return the final accounting.

        Idempotent.  The digest is computed from the bytes on disk, so it is
        exactly reproducible for identical input.
        """

        if self._result is not None:
            return self._result
        try:
            self._fh.flush()
            if self._fsync:
                os.fsync(self._fh.fileno())
        finally:
            self._fh.close()
        self._result = SinkResult(
            path=str(self._path),
            records=self._records,
            bytes=self._bytes,
            sha256=_sha256_file(self._path),
        )
        return self._result

    def __enter__(self) -> JsonlSink:
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        self.close()
