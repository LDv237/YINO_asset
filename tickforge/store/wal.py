"""Binary write-ahead log for raw exchange frames.

On-disk format
--------------
Every record is a 40-byte little-endian header followed by a payload::

    offset  type   meaning
    0       4s     magic ``b"TFW1"``
    4       B      kind: 1 = raw WebSocket frame, 2 = REST snapshot JSON
    5       3x     alignment padding
    8       I      payload length in bytes
    12      I      ``zlib.crc32(payload)``
    16      q      receive wall-clock nanoseconds (``time.time_ns()``)
    24      q      receive monotonic nanoseconds (``time.perf_counter_ns()``)
    32      Q      globally increasing sequence number
    40      ...    payload

Segments are named ``raw-000001.wal``, ``raw-000002.wal`` and are rotated
before a record would exceed ``segment_bytes``.  A single record never spans
two segments.

Concurrency model
-----------------
``append_ws`` / ``append_snapshot`` are called from the asyncio event loop and
only perform an in-memory, bounded, non-blocking queue insertion.  A dedicated
writer thread drains the queue, performs all file I/O and fsyncs according to
the configured policy.  A full queue raises :class:`WalQueueFull` (never drops
data); the caller is expected to apply backpressure and retry.

The sequence number is part of the record header and must increase strictly.
Both the writer (on append) and the reader (on scan/replay) enforce this.
"""

from __future__ import annotations

import hashlib
import logging
import os
import queue
import re
import struct
import threading
import time
import zlib
from dataclasses import dataclass
from pathlib import Path
from typing import Callable, Iterator

__all__ = [
    "HEADER",
    "HEADER_SIZE",
    "MAGIC",
    "KIND_WS",
    "KIND_SNAPSHOT",
    "DEFAULT_RECORD_MAX_BYTES",
    "WalError",
    "WalCorruptionError",
    "WalQueueFull",
    "WalClosed",
    "WalRecord",
    "WalManifest",
    "WalScanResult",
    "WalWriter",
    "WalReader",
]

#: Fixed binary header.  The format string is part of the frozen contract.
HEADER = struct.Struct("<4sB3xIIqqQ")
HEADER_SIZE = HEADER.size  # 40

MAGIC = b"TFW1"
KIND_WS = 1
KIND_SNAPSHOT = 2
VALID_KINDS = frozenset((KIND_WS, KIND_SNAPSHOT))

#: Default upper bound for a single payload.  The reader uses this to reject
#: absurd lengths before allocating memory for a corrupt header.
DEFAULT_RECORD_MAX_BYTES = 8 * 1024 * 1024

_FSYNC_POLICIES = ("interval", "always", "never")
_SEGMENT_RE = re.compile(r"^raw-(\d+)\.wal$")
_UINT64_MAX = (1 << 64) - 1
_INT64_MIN = -(1 << 63)
_INT64_MAX = (1 << 63) - 1

_log = logging.getLogger(__name__)


class WalError(Exception):
    """Base class for WAL configuration, usage and I/O failures."""


class WalCorruptionError(WalError):
    """Raised when a WAL segment fails structural validation."""


class WalQueueFull(Exception):
    """The bounded append queue is full; the caller should retry later."""


class WalClosed(Exception):
    """The WAL writer has been closed or its background thread failed."""


@dataclass(slots=True, frozen=True)
class WalRecord:
    """One validated WAL record as yielded by :class:`WalReader`."""

    kind: int
    seq: int
    recv_wall_ns: int
    recv_mono_ns: int
    payload: bytes


@dataclass(slots=True)
class WalManifest:
    """Result of :meth:`WalWriter.close`.

    ``segments`` is ordered by segment number and contains one dictionary per
    segment with ``name``, ``records``, ``bytes`` and ``sha256`` keys.  The
    digests cover the exact bytes on disk and can be used to verify a copy of
    the WAL before replay.
    """

    directory: str
    segments: list[dict]
    records: int
    bytes: int
    first_seq: int | None
    last_seq: int | None
    fsync_policy: str
    durable_seq: int | None

    def to_dict(self) -> dict:
        """Return a JSON-serialisable representation (stable key order)."""

        return {
            "directory": self.directory,
            "segments": [dict(seg) for seg in self.segments],
            "records": self.records,
            "bytes": self.bytes,
            "first_seq": self.first_seq,
            "last_seq": self.last_seq,
            "fsync_policy": self.fsync_policy,
            "durable_seq": self.durable_seq,
        }


@dataclass(slots=True)
class WalScanResult:
    """Summary produced by :meth:`WalReader.scan`."""

    records: int
    records_by_kind: dict[int, int]
    bytes: int
    segments: list[str]
    first_seq: int | None
    last_seq: int | None
    truncated_tail_bytes: int
    errors: list[str]


@dataclass(slots=True, frozen=True)
class _QueuedRecord:
    kind: int
    seq: int
    recv_wall_ns: int
    recv_mono_ns: int
    payload: bytes


class _SyncRequest:
    """Queue sentinel asking the writer to flush and fsync everything before it."""

    __slots__ = ("event",)

    def __init__(self) -> None:
        self.event = threading.Event()


class _CloseRequest:
    """Queue sentinel asking the writer to finalise and exit."""

    __slots__ = ("event",)

    def __init__(self) -> None:
        self.event = threading.Event()


@dataclass(slots=True)
class _ScanState:
    """Mutable accumulator shared between ``scan`` and ``__iter__``."""

    records: int = 0
    records_by_kind: dict[int, int] | None = None
    bytes: int = 0
    segments: list[str] | None = None
    first_seq: int | None = None
    last_seq: int | None = None
    truncated_tail_bytes: int = 0
    errors: list[str] | None = None

    def __post_init__(self) -> None:
        if self.records_by_kind is None:
            self.records_by_kind = {}
        if self.segments is None:
            self.segments = []
        if self.errors is None:
            self.errors = []

    def add(self, record: WalRecord) -> None:
        self.records += 1
        assert self.records_by_kind is not None
        self.records_by_kind[record.kind] = self.records_by_kind.get(record.kind, 0) + 1
        self.bytes += HEADER_SIZE + len(record.payload)
        if self.first_seq is None:
            self.first_seq = record.seq
        self.last_seq = record.seq


def _sha256_file(path: Path) -> str:
    """Return the hex SHA-256 digest of *path*, reading in bounded chunks."""

    digest = hashlib.sha256()
    with path.open("rb") as fh:
        for chunk in iter(lambda: fh.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _segment_sort_key(path: Path) -> tuple[int, str]:
    match = _SEGMENT_RE.match(path.name)
    index = int(match.group(1)) if match else (1 << 62)
    return index, path.name


class WalWriter:
    """Single-threaded WAL writer with a bounded, non-blocking ingest queue.

    Parameters mirror :class:`tickforge.config.WalConfig`.  ``start()`` is
    idempotent; appends may be enqueued before it is called and will be written
    once the writer starts (``close()`` guarantees a start).  If a payload is
    rejected (empty, too large, wrong type) or a sequence number does not
    increase, :class:`WalError` is raised synchronously.
    """

    def __init__(
        self,
        directory: str | Path,
        *,
        segment_bytes: int = 64 * 1024 * 1024,
        fsync_interval_ms: int = 250,
        fsync_policy: str = "interval",
        queue_max: int = 500_000,
        record_max_bytes: int = DEFAULT_RECORD_MAX_BYTES,
        logger: logging.Logger | None = None,
        on_error: Callable[[BaseException], None] | None = None,
    ) -> None:
        if fsync_policy not in _FSYNC_POLICIES:
            raise ValueError(f"fsync_policy must be one of {_FSYNC_POLICIES}")
        if segment_bytes <= 0:
            raise ValueError("segment_bytes must be positive")
        if fsync_interval_ms < 0:
            raise ValueError("fsync_interval_ms must be >= 0")
        if queue_max < 1:
            raise ValueError("queue_max must be >= 1")
        if record_max_bytes <= 0 or record_max_bytes > 0xFFFFFFFF:
            raise ValueError("record_max_bytes must be in (0, 2**32)")

        self._dir = Path(directory)
        self._segment_bytes = int(segment_bytes)
        self._fsync_interval_ms = int(fsync_interval_ms)
        self._fsync_policy = fsync_policy
        self._queue_max = int(queue_max)
        self._record_max_bytes = int(record_max_bytes)
        self._log = logger or _log
        self._on_error = on_error

        self._queue: queue.Queue[object] = queue.Queue(maxsize=self._queue_max)

        self._lock = threading.Lock()
        self._start_lock = threading.Lock()
        self._close_lock = threading.Lock()

        self._thread: threading.Thread | None = None
        self._accepting = True
        self._error: BaseException | None = None
        self._close_error: BaseException | None = None
        self._manifest: WalManifest | None = None

        # Counters below are updated by the writer thread and read by stats().
        self._records_ws = 0
        self._records_snapshot = 0
        self._bytes = 0
        self._fsyncs = 0
        self._fsync_total_ns = 0
        self._last_enqueued_seq: int | None = None
        self._written_seq: int | None = None
        self._durable_seq: int | None = None
        self._first_seq: int | None = None
        self._last_seq: int | None = None

        # Per-segment accounting for the manifest.
        self._segment_names: list[str] = []
        self._segment_records: dict[str, int] = {}
        self._next_segment_index = 1

        # Writer-thread-only state.
        self._file = None  # type: ignore[assignment]
        self._current_name: str | None = None
        self._current_bytes = 0
        self._needs_sync = False
        self._pending_fsync: list[Path] = []

    # ------------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------------
    def start(self) -> None:
        """Create the output directory and launch the background writer.

        Idempotent.  Raises :class:`WalClosed` if the writer has already been
        closed.
        """

        with self._lock:
            if not self._accepting:
                raise WalClosed("WAL writer is closed")
        self._start_thread()

    def _start_thread(self) -> None:
        with self._start_lock:
            if self._thread is not None:
                return
            with self._lock:
                if self._manifest is not None:
                    raise WalClosed("WAL writer is closed")
            self._dir.mkdir(parents=True, exist_ok=True)
            self._next_segment_index = self._find_next_segment_index()
            thread = threading.Thread(
                target=self._run,
                name="tickforge-wal",
                daemon=True,
            )
            self._thread = thread
            try:
                thread.start()
            except BaseException:
                # Leave the object in a retryable state if the OS refuses to
                # create the thread.
                self._thread = None
                raise

    def _find_next_segment_index(self) -> int:
        highest = 0
        try:
            entries = list(self._dir.iterdir())
        except FileNotFoundError:
            return 1
        for entry in entries:
            match = _SEGMENT_RE.match(entry.name)
            if match:
                highest = max(highest, int(match.group(1)))
        return highest + 1

    # ------------------------------------------------------------------
    # Append API (event-loop thread, never blocks on disk)
    # ------------------------------------------------------------------
    @property
    def queue_depth(self) -> int:
        """Number of records currently waiting in the bounded queue."""

        return self._queue.qsize()

    def append_ws(
        self,
        payload: bytes,
        *,
        recv_wall_ns: int,
        recv_mono_ns: int,
        seq: int,
    ) -> None:
        """Enqueue a raw WebSocket frame (kind 1) for durable storage."""

        self._append(KIND_WS, payload, recv_wall_ns, recv_mono_ns, seq)

    def append_snapshot(
        self,
        payload: bytes,
        *,
        recv_wall_ns: int,
        recv_mono_ns: int,
        seq: int,
    ) -> None:
        """Enqueue a REST snapshot payload (kind 2) for durable storage."""

        self._append(KIND_SNAPSHOT, payload, recv_wall_ns, recv_mono_ns, seq)

    def _append(
        self,
        kind: int,
        payload: bytes | bytearray | memoryview,
        recv_wall_ns: int,
        recv_mono_ns: int,
        seq: int,
    ) -> None:
        if kind not in VALID_KINDS:  # defensive; callers use append_ws/snapshot
            raise WalError(f"invalid WAL record kind: {kind}")
        if not isinstance(payload, (bytes, bytearray, memoryview)):
            raise WalError(f"payload must be bytes-like, got {type(payload).__name__}")
        data = bytes(payload)
        if not data:
            raise WalError("empty WAL payload")
        if len(data) > self._record_max_bytes:
            raise WalError(
                f"payload of {len(data)} bytes exceeds record_max_bytes="
                f"{self._record_max_bytes}"
            )
        wall = int(recv_wall_ns)
        mono = int(recv_mono_ns)
        sequence = int(seq)
        if not 0 <= sequence <= _UINT64_MAX:
            raise WalError(f"seq out of range for uint64: {sequence}")
        if not _INT64_MIN <= wall <= _INT64_MAX:
            raise WalError(f"recv_wall_ns out of range for int64: {wall}")
        if not _INT64_MIN <= mono <= _INT64_MAX:
            raise WalError(f"recv_mono_ns out of range for int64: {mono}")

        record = _QueuedRecord(kind, sequence, wall, mono, data)
        with self._lock:
            if not self._accepting:
                raise WalClosed(self._closed_reason())
            if self._error is not None:
                raise WalClosed(self._closed_reason())
            if self._last_enqueued_seq is not None and sequence <= self._last_enqueued_seq:
                raise WalError(
                    f"WAL seq must increase strictly: {sequence} after "
                    f"{self._last_enqueued_seq}"
                )
            try:
                self._queue.put_nowait(record)
            except queue.Full as exc:
                raise WalQueueFull(
                    f"WAL queue is full (queue_max={self._queue_max}); retry after drain"
                ) from exc
            self._last_enqueued_seq = sequence

    def _closed_reason(self) -> str:
        if self._error is not None:
            return f"WAL writer failed: {type(self._error).__name__}: {self._error}"
        return "WAL writer is closed"

    # ------------------------------------------------------------------
    # Synchronisation and statistics
    # ------------------------------------------------------------------
    def flush_and_sync(self) -> None:
        """Block until every record enqueued so far has been flushed and fsynced.

        Safe to call from any thread.  Raises :class:`WalClosed` if the writer
        has been closed or failed.
        """

        with self._lock:
            if self._error is not None:
                raise WalClosed(self._closed_reason())
            if not self._accepting:
                raise WalClosed(self._closed_reason())
        self._start_thread()
        request = _SyncRequest()

        # Enqueue the sentinel (blocking only when the queue is full, which is
        # precisely when we have to wait for durability anyway).
        while True:
            with self._lock:
                if self._error is not None:
                    raise WalClosed(self._closed_reason())
                if not self._accepting:
                    raise WalClosed(self._closed_reason())
                try:
                    self._queue.put_nowait(request)
                    break
                except queue.Full:
                    pass
            request.event.wait(0.02)

        while not request.event.wait(0.05):
            with self._lock:
                if self._error is not None:
                    raise WalClosed(self._closed_reason())
                thread = self._thread
                if thread is None or not thread.is_alive():
                    raise WalClosed(self._closed_reason())

        with self._lock:
            if self._error is not None:
                raise WalClosed(self._closed_reason())

    def stats(self) -> dict:
        """Return a consistent snapshot of writer counters and state."""

        with self._lock:
            error = self._error
            return {
                "records_ws": self._records_ws,
                "records_snapshot": self._records_snapshot,
                "records": self._records_ws + self._records_snapshot,
                "bytes": self._bytes,
                "segments": len(self._segment_names),
                "queue_depth": self._queue.qsize(),
                "fsyncs": self._fsyncs,
                "fsync_total_ns": self._fsync_total_ns,
                "written_seq": self._written_seq,
                "durable_seq": self._durable_seq,
                "enqueued_seq": self._last_enqueued_seq,
                "first_seq": self._first_seq,
                "last_seq": self._last_seq,
                "error": None
                if error is None
                else f"{type(error).__name__}: {error}",
                "closed": not self._accepting,
                "started": self._thread is not None,
            }

    # ------------------------------------------------------------------
    # Close
    # ------------------------------------------------------------------
    def close(self, timeout: float = 30.0) -> WalManifest:
        """Drain the queue, fsync, hash every segment and return the manifest.

        Idempotent: repeated calls return the same :class:`WalManifest`.
        """

        with self._close_lock:
            if self._manifest is not None:
                self._raise_if_error()
                return self._manifest

            # Make sure queued records (if any) are drained even when the
            # caller forgot to start the writer.
            self._start_thread()
            with self._lock:
                self._accepting = False

            thread = self._thread
            deadline = time.monotonic() + max(0.0, float(timeout))
            if thread is not None and thread.is_alive():
                request = _CloseRequest()
                enqueued = False
                while thread.is_alive():
                    with self._lock:
                        if self._error is not None:
                            break
                    try:
                        self._queue.put(request, timeout=0.05)
                        enqueued = True
                        break
                    except queue.Full:
                        if time.monotonic() >= deadline:
                            raise WalError(
                                "close timed out waiting for WAL queue capacity"
                            )
                if enqueued:
                    # Poll so a writer-thread failure (or a writer that exited
                    # on an earlier close sentinel) wakes us immediately
                    # instead of waiting for the whole close timeout.
                    while not request.event.wait(0.05):
                        with self._lock:
                            if self._error is not None:
                                break
                        if not thread.is_alive():
                            break
                        if time.monotonic() >= deadline:
                            break
                if thread.is_alive():
                    thread.join(max(0.0, deadline - time.monotonic()))
                if thread.is_alive():
                    raise WalError(f"WAL writer did not finish within {timeout}s")

            self._raise_if_error()
            manifest = self._compute_manifest()
            self._manifest = manifest
            return manifest

    def _raise_if_error(self) -> None:
        """Surface a failure of the final close/fsync to the close() caller.

        A writer error before close is exposed through stats() and the
        engine's clean_shutdown check; close() still returns what reached
        disk.  A failure *during* the final close must not be reported as a
        successful clean close.
        """

        with self._lock:
            error = self._close_error
        if error is not None:
            raise WalError(
                f"WAL close failed: {type(error).__name__}: {error}"
            ) from error

    def _compute_manifest(self) -> WalManifest:
        with self._lock:
            names = list(self._segment_names)
            segment_records = dict(self._segment_records)
            first_seq = self._first_seq
            last_seq = self._last_seq
            durable_seq = self._durable_seq
            policy = self._fsync_policy

        segments: list[dict] = []
        total_records = 0
        total_bytes = 0
        for name in names:
            path = self._dir / name
            try:
                size = path.stat().st_size
                digest = _sha256_file(path)
            except FileNotFoundError:
                continue
            records = segment_records.get(name, 0)
            segments.append(
                {
                    "name": name,
                    "records": records,
                    "bytes": size,
                    "sha256": digest,
                }
            )
            total_records += records
            total_bytes += size
        return WalManifest(
            directory=str(self._dir),
            segments=segments,
            records=total_records,
            bytes=total_bytes,
            first_seq=first_seq,
            last_seq=last_seq,
            fsync_policy=policy,
            durable_seq=durable_seq,
        )

    # ------------------------------------------------------------------
    # Writer thread
    # ------------------------------------------------------------------
    def _run(self) -> None:
        self._log.debug("WAL writer started in %s", self._dir)
        interval_s = (
            max(self._fsync_interval_ms, 1) / 1000.0
            if self._fsync_policy == "interval"
            else None
        )
        deadline = (
            time.monotonic() + interval_s if interval_s is not None else None
        )
        try:
            while True:
                timeout: float | None = None
                if deadline is not None:
                    timeout = max(0.0, deadline - time.monotonic())
                try:
                    item = self._queue.get(timeout=timeout)
                except queue.Empty:
                    self._flush_sync()
                    deadline = time.monotonic() + interval_s  # type: ignore[operator]
                    continue

                if isinstance(item, _QueuedRecord):
                    self._write_record(item)
                    if self._fsync_policy == "always":
                        self._flush_sync()
                    if deadline is not None and time.monotonic() >= deadline:
                        self._flush_sync()
                        deadline = time.monotonic() + interval_s  # type: ignore[operator]
                elif isinstance(item, _SyncRequest):
                    self._flush_sync()
                    item.event.set()
                elif isinstance(item, _CloseRequest):
                    try:
                        self._finalize_close()
                    except BaseException as exc:
                        with self._lock:
                            self._close_error = exc
                        raise
                    item.event.set()
                    self._log.debug("WAL writer stopped")
                    return
                else:  # pragma: no cover - defensive
                    raise WalError(f"unexpected WAL queue item: {item!r}")
        except BaseException as exc:  # surfaced through stats() and append()
            self._fail(exc)

    def _open_segment(self) -> None:
        name = f"raw-{self._next_segment_index:06d}.wal"
        self._next_segment_index += 1
        path = self._dir / name
        fh = path.open("wb")
        self._file = fh
        self._current_name = name
        self._current_bytes = 0
        with self._lock:
            self._segment_names.append(name)
            self._segment_records[name] = 0

    def _write_record(self, record: _QueuedRecord) -> None:
        payload = record.payload
        crc = zlib.crc32(payload) & 0xFFFFFFFF
        header = HEADER.pack(
            MAGIC,
            record.kind,
            len(payload),
            crc,
            record.recv_wall_ns,
            record.recv_mono_ns,
            record.seq,
        )
        record_len = HEADER_SIZE + len(payload)

        if self._file is None:
            self._open_segment()
        elif (
            self._current_bytes > 0
            and self._current_bytes + record_len > self._segment_bytes
        ):
            self._rotate()
            self._open_segment()

        written = self._file.write(header)
        if written != HEADER_SIZE:
            raise WalError(f"short header write: {written}/{HEADER_SIZE} bytes")
        written = self._file.write(payload)
        if written != len(payload):
            raise WalError(
                f"short payload write: {written}/{len(payload)} bytes"
            )

        with self._lock:
            if record.kind == KIND_WS:
                self._records_ws += 1
            else:
                self._records_snapshot += 1
            self._bytes += record_len
            if self._first_seq is None:
                self._first_seq = record.seq
            self._last_seq = record.seq
            self._written_seq = record.seq
            self._segment_records[self._current_name] = (  # type: ignore[index]
                self._segment_records.get(self._current_name, 0) + 1  # type: ignore[arg-type]
            )
        self._current_bytes += record_len
        self._needs_sync = True

    def _rotate(self) -> None:
        """Flush and close the current segment; called before a record won't fit."""

        fh = self._file
        if fh is None:
            return
        fh.flush()
        if self._fsync_policy in ("interval", "always"):
            if self._needs_sync:
                self._fsync_open_file(fh)
                self._needs_sync = False
                with self._lock:
                    self._durable_seq = self._written_seq
        elif self._current_name is not None:
            # Policy "never": no fsync now, but fsync these bytes at the next
            # explicit flush_and_sync()/close() so zero-loss mode stays honest.
            self._pending_fsync.append(self._dir / self._current_name)
        fh.close()
        self._file = None
        self._current_name = None
        self._current_bytes = 0

    def _fsync_open_file(self, fh) -> None:
        started = time.perf_counter_ns()
        os.fsync(fh.fileno())
        elapsed = time.perf_counter_ns() - started
        with self._lock:
            self._fsyncs += 1
            self._fsync_total_ns += elapsed

    def _flush_sync(self) -> None:
        """Flush and fsync all data written so far (writer thread only)."""

        if not self._needs_sync and not self._pending_fsync:
            return
        if self._file is not None:
            self._file.flush()
        if self._needs_sync and self._file is not None:
            self._fsync_open_file(self._file)
        for path in self._pending_fsync:
            with path.open("rb+") as fh:
                self._fsync_open_file(fh)
        self._pending_fsync.clear()
        self._needs_sync = False
        with self._lock:
            self._durable_seq = self._written_seq

    def _finalize_close(self) -> None:
        self._flush_sync()
        if self._file is not None:
            try:
                self._file.flush()
            finally:
                self._file.close()
                self._file = None

    def _fail(self, exc: BaseException) -> None:
        """Record a fatal writer-thread error and release resources."""

        try:
            self._log.error("WAL writer thread failed: %s", exc, exc_info=exc)
        except Exception:  # pragma: no cover - logging must never mask the error
            pass
        with self._lock:
            if self._error is None:
                self._error = exc
            self._accepting = False
        # Best-effort: make whatever was written visible, but never mask the
        # original failure with a secondary exception.
        if self._file is not None:
            try:
                self._file.flush()
            except Exception:
                pass
            try:
                self._file.close()
            except Exception:
                pass
            self._file = None
        if self._on_error is not None:
            try:
                self._on_error(exc)
            except Exception:
                self._log.exception("WAL on_error callback failed")


class WalReader:
    """Validate and iterate records from a WAL directory.

    Records are yielded in ascending segment order (numeric suffix) and, inside
    a segment, in write order.  Magic, payload length, CRC and strict sequence
    monotonicity are validated.  A record cut off by the end of the *last
    non-empty* segment is only tolerated when ``allow_truncated_tail=True``;
    it is then reported through :attr:`WalScanResult.truncated_tail_bytes`
    (bytes from the start of the incomplete record to EOF) and
    :attr:`WalScanResult.errors`.

    Payload lengths are trusted only up to :data:`DEFAULT_RECORD_MAX_BYTES`
    (the writer's default), so a corrupt length field cannot trigger a huge
    allocation.
    """

    def __init__(
        self,
        directory: str | Path,
        *,
        verify_crc: bool = True,
        allow_truncated_tail: bool = False,
    ) -> None:
        self._dir = Path(directory)
        self._verify_crc = bool(verify_crc)
        self._allow_truncated_tail = bool(allow_truncated_tail)
        self._record_max_bytes = DEFAULT_RECORD_MAX_BYTES

    def segment_paths(self) -> list[Path]:
        """Return segment files sorted by numeric suffix."""

        try:
            entries = [
                p
                for p in self._dir.iterdir()
                if p.is_file() and _SEGMENT_RE.match(p.name)
            ]
        except FileNotFoundError:
            return []
        return sorted(entries, key=_segment_sort_key)

    def __iter__(self) -> Iterator[WalRecord]:
        state = _ScanState()
        yield from self._iter_records(state)

    def scan(self) -> WalScanResult:
        """Validate the whole WAL and return aggregate statistics.

        Raises :class:`WalCorruptionError` on any structural, checksum or
        sequence error that is not a tolerated final-segment truncation.
        """

        state = _ScanState()
        for _ in self._iter_records(state):
            pass
        assert state.records_by_kind is not None
        assert state.segments is not None
        assert state.errors is not None
        return WalScanResult(
            records=state.records,
            records_by_kind=dict(state.records_by_kind),
            bytes=state.bytes,
            segments=list(state.segments),
            first_seq=state.first_seq,
            last_seq=state.last_seq,
            truncated_tail_bytes=state.truncated_tail_bytes,
            errors=list(state.errors),
        )

    def _iter_records(self, state: _ScanState) -> Iterator[WalRecord]:
        paths = self.segment_paths()
        assert state.segments is not None
        state.segments.extend(p.name for p in paths)

        # A truncation may only happen at the very end of the WAL.  Empty
        # trailing segment files (unusual, but possible after a copy) do not
        # move the tail: the last non-empty file owns it.
        sizes: list[int] = []
        for path in paths:
            try:
                sizes.append(path.stat().st_size)
            except FileNotFoundError:
                sizes.append(0)
        last_data_index = -1
        for index, size in enumerate(sizes):
            if size > 0:
                last_data_index = index

        last_seq: int | None = None
        for file_index, (path, size) in enumerate(zip(paths, sizes)):
            is_last_file = file_index == last_data_index
            with path.open("rb") as fh:
                offset = 0
                while offset < size:
                    header = fh.read(HEADER_SIZE)
                    if len(header) < HEADER_SIZE:
                        remaining = size - offset
                        if self._allow_truncated_tail and is_last_file:
                            state.truncated_tail_bytes = remaining
                            assert state.errors is not None
                            state.errors.append(
                                f"truncated tail in {path.name}: "
                                f"{remaining} byte(s) of a partial header"
                            )
                            return
                        raise WalCorruptionError(
                            f"truncated header in {path.name} at offset {offset}: "
                            f"got {len(header)} of {HEADER_SIZE} bytes"
                        )

                    magic, kind, length, crc, wall, mono, seq = HEADER.unpack(header)

                    if magic != MAGIC:
                        raise WalCorruptionError(
                            f"bad magic in {path.name} at offset {offset}: "
                            f"{magic!r}"
                        )
                    if kind not in VALID_KINDS:
                        raise WalCorruptionError(
                            f"unknown record kind {kind} in {path.name} "
                            f"at offset {offset}"
                        )
                    if length == 0 or length > self._record_max_bytes:
                        raise WalCorruptionError(
                            f"invalid payload length {length} in {path.name} "
                            f"at offset {offset}"
                        )
                    if length > size - offset - HEADER_SIZE:
                        remaining = size - offset
                        if self._allow_truncated_tail and is_last_file:
                            state.truncated_tail_bytes = remaining
                            assert state.errors is not None
                            state.errors.append(
                                f"truncated tail in {path.name}: "
                                f"{remaining} byte(s) of a partial record"
                            )
                            return
                        raise WalCorruptionError(
                            f"truncated payload in {path.name} at offset {offset}: "
                            f"need {length} byte(s), file ends "
                            f"{size - offset - HEADER_SIZE} byte(s) later"
                        )

                    payload = fh.read(length)
                    if len(payload) != length:  # pragma: no cover - race/truncation
                        remaining = size - offset
                        if self._allow_truncated_tail and is_last_file:
                            state.truncated_tail_bytes = remaining
                            assert state.errors is not None
                            state.errors.append(
                                f"truncated tail in {path.name}: {remaining} byte(s)"
                            )
                            return
                        raise WalCorruptionError(
                            f"short read in {path.name} at offset {offset}"
                        )

                    if self._verify_crc:
                        actual_crc = zlib.crc32(payload) & 0xFFFFFFFF
                        if actual_crc != crc:
                            raise WalCorruptionError(
                                f"CRC mismatch in {path.name} at offset {offset}: "
                                f"stored {crc:#010x}, computed {actual_crc:#010x}"
                            )
                    if last_seq is not None and seq <= last_seq:
                        raise WalCorruptionError(
                            f"non-monotonic seq in {path.name} at offset {offset}: "
                            f"{seq} after {last_seq}"
                        )

                    record = WalRecord(
                        kind=kind,
                        seq=seq,
                        recv_wall_ns=wall,
                        recv_mono_ns=mono,
                        payload=payload,
                    )
                    state.add(record)
                    last_seq = seq
                    offset += HEADER_SIZE + length
                    yield record
