"""Tests for the binary WAL writer, reader and manifest.

All tests are local and deterministic: they use temporary directories and
synthetic payloads, never the network or the wall clock (receive timestamps
are supplied explicitly).
"""

from __future__ import annotations

import hashlib
import json
import os
import random
import struct
import threading
import time
import zlib
from pathlib import Path

import pytest

import tickforge.store.wal as wal_module
from tickforge.store.wal import (
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


# ----------------------------------------------------------------------
# Helpers
# ----------------------------------------------------------------------
def make_writer(directory: Path, **overrides) -> WalWriter:
    """Create a writer with test-friendly defaults (long fsync interval)."""

    params = dict(
        segment_bytes=1 << 20,
        fsync_interval_ms=60_000,
        fsync_policy="never",
        queue_max=100_000,
    )
    params.update(overrides)
    return WalWriter(directory, **params)


def append_record(
    writer: WalWriter,
    payload: bytes,
    seq: int,
    *,
    kind: int = KIND_WS,
    wall: int | None = None,
    mono: int | None = None,
) -> tuple[int, int, int, int, bytes]:
    wall_ns = 1_700_000_000_000_000_000 + seq if wall is None else wall
    mono_ns = 9_000_000_000 + seq if mono is None else mono
    if kind == KIND_WS:
        writer.append_ws(
            payload, recv_wall_ns=wall_ns, recv_mono_ns=mono_ns, seq=seq
        )
    else:
        writer.append_snapshot(
            payload, recv_wall_ns=wall_ns, recv_mono_ns=mono_ns, seq=seq
        )
    return kind, seq, wall_ns, mono_ns, payload


def record_bytes(kind: int, seq: int, wall: int, mono: int, payload: bytes) -> bytes:
    """Build one raw WAL record exactly as the writer would."""

    return HEADER.pack(
        MAGIC, kind, len(payload), zlib.crc32(payload) & 0xFFFFFFFF, wall, mono, seq
    ) + payload


def wait_until(predicate, timeout: float = 5.0, interval: float = 0.005) -> None:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if predicate():
            return
        time.sleep(interval)
    raise AssertionError("condition was not met within timeout")


# ----------------------------------------------------------------------
# Format and round-trip
# ----------------------------------------------------------------------
def test_header_layout_is_frozen() -> None:
    assert HEADER.size == 40
    packed = HEADER.pack(MAGIC, KIND_WS, 123, 0xDEADBEEF, -5, 6, 2**63 + 7)
    assert len(packed) == 40
    magic, kind, length, crc, wall, mono, seq = HEADER.unpack(packed)
    assert magic == b"TFW1"
    assert (kind, length, crc, wall, mono, seq) == (
        KIND_WS,
        123,
        0xDEADBEEF,
        -5,
        6,
        2**63 + 7,
    )
    # Payload starts at byte 40; padding bytes 5..8 are zero.
    assert packed[5:8] == b"\x00\x00\x00"


def test_roundtrip_10k_records(tmp_path: Path) -> None:
    wal_dir = tmp_path / "wal"
    writer = make_writer(
        wal_dir,
        segment_bytes=256 * 1024,
        queue_max=50_000,
        fsync_policy="interval",
        fsync_interval_ms=60_000,
    )
    writer.start()

    expected: list[tuple[int, int, int, int, bytes]] = []
    for i in range(10_000):
        seq = 1_000 + i
        kind = KIND_SNAPSHOT if i % 97 == 0 else KIND_WS
        payload = (
            b'{"stream":"btcusdt@depth","n":%d,"pad":"' % i
            + bytes([i % 256]) * (i % 23)
            + b'"}'
        )
        expected.append(append_record(writer, payload, seq, kind=kind))

    writer.flush_and_sync()
    stats_before_close = writer.stats()
    manifest = writer.close()

    assert stats_before_close["records"] == 10_000
    assert stats_before_close["written_seq"] == 10_999
    assert stats_before_close["durable_seq"] == 10_999
    assert stats_before_close["error"] is None

    assert isinstance(manifest, WalManifest)
    assert manifest.records == 10_000
    assert manifest.first_seq == 1_000
    assert manifest.last_seq == 10_999
    assert manifest.fsync_policy == "interval"
    assert manifest.durable_seq == 10_999
    assert manifest.directory == str(wal_dir)
    assert len(manifest.segments) > 1  # segment_bytes forced rotation

    reader = WalReader(wal_dir)
    scan = reader.scan()
    assert isinstance(scan, WalScanResult)
    assert scan.records == 10_000
    ws_count = sum(1 for i in range(10_000) if i % 97 != 0)
    assert scan.records_by_kind == {KIND_WS: ws_count, KIND_SNAPSHOT: 10_000 - ws_count}
    assert scan.first_seq == 1_000
    assert scan.last_seq == 10_999
    assert scan.truncated_tail_bytes == 0
    assert scan.errors == []
    assert scan.segments == [seg["name"] for seg in manifest.segments]
    assert scan.bytes == manifest.bytes

    got = [
        (rec.kind, rec.seq, rec.recv_wall_ns, rec.recv_mono_ns, rec.payload)
        for rec in reader
    ]
    assert got == expected


def test_reader_yields_exact_payload_bytes_and_metadata(tmp_path: Path) -> None:
    """Self-check: payload bytes (including NULs/headers) survive verbatim."""

    wal_dir = tmp_path / "wal"
    writer = make_writer(wal_dir)
    writer.start()

    # Payloads deliberately contain NULs, invalid UTF-8 and a complete WAL
    # header so that any accidental text/decoding treatment is caught.
    embedded = HEADER.pack(MAGIC, KIND_WS, 3, 0, 1, 2, 3)
    payloads = [
        b"\x00\x01\x02\xff\xfe" + embedded,
        b'{"raw":"\\u0000\\n"}',
        b"",
    ]
    payloads[2] = b"x" * 257  # empty payloads are rejected by design
    writer.append_ws(payloads[0], recv_wall_ns=11, recv_mono_ns=22, seq=7)
    writer.append_snapshot(payloads[1], recv_wall_ns=33, recv_mono_ns=44, seq=8)
    writer.append_ws(payloads[2], recv_wall_ns=55, recv_mono_ns=66, seq=9)
    manifest = writer.close()

    records = list(WalReader(wal_dir))
    assert records == [
        WalRecord(kind=KIND_WS, seq=7, recv_wall_ns=11, recv_mono_ns=22, payload=payloads[0]),
        WalRecord(
            kind=KIND_SNAPSHOT,
            seq=8,
            recv_wall_ns=33,
            recv_mono_ns=44,
            payload=payloads[1],
        ),
        WalRecord(kind=KIND_WS, seq=9, recv_wall_ns=55, recv_mono_ns=66, payload=payloads[2]),
    ]
    for rec, payload in zip(records, payloads):
        assert isinstance(rec.payload, bytes)
        assert rec.payload == payload
        assert len(rec.payload) == len(payload)
    assert manifest.records == 3
    assert manifest.bytes == sum(HEADER_SIZE + len(p) for p in payloads)


# ----------------------------------------------------------------------
# Rotation and manifests
# ----------------------------------------------------------------------
def test_rotation_creates_ordered_segments(tmp_path: Path) -> None:
    wal_dir = tmp_path / "wal"
    payload = b"p" * 100
    # Record size is 141 bytes; two records do not fit into 256 bytes.
    writer = make_writer(wal_dir, segment_bytes=256)
    writer.start()
    expected = [append_record(writer, payload + bytes([i]), seq=i + 1) for i in range(7)]
    manifest = writer.close()

    names = [seg["name"] for seg in manifest.segments]
    assert names == [f"raw-{i:06d}.wal" for i in range(1, 8)]
    assert manifest.records == 7
    assert all(seg["records"] == 1 for seg in manifest.segments)
    assert all(seg["bytes"] == HEADER_SIZE + 101 for seg in manifest.segments)
    assert sum(seg["bytes"] for seg in manifest.segments) == manifest.bytes
    assert [p.name for p in WalReader(wal_dir).segment_paths()] == names

    got = [
        (r.kind, r.seq, r.recv_wall_ns, r.recv_mono_ns, r.payload)
        for r in WalReader(wal_dir)
    ]
    assert got == expected


def test_manifest_digests_match_disk(tmp_path: Path) -> None:
    wal_dir = tmp_path / "wal"
    writer = make_writer(wal_dir, segment_bytes=512)
    writer.start()
    for i in range(12):
        append_record(writer, b"data-%03d" % i + b"\x00" * 30, seq=i + 1)
    manifest = writer.close()

    assert len(manifest.segments) > 1
    seen_records = 0
    seen_bytes = 0
    for seg in manifest.segments:
        path = wal_dir / seg["name"]
        raw = path.read_bytes()
        assert seg["sha256"] == hashlib.sha256(raw).hexdigest()
        assert seg["bytes"] == len(raw)
        assert seg["records"] <= raw.count(MAGIC)
        seen_records += seg["records"]
        seen_bytes += seg["bytes"]
    assert seen_records == manifest.records == 12
    assert seen_bytes == manifest.bytes

    # The manifest must be JSON serialisable (the runtime persists it).
    doc = manifest.to_dict()
    assert json.loads(json.dumps(doc))["records"] == 12
    assert doc["first_seq"] == 1 and doc["last_seq"] == 12


def test_writer_continues_numbering_and_never_overwrites(tmp_path: Path) -> None:
    wal_dir = tmp_path / "wal"
    first = make_writer(wal_dir, segment_bytes=128)
    first.start()
    append_record(first, b"a" * 50, seq=1)
    append_record(first, b"b" * 50, seq=2)  # 90 bytes each -> 2 segments
    m1 = first.close()
    assert [s["name"] for s in m1.segments] == ["raw-000001.wal", "raw-000002.wal"]

    second = make_writer(wal_dir, segment_bytes=128)
    second.start()
    append_record(second, b"c" * 50, seq=3)
    m2 = second.close()
    assert [s["name"] for s in m2.segments] == ["raw-000003.wal"]

    scan = WalReader(wal_dir).scan()
    assert scan.records == 3
    assert scan.first_seq == 1 and scan.last_seq == 3
    assert [r.seq for r in WalReader(wal_dir)] == [1, 2, 3]


# ----------------------------------------------------------------------
# Corruption
# ----------------------------------------------------------------------
def _payload_offset(payload_sizes: list[int], index: int) -> int:
    offsets = []
    pos = 0
    for size in payload_sizes:
        offsets.append(pos)
        pos += HEADER_SIZE + size
    return offsets[index] + HEADER_SIZE


def test_crc_corruption_detected(tmp_path: Path) -> None:
    wal_dir = tmp_path / "wal"
    payloads = [b"alpha" * 4, b"beta" * 5, b"gamma" * 6]
    writer = make_writer(wal_dir)
    writer.start()
    for i, payload in enumerate(payloads):
        append_record(writer, payload, seq=i + 1)
    writer.close()

    segment = wal_dir / "raw-000001.wal"
    raw = bytearray(segment.read_bytes())
    target = _payload_offset([len(p) for p in payloads], 1)
    raw[target] ^= 0xFF
    segment.write_bytes(bytes(raw))

    with pytest.raises(WalCorruptionError, match="CRC"):
        list(WalReader(wal_dir))
    with pytest.raises(WalCorruptionError, match="CRC"):
        WalReader(wal_dir).scan()

    # With verification disabled the reader still parses the structure.
    records = list(WalReader(wal_dir, verify_crc=False))
    assert len(records) == 3
    assert records[1].payload != payloads[1]
    assert WalReader(wal_dir, verify_crc=False).scan().records == 3


def test_bad_magic_detected(tmp_path: Path) -> None:
    wal_dir = tmp_path / "wal"
    writer = make_writer(wal_dir)
    writer.start()
    append_record(writer, b"payload", seq=1)
    writer.close()

    segment = wal_dir / "raw-000001.wal"
    raw = bytearray(segment.read_bytes())
    raw[0:4] = b"BAD!"
    segment.write_bytes(bytes(raw))

    with pytest.raises(WalCorruptionError, match="magic"):
        WalReader(wal_dir).scan()


def test_truncated_tail_tolerated_only_when_allowed(tmp_path: Path) -> None:
    wal_dir = tmp_path / "wal"
    writer = make_writer(wal_dir)
    writer.start()
    for i in range(4):
        append_record(writer, b"z" * 32, seq=i + 1)
    writer.close()

    segment = wal_dir / "raw-000001.wal"
    full = segment.read_bytes()
    assert len(full) == 4 * (HEADER_SIZE + 32)
    cut = 10  # inside the last payload
    segment.write_bytes(full[:-cut])

    with pytest.raises(WalCorruptionError):
        WalReader(wal_dir).scan()
    with pytest.raises(WalCorruptionError):
        list(WalReader(wal_dir))

    scan = WalReader(wal_dir, allow_truncated_tail=True).scan()
    assert scan.records == 3
    assert scan.first_seq == 1
    assert scan.last_seq == 3
    # The incomplete record starts with its 40-byte header, so the tail is the
    # header plus the surviving 22 payload bytes.
    assert scan.truncated_tail_bytes == HEADER_SIZE + 32 - cut
    assert scan.errors and "truncated" in scan.errors[0]
    assert [r.seq for r in WalReader(wal_dir, allow_truncated_tail=True)] == [1, 2, 3]


def test_truncated_header_tail(tmp_path: Path) -> None:
    wal_dir = tmp_path / "wal"
    writer = make_writer(wal_dir)
    writer.start()
    append_record(writer, b"a" * 10, seq=1)
    append_record(writer, b"b" * 10, seq=2)
    writer.close()

    segment = wal_dir / "raw-000001.wal"
    raw = segment.read_bytes()
    first_len = HEADER_SIZE + 10
    segment.write_bytes(raw[: first_len + 7])  # 7 bytes of the next header

    scan = WalReader(wal_dir, allow_truncated_tail=True).scan()
    assert scan.records == 1
    assert scan.truncated_tail_bytes == 7
    with pytest.raises(WalCorruptionError, match="truncated header"):
        WalReader(wal_dir).scan()


def test_empty_trailing_segment_does_not_hide_a_truncated_tail(tmp_path: Path) -> None:
    wal_dir = tmp_path / "wal"
    writer = make_writer(wal_dir)
    writer.start()
    append_record(writer, b"a" * 40, seq=1)
    append_record(writer, b"b" * 40, seq=2)
    writer.close()

    segment = wal_dir / "raw-000001.wal"
    raw = segment.read_bytes()
    segment.write_bytes(raw[:-6])
    (wal_dir / "raw-000002.wal").write_bytes(b"")  # empty file at the end

    scan = WalReader(wal_dir, allow_truncated_tail=True).scan()
    assert scan.records == 1
    assert scan.segments == ["raw-000001.wal", "raw-000002.wal"]
    assert scan.truncated_tail_bytes == HEADER_SIZE + 40 - 6


def test_truncated_non_final_segment_is_always_corruption(tmp_path: Path) -> None:
    wal_dir = tmp_path / "wal"
    writer = make_writer(wal_dir, segment_bytes=128)
    writer.start()
    append_record(writer, b"first" * 10, seq=1)
    append_record(writer, b"second" * 10, seq=2)
    append_record(writer, b"third" * 10, seq=3)
    writer.close()
    names = sorted(p.name for p in wal_dir.glob("raw-*.wal"))
    assert len(names) >= 2

    first = wal_dir / names[0]
    raw = first.read_bytes()
    first.write_bytes(raw[:-5])

    with pytest.raises(WalCorruptionError):
        WalReader(wal_dir, allow_truncated_tail=True).scan()


def test_reader_rejects_invalid_headers(tmp_path: Path) -> None:
    bad_kind = record_bytes(9, 1, 0, 0, b"x")
    zero_length = HEADER.pack(MAGIC, KIND_WS, 0, zlib.crc32(b""), 0, 0, 1)
    huge_length = HEADER.pack(MAGIC, KIND_WS, 1 << 30, 0, 0, 0, 1)
    bad_crc = HEADER.pack(MAGIC, KIND_WS, 1, 12345, 0, 0, 1) + b"x"

    cases = {
        "kind": (bad_kind, "kind"),
        "zero-length": (zero_length, "length"),
        "huge-length": (huge_length, "length"),
        "crc": (bad_crc, "CRC"),
    }
    for name, (blob, match) in cases.items():
        directory = tmp_path / name
        directory.mkdir()
        (directory / "raw-000001.wal").write_bytes(blob)
        with pytest.raises(WalCorruptionError, match=match):
            WalReader(directory).scan()


def test_reader_rejects_non_monotonic_seq(tmp_path: Path) -> None:
    directory = tmp_path / "wal"
    directory.mkdir()
    blob = record_bytes(KIND_WS, 5, 0, 0, b"a") + record_bytes(KIND_WS, 5, 0, 0, b"b")
    (directory / "raw-000001.wal").write_bytes(blob)
    with pytest.raises(WalCorruptionError, match="seq"):
        WalReader(directory).scan()

    # A regression across a segment boundary is also rejected.
    directory2 = tmp_path / "wal2"
    directory2.mkdir()
    (directory2 / "raw-000001.wal").write_bytes(
        record_bytes(KIND_WS, 10, 0, 0, b"a")
    )
    (directory2 / "raw-000002.wal").write_bytes(
        record_bytes(KIND_WS, 9, 0, 0, b"b")
    )
    with pytest.raises(WalCorruptionError, match="seq"):
        WalReader(directory2).scan()


def test_reader_missing_directory_is_empty(tmp_path: Path) -> None:
    scan = WalReader(tmp_path / "does-not-exist").scan()
    assert scan.records == 0
    assert scan.records_by_kind == {}
    assert scan.segments == []
    assert scan.first_seq is None and scan.last_seq is None
    assert list(WalReader(tmp_path / "does-not-exist")) == []


def test_reader_ignores_unrelated_files(tmp_path: Path) -> None:
    wal_dir = tmp_path / "wal"
    writer = make_writer(wal_dir)
    writer.start()
    append_record(writer, b"x", seq=1)
    writer.close()
    (wal_dir / "notes.txt").write_text("ignore me")
    (wal_dir / "raw-abc.wal").write_bytes(b"garbage")
    scan = WalReader(wal_dir).scan()
    assert scan.records == 1
    assert scan.segments == ["raw-000001.wal"]


# ----------------------------------------------------------------------
# Queue, close and error handling
# ----------------------------------------------------------------------
def test_queue_full_raises_and_retry_succeeds(tmp_path: Path, monkeypatch) -> None:
    """A blocked disk must produce WalQueueFull, never silent data loss."""

    entered = threading.Event()
    release = threading.Event()
    real_fsync = os.fsync

    def blocking_fsync(fd: int) -> None:
        entered.set()
        if not release.wait(5.0):
            raise TimeoutError("test did not release fsync")
        real_fsync(fd)

    monkeypatch.setattr(wal_module.os, "fsync", blocking_fsync)
    wal_dir = tmp_path / "wal"
    writer = make_writer(
        wal_dir, fsync_policy="always", queue_max=2, segment_bytes=1 << 20
    )
    writer.start()

    writer.append_ws(b"one", recv_wall_ns=1, recv_mono_ns=2, seq=1)
    assert entered.wait(5.0)
    wait_until(lambda: writer.queue_depth == 0)

    writer.append_ws(b"two", recv_wall_ns=3, recv_mono_ns=4, seq=2)
    writer.append_ws(b"three", recv_wall_ns=5, recv_mono_ns=6, seq=3)
    with pytest.raises(WalQueueFull):
        writer.append_ws(b"four", recv_wall_ns=7, recv_mono_ns=8, seq=4)

    release.set()
    writer.flush_and_sync()
    manifest = writer.close()
    assert manifest.records == 3
    assert [r.payload for r in WalReader(wal_dir)] == [b"one", b"two", b"three"]
    assert writer.stats()["error"] is None


def test_close_is_idempotent_and_append_after_close_raises(tmp_path: Path) -> None:
    wal_dir = tmp_path / "wal"
    writer = make_writer(wal_dir)
    writer.start()
    append_record(writer, b"hello", seq=1)
    first = writer.close()
    second = writer.close()
    assert second is first
    assert second == first
    assert writer.stats()["queue_depth"] == 0

    with pytest.raises(WalClosed):
        writer.append_ws(b"late", recv_wall_ns=1, recv_mono_ns=2, seq=2)
    with pytest.raises(WalClosed):
        writer.append_snapshot(b"late", recv_wall_ns=1, recv_mono_ns=2, seq=3)
    with pytest.raises(WalClosed):
        writer.flush_and_sync()
    with pytest.raises(WalClosed):
        writer.start()


def test_close_empty_writer(tmp_path: Path) -> None:
    wal_dir = tmp_path / "wal"
    writer = make_writer(wal_dir)
    manifest = writer.close()
    assert manifest.segments == []
    assert manifest.records == 0
    assert manifest.bytes == 0
    assert manifest.first_seq is None
    assert manifest.last_seq is None
    assert manifest.durable_seq is None
    scan = WalReader(wal_dir).scan()
    assert scan.records == 0
    assert scan.segments == []
    assert scan.first_seq is None and scan.last_seq is None


def test_durable_lag_callback_receives_sample(tmp_path: Path) -> None:
    samples: list[int] = []
    writer = make_writer(
        tmp_path / "wal",
        fsync_policy="always",
        on_durable_lag_ns=samples.append,
    )
    append_record(writer, b"payload", seq=1, mono=time.monotonic_ns())
    writer.flush_and_sync()
    assert samples and samples[-1] >= 0
    writer.close()


def test_reader_uses_configured_record_limit(tmp_path: Path) -> None:
    wal_dir = tmp_path / "wal"
    writer = make_writer(wal_dir, record_max_bytes=2048)
    append_record(writer, b"x" * 2000, seq=1)
    writer.close()

    with pytest.raises(WalCorruptionError):
        WalReader(wal_dir, record_max_bytes=1024).scan()
    scan = WalReader(wal_dir, record_max_bytes=2048).scan()
    assert scan.records == 1


def test_close_propagates_finalize_failure(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A failed final fsync must not be reported as a clean close."""

    wal_dir = tmp_path / "wal"
    writer = make_writer(wal_dir, fsync_policy="always")
    append_record(writer, b"payload", seq=1)

    def boom() -> None:
        raise OSError("injected ENOSPC")

    monkeypatch.setattr(writer, "_finalize_close", boom)
    with pytest.raises(WalError, match="injected ENOSPC"):
        writer.close()


def test_drain_before_start_via_close(tmp_path: Path) -> None:
    """Records enqueued before start() are never lost on close()."""

    wal_dir = tmp_path / "wal"
    writer = make_writer(wal_dir)
    append_record(writer, b"pre-start-1", seq=1)
    append_record(writer, b"pre-start-2", seq=2)
    assert writer.queue_depth == 2
    manifest = writer.close()
    assert manifest.records == 2
    assert [r.payload for r in WalReader(wal_dir)] == [b"pre-start-1", b"pre-start-2"]


def test_flush_and_sync_starts_lazy_writer(tmp_path: Path) -> None:
    wal_dir = tmp_path / "wal"
    writer = make_writer(wal_dir)
    append_record(writer, b"lazy", seq=1)
    writer.flush_and_sync()
    assert writer.stats()["durable_seq"] == 1
    assert writer.close().records == 1


def test_writer_thread_error_is_surfaced(tmp_path: Path, monkeypatch) -> None:
    callback_errors: list[BaseException] = []
    wal_dir = tmp_path / "wal"

    def explode(self, record) -> None:  # noqa: ANN001 - test double
        raise OSError("disk exploded")

    monkeypatch.setattr(WalWriter, "_write_record", explode)
    writer = make_writer(wal_dir, fsync_policy="never", on_error=callback_errors.append)
    writer.start()
    append_record(writer, b"boom", seq=1)

    wait_until(lambda: writer.stats()["error"] is not None)
    stats = writer.stats()
    assert "OSError" in stats["error"] and "disk exploded" in stats["error"]
    assert stats["closed"] is True
    assert len(callback_errors) == 1 and isinstance(callback_errors[0], OSError)

    with pytest.raises(WalClosed):
        append_record(writer, b"after", seq=2)
    with pytest.raises(WalClosed):
        writer.flush_and_sync()

    # close must not hang and must still describe what reached disk.
    manifest = writer.close(timeout=5.0)
    assert manifest.records == 0
    assert writer.close(timeout=5.0) is manifest


def test_writer_rejects_bad_inputs(tmp_path: Path) -> None:
    writer = make_writer(tmp_path / "wal", record_max_bytes=100)
    append_record(writer, b"first", seq=5)

    with pytest.raises(WalError, match="increase"):
        writer.append_ws(b"x", recv_wall_ns=0, recv_mono_ns=0, seq=5)
    with pytest.raises(WalError, match="increase"):
        writer.append_ws(b"x", recv_wall_ns=0, recv_mono_ns=0, seq=4)
    with pytest.raises(WalError, match="empty"):
        writer.append_ws(b"", recv_wall_ns=0, recv_mono_ns=0, seq=6)
    with pytest.raises(WalError, match="record_max_bytes"):
        writer.append_ws(b"x" * 101, recv_wall_ns=0, recv_mono_ns=0, seq=6)
    with pytest.raises(WalError, match="bytes-like"):
        writer.append_ws("nope", recv_wall_ns=0, recv_mono_ns=0, seq=6)  # type: ignore[arg-type]
    with pytest.raises(WalError, match="seq"):
        writer.append_ws(b"x", recv_wall_ns=0, recv_mono_ns=0, seq=-1)
    with pytest.raises(WalError, match="recv_wall_ns"):
        writer.append_ws(b"x", recv_wall_ns=1 << 63, recv_mono_ns=0, seq=6)

    manifest = writer.close()
    assert manifest.records == 1
    assert writer.stats()["error"] is None


# ----------------------------------------------------------------------
# fsync policies
# ----------------------------------------------------------------------
def test_fsync_policy_always_syncs_every_record(tmp_path: Path) -> None:
    wal_dir = tmp_path / "wal"
    writer = make_writer(wal_dir, fsync_policy="always", fsync_interval_ms=0)
    writer.start()
    for i in range(20):
        append_record(writer, b"r%02d" % i, seq=i + 1)
    writer.flush_and_sync()
    stats = writer.stats()
    assert stats["fsyncs"] == 20
    assert stats["written_seq"] == 20
    assert stats["durable_seq"] == 20
    manifest = writer.close()
    assert manifest.durable_seq == 20
    assert writer.stats()["fsyncs"] == 20  # close had nothing left to sync


def test_fsync_policy_never_only_syncs_on_request(tmp_path: Path) -> None:
    wal_dir = tmp_path / "wal"
    writer = make_writer(wal_dir, fsync_policy="never", fsync_interval_ms=1)
    writer.start()
    for i in range(10):
        append_record(writer, b"n%02d" % i, seq=i + 1)
    time.sleep(0.05)  # interval policy would have fsynced by now
    assert writer.stats()["fsyncs"] == 0
    writer.flush_and_sync()
    assert writer.stats()["fsyncs"] == 1
    assert writer.stats()["durable_seq"] == 10
    manifest = writer.close()
    assert manifest.records == 10
    assert manifest.durable_seq == 10
    assert writer.stats()["fsyncs"] == 1


def test_fsync_policy_interval_syncs_periodically(tmp_path: Path) -> None:
    wal_dir = tmp_path / "wal"
    writer = make_writer(wal_dir, fsync_policy="interval", fsync_interval_ms=20)
    writer.start()
    for i in range(10):
        append_record(writer, b"i%02d" % i, seq=i + 1)
    wait_until(lambda: writer.stats()["fsyncs"] >= 1, timeout=3.0)
    # The periodic fsync may fire while records are still queued; the explicit
    # barrier guarantees all ten are durable before checking the counters.
    writer.flush_and_sync()
    stats = writer.stats()
    assert stats["durable_seq"] == stats["written_seq"] == 10
    assert stats["fsync_total_ns"] > 0
    writer.close()
    assert writer.stats()["error"] is None


def test_flush_and_sync_covers_rotated_never_policy_segments(tmp_path: Path) -> None:
    """Under policy=never, zero-loss sync must fsync closed segments too."""

    wal_dir = tmp_path / "wal"
    writer = make_writer(wal_dir, fsync_policy="never", segment_bytes=128)
    writer.start()
    for i in range(4):
        append_record(writer, b"r" * 50, seq=i + 1)
    wait_until(lambda: writer.stats()["records"] == 4)
    assert writer.stats()["segments"] >= 3
    writer.flush_and_sync()
    stats = writer.stats()
    # One fsync for every segment that was rotated without one plus the open
    # one on its first explicit sync.
    assert stats["fsyncs"] >= stats["segments"]
    assert stats["durable_seq"] == 4
    writer.close()
    assert writer.stats()["error"] is None


# ----------------------------------------------------------------------
# Reader/scan consistency and extra properties
# ----------------------------------------------------------------------
def test_scan_matches_iter_and_manifest(tmp_path: Path) -> None:
    wal_dir = tmp_path / "wal"
    writer = make_writer(wal_dir, segment_bytes=700)
    writer.start()
    expected = []
    seq = 0
    for i in range(60):
        seq += 1 + (i % 3)
        expected.append(append_record(writer, b"payload-%02d" % i * (i % 4 + 1), seq=seq))
    manifest = writer.close()

    reader = WalReader(wal_dir)
    scan = reader.scan()
    records = list(reader)
    assert scan.records == len(records) == 60
    assert scan.bytes == sum(HEADER_SIZE + len(r.payload) for r in records)
    assert scan.bytes == manifest.bytes
    assert manifest.records == 60
    assert [r.seq for r in records] == [e[1] for e in expected]
    assert scan.first_seq == expected[0][1]
    assert scan.last_seq == expected[-1][1]


@pytest.mark.parametrize("segment_bytes", [64, 128, 257, 1024])
def test_property_roundtrip_across_segment_sizes(
    tmp_path: Path, segment_bytes: int
) -> None:
    """Seeded differential check: arbitrary payloads survive any rotation."""

    rng = random.Random(segment_bytes)
    wal_dir = tmp_path / "wal"
    writer = make_writer(wal_dir, segment_bytes=segment_bytes)
    writer.start()
    expected: list[tuple[int, int, int, int, bytes]] = []
    seq = 0
    for i in range(150):
        seq += 1 + rng.randrange(3)
        payload = rng.randbytes(rng.randrange(1, 300))
        kind = KIND_SNAPSHOT if i % 13 == 0 else KIND_WS
        expected.append(append_record(writer, payload, seq, kind=kind))
    manifest = writer.close()

    got = [
        (r.kind, r.seq, r.recv_wall_ns, r.recv_mono_ns, r.payload)
        for r in WalReader(wal_dir)
    ]
    assert got == expected

    scan = WalReader(wal_dir).scan()
    assert scan.records == 150
    assert scan.first_seq == expected[0][1]
    assert scan.last_seq == expected[-1][1]
    assert scan.bytes == sum(HEADER_SIZE + len(rec[4]) for rec in expected)
    assert manifest.records == 150
    assert sum(seg["records"] for seg in manifest.segments) == 150

    # Rotation invariant: a segment fits the target unless it holds a single
    # record that is itself larger than the target.
    for seg in manifest.segments:
        assert seg["bytes"] <= segment_bytes or seg["records"] == 1
        raw = (wal_dir / seg["name"]).read_bytes()
        assert seg["sha256"] == hashlib.sha256(raw).hexdigest()


def test_records_never_span_segments(tmp_path: Path) -> None:
    wal_dir = tmp_path / "wal"
    payload = b"x" * 200
    writer = make_writer(wal_dir, segment_bytes=250)  # smaller than one record
    writer.start()
    for i in range(3):
        append_record(writer, payload + bytes([i]), seq=i + 1)
    manifest = writer.close()

    # Each oversized record gets a dedicated segment rather than being split.
    assert [seg["records"] for seg in manifest.segments] == [1, 1, 1]
    for seg in manifest.segments:
        assert seg["bytes"] == HEADER_SIZE + len(payload) + 1
    assert [r.seq for r in WalReader(wal_dir)] == [1, 2, 3]
