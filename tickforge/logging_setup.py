"""Structured logging setup for file + console output."""

from __future__ import annotations

import json
import logging
import logging.handlers
import sys
import time
from pathlib import Path
from typing import Any

_RESERVED = {
    "name",
    "msg",
    "args",
    "levelname",
    "levelno",
    "pathname",
    "filename",
    "module",
    "exc_info",
    "exc_text",
    "stack_info",
    "lineno",
    "funcName",
    "created",
    "msecs",
    "relativeCreated",
    "thread",
    "threadName",
    "processName",
    "process",
    "taskName",
}


class JsonFormatter(logging.Formatter):
    def format(self, record: logging.LogRecord) -> str:
        entry: dict[str, Any] = {
            "ts": time.strftime("%Y-%m-%dT%H:%M:%S", time.gmtime(record.created))
            + f".{int(record.msecs):03d}Z",
            "level": record.levelname,
            "logger": record.name,
            "msg": record.getMessage(),
        }
        if record.exc_info:
            entry["exc"] = self.formatException(record.exc_info)
        for key, value in record.__dict__.items():
            if key not in _RESERVED and not key.startswith("_"):
                try:
                    json.dumps(value)
                    entry[key] = value
                except (TypeError, ValueError):
                    entry[key] = repr(value)
        return json.dumps(entry, ensure_ascii=False, separators=(",", ":"))


class ConsoleFormatter(logging.Formatter):
    def format(self, record: logging.LogRecord) -> str:
        ts = time.strftime("%H:%M:%S", time.localtime(record.created))
        base = f"{ts} {record.levelname:<7} {record.name}: {record.getMessage()}"
        extras = []
        for key, value in record.__dict__.items():
            if key not in _RESERVED and not key.startswith("_"):
                extras.append(f"{key}={value!r}")
        if extras:
            base += "  " + " ".join(extras)
        if record.exc_info:
            base += "\n" + self.formatException(record.exc_info)
        return base


def configure_logging(
    log_dir: str | Path,
    *,
    level: str = "INFO",
    reset: bool = True,
) -> Path:
    """Configure root logging with a rotating JSONL file and a console stream.

    Returns the log file path.  Safe to call more than once (tests / replay).
    """

    log_path = Path(log_dir)
    log_path.mkdir(parents=True, exist_ok=True)
    file_path = log_path / "tickforge.log"

    root = logging.getLogger()
    if reset:
        for handler in list(root.handlers):
            root.removeHandler(handler)
            try:
                handler.close()
            except Exception:
                pass

    root.setLevel(getattr(logging, level.upper(), logging.INFO))
    file_handler = logging.handlers.RotatingFileHandler(
        file_path,
        maxBytes=64 * 1024 * 1024,
        backupCount=5,
        encoding="utf-8",
    )
    file_handler.setFormatter(JsonFormatter())
    file_handler.setLevel(logging.DEBUG)

    stream = sys.stdout
    try:
        stream.reconfigure(encoding="utf-8", errors="replace")  # type: ignore[attr-defined]
    except Exception:
        pass
    console_handler = logging.StreamHandler(stream)
    console_handler.setFormatter(ConsoleFormatter())
    console_handler.setLevel(getattr(logging, level.upper(), logging.INFO))

    root.addHandler(file_handler)
    root.addHandler(console_handler)

    # Third-party libraries are very chatty at DEBUG; keep them at WARNING.
    for noisy in ("websockets", "aiohttp", "asyncio"):
        logging.getLogger(noisy).setLevel(logging.WARNING)
    return file_path


__all__ = ["configure_logging", "JsonFormatter", "ConsoleFormatter"]
