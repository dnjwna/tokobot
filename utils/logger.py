"""Logging terstruktur: console (human-readable) + file JSON Lines berotasi."""

from __future__ import annotations

import json
import logging
import sys
from datetime import datetime, timezone
from logging.handlers import RotatingFileHandler
from pathlib import Path
from typing import Any

_RESERVED = set(vars(logging.LogRecord("", 0, "", 0, "", (), None)).keys()) | {"message", "asctime"}


class JsonFormatter(logging.Formatter):
    """Format setiap record sebagai satu baris JSON (mudah di-grep / di-ingest)."""

    def format(self, record: logging.LogRecord) -> str:
        payload: dict[str, Any] = {
            "ts": datetime.fromtimestamp(record.created, tz=timezone.utc).isoformat(timespec="milliseconds"),
            "level": record.levelname,
            "logger": record.name,
            "msg": record.getMessage(),
        }
        for key, value in record.__dict__.items():
            if key not in _RESERVED and not key.startswith("_"):
                payload[key] = value
        if record.exc_info:
            payload["exc"] = self.formatException(record.exc_info)
        return json.dumps(payload, default=str, ensure_ascii=False)


class ConsoleFormatter(logging.Formatter):
    """Format ringkas untuk console; field ``event`` dan ``data`` ditampilkan inline."""

    def format(self, record: logging.LogRecord) -> str:
        base = f"{self.formatTime(record, '%Y-%m-%d %H:%M:%S')} {record.levelname:<7} {record.name}: {record.getMessage()}"
        data = getattr(record, "data", None)
        if data:
            base += " | " + " ".join(f"{k}={v}" for k, v in data.items())
        if record.exc_info:
            base += "\n" + self.formatException(record.exc_info)
        return base


def setup_logging(log_dir: Path, level: str = "INFO", name: str = "tokobot") -> logging.Logger:
    """Konfigurasi root logger aplikasi. Aman dipanggil berulang (idempotent)."""
    log_dir.mkdir(parents=True, exist_ok=True)
    logger = logging.getLogger(name)
    logger.setLevel(getattr(logging, level.upper(), logging.INFO))
    logger.propagate = False
    for handler in list(logger.handlers):
        logger.removeHandler(handler)
        handler.close()

    console = logging.StreamHandler(sys.stdout)
    console.setFormatter(ConsoleFormatter())
    logger.addHandler(console)

    file_handler = RotatingFileHandler(log_dir / f"{name}.jsonl", maxBytes=20 * 1024 * 1024,
                                       backupCount=10, encoding="utf-8")
    file_handler.setFormatter(JsonFormatter())
    logger.addHandler(file_handler)
    return logger


def get_logger(component: str) -> logging.Logger:
    """Logger anak, mis. ``tokobot.exchange``."""
    return logging.getLogger(f"tokobot.{component}")


def log_event(logger: logging.Logger, event: str, level: int = logging.INFO, **data: Any) -> None:
    """Catat event terstruktur. ``event`` = jenis (signal, filter, order, fill, risk, error, ...)."""
    logger.log(level, event, extra={"event": event, "data": data})
