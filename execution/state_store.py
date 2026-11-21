"""Persistensi state (JSON atomic) dan jurnal trade (JSON Lines)."""

from __future__ import annotations

import json
import os
from pathlib import Path
from typing import Any

from utils.logger import get_logger, log_event


class StateStore:
    """Simpan state bot secara atomic (tulis ke file sementara -> fsync -> rename)."""

    def __init__(self, state_path: Path, trades_path: Path) -> None:
        self.state_path = state_path
        self.trades_path = trades_path
        self.state_path.parent.mkdir(parents=True, exist_ok=True)
        self._log = get_logger("state")

    def load(self) -> dict[str, Any]:
        if not self.state_path.exists():
            return {}
        try:
            return json.loads(self.state_path.read_text(encoding="utf-8"))
        except json.JSONDecodeError as exc:
            backup = self.state_path.with_suffix(".corrupt")
            self.state_path.replace(backup)
            log_event(self._log, "state_corrupt", 50, error=str(exc), backup=str(backup))
            raise RuntimeError(f"State file korup, dipindah ke {backup}. Periksa manual sebelum lanjut.") from exc

    def save(self, state: dict[str, Any]) -> None:
        tmp = self.state_path.with_suffix(".tmp")
        data = json.dumps(state, indent=2, default=str, sort_keys=True)
        with tmp.open("w", encoding="utf-8") as fh:
            fh.write(data)
            fh.flush()
            os.fsync(fh.fileno())
        tmp.replace(self.state_path)

    def append_trade(self, record: dict[str, Any]) -> None:
        with self.trades_path.open("a", encoding="utf-8") as fh:
            fh.write(json.dumps(record, default=str) + "\n")
