"""Notifikasi Telegram non-blocking.

Pesan masuk ke antrean dan dikirim oleh worker terpisah, sehingga kegagalan
Telegram (jaringan, rate-limit 429) tidak pernah menghambat loop trading.
"""

from __future__ import annotations

import asyncio
import html
import logging
from typing import Optional

import aiohttp

from config import TelegramConfig
from utils.logger import get_logger, log_event

_LEVEL_ICON = {"INFO": "ℹ️", "FILL": "✅", "STOP": "🛑", "WARN": "⚠️", "CRITICAL": "🚨", "KILL": "⛔"}


class Notifier:
    """Antarmuka notifier; implementasi default tidak melakukan apa-apa."""

    async def start(self) -> None:  # pragma: no cover - trivial
        return None

    async def close(self) -> None:  # pragma: no cover - trivial
        return None

    def send(self, text: str, level: str = "INFO") -> None:  # pragma: no cover - trivial
        return None


class TelegramNotifier(Notifier):
    """Kirim pesan ke chat Telegram via Bot API ``sendMessage``."""

    API = "https://api.telegram.org/bot{token}/sendMessage"

    def __init__(self, cfg: TelegramConfig, prefix: str = "TokoBot", max_queue: int = 500) -> None:
        self._cfg = cfg
        self._prefix = prefix
        self._queue: asyncio.Queue[str] = asyncio.Queue(maxsize=max_queue)
        self._session: Optional[aiohttp.ClientSession] = None
        self._worker: Optional[asyncio.Task[None]] = None
        self._log = get_logger("telegram")

    async def start(self) -> None:
        self._session = aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=15))
        self._worker = asyncio.create_task(self._run(), name="telegram-worker")

    async def close(self) -> None:
        """Kuras antrean (maks 10 detik) lalu tutup sesi."""
        if self._worker:
            try:
                await asyncio.wait_for(self._queue.join(), timeout=10)
            except asyncio.TimeoutError:
                log_event(self._log, "telegram_flush_timeout", logging.WARNING, pending=self._queue.qsize())
            self._worker.cancel()
            await asyncio.gather(self._worker, return_exceptions=True)
        if self._session:
            await self._session.close()

    def send(self, text: str, level: str = "INFO") -> None:
        icon = _LEVEL_ICON.get(level, "")
        message = f"{icon} <b>{html.escape(self._prefix)}</b> [{html.escape(level)}]\n{html.escape(text)}"
        try:
            self._queue.put_nowait(message[:4000])
        except asyncio.QueueFull:
            log_event(self._log, "telegram_queue_full", logging.WARNING)

    async def _run(self) -> None:
        while True:
            message = await self._queue.get()
            try:
                await self._deliver(message)
            finally:
                self._queue.task_done()
            await asyncio.sleep(1.0)  # batas aman ~1 pesan/detik per chat

    async def _deliver(self, message: str) -> None:
        assert self._session is not None
        url = self.API.format(token=self._cfg.bot_token)
        payload = {"chat_id": self._cfg.chat_id, "text": message, "parse_mode": "HTML",
                   "disable_web_page_preview": True}
        for attempt in range(1, 4):
            try:
                async with self._session.post(url, json=payload) as resp:
                    if resp.status == 200:
                        return
                    body = await resp.json(content_type=None)
                    if resp.status == 429:
                        retry_after = float(body.get("parameters", {}).get("retry_after", 5))
                        await asyncio.sleep(retry_after)
                        continue
                    log_event(self._log, "telegram_send_failed", logging.WARNING,
                              status=resp.status, description=body.get("description"))
                    return
            except (aiohttp.ClientError, asyncio.TimeoutError) as exc:
                log_event(self._log, "telegram_network_error", logging.WARNING, attempt=attempt, error=str(exc))
                await asyncio.sleep(2 ** attempt)


def build_notifier(cfg: TelegramConfig, prefix: str) -> Notifier:
    """Factory: TelegramNotifier bila diaktifkan, selain itu Notifier no-op."""
    return TelegramNotifier(cfg, prefix=prefix) if cfg.enabled else Notifier()
