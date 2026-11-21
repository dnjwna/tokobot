"""Test ketahanan: 429 + Retry-After, reconnect WebSocket, order tidak di-retry buta."""

from __future__ import annotations

import asyncio
import json
import time

import pytest
from aiohttp import web
from websockets.asyncio.server import serve

from config import ExchangeConfig
from core.models import Candle
from exchange.tokocrypto_client import (MarketStream, OrderStateUnknownError, SymbolRules, TokocryptoClient)
from tests.test_core import RULES_ITEM


async def _serve(app: web.Application) -> tuple[web.AppRunner, str]:
    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, "127.0.0.1", 0)
    await site.start()
    port = site._server.sockets[0].getsockname()[1]  # type: ignore[union-attr]
    return runner, f"http://127.0.0.1:{port}"


async def test_429_retry_after_then_success() -> None:
    calls: list[float] = []

    async def handler(request: web.Request) -> web.Response:
        calls.append(time.monotonic())
        if len(calls) == 1:
            return web.Response(status=429, headers={"Retry-After": "1"})
        return web.json_response({"code": 0, "msg": "success", "timestamp": 1})

    app = web.Application()
    app.router.add_get("/open/v1/common/time", handler)
    runner, base = await _serve(app)
    try:
        async with TokocryptoClient(ExchangeConfig("", "", rest_base=base, max_retries=2)) as client:
            payload = await client._request("GET", f"{base}/open/v1/common/time", unwrap=False)
        assert payload["code"] == 0 and len(calls) == 2
        assert calls[1] - calls[0] >= 0.95  # Retry-After dihormati
    finally:
        await runner.cleanup()


async def test_order_post_not_blindly_retried_on_5xx() -> None:
    hits = 0

    async def handler(request: web.Request) -> web.Response:
        nonlocal hits
        hits += 1
        return web.Response(status=503, text="unavailable")

    app = web.Application()
    app.router.add_post("/open/v1/orders", handler)
    runner, base = await _serve(app)
    rules = SymbolRules.from_api(RULES_ITEM)
    try:
        async with TokocryptoClient(ExchangeConfig("k", "s", rest_base=base, max_retries=5)) as client:
            from decimal import Decimal
            with pytest.raises(OrderStateUnknownError) as info:
                await client.create_order(rules, "SELL", "MARKET", "tbX1", quantity=Decimal("0.001"),
                                          ref_price=Decimal("50000"))
        assert hits == 1 and info.value.client_id == "tbX1"
    finally:
        await runner.cleanup()


async def test_market_stream_reconnects_and_backfills() -> None:
    connects = 0
    received: list[int] = []

    async def ws_handler(websocket) -> None:  # type: ignore[no-untyped-def]
        nonlocal connects
        connects += 1
        k = {"t": connects, "T": connects + 1, "s": "BTCUSDT", "i": "1h", "o": "1", "c": "1", "h": "1", "l": "1",
             "v": "1", "x": True}
        await websocket.send(json.dumps({"stream": "btcusdt@kline_1h", "data": {"e": "kline", "s": "BTCUSDT", "k": k}}))
        await websocket.close()  # putus paksa -> klien harus reconnect

    server = await serve(ws_handler, "127.0.0.1", 0)
    port = list(server.sockets)[0].getsockname()[1]
    rules = SymbolRules.from_api(RULES_ITEM)
    stop = asyncio.Event()
    on_connect_calls = 0

    async def on_kline(symbol: str, candle: Candle) -> None:
        received.append(candle.open_time)
        if len(received) >= 3:
            stop.set()

    async def on_connect() -> None:
        nonlocal on_connect_calls
        on_connect_calls += 1

    stream = MarketStream(ExchangeConfig("", "", ws_market_type1=f"ws://127.0.0.1:{port}"), [rules], "1h",
                          on_kline=on_kline, on_connect=on_connect, stale_timeout_s=5)
    try:
        await asyncio.wait_for(stream.run(stop), timeout=30)
    finally:
        server.close()
    assert received[:3] == [1, 2, 3] and on_connect_calls >= 3
