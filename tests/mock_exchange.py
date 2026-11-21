"""Mock server Tokocrypto (REST + WebSocket) untuk integration test tanpa uang riil.

Meniru format response apidocs: wrapper code/msg/data, enum numerik, filter string,
verifikasi signature HMAC SHA256, komisi beli dipotong dari aset base (untuk menguji
perhitungan qty bersih), STOP_LOSS_LIMIT yang mengunci saldo, dan injeksi HTTP 500.
"""

from __future__ import annotations

import asyncio
import hashlib
import hmac
import json
import time
from dataclasses import dataclass, field
from decimal import Decimal
from typing import Any, Optional
from urllib.parse import parse_qsl

from aiohttp import web
from websockets.asyncio.server import serve

from core.models import Candle

API_KEY = "test-key"
API_SECRET = "test-secret"
FEE = Decimal("0.001")
TAX = Decimal("0.0021")


@dataclass
class MockOrder:
    order_id: int
    client_id: str
    symbol: str
    side: int
    type: int
    status: int
    price: Decimal
    orig_qty: Decimal
    stop_price: Decimal
    executed_qty: Decimal = Decimal(0)
    executed_quote: Decimal = Decimal(0)
    create_time: int = field(default_factory=lambda: int(time.time() * 1000))

    def to_api(self) -> dict[str, Any]:
        return {"orderId": self.order_id, "clientId": self.client_id, "symbol": self.symbol, "side": self.side,
                "type": self.type, "status": self.status, "price": str(self.price), "origQty": str(self.orig_qty),
                "executedQty": str(self.executed_qty), "executedQuoteQty": str(self.executed_quote),
                "stopPrice": str(self.stop_price), "createTime": self.create_time, "timeInForce": 1}


class MockExchange:
    """State bursa tiruan untuk satu simbol BTC_USDT."""

    def __init__(self, candles: list[Candle], usdt: Decimal = Decimal("1000")) -> None:
        self.candles = candles
        self.balances: dict[str, list[Decimal]] = {"USDT": [usdt, Decimal(0)], "BTC": [Decimal(0), Decimal(0)]}
        self.orders: dict[int, MockOrder] = {}
        self.next_id = 1000
        self.price = Decimal(str(candles[-1].close))
        self.fail_next_order_with_500 = False
        self.bad_signatures = 0
        self.requests: list[str] = []
        self.ws_queue: asyncio.Queue[str] = asyncio.Queue()

    # -- helpers ----------------------------------------------------------- #
    @staticmethod
    def wrap(data: Any) -> web.Response:
        return web.json_response({"code": 0, "msg": "success", "data": data, "timestamp": int(time.time() * 1000)})

    def _verify(self, request: web.Request, raw: str) -> Optional[web.Response]:
        if request.headers.get("X-MBX-APIKEY") != API_KEY:
            return web.json_response({"code": -2014, "msg": "API-key format invalid."}, status=401)
        if "&signature=" not in raw:
            return web.json_response({"code": -1102, "msg": "signature missing"}, status=400)
        payload, sig = raw.rsplit("&signature=", 1)
        expected = hmac.new(API_SECRET.encode(), payload.encode(), hashlib.sha256).hexdigest()
        if not hmac.compare_digest(expected, sig):
            self.bad_signatures += 1
            return web.json_response({"code": -1022, "msg": "Signature for this request is not valid."}, status=400)
        return None

    async def _params(self, request: web.Request) -> tuple[dict[str, str], Optional[web.Response]]:
        raw = (await request.text()) if request.method == "POST" else request.query_string
        self.requests.append(f"{request.method} {request.path}")
        err = self._verify(request, raw)
        return dict(parse_qsl(raw)), err

    def _bal(self, asset: str) -> list[Decimal]:
        return self.balances.setdefault(asset, [Decimal(0), Decimal(0)])

    # -- public ------------------------------------------------------------- #
    async def time_h(self, request: web.Request) -> web.Response:
        return web.json_response({"code": 0, "msg": "success", "timestamp": int(time.time() * 1000)})

    async def symbols_h(self, request: web.Request) -> web.Response:
        item = {
            "type": 1, "symbol": "BTC_USDT", "baseAsset": "BTC", "basePrecision": 8, "quoteAsset": "USDT",
            "quotePrecision": 8, "spotTradingEnable": 1, "orderTypes": [1, 2, 4, 7],
            "filters": [
                {"filterType": "PRICE_FILTER", "minPrice": "0.01000000", "maxPrice": "1000000.00000000", "tickSize": "0.01000000"},
                {"filterType": "PERCENT_PRICE", "multiplierUp": 5, "multiplierDown": 0.2, "avgPriceMins": "5"},
                {"filterType": "LOT_SIZE", "minQty": "0.00001000", "maxQty": "9000.00000000", "stepSize": "0.00001000"},
                {"filterType": "MARKET_LOT_SIZE", "minQty": "0.00001000", "maxQty": "100.00000000", "stepSize": "0.00001000"},
                {"filterType": "NOTIONAL", "minNotional": "5.00000000", "maxNotional": "9000000.00000000", "applyToMarket": True},
            ],
        }
        return self.wrap({"list": [item]})

    async def klines_h(self, request: web.Request) -> web.Response:
        limit = int(request.query.get("limit", 500))
        rows = [[c.open_time, str(c.open), str(c.high), str(c.low), str(c.close), str(c.volume), c.close_time,
                 "0", 10, "0", "0", "0"] for c in self.candles[-limit:]]
        return self.wrap(rows)

    async def depth_h(self, request: web.Request) -> web.Response:
        p = self.price
        return self.wrap({"lastUpdateId": 1, "bids": [[str(p - Decimal("0.5")), "5"]],
                          "asks": [[str(p + Decimal("0.5")), "5"]]})

    # -- signed ------------------------------------------------------------- #
    async def account_h(self, request: web.Request) -> web.Response:
        _, err = await self._params(request)
        if err:
            return err
        assets = [{"asset": a, "free": str(v[0]), "locked": str(v[1])} for a, v in self.balances.items()]
        return self.wrap({"makerCommission": "10", "takerCommission": "10", "canTrade": 1, "accountAssets": assets})

    async def order_create_h(self, request: web.Request) -> web.Response:
        params, err = await self._params(request)
        if err:
            return err
        self.next_id += 1
        side, otype = int(params["side"]), int(params["type"])
        qty = Decimal(params["quantity"])
        order = MockOrder(self.next_id, params.get("clientId", ""), params["symbol"], side, otype, 0,
                          Decimal(params.get("price", "0")), qty, Decimal(params.get("stopPrice", "0")))
        usdt, btc = self._bal("USDT"), self._bal("BTC")
        ask, bid = self.price + Decimal("0.5"), self.price - Decimal("0.5")
        if otype == 1 and side == 0:  # LIMIT IOC BUY
            if order.price >= ask:
                cost = qty * ask
                usdt[0] -= cost * (1 + TAX)            # pajak dipotong di quote
                btc[0] += qty * (1 - FEE)              # komisi dipotong di base
                order.executed_qty, order.executed_quote, order.status = qty, cost, 2
            else:
                order.status = 6
        elif otype == 2 and side == 1:  # MARKET SELL
            if btc[0] < qty:
                return web.json_response({"code": -2010, "msg": "insufficient balance"}, status=400)
            btc[0] -= qty
            gross = qty * bid
            usdt[0] += gross * (1 - FEE - TAX)
            order.executed_qty, order.executed_quote, order.status = qty, gross, 2
        elif otype == 4 and side == 1:  # STOP_LOSS_LIMIT SELL -> kunci saldo
            if btc[0] < qty:
                return web.json_response({"code": -2010, "msg": "insufficient balance"}, status=400)
            btc[0] -= qty
            btc[1] += qty
            order.status = 0
        else:
            return web.json_response({"code": -1116, "msg": "unsupported in mock"}, status=400)
        self.orders[order.order_id] = order
        if self.fail_next_order_with_500:
            self.fail_next_order_with_500 = False
            return web.Response(status=502, text="bad gateway")
        return web.json_response({"code": 0, "message": "success", "data": order.to_api(), "success": True,
                                  "timestamp": int(time.time() * 1000)})

    async def order_detail_h(self, request: web.Request) -> web.Response:
        params, err = await self._params(request)
        if err:
            return err
        order = self.orders.get(int(params["orderId"]))
        if order is None:
            return web.json_response({"code": -2013, "msg": "Order does not exist."}, status=400)
        return self.wrap(order.to_api())

    async def order_list_h(self, request: web.Request) -> web.Response:
        params, err = await self._params(request)
        if err:
            return err
        kind = int(params.get("type", -1))
        items = [o.to_api() for o in self.orders.values() if kind != 1 or o.status in (0, 1)]
        return self.wrap({"list": items})

    async def order_cancel_h(self, request: web.Request) -> web.Response:
        params, err = await self._params(request)
        if err:
            return err
        order = self.orders.get(int(params["orderId"]))
        if order is None or order.status not in (0, 1):
            return web.json_response({"code": -2011, "msg": "Unknown order sent."}, status=400)
        order.status = 3
        if order.type == 4:
            btc = self._bal("BTC")
            btc[1] -= order.orig_qty
            btc[0] += order.orig_qty
        return self.wrap(order.to_api())

    def trigger_stop(self, order_id: int, fill_price: Decimal) -> None:
        """Simulasikan stop di bursa tereksekusi."""
        order = self.orders[order_id]
        btc, usdt = self._bal("BTC"), self._bal("USDT")
        btc[1] -= order.orig_qty
        gross = order.orig_qty * fill_price
        usdt[0] += gross * (1 - FEE - TAX)
        order.executed_qty, order.executed_quote, order.status = order.orig_qty, gross, 2

    # -- server ------------------------------------------------------------- #
    def app(self) -> web.Application:
        app = web.Application()
        app.router.add_get("/open/v1/common/time", self.time_h)
        app.router.add_get("/open/v1/common/symbols", self.symbols_h)
        app.router.add_get("/api/v3/klines", self.klines_h)
        app.router.add_get("/api/v3/depth", self.depth_h)
        app.router.add_get("/open/v1/account/spot", self.account_h)
        app.router.add_post("/open/v1/orders", self.order_create_h)
        app.router.add_get("/open/v1/orders", self.order_list_h)
        app.router.add_get("/open/v1/orders/detail", self.order_detail_h)
        app.router.add_post("/open/v1/orders/cancel", self.order_cancel_h)
        return app

    async def ws_handler(self, websocket: Any) -> None:
        while True:
            msg = await self.ws_queue.get()
            await websocket.send(msg)

    def push_kline(self, c: Candle, symbol: str = "BTCUSDT", interval: str = "1h") -> None:
        k = {"t": c.open_time, "T": c.close_time, "s": symbol, "i": interval, "o": str(c.open), "c": str(c.close),
             "h": str(c.high), "l": str(c.low), "v": str(c.volume), "x": c.closed}
        self.ws_queue.put_nowait(json.dumps({"stream": f"{symbol.lower()}@kline_{interval}",
                                             "data": {"e": "kline", "E": int(time.time() * 1000), "s": symbol, "k": k}}))


async def start_servers(ex: MockExchange) -> tuple[web.AppRunner, Any, int, int]:
    runner = web.AppRunner(ex.app())
    await runner.setup()
    site = web.TCPSite(runner, "127.0.0.1", 0)
    await site.start()
    http_port = site._server.sockets[0].getsockname()[1]  # type: ignore[union-attr]
    ws_server = await serve(ex.ws_handler, "127.0.0.1", 0)
    ws_port = list(ws_server.sockets)[0].getsockname()[1]
    return runner, ws_server, http_port, ws_port
