"""Konektor Tokocrypto: REST (HMAC SHA256), WebSocket market/user stream, precision formatter.

Referensi: https://www.tokocrypto.com/apidocs/

Poin penting dari dokumentasi yang diimplementasikan di sini:
* Simbol REST memakai underscore (``BTC_USDT``); market data symbolType 1 memakai
  format tanpa underscore (``BTCUSDT``) di ``https://www.tokocrypto.site/api/v3/*``,
  dan stream WebSocket memakai huruf kecil (``btcusdt@kline_1h``).
* SIGNED: ``signature = HMAC_SHA256(secret, totalParams)``, header ``X-MBX-APIKEY``,
  parameter ``timestamp`` (ms) dan ``recvWindow`` (<= 60000).
* Response dibungkus ``{"code":0,"msg":...,"data":...,"timestamp":...}``; ``code != 0`` = gagal.
* Enum numerik: side 0=BUY/1=SELL; type 1=LIMIT, 2=MARKET, 4=STOP_LOSS_LIMIT, ...;
  timeInForce 1=GTC, 2=IOC; status 0=NEW, 1=PARTIALLY_FILLED, 2=FILLED, 3=CANCELED, ...
* HTTP 429/418 membawa ``Retry-After``; 5XX berarti status eksekusi UNKNOWN.
* Listen key symbolType 1 deprecated (decommission 30 Apr 2026) -> pakai
  ``POST /open/v1/user-listen-token`` + WebSocket API ``userDataStream.subscribe.listenToken``.
"""

from __future__ import annotations

import asyncio
import hashlib
import hmac
import json
import logging
import random
import time
from dataclasses import dataclass
from decimal import ROUND_CEILING, ROUND_FLOOR, Decimal, InvalidOperation
from typing import Any, Awaitable, Callable, Optional, Sequence
from urllib.parse import urlencode

import aiohttp
from websockets.asyncio.client import connect as ws_connect
from websockets.exceptions import ConnectionClosed, InvalidHandshake, InvalidURI
from yarl import URL

from config import ExchangeConfig
from core.models import Candle
from utils.logger import get_logger, log_event

# --------------------------------------------------------------------------- #
# Enum API
# --------------------------------------------------------------------------- #
ORDER_SIDE: dict[str, int] = {"BUY": 0, "SELL": 1}
ORDER_TYPE: dict[str, int] = {
    "LIMIT": 1, "MARKET": 2, "STOP_LOSS": 3, "STOP_LOSS_LIMIT": 4,
    "TAKE_PROFIT": 5, "TAKE_PROFIT_LIMIT": 6, "LIMIT_MAKER": 7,
}
TIME_IN_FORCE: dict[str, int] = {"GTC": 1, "IOC": 2, "FOK": 3, "GTX": 4}
ORDER_STATUS: dict[int, str] = {
    -2: "SYSTEM_PROCESSING", 0: "NEW", 1: "PARTIALLY_FILLED", 2: "FILLED",
    3: "CANCELED", 4: "PENDING_CANCEL", 5: "REJECTED", 6: "EXPIRED",
}
SIDE_BY_CODE = {v: k for k, v in ORDER_SIDE.items()}
TYPE_BY_CODE = {v: k for k, v in ORDER_TYPE.items()}
FINAL_STATUSES = frozenset({"FILLED", "CANCELED", "REJECTED", "EXPIRED"})
OPEN_STATUSES = frozenset({"NEW", "PARTIALLY_FILLED", "SYSTEM_PROCESSING", "PENDING_CANCEL"})
LIMIT_LIKE = frozenset({"LIMIT", "STOP_LOSS_LIMIT", "TAKE_PROFIT_LIMIT", "LIMIT_MAKER"})
SUPPORTED_SYMBOL_TYPES = frozenset({1})


# --------------------------------------------------------------------------- #
# Error
# --------------------------------------------------------------------------- #
class ExchangeError(Exception):
    """Error umum dari bursa (HTTP non-2xx atau ``code != 0``)."""

    def __init__(self, message: str, code: Optional[int] = None, http_status: Optional[int] = None) -> None:
        super().__init__(message)
        self.code = code
        self.http_status = http_status


class ExchangeAuthError(ExchangeError):
    """Kredensial tidak ada / ditolak."""


class RateLimitError(ExchangeError):
    """HTTP 429/418 setelah retry habis."""

    def __init__(self, message: str, retry_after: float, http_status: int) -> None:
        super().__init__(message, http_status=http_status)
        self.retry_after = retry_after


class NetworkError(ExchangeError):
    """Gagal koneksi / timeout."""


class OrderStateUnknownError(ExchangeError):
    """Order mungkin terkirim tapi status tidak diketahui (timeout / 5XX). Wajib rekonsiliasi."""

    def __init__(self, message: str, client_id: str) -> None:
        super().__init__(message)
        self.client_id = client_id


class PrecisionError(ValueError):
    """Order melanggar aturan presisi/filter bursa (dicegah sebelum dikirim)."""


# --------------------------------------------------------------------------- #
# Precision / exchange filters
# --------------------------------------------------------------------------- #
def to_decimal(value: Any) -> Decimal:
    """Konversi aman ke Decimal (float lewat ``str`` agar tidak membawa noise biner)."""
    if isinstance(value, Decimal):
        return value
    try:
        return Decimal(str(value))
    except (InvalidOperation, ValueError) as exc:
        raise PrecisionError(f"Nilai numerik tidak valid: {value!r}") from exc


def _quantize_to_step(value: Decimal, step: Decimal, rounding: str) -> Decimal:
    if step <= 0:
        return value
    step_n = step.normalize()
    ratio = value / step_n
    if rounding == ROUND_FLOOR:
        ratio += Decimal("1e-9")   # toleransi noise float, mis. 0.0029999999999
    else:
        ratio -= Decimal("1e-9")
    units = ratio.to_integral_value(rounding=rounding)
    result = units * step_n
    exponent = step_n.as_tuple().exponent
    return result.quantize(step_n) if isinstance(exponent, int) and exponent < 0 else result


def fmt_decimal(value: Decimal) -> str:
    """Format Decimal tanpa notasi ilmiah dan tanpa nol berlebih."""
    text = format(value.normalize(), "f")
    return text if text not in ("-0", "") else "0"


@dataclass(frozen=True)
class SymbolRules:
    """Aturan perdagangan satu simbol, dari ``GET /open/v1/common/symbols``."""

    symbol: str
    market_symbol: str
    symbol_type: int
    base_asset: str
    quote_asset: str
    tick_size: Decimal
    min_price: Decimal
    max_price: Decimal
    step_size: Decimal
    min_qty: Decimal
    max_qty: Decimal
    market_step_size: Decimal
    market_min_qty: Decimal
    market_max_qty: Decimal
    min_notional: Decimal
    max_notional: Decimal
    notional_applies_to_market: bool
    multiplier_up: Optional[Decimal]
    multiplier_down: Optional[Decimal]
    order_types: frozenset[str]
    spot_enabled: bool

    @classmethod
    def from_api(cls, item: dict[str, Any]) -> "SymbolRules":
        """Parse satu elemen ``data.list[]``. Nilai filter bisa string atau angka."""
        filters = {f.get("filterType"): f for f in item.get("filters", []) if isinstance(f, dict)}
        price_f = filters.get("PRICE_FILTER", {})
        lot_f = filters.get("LOT_SIZE", {})
        mlot_f = filters.get("MARKET_LOT_SIZE", {}) or lot_f
        notional_f = filters.get("NOTIONAL") or filters.get("MIN_NOTIONAL") or {}
        pct_f = filters.get("PERCENT_PRICE") or filters.get("PERCENT_PRICE_BY_SIDE") or {}

        def d(src: dict[str, Any], key: str, default: str = "0") -> Decimal:
            raw = src.get(key, default)
            return to_decimal(raw if raw not in (None, "") else default)

        raw_types = item.get("orderTypes") or []
        order_types = frozenset(
            TYPE_BY_CODE.get(int(t), str(t)) if isinstance(t, int) or str(t).lstrip("-").isdigit() else str(t).upper()
            for t in raw_types
        )
        rest_symbol = str(item["symbol"])
        mult_up = pct_f.get("multiplierUp")
        mult_down = pct_f.get("multiplierDown")
        return cls(
            symbol=rest_symbol,
            market_symbol=rest_symbol.replace("_", ""),
            symbol_type=int(item.get("type", 1)),
            base_asset=str(item.get("baseAsset", rest_symbol.split("_")[0])),
            quote_asset=str(item.get("quoteAsset", rest_symbol.split("_")[-1])),
            tick_size=d(price_f, "tickSize"),
            min_price=d(price_f, "minPrice"),
            max_price=d(price_f, "maxPrice"),
            step_size=d(lot_f, "stepSize"),
            min_qty=d(lot_f, "minQty"),
            max_qty=d(lot_f, "maxQty"),
            market_step_size=d(mlot_f, "stepSize"),
            market_min_qty=d(mlot_f, "minQty"),
            market_max_qty=d(mlot_f, "maxQty"),
            min_notional=d(notional_f, "minNotional"),
            max_notional=d(notional_f, "maxNotional"),
            notional_applies_to_market=bool(notional_f.get("applyToMarket", True)),
            multiplier_up=to_decimal(mult_up) if mult_up not in (None, "") else None,
            multiplier_down=to_decimal(mult_down) if mult_down not in (None, "") else None,
            order_types=order_types,
            spot_enabled=bool(item.get("spotTradingEnable", 1)),
        )

    # -- pembulatan ------------------------------------------------------- #
    def floor_qty(self, qty: Any, market: bool = False) -> Decimal:
        """Bulatkan qty ke bawah sesuai ``stepSize`` (LOT_SIZE / MARKET_LOT_SIZE)."""
        step = self.market_step_size if market and self.market_step_size > 0 else self.step_size
        value = _quantize_to_step(to_decimal(qty), step, ROUND_FLOOR)
        return max(value, Decimal(0))

    def round_price(self, price: Any, up: bool = False) -> Decimal:
        """Bulatkan harga ke ``tickSize`` (default ke bawah)."""
        return _quantize_to_step(to_decimal(price), self.tick_size, ROUND_CEILING if up else ROUND_FLOOR)

    def supports(self, order_type: str) -> bool:
        return not self.order_types or order_type in self.order_types

    def min_notional_for(self, order_type: str) -> Decimal:
        if order_type == "MARKET" and not self.notional_applies_to_market:
            return Decimal(0)
        return self.min_notional

    # -- validasi --------------------------------------------------------- #
    def validate(self, order_type: str, qty: Decimal, price: Optional[Decimal], ref_price: Decimal,
                 stop_price: Optional[Decimal] = None) -> None:
        """Validasi order terhadap filter bursa. Lempar ``PrecisionError`` bila melanggar."""
        if not self.spot_enabled:
            raise PrecisionError(f"{self.symbol}: spot trading nonaktif")
        if not self.supports(order_type):
            raise PrecisionError(f"{self.symbol}: tipe order {order_type} tidak didukung ({sorted(self.order_types)})")
        is_market = order_type == "MARKET"
        step = self.market_step_size if is_market and self.market_step_size > 0 else self.step_size
        min_q = self.market_min_qty if is_market and self.market_min_qty > 0 else self.min_qty
        max_q = self.market_max_qty if is_market and self.market_max_qty > 0 else self.max_qty
        if qty <= 0:
            raise PrecisionError(f"{self.symbol}: qty harus > 0")
        if step > 0 and (qty % step.normalize()) != 0:
            raise PrecisionError(f"{self.symbol}: qty {qty} bukan kelipatan stepSize {step}")
        if min_q > 0 and qty < min_q:
            raise PrecisionError(f"{self.symbol}: qty {qty} < minQty {min_q}")
        if max_q > 0 and qty > max_q:
            raise PrecisionError(f"{self.symbol}: qty {qty} > maxQty {max_q}")
        for label, p in (("price", price), ("stopPrice", stop_price)):
            if p is None:
                continue
            if p <= 0:
                raise PrecisionError(f"{self.symbol}: {label} harus > 0")
            if self.tick_size > 0 and (p % self.tick_size.normalize()) != 0:
                raise PrecisionError(f"{self.symbol}: {label} {p} bukan kelipatan tickSize {self.tick_size}")
            if self.min_price > 0 and p < self.min_price:
                raise PrecisionError(f"{self.symbol}: {label} {p} < minPrice {self.min_price}")
            if self.max_price > 0 and p > self.max_price:
                raise PrecisionError(f"{self.symbol}: {label} {p} > maxPrice {self.max_price}")
        effective_price = price if price is not None else ref_price
        notional = qty * effective_price
        min_notional = self.min_notional_for(order_type)
        if min_notional > 0 and notional < min_notional:
            raise PrecisionError(f"{self.symbol}: notional {notional} < minNotional {min_notional}")
        if self.max_notional > 0 and notional > self.max_notional:
            raise PrecisionError(f"{self.symbol}: notional {notional} > maxNotional {self.max_notional}")
        if price is not None and ref_price > 0:
            if self.multiplier_up is not None and price > ref_price * self.multiplier_up:
                raise PrecisionError(f"{self.symbol}: price {price} > ref*multiplierUp")
            if self.multiplier_down is not None and price < ref_price * self.multiplier_down:
                raise PrecisionError(f"{self.symbol}: price {price} < ref*multiplierDown")


# --------------------------------------------------------------------------- #
# Model response
# --------------------------------------------------------------------------- #
@dataclass(frozen=True)
class Balance:
    asset: str
    free: Decimal
    locked: Decimal

    @property
    def total(self) -> Decimal:
        return self.free + self.locked


@dataclass(frozen=True)
class OrderInfo:
    """Representasi order yang dinormalisasi (enum numerik -> string)."""

    order_id: str
    client_id: str
    symbol: str
    side: str
    order_type: str
    status: str
    price: Decimal
    orig_qty: Decimal
    executed_qty: Decimal
    executed_quote_qty: Decimal
    stop_price: Decimal
    create_time: int

    @classmethod
    def from_api(cls, data: dict[str, Any]) -> "OrderInfo":
        def code_to(mapping: dict[int, str], raw: Any, default: str) -> str:
            if raw is None:
                return default
            try:
                return mapping.get(int(raw), str(raw))
            except (TypeError, ValueError):
                return str(raw).upper()

        def dec(key: str) -> Decimal:
            raw = data.get(key)
            return to_decimal(raw) if raw not in (None, "") else Decimal(0)

        return cls(
            order_id=str(data.get("orderId", "")),
            client_id=str(data.get("clientId") or data.get("clientOrderId") or ""),
            symbol=str(data.get("symbol", "")),
            side=code_to(SIDE_BY_CODE, data.get("side"), "UNKNOWN"),
            order_type=code_to(TYPE_BY_CODE, data.get("type"), "UNKNOWN"),
            status=code_to(ORDER_STATUS, data.get("status"), "UNKNOWN"),
            price=dec("price"),
            orig_qty=dec("origQty"),
            executed_qty=dec("executedQty"),
            executed_quote_qty=dec("executedQuoteQty"),
            stop_price=dec("stopPrice"),
            create_time=int(data.get("createTime") or 0),
        )

    @property
    def avg_price(self) -> Decimal:
        if self.executed_qty > 0 and self.executed_quote_qty > 0:
            return self.executed_quote_qty / self.executed_qty
        return self.price

    @property
    def is_final(self) -> bool:
        return self.status in FINAL_STATUSES

    @property
    def is_open(self) -> bool:
        return self.status in OPEN_STATUSES


# --------------------------------------------------------------------------- #
# Rate limiter
# --------------------------------------------------------------------------- #
class RateLimiter:
    """Token bucket berbasis weight per menit + blokir global untuk Retry-After."""

    def __init__(self, weight_per_minute: int) -> None:
        self._capacity = float(max(weight_per_minute, 1))
        self._tokens = self._capacity
        self._rate = self._capacity / 60.0
        self._updated = time.monotonic()
        self._blocked_until = 0.0
        self._lock = asyncio.Lock()

    async def acquire(self, weight: int = 1) -> None:
        weight_f = min(float(weight), self._capacity)
        async with self._lock:
            while True:
                now = time.monotonic()
                if now < self._blocked_until:
                    await asyncio.sleep(self._blocked_until - now)
                    continue
                self._tokens = min(self._capacity, self._tokens + (now - self._updated) * self._rate)
                self._updated = now
                if self._tokens >= weight_f:
                    self._tokens -= weight_f
                    return
                await asyncio.sleep((weight_f - self._tokens) / self._rate)

    def observe_used_weight(self, used: int) -> None:
        """Sinkronkan bucket dengan header ``X-MBX-USED-WEIGHT-1M`` dari server."""
        remaining = self._capacity - float(used)
        if remaining < self._tokens:
            self._tokens = max(remaining, 0.0)

    def block_for(self, seconds: float) -> None:
        self._blocked_until = max(self._blocked_until, time.monotonic() + max(seconds, 0.0))


def backoff_delay(attempt: int, base: float = 0.5, cap: float = 30.0) -> float:
    """Exponential backoff dengan full jitter."""
    return random.uniform(0, min(cap, base * (2 ** attempt)))


# --------------------------------------------------------------------------- #
# REST client
# --------------------------------------------------------------------------- #
class TokocryptoClient:
    """Client REST async Tokocrypto."""

    def __init__(self, cfg: ExchangeConfig, session: Optional[aiohttp.ClientSession] = None) -> None:
        self._cfg = cfg
        self._session = session
        self._own_session = session is None
        self._limiter = RateLimiter(cfg.weight_limit_per_min)
        self._time_offset_ms = 0
        self._log = get_logger("exchange")

    # -- lifecycle -------------------------------------------------------- #
    async def start(self) -> None:
        if self._session is None:
            timeout = aiohttp.ClientTimeout(total=self._cfg.request_timeout_s)
            self._session = aiohttp.ClientSession(timeout=timeout)

    async def close(self) -> None:
        if self._session is not None and self._own_session:
            await self._session.close()
        self._session = None

    async def __aenter__(self) -> "TokocryptoClient":
        await self.start()
        return self

    async def __aexit__(self, *exc: Any) -> None:
        await self.close()

    @property
    def config(self) -> ExchangeConfig:
        return self._cfg

    # -- signing ---------------------------------------------------------- #
    def sign(self, payload: str) -> str:
        """HMAC SHA256 hex atas ``totalParams`` dengan secret key."""
        return hmac.new(self._cfg.api_secret.encode(), payload.encode(), hashlib.sha256).hexdigest()

    def timestamp_ms(self) -> int:
        return int(time.time() * 1000) + self._time_offset_ms

    async def sync_time(self) -> int:
        """Hitung offset jam lokal vs server (dikompensasi setengah RTT). Return offset ms."""
        t0 = time.time() * 1000
        payload = await self._request("GET", f"{self._cfg.rest_base}/open/v1/common/time", unwrap=False)
        t1 = time.time() * 1000
        server_ts = int(payload.get("timestamp") or payload.get("data", {}).get("serverTime") or 0)
        if server_ts <= 0:
            raise ExchangeError(f"Response server time tidak valid: {payload}")
        self._time_offset_ms = int(server_ts - (t0 + t1) / 2)
        log_event(self._log, "time_sync", offset_ms=self._time_offset_ms, rtt_ms=round(t1 - t0, 1))
        return self._time_offset_ms

    # -- inti request ----------------------------------------------------- #
    def _observe_headers(self, headers: Any) -> None:
        for key, value in headers.items():
            lk = key.lower()
            if lk.startswith("x-mbx-used-weight") and lk.endswith("1m"):
                try:
                    self._limiter.observe_used_weight(int(value))
                except ValueError:
                    pass

    @staticmethod
    def _unwrap(payload: Any, http_status: int) -> Any:
        if isinstance(payload, dict) and "code" in payload:
            code: Any = payload.get("code")
            try:
                code_int = int(code)
            except (TypeError, ValueError):
                code_int = -1
            if code_int != 0:
                msg = payload.get("msg") or payload.get("message") or payload.get("messageDetail") or "unknown"
                raise ExchangeError(f"API error {code_int}: {msg}", code=code_int, http_status=http_status)
            return payload.get("data", payload)
        return payload

    async def _request(self, method: str, url: str, params: Optional[dict[str, Any]] = None,
                       signed: bool = False, weight: int = 1, retry: bool = True,
                       unwrap: bool = True) -> Any:
        """Kirim request dengan rate-limit, retry (khusus idempotent), dan penanganan error.

        ``retry=False`` dipakai untuk endpoint yang tidak idempotent (pembuatan order);
        di sana network error / 5XX dilempar apa adanya agar pemanggil merekonsiliasi.
        """
        if self._session is None:
            await self.start()
        assert self._session is not None
        base_params = {k: v for k, v in (params or {}).items() if v is not None}
        if signed and not self._cfg.has_credentials:
            raise ExchangeAuthError("API key/secret belum dikonfigurasi")
        max_attempts = (self._cfg.max_retries + 1) if retry else 1
        resynced = False
        last_error: Optional[Exception] = None

        for attempt in range(max_attempts):
            await self._limiter.acquire(weight)
            req = dict(base_params)
            headers: dict[str, str] = {}
            if signed:
                req["recvWindow"] = self._cfg.recv_window_ms
                req["timestamp"] = self.timestamp_ms()
            query = urlencode(req)
            if signed:
                query = f"{query}&signature={self.sign(query)}"
            if signed or self._cfg.api_key:
                headers["X-MBX-APIKEY"] = self._cfg.api_key
            body: Optional[str] = None
            if method == "POST":
                body = query
                headers["Content-Type"] = "application/x-www-form-urlencoded"
                target = URL(url, encoded=True)
            else:
                target = URL(f"{url}?{query}" if query else url, encoded=True)

            try:
                async with self._session.request(method, target, data=body, headers=headers) as resp:
                    self._observe_headers(resp.headers)
                    status = resp.status
                    text = await resp.text()
                    retry_after_hdr = resp.headers.get("Retry-After")
            except (aiohttp.ClientError, asyncio.TimeoutError) as exc:
                last_error = exc
                log_event(self._log, "http_network_error", logging.WARNING, method=method, url=url,
                          attempt=attempt, error=repr(exc))
                if attempt + 1 >= max_attempts:
                    raise NetworkError(f"{method} {url} gagal: {exc!r}") from exc
                await asyncio.sleep(backoff_delay(attempt))
                continue

            if status in (418, 429):
                retry_after = float(retry_after_hdr) if retry_after_hdr else min(60.0, 2.0 ** (attempt + 1))
                self._limiter.block_for(retry_after)
                log_event(self._log, "http_rate_limited", logging.ERROR, status=status, retry_after=retry_after, url=url)
                if attempt + 1 >= max_attempts:
                    raise RateLimitError(f"Rate limited ({status})", retry_after, status)
                continue
            if status == 403:
                raise ExchangeError(f"HTTP 403 (WAF limit) untuk {url}", http_status=403)
            if status >= 500:
                log_event(self._log, "http_server_error", logging.WARNING, status=status, url=url, attempt=attempt)
                if attempt + 1 >= max_attempts:
                    raise ExchangeError(f"HTTP {status} dari server: {text[:200]}", http_status=status)
                await asyncio.sleep(backoff_delay(attempt))
                continue

            try:
                payload = json.loads(text) if text else {}
            except json.JSONDecodeError as exc:
                raise ExchangeError(f"Response bukan JSON (HTTP {status}): {text[:200]}", http_status=status) from exc
            if status == 401:
                raise ExchangeAuthError(f"Auth ditolak: {text[:200]}", http_status=status)
            if 400 <= status < 500 and not (isinstance(payload, dict) and "code" in payload):
                raise ExchangeError(f"HTTP {status}: {text[:200]}", http_status=status)
            try:
                return self._unwrap(payload, status) if unwrap else payload
            except ExchangeError as exc:
                msg = str(exc).lower()
                if signed and not resynced and (exc.code == -1021 or "recvwindow" in msg or "timestamp" in msg):
                    resynced = True
                    await self.sync_time()
                    continue
                raise
        raise NetworkError(f"{method} {url} gagal setelah retry: {last_error!r}")

    # -- market data ------------------------------------------------------ #
    def _market_url(self, rules: SymbolRules, path: str) -> str:
        if rules.symbol_type not in SUPPORTED_SYMBOL_TYPES:
            raise ExchangeError(f"{rules.symbol}: symbolType {rules.symbol_type} belum didukung bot ini")
        return f"{self._cfg.market_base_type1}/api/v3/{path}"

    async def exchange_info(self) -> dict[str, SymbolRules]:
        """Ambil aturan semua simbol; key = simbol REST (``BTC_USDT``)."""
        data = await self._request("GET", f"{self._cfg.rest_base}/open/v1/common/symbols", weight=1)
        items = data.get("list", []) if isinstance(data, dict) else data
        rules: dict[str, SymbolRules] = {}
        for item in items:
            try:
                parsed = SymbolRules.from_api(item)
                rules[parsed.symbol] = parsed
            except (KeyError, PrecisionError) as exc:
                log_event(self._log, "symbol_parse_skip", logging.DEBUG, item=item.get("symbol"), error=str(exc))
        return rules

    async def klines(self, rules: SymbolRules, interval: str, limit: int = 500,
                     start_ms: Optional[int] = None, end_ms: Optional[int] = None) -> list[Candle]:
        """Ambil candle. Candle terakhir yang belum close ditandai ``closed=False``."""
        params = {"symbol": rules.market_symbol, "interval": interval, "limit": min(limit, 1000),
                  "startTime": start_ms, "endTime": end_ms}
        data = await self._request("GET", self._market_url(rules, "klines"), params=params, weight=2)
        rows = data if isinstance(data, list) else data.get("list", [])
        now = self.timestamp_ms()
        candles = [Candle.from_rest(row) for row in rows]
        for c in candles:
            c.closed = c.close_time < now
        return candles

    async def klines_history(self, rules: SymbolRules, interval: str, start_ms: int, end_ms: int) -> list[Candle]:
        """Unduh kline historis dengan paginasi (maks 1000 per request), hanya candle closed."""
        out: list[Candle] = []
        cursor = start_ms
        while cursor < end_ms:
            batch = await self.klines(rules, interval, limit=1000, start_ms=cursor, end_ms=end_ms)
            batch = [c for c in batch if c.closed]
            if not batch:
                break
            if out and batch[0].open_time <= out[-1].open_time:
                batch = [c for c in batch if c.open_time > out[-1].open_time]
                if not batch:
                    break
            out.extend(batch)
            cursor = batch[-1].close_time + 1
        return out

    async def depth(self, rules: SymbolRules, limit: int = 5) -> tuple[list[tuple[Decimal, Decimal]], list[tuple[Decimal, Decimal]]]:
        """Order book (bids, asks) sebagai list (price, qty) Decimal."""
        data = await self._request("GET", self._market_url(rules, "depth"),
                                   params={"symbol": rules.market_symbol, "limit": limit}, weight=1)
        bids = [(to_decimal(p), to_decimal(q)) for p, q in data.get("bids", [])]
        asks = [(to_decimal(p), to_decimal(q)) for p, q in data.get("asks", [])]
        return bids, asks

    # -- akun ------------------------------------------------------------- #
    async def account(self) -> dict[str, Balance]:
        """Saldo spot per aset."""
        data = await self._request("GET", f"{self._cfg.rest_base}/open/v1/account/spot", signed=True, weight=5)
        balances: dict[str, Balance] = {}
        for item in data.get("accountAssets", []):
            asset = str(item["asset"])
            balances[asset] = Balance(asset, to_decimal(item.get("free", "0")), to_decimal(item.get("locked", "0")))
        return balances

    async def account_commission(self) -> dict[str, Decimal]:
        """Komisi akun mentah (format Binance: 10 = 0.1%)."""
        data = await self._request("GET", f"{self._cfg.rest_base}/open/v1/account/spot", signed=True, weight=5)
        return {k: to_decimal(data.get(k, "0")) for k in ("makerCommission", "takerCommission",
                                                          "buyerCommission", "sellerCommission")}

    # -- order ------------------------------------------------------------ #
    async def create_order(self, rules: SymbolRules, side: str, order_type: str, client_id: str,
                           quantity: Optional[Decimal] = None, quote_qty: Optional[Decimal] = None,
                           price: Optional[Decimal] = None, stop_price: Optional[Decimal] = None,
                           time_in_force: Optional[str] = None, ref_price: Optional[Decimal] = None) -> OrderInfo:
        """Validasi presisi lalu kirim order. Tidak pernah di-retry otomatis.

        Raises:
            PrecisionError: order melanggar filter (tidak dikirim).
            OrderStateUnknownError: timeout/5XX — status order harus dicek via ``find_order``.
            ExchangeError: ditolak bursa (pasti tidak tereksekusi).
        """
        side = side.upper()
        order_type = order_type.upper()
        if quantity is not None:
            rules.validate(order_type, quantity, price, ref_price or price or Decimal(0), stop_price)
        elif quote_qty is None:
            raise PrecisionError("quantity atau quoteOrderQty wajib diisi")
        params: dict[str, Any] = {
            "symbol": rules.symbol,
            "side": ORDER_SIDE[side],
            "type": ORDER_TYPE[order_type],
            "quantity": fmt_decimal(quantity) if quantity is not None else None,
            "quoteOrderQty": fmt_decimal(quote_qty) if quote_qty is not None and quantity is None else None,
            "price": fmt_decimal(price) if price is not None else None,
            "stopPrice": fmt_decimal(stop_price) if stop_price is not None else None,
            "timeInForce": TIME_IN_FORCE[time_in_force] if time_in_force else None,
            "clientId": client_id,
        }
        log_event(self._log, "order_submit", **{k: v for k, v in params.items() if v is not None})
        try:
            data = await self._request("POST", f"{self._cfg.rest_base}/open/v1/orders", params=params,
                                       signed=True, weight=1, retry=False)
        except NetworkError as exc:
            raise OrderStateUnknownError(f"Order {client_id} status unknown (network): {exc}", client_id) from exc
        except ExchangeError as exc:
            if exc.http_status is not None and exc.http_status >= 500:
                raise OrderStateUnknownError(f"Order {client_id} status unknown (HTTP {exc.http_status})",
                                             client_id) from exc
            raise
        order = OrderInfo.from_api(data)
        log_event(self._log, "order_ack", order_id=order.order_id, client_id=client_id, status=order.status,
                  executed_qty=str(order.executed_qty), executed_quote=str(order.executed_quote_qty))
        return order

    async def get_order(self, order_id: str) -> OrderInfo:
        data = await self._request("GET", f"{self._cfg.rest_base}/open/v1/orders/detail",
                                   params={"orderId": order_id}, signed=True, weight=1)
        return OrderInfo.from_api(data)

    async def list_orders(self, rules: SymbolRules, kind: int = -1, limit: int = 50) -> list[OrderInfo]:
        """``kind``: 1 = open, 2 = history, -1 = semua."""
        data = await self._request("GET", f"{self._cfg.rest_base}/open/v1/orders",
                                   params={"symbol": rules.symbol, "type": kind, "limit": limit},
                                   signed=True, weight=5)
        items = data.get("list", []) if isinstance(data, dict) else data
        return [OrderInfo.from_api(item) for item in items]

    async def open_orders(self, rules: SymbolRules) -> list[OrderInfo]:
        return [o for o in await self.list_orders(rules, kind=1, limit=100) if o.is_open]

    async def find_order(self, rules: SymbolRules, client_id: str) -> Optional[OrderInfo]:
        """Cari order berdasarkan ``clientId`` (dipakai setelah ``OrderStateUnknownError``)."""
        for order in await self.list_orders(rules, kind=-1, limit=100):
            if order.client_id == client_id:
                return order
        return None

    async def cancel_order(self, order_id: Optional[str] = None, client_id: Optional[str] = None) -> OrderInfo:
        if not order_id and not client_id:
            raise ValueError("order_id atau client_id wajib")
        data = await self._request("POST", f"{self._cfg.rest_base}/open/v1/orders/cancel",
                                   params={"orderId": order_id, "clientId": client_id if not order_id else None},
                                   signed=True, weight=1, retry=True)
        return OrderInfo.from_api(data)

    async def my_trades(self, rules: SymbolRules, order_id: Optional[str] = None, limit: int = 100) -> list[dict[str, Any]]:
        data = await self._request("GET", f"{self._cfg.rest_base}/open/v1/orders/trades",
                                   params={"symbol": rules.symbol, "orderId": order_id, "limit": limit},
                                   signed=True, weight=5)
        return data.get("list", []) if isinstance(data, dict) else data

    async def create_listen_token(self, validity_ms: int = 23 * 3600 * 1000) -> tuple[str, int]:
        """Token untuk user data stream WebSocket API. Return (token, expirationTime ms)."""
        data = await self._request("POST", f"{self._cfg.rest_base}/open/v1/user-listen-token",
                                   params={"validity": validity_ms}, signed=True, weight=1)
        return str(data["token"]), int(data["expirationTime"])


# --------------------------------------------------------------------------- #
# WebSocket: market stream
# --------------------------------------------------------------------------- #
KlineHandler = Callable[[str, Candle], Awaitable[None]]
ConnectHandler = Callable[[], Awaitable[None]]


class MarketStream:
    """Combined kline stream dengan reconnect exponential backoff + watchdog data basi.

    * Server ping tiap 3 menit; library ``websockets`` membalas pong otomatis.
    * Umur koneksi maks 24 jam -> diputus proaktif di ``max_lifetime_s``.
    * Tidak ada pesan selama ``stale_timeout_s`` -> dianggap mati, reconnect.
    * ``on_connect`` dipanggil setiap (re)connect untuk backfill gap via REST.
    """

    def __init__(self, cfg: ExchangeConfig, rules: Sequence[SymbolRules], interval: str,
                 on_kline: KlineHandler, on_connect: Optional[ConnectHandler] = None,
                 stale_timeout_s: float = 90.0, max_lifetime_s: float = 23 * 3600) -> None:
        if any(r.symbol_type not in SUPPORTED_SYMBOL_TYPES for r in rules):
            raise ExchangeError("MarketStream hanya mendukung symbolType 1")
        self._cfg = cfg
        self._by_market = {r.market_symbol.upper(): r.symbol for r in rules}
        streams = "/".join(f"{r.market_symbol.lower()}@kline_{interval}" for r in rules)
        self.url = f"{cfg.ws_market_type1}/stream?streams={streams}"
        self._on_kline = on_kline
        self._on_connect = on_connect
        self._stale = stale_timeout_s
        self._max_lifetime = max_lifetime_s
        self._log = get_logger("ws.market")
        self.last_message_at = 0.0

    async def run(self, stop: asyncio.Event) -> None:
        attempt = 0
        while not stop.is_set():
            started = time.monotonic()
            try:
                async with ws_connect(self.url, ping_interval=20, ping_timeout=20, close_timeout=5,
                                      max_size=2 ** 22, open_timeout=15) as ws:
                    log_event(self._log, "ws_connected", url=self.url)
                    if self._on_connect:
                        await self._on_connect()
                    while not stop.is_set():
                        if time.monotonic() - started > self._max_lifetime:
                            log_event(self._log, "ws_lifetime_rotate")
                            break
                        raw = await asyncio.wait_for(ws.recv(), timeout=self._stale)
                        self.last_message_at = time.monotonic()
                        await self._dispatch(raw)
            except asyncio.TimeoutError:
                log_event(self._log, "ws_stale", logging.WARNING, timeout_s=self._stale)
            except (ConnectionClosed, InvalidHandshake, InvalidURI, OSError) as exc:
                log_event(self._log, "ws_disconnected", logging.WARNING, error=repr(exc))
            except asyncio.CancelledError:
                raise
            except Exception as exc:  # noqa: BLE001 - jangan biarkan stream mati karena bug parsing
                log_event(self._log, "ws_unexpected_error", logging.ERROR, error=repr(exc))
            if stop.is_set():
                break
            attempt = 0 if time.monotonic() - started > 60 else attempt + 1
            delay = backoff_delay(attempt, base=1.0, cap=60.0) + 0.5
            log_event(self._log, "ws_reconnect_wait", attempt=attempt, delay_s=round(delay, 2))
            try:
                await asyncio.wait_for(stop.wait(), timeout=delay)
            except asyncio.TimeoutError:
                pass

    async def _dispatch(self, raw: str | bytes) -> None:
        try:
            msg = json.loads(raw)
        except json.JSONDecodeError:
            log_event(self._log, "ws_bad_json", logging.WARNING, raw=str(raw)[:200])
            return
        data = msg.get("data", msg) if isinstance(msg, dict) else None
        if not isinstance(data, dict) or data.get("e") != "kline":
            return
        symbol = self._by_market.get(str(data.get("s", "")).upper())
        if symbol is None:
            return
        try:
            candle = Candle.from_ws(data["k"])
        except (KeyError, TypeError, ValueError) as exc:
            log_event(self._log, "ws_bad_kline", logging.WARNING, error=repr(exc))
            return
        try:
            await self._on_kline(symbol, candle)
        except Exception as exc:  # noqa: BLE001 - handler error tidak boleh memutus stream
            log_event(self._log, "kline_handler_error", logging.ERROR, symbol=symbol, error=repr(exc))


# --------------------------------------------------------------------------- #
# WebSocket API: user data stream (listenToken)
# --------------------------------------------------------------------------- #
UserEventHandler = Callable[[dict[str, Any]], Awaitable[None]]


class UserDataStream:
    """User data stream via WebSocket API ``userDataStream.subscribe.listenToken``.

    Token tidak auto-renew; stream dibuka ulang dengan token baru sebelum kedaluwarsa.
    Event ``executionReport`` / ``outboundAccountPosition`` diteruskan ke handler.
    REST reconciliation tetap menjadi sumber kebenaran — stream ini mempercepat reaksi.
    """

    def __init__(self, client: TokocryptoClient, on_event: UserEventHandler,
                 renew_margin_s: float = 600.0, stale_timeout_s: float = 300.0) -> None:
        self._client = client
        self._on_event = on_event
        self._renew_margin = renew_margin_s
        self._stale = stale_timeout_s
        self._log = get_logger("ws.user")

    async def run(self, stop: asyncio.Event) -> None:
        attempt = 0
        while not stop.is_set():
            started = time.monotonic()
            try:
                token, expires_ms = await self._client.create_listen_token()
                async with ws_connect(self._client.config.ws_api_url, ping_interval=20, ping_timeout=20,
                                      open_timeout=15) as ws:
                    request_id = f"sub-{int(time.time() * 1000)}"
                    await ws.send(json.dumps({"id": request_id, "method": "userDataStream.subscribe.listenToken",
                                              "params": {"listenToken": token}}))
                    log_event(self._log, "user_stream_subscribed", expires_ms=expires_ms)
                    while not stop.is_set():
                        if self._client.timestamp_ms() > expires_ms - self._renew_margin * 1000:
                            log_event(self._log, "user_stream_renew")
                            break
                        raw = await asyncio.wait_for(ws.recv(), timeout=self._stale)
                        msg = json.loads(raw)
                        if isinstance(msg, dict) and msg.get("error"):
                            raise ExchangeError(f"WS API error: {msg['error']}")
                        event = msg.get("event") if isinstance(msg, dict) else None
                        if not isinstance(event, dict):
                            continue
                        if event.get("e") == "eventStreamTerminated":
                            log_event(self._log, "user_stream_terminated", logging.WARNING)
                            break
                        try:
                            await self._on_event(event)
                        except Exception as exc:  # noqa: BLE001
                            log_event(self._log, "user_event_handler_error", logging.ERROR, error=repr(exc))
            except asyncio.TimeoutError:
                log_event(self._log, "user_stream_idle_reconnect", logging.INFO)
            except (ConnectionClosed, InvalidHandshake, OSError, ExchangeError, json.JSONDecodeError) as exc:
                log_event(self._log, "user_stream_error", logging.WARNING, error=repr(exc))
            except asyncio.CancelledError:
                raise
            if stop.is_set():
                break
            attempt = 0 if time.monotonic() - started > 60 else attempt + 1
            try:
                await asyncio.wait_for(stop.wait(), timeout=backoff_delay(attempt, base=1.0, cap=60.0) + 0.5)
            except asyncio.TimeoutError:
                pass
