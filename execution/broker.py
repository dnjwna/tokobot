"""Broker: lapisan eksekusi PAPER (simulasi saldo lokal) dan LIVE (Tokocrypto).

Keduanya mengekspos antarmuka yang sama sehingga orchestrator tidak perlu tahu
mode yang berjalan. Semua qty dikirim sebagai ``Decimal`` yang sudah dibulatkan
oleh ``SymbolRules``.
"""

from __future__ import annotations

import asyncio
import logging
from abc import ABC, abstractmethod
from decimal import Decimal
from typing import Any, Optional

from config import CostConfig, RiskConfig
from core.models import FillResult
from exchange.tokocrypto_client import (Balance, ExchangeError, OrderInfo, OrderStateUnknownError, PrecisionError,
                                        SymbolRules, TokocryptoClient, to_decimal)
from utils.logger import get_logger, log_event

CLIENT_PREFIX = "tb"


class EntryStateUnknownError(ExchangeError):
    """Entry mungkin tereksekusi namun belum terkonfirmasi; ditangani rekonsiliasi."""

    def __init__(self, message: str, client_id: str) -> None:
        super().__init__(message)
        self.client_id = client_id


class Broker(ABC):
    """Antarmuka eksekusi."""

    name: str = "base"
    supports_exchange_stop: bool = False

    async def start(self) -> None:
        return None

    async def close(self) -> None:
        return None

    @abstractmethod
    async def balances(self) -> dict[str, Balance]: ...

    @abstractmethod
    async def buy(self, rules: SymbolRules, qty: Decimal, ref_price: float, client_id: str) -> FillResult: ...

    @abstractmethod
    async def sell(self, rules: SymbolRules, qty: Decimal, ref_price: float, client_id: str) -> FillResult: ...

    async def place_stop(self, rules: SymbolRules, qty: Decimal, stop_price: float, ref_price: float,
                         client_id: str) -> Optional[OrderInfo]:
        return None

    async def cancel(self, rules: SymbolRules, order_id: str) -> Optional[OrderInfo]:
        return None

    async def get_order(self, rules: SymbolRules, order_id: str) -> Optional[OrderInfo]:
        return None

    async def find_order(self, rules: SymbolRules, client_id: str) -> Optional[OrderInfo]:
        return None

    async def open_orders(self, rules: SymbolRules) -> list[OrderInfo]:
        return []

    def export_state(self) -> dict[str, Any]:
        return {}

    async def free_quote(self, quote_asset: str) -> float:
        bal = (await self.balances()).get(quote_asset)
        return float(bal.free) if bal else 0.0


# --------------------------------------------------------------------------- #
# PAPER
# --------------------------------------------------------------------------- #
class PaperBroker(Broker):
    """Simulasi real-time: harga dari stream live, saldo & biaya disimulasikan lokal."""

    name = "paper"
    supports_exchange_stop = False

    def __init__(self, costs: CostConfig, quote_asset: str, initial_balance: float,
                 saved: Optional[dict[str, Any]] = None) -> None:
        self._costs = costs
        self._quote = quote_asset
        self._free: dict[str, float] = {quote_asset: float(initial_balance)}
        if saved and saved.get("balances"):
            self._free = {k: float(v) for k, v in saved["balances"].items()}
        self._log = get_logger("broker.paper")
        self._seq = 0

    async def balances(self) -> dict[str, Balance]:
        return {a: Balance(a, to_decimal(v), Decimal(0)) for a, v in self._free.items()}

    def export_state(self) -> dict[str, Any]:
        return {"balances": dict(self._free)}

    async def buy(self, rules: SymbolRules, qty: Decimal, ref_price: float, client_id: str) -> FillResult:
        rules.validate("MARKET", qty, None, to_decimal(ref_price))
        q = float(qty)
        px = ref_price * (1.0 + self._costs.slippage_pct)
        gross = q * px
        fee = gross * (self._costs.taker_fee + self._costs.tax_rate_buy)
        if gross + fee > self._free.get(self._quote, 0.0) + 1e-9:
            raise ExchangeError(f"PAPER: saldo {self._quote} tidak cukup ({gross + fee:.4f})")
        self._free[self._quote] -= gross + fee
        self._free[rules.base_asset] = self._free.get(rules.base_asset, 0.0) + q
        self._seq += 1
        log_event(self._log, "paper_fill", side="BUY", symbol=rules.symbol, qty=q, price=round(px, 8), fee=round(fee, 6))
        return FillResult(rules.symbol, "BUY", q, q, px, gross + fee, fee, f"paper-{self._seq}", client_id)

    async def sell(self, rules: SymbolRules, qty: Decimal, ref_price: float, client_id: str) -> FillResult:
        q = min(float(qty), self._free.get(rules.base_asset, 0.0))
        if q <= 0:
            raise ExchangeError(f"PAPER: saldo {rules.base_asset} kosong")
        px = ref_price * (1.0 - self._costs.slippage_pct)
        gross = q * px
        fee = gross * (self._costs.taker_fee + self._costs.tax_rate_sell)
        self._free[rules.base_asset] -= q
        self._free[self._quote] = self._free.get(self._quote, 0.0) + gross - fee
        self._seq += 1
        log_event(self._log, "paper_fill", side="SELL", symbol=rules.symbol, qty=q, price=round(px, 8), fee=round(fee, 6))
        return FillResult(rules.symbol, "SELL", float(qty), q, px, gross - fee, fee, f"paper-{self._seq}", client_id)


# --------------------------------------------------------------------------- #
# LIVE
# --------------------------------------------------------------------------- #
class LiveBroker(Broker):
    """Eksekusi riil.

    * Entry: LIMIT IOC "marketable" di best ask x (1 + max_entry_slippage) -> slippage terbatasi.
    * Exit: MARKET (kepastian keluar > harga).
    * Proteksi: STOP_LOSS_LIMIT GTC di bursa (tetap aktif walau bot/VPS mati).
    * Qty bersih & biaya diukur dari selisih saldo sebelum/sesudah, sehingga tidak
      bergantung pada aset komisi (base/quote/token diskon).
    Pemanggil WAJIB menyerialisasi order (satu lock) agar selisih saldo valid.
    """

    name = "live"
    supports_exchange_stop = True

    def __init__(self, client: TokocryptoClient, costs: CostConfig, risk: RiskConfig) -> None:
        self._client = client
        self._costs = costs
        self._risk = risk
        self._log = get_logger("broker.live")

    async def balances(self) -> dict[str, Balance]:
        return await self._client.account()

    async def _await_final(self, rules: SymbolRules, order: OrderInfo, timeout_s: float = 15.0) -> OrderInfo:
        waited, delay = 0.0, 0.5
        while not order.is_final and waited < timeout_s:
            await asyncio.sleep(delay)
            waited += delay
            delay = min(delay * 2, 3.0)
            order = await self._client.get_order(order.order_id)
        return order

    async def _resolve_unknown(self, rules: SymbolRules, client_id: str, attempts: int = 5) -> Optional[OrderInfo]:
        for attempt in range(attempts):
            await asyncio.sleep(min(2 ** attempt, 8))
            try:
                found = await self._client.find_order(rules, client_id)
            except ExchangeError as exc:
                log_event(self._log, "resolve_unknown_error", logging.WARNING, client_id=client_id, error=str(exc))
                continue
            if found is not None:
                return found
        return None

    async def _balance_pair(self, rules: SymbolRules) -> tuple[Decimal, Decimal, Decimal]:
        """Return (base_total, quote_total, base_free)."""
        bal = await self._client.account()
        base = bal.get(rules.base_asset)
        quote = bal.get(rules.quote_asset)
        return ((base.total if base else Decimal(0)), (quote.total if quote else Decimal(0)),
                (base.free if base else Decimal(0)))

    async def buy(self, rules: SymbolRules, qty: Decimal, ref_price: float, client_id: str) -> FillResult:
        base_before, quote_before, _ = await self._balance_pair(rules)
        _, asks = await self._client.depth(rules, limit=5)
        if not asks:
            raise ExchangeError(f"{rules.symbol}: order book kosong")
        best_ask = asks[0][0]
        limit_price = rules.round_price(best_ask * (Decimal(1) + to_decimal(self._risk.max_entry_slippage)))
        try:
            order = await self._client.create_order(rules, "BUY", "LIMIT", client_id, quantity=qty,
                                                    price=limit_price, time_in_force="IOC", ref_price=best_ask)
        except OrderStateUnknownError as exc:
            found = await self._resolve_unknown(rules, client_id)
            if found is None:
                raise EntryStateUnknownError(str(exc), client_id) from exc
            order = found
        order = await self._await_final(rules, order)
        if not order.is_final:
            try:
                order = await self._client.cancel_order(order_id=order.order_id)
            except ExchangeError:
                order = await self._client.get_order(order.order_id)
        if order.executed_qty <= 0:
            return FillResult(rules.symbol, "BUY", float(qty), 0.0, 0.0, 0.0, 0.0, order.order_id, client_id, order.status)

        base_after, quote_after, _ = await self._balance_pair(rules)
        if base_after - base_before <= 0:
            await asyncio.sleep(1.0)
            base_after, quote_after, _ = await self._balance_pair(rules)
        net_base = min(max(base_after - base_before, Decimal(0)), order.executed_qty)
        if net_base <= 0:
            net_base = order.executed_qty * (Decimal(1) - to_decimal(self._costs.taker_fee))
        spent = quote_before - quote_after
        if spent <= 0:
            spent = order.executed_quote_qty * (Decimal(1) + to_decimal(self._costs.taker_fee + self._costs.tax_rate_buy))
        avg = order.avg_price
        fees = float(spent - order.executed_quote_qty) + float(order.executed_qty - net_base) * float(avg)
        log_event(self._log, "live_fill", side="BUY", symbol=rules.symbol, executed=str(order.executed_qty),
                  net_base=str(net_base), avg=str(avg), spent=str(spent), status=order.status)
        return FillResult(rules.symbol, "BUY", float(qty), float(net_base), float(avg), float(spent),
                          max(fees, 0.0), order.order_id, client_id, order.status)

    async def sell(self, rules: SymbolRules, qty: Decimal, ref_price: float, client_id: str) -> FillResult:
        _, quote_before, base_free = await self._balance_pair(rules)
        qty = rules.floor_qty(min(qty, base_free), market=True)
        try:
            order = await self._client.create_order(rules, "SELL", "MARKET", client_id, quantity=qty,
                                                    ref_price=to_decimal(ref_price))
        except OrderStateUnknownError as exc:
            found = await self._resolve_unknown(rules, client_id)
            if found is None:
                raise ExchangeError(f"SELL {rules.symbol} status tidak diketahui: {exc}") from exc
            order = found
        order = await self._await_final(rules, order)
        if order.executed_qty <= 0:
            raise ExchangeError(f"SELL {rules.symbol} tidak tereksekusi (status {order.status})")
        _, quote_after, _ = await self._balance_pair(rules)
        received = quote_after - quote_before
        if received <= 0:
            received = order.executed_quote_qty * (Decimal(1) - to_decimal(self._costs.taker_fee + self._costs.tax_rate_sell))
        fees = float(order.executed_quote_qty - received)
        log_event(self._log, "live_fill", side="SELL", symbol=rules.symbol, executed=str(order.executed_qty),
                  avg=str(order.avg_price), received=str(received), status=order.status)
        return FillResult(rules.symbol, "SELL", float(qty), float(order.executed_qty), float(order.avg_price),
                          float(received), max(fees, 0.0), order.order_id, client_id, order.status)

    async def place_stop(self, rules: SymbolRules, qty: Decimal, stop_price: float, ref_price: float,
                         client_id: str) -> Optional[OrderInfo]:
        if not rules.supports("STOP_LOSS_LIMIT"):
            log_event(self._log, "stop_type_unsupported", logging.WARNING, symbol=rules.symbol)
            return None
        stop = rules.round_price(stop_price)
        if float(stop) >= ref_price:
            raise PrecisionError(f"{rules.symbol}: stop {stop} >= harga {ref_price}, harus exit market")
        limit = rules.round_price(stop * (Decimal(1) - to_decimal(self._risk.stop_limit_buffer)))
        qty = rules.floor_qty(qty)
        return await self._client.create_order(rules, "SELL", "STOP_LOSS_LIMIT", client_id, quantity=qty,
                                               price=limit, stop_price=stop, time_in_force="GTC",
                                               ref_price=to_decimal(ref_price))

    async def cancel(self, rules: SymbolRules, order_id: str) -> Optional[OrderInfo]:
        return await self._client.cancel_order(order_id=order_id)

    async def get_order(self, rules: SymbolRules, order_id: str) -> Optional[OrderInfo]:
        return await self._client.get_order(order_id)

    async def find_order(self, rules: SymbolRules, client_id: str) -> Optional[OrderInfo]:
        return await self._client.find_order(rules, client_id)

    async def open_orders(self, rules: SymbolRules) -> list[OrderInfo]:
        return await self._client.open_orders(rules)
