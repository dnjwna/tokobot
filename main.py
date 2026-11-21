"""Orchestrator TokoBot untuk mode PAPER dan LIVE (BACKTEST didelegasikan ke backtest.engine).

Contoh:
    python main.py --mode BACKTEST
    python main.py --mode PAPER
    python main.py --mode LIVE            # butuh LIVE_CONFIRM + API key
    python main.py --status               # tampilkan state tersimpan
    python main.py --reset-halt           # buka hard halt setelah evaluasi manual
"""

from __future__ import annotations

import argparse
import asyncio
import json
import logging
import signal
import sys
import time
from decimal import Decimal
from typing import Any, Optional

from backtest.engine import run_backtest_cli
from config import AppConfig, ConfigError, Mode
from core.models import Candle, FillResult, Position, TradeRecord
from exchange.tokocrypto_client import (SUPPORTED_SYMBOL_TYPES, ExchangeError, MarketStream, OrderInfo,
                                        PrecisionError, SymbolRules, TokocryptoClient, UserDataStream)
from execution.broker import CLIENT_PREFIX, Broker, EntryStateUnknownError, LiveBroker, PaperBroker
from execution.state_store import StateStore
from notify.telegram import Notifier, build_notifier
from risk.risk_manager import RiskEvent, RiskManager, RiskState
from strategy.momentum_regime import Action, Decision, MomentumRegimeStrategy
from utils.logger import get_logger, log_event, setup_logging

INTERVAL_MS = {"1m": 60_000, "3m": 180_000, "5m": 300_000, "15m": 900_000, "30m": 1_800_000, "1h": 3_600_000,
               "2h": 7_200_000, "4h": 14_400_000, "6h": 21_600_000, "8h": 28_800_000, "12h": 43_200_000,
               "1d": 86_400_000}
PENDING_ENTRY_TTL_MS = 15 * 60 * 1000


def make_client_id(kind: str, symbol: str, stamp: int) -> str:
    """clientId pendek & deterministik. Entry memakai waktu bar -> idempoten per bar."""
    return f"{CLIENT_PREFIX}{kind}{symbol.replace('_', '')[:10]}{stamp % 10 ** 10}"


class TradingBot:
    """Event-driven trading loop: kline WS -> strategi -> risk -> broker -> state/notify."""

    def __init__(self, cfg: AppConfig) -> None:
        if cfg.runtime.mode not in (Mode.PAPER, Mode.LIVE):
            raise ConfigError("TradingBot hanya untuk mode PAPER/LIVE")
        self.cfg = cfg
        self.mode = cfg.runtime.mode
        self.log = get_logger("bot")
        self.notifier: Notifier = build_notifier(cfg.telegram, prefix=f"TokoBot-{self.mode.value}")
        self.client = TokocryptoClient(cfg.exchange)
        self.strategy = MomentumRegimeStrategy(cfg.strategy, cfg.costs)
        sd = cfg.runtime.state_dir
        self.store = StateStore(sd / f"state_{self.mode.value.lower()}.json",
                                sd / f"trades_{self.mode.value.lower()}.jsonl")
        self.interval_ms = INTERVAL_MS[cfg.strategy.timeframe]
        self.rules: dict[str, SymbolRules] = {}
        self.candles: dict[str, list[Candle]] = {}
        self.last_price: dict[str, tuple[float, float]] = {}
        self.positions: dict[str, Position] = {}
        self.pending_entries: dict[str, dict[str, Any]] = {}
        self.last_signal_bar: dict[str, int] = {}
        self.legs: dict[str, list[dict[str, Any]]] = {}
        self.busy: set[str] = set()
        self.warned_unmanaged: set[str] = set()
        self.kill_file_notified = False
        self.order_lock = asyncio.Lock()
        self.stop_event = asyncio.Event()
        self.reconcile_now = asyncio.Event()
        self.broker: Broker
        self.risk: RiskManager

    # ================================================================== #
    # Lifecycle
    # ================================================================== #
    async def setup(self) -> None:
        await self.notifier.start()
        await self.client.start()
        await self.client.sync_time()
        all_rules = await self.client.exchange_info()
        for sym in self.cfg.runtime.symbols:
            rules = all_rules.get(sym)
            if rules is None:
                raise ConfigError(f"Simbol {sym} tidak ada di Tokocrypto")
            if rules.symbol_type not in SUPPORTED_SYMBOL_TYPES:
                raise ConfigError(f"{sym} symbolType {rules.symbol_type} belum didukung (hanya type 1)")
            if not rules.spot_enabled:
                raise ConfigError(f"{sym}: spot trading nonaktif")
            self.rules[sym] = rules
            log_event(self.log, "symbol_rules", symbol=sym, tick=str(rules.tick_size), step=str(rules.step_size),
                      min_notional=str(rules.min_notional), order_types=",".join(sorted(rules.order_types)))

        saved = self.store.load()
        self.positions = {k: Position.from_dict(v) for k, v in saved.get("positions", {}).items()}
        self.pending_entries = saved.get("pending_entries", {})
        self.last_signal_bar = {k: int(v) for k, v in saved.get("last_signal_bar", {}).items()}
        self.legs = saved.get("legs", {})
        self.risk = RiskManager(self.cfg.risk, self.cfg.costs, RiskState.from_dict(saved.get("risk", {})),
                                kill_file=self.cfg.runtime.kill_file)
        if self.mode is Mode.PAPER:
            self.broker = PaperBroker(self.cfg.costs, self.cfg.runtime.quote_asset,
                                      self.cfg.runtime.paper_initial_balance, saved.get("broker"))
        else:
            self.broker = LiveBroker(self.client, self.cfg.costs, self.cfg.risk)
            await self._check_live_account()

        for sym, rules in self.rules.items():
            await self._load_history(sym, rules)
        if self.mode is Mode.LIVE:
            await self.reconcile()
        equity = await self.compute_equity()
        await self._handle_risk_events(self.risk.update_equity(equity, self.client.timestamp_ms()))
        self.save_state()
        summary = (f"Start {self.mode.value} | simbol={','.join(self.rules)} tf={self.cfg.strategy.timeframe} "
                   f"ekuitas={equity:.2f} posisi={list(self.positions)} risk/trade={self.cfg.risk.risk_per_trade:.2%}")
        log_event(self.log, "startup", equity=round(equity, 4), positions=list(self.positions))
        self.notifier.send(summary, "INFO")

    async def _check_live_account(self) -> None:
        commission = await self.client.account_commission()
        taker_bps = commission.get("takerCommission", Decimal(0))
        actual = float(taker_bps) / 10_000 if taker_bps > 1 else float(taker_bps)
        if actual > self.cfg.costs.taker_fee + 1e-9:
            msg = f"Fee taker akun ({actual:.4%}) > FEE_TAKER config ({self.cfg.costs.taker_fee:.4%}). Perbarui config!"
            log_event(self.log, "fee_mismatch", logging.WARNING, actual=actual, configured=self.cfg.costs.taker_fee)
            self.notifier.send(msg, "WARN")

    async def _load_history(self, sym: str, rules: SymbolRules) -> None:
        bars = await self.client.klines(rules, self.cfg.strategy.timeframe, limit=self.cfg.strategy.history_bars)
        closed = [c for c in bars if c.closed]
        if len(closed) < self.strategy.warmup_bars:
            raise ExchangeError(f"{sym}: histori {len(closed)} bar < warmup {self.strategy.warmup_bars}")
        self.candles[sym] = closed
        self.last_price[sym] = (bars[-1].close, time.monotonic())
        log_event(self.log, "history_loaded", symbol=sym, bars=len(closed))

    async def run(self) -> None:
        await self.setup()
        loop = asyncio.get_running_loop()
        for sig in (signal.SIGINT, signal.SIGTERM):
            try:
                loop.add_signal_handler(sig, self.stop_event.set)
            except NotImplementedError:  # Windows
                pass
        stream = MarketStream(self.cfg.exchange, list(self.rules.values()), self.cfg.strategy.timeframe,
                              on_kline=self.on_kline, on_connect=self.on_stream_connect,
                              stale_timeout_s=self.cfg.runtime.ws_stale_timeout_s)
        tasks = [
            asyncio.create_task(stream.run(self.stop_event), name="market-stream"),
            asyncio.create_task(self._risk_loop(), name="risk-loop"),
            asyncio.create_task(self._time_sync_loop(), name="time-sync"),
            asyncio.create_task(self._heartbeat_loop(), name="heartbeat"),
        ]
        if self.mode is Mode.LIVE:
            tasks.append(asyncio.create_task(self._reconcile_loop(), name="reconcile-loop"))
            if self.cfg.runtime.use_user_stream:
                user = UserDataStream(self.client, self.on_user_event)
                tasks.append(asyncio.create_task(user.run(self.stop_event), name="user-stream"))
        stop_waiter = asyncio.create_task(self.stop_event.wait(), name="stop-waiter")
        try:
            done, _ = await asyncio.wait([stop_waiter, *tasks], return_when=asyncio.FIRST_COMPLETED)
            for task in done:
                if task is not stop_waiter and not task.cancelled() and task.exception() is not None:
                    raise task.exception()  # type: ignore[misc]
        except Exception as exc:
            log_event(self.log, "fatal_task_error", logging.CRITICAL, error=repr(exc))
            self.notifier.send(f"Error fatal, bot berhenti: {exc!r}", "CRITICAL")
            raise
        finally:
            self.stop_event.set()
            stop_waiter.cancel()
            for task in tasks:
                task.cancel()
            await asyncio.gather(*tasks, return_exceptions=True)
            await self.shutdown()

    async def shutdown(self) -> None:
        self.save_state()
        protected = {s: bool(p.stop_order_id) for s, p in self.positions.items()}
        msg = f"Bot berhenti. Posisi terbuka: {protected or 'tidak ada'} (True = stop di bursa tetap aktif)"
        log_event(self.log, "shutdown", positions=protected)
        self.notifier.send(msg, "WARN" if self.positions else "INFO")
        await self.notifier.close()
        await self.client.close()

    def save_state(self) -> None:
        self.store.save({
            "mode": self.mode.value,
            "updated_ms": self.client.timestamp_ms(),
            "positions": {k: v.to_dict() for k, v in self.positions.items()},
            "pending_entries": self.pending_entries,
            "last_signal_bar": self.last_signal_bar,
            "legs": self.legs,
            "risk": self.risk.state.to_dict(),
            "broker": self.broker.export_state(),
        })

    # ================================================================== #
    # Market data
    # ================================================================== #
    async def on_stream_connect(self) -> None:
        """Backfill bar yang terlewat selama disconnect."""
        for sym, rules in self.rules.items():
            try:
                bars = await self.client.klines(rules, self.cfg.strategy.timeframe, limit=10)
            except ExchangeError as exc:
                log_event(self.log, "backfill_failed", logging.WARNING, symbol=sym, error=str(exc))
                continue
            buf = self.candles.get(sym, [])
            new = [c for c in bars if c.closed and (not buf or c.open_time > buf[-1].open_time)]
            if new:
                buf.extend(new)
                del buf[:-self.cfg.strategy.history_bars]
                log_event(self.log, "backfill", symbol=sym, bars=len(new))
                await self.on_bar_close(sym)

    async def on_kline(self, symbol: str, candle: Candle) -> None:
        self.last_price[symbol] = (candle.close, time.monotonic())
        await self.check_intrabar(symbol, candle.close)
        if not candle.closed:
            return
        buf = self.candles[symbol]
        if buf and candle.open_time <= buf[-1].open_time:
            return
        if buf and candle.open_time > buf[-1].open_time + self.interval_ms:
            log_event(self.log, "bar_gap_detected", logging.WARNING, symbol=symbol)
            await self.on_stream_connect()
            return
        buf.append(candle)
        del buf[:-self.cfg.strategy.history_bars]
        await self.on_bar_close(symbol)

    def fresh_price(self, symbol: str) -> Optional[float]:
        price, ts = self.last_price.get(symbol, (0.0, 0.0))
        if price <= 0 or time.monotonic() - ts > self.cfg.runtime.max_price_age_s:
            return None
        return price

    # ================================================================== #
    # Strategy hooks
    # ================================================================== #
    async def on_bar_close(self, symbol: str) -> None:
        candles = self.candles[symbol]
        features = self.strategy.compute_features(candles)
        i = len(candles) - 1
        bar = candles[-1]
        pos = self.positions.get(symbol)
        if pos is not None:
            pos.highest_high = max(pos.highest_high, bar.high)
            new_stop = self.strategy.trailing_stop(pos, features, i)
            if bar.close <= new_stop:
                await self.exit_position(symbol, bar.close, "trailing_close")
            elif new_stop > pos.stop_price:
                old = pos.stop_price
                pos.stop_price = new_stop
                log_event(self.log, "trail_update", symbol=symbol, old=round(old, 8), new=round(new_stop, 8))
                await self._sync_exchange_stop(symbol)
            self.save_state()
            return
        if symbol in self.pending_entries or self.last_signal_bar.get(symbol) == bar.open_time:
            return
        decision = self.strategy.signal_at(features, i, symbol)
        log_event(self.log, "decision", **decision.to_log())
        if decision.action is Action.ENTER_LONG:
            self.last_signal_bar[symbol] = bar.open_time
            self.save_state()
            await self.enter(symbol, decision)

    async def enter(self, symbol: str, decision: Decision) -> None:
        rules = self.rules[symbol]
        async with self.order_lock:
            now = self.client.timestamp_ms()
            ok, reason = self.risk.can_open(symbol, set(self.positions) | set(self.pending_entries), now)
            if not ok:
                log_event(self.log, "entry_blocked", symbol=symbol, reason=reason)
                return
            price = self.fresh_price(symbol)
            if price is None:
                log_event(self.log, "entry_blocked", logging.WARNING, symbol=symbol, reason="stale_price")
                return
            stop_ref, _ = self.strategy.initial_levels(price, decision.atr)
            equity = await self.compute_equity()
            in_positions = sum(p.qty * p.entry_price for p in self.positions.values())
            cash_cap = max(self.risk.effective_equity(equity) - in_positions, 0.0)
            available = min(await self.broker.free_quote(self.cfg.runtime.quote_asset), cash_cap)
            sizing = self.risk.size_position(equity, available, price, stop_ref, rules)
            if not sizing.ok:
                log_event(self.log, "entry_blocked", symbol=symbol, reason=sizing.reason)
                return
            client_id = make_client_id("E", symbol, decision.bar_time // 1000)
            self.pending_entries[symbol] = {"client_id": client_id, "atr": decision.atr,
                                            "bar_time": decision.bar_time, "created_ms": now}
            self.save_state()
            log_event(self.log, "entry_submit", symbol=symbol, qty=str(sizing.qty), price=price,
                      stop=round(stop_ref, 8), risk_amount=round(sizing.risk_amount, 4), sizing=sizing.reason)
            try:
                fill = await self.broker.buy(rules, sizing.qty, price, client_id)
            except EntryStateUnknownError as exc:
                log_event(self.log, "entry_unknown", logging.ERROR, symbol=symbol, client_id=client_id, error=str(exc))
                self.notifier.send(f"Entry {symbol} status UNKNOWN ({client_id}); rekonsiliasi akan menyelesaikan.", "WARN")
                self.reconcile_now.set()
                return
            except (ExchangeError, PrecisionError) as exc:
                self.pending_entries.pop(symbol, None)
                self.save_state()
                log_event(self.log, "entry_failed", logging.ERROR, symbol=symbol, error=str(exc))
                self.notifier.send(f"Entry {symbol} gagal: {exc}", "WARN")
                return
            self.pending_entries.pop(symbol, None)
            if not fill.is_filled:
                log_event(self.log, "entry_not_filled", logging.WARNING, symbol=symbol, status=fill.status)
                self.save_state()
                return
            await self._open_position(symbol, fill, decision.atr, decision.bar_time)
            self.save_state()

    async def _open_position(self, symbol: str, fill: FillResult, atr_v: float, bar_time: int) -> None:
        """Buat posisi dari fill aktual; level stop/TP dihitung ulang dari harga fill."""
        stop, tp1 = self.strategy.rebase_levels(fill.avg_price, atr_v)
        entry_eff = fill.quote_amount / fill.filled_qty
        pos = Position(symbol=symbol, entry_price=entry_eff, qty=fill.filled_qty, initial_qty=fill.filled_qty,
                       stop_price=stop, initial_stop=stop, tp1_price=tp1,
                       risk_per_unit=self.risk.risk_per_unit(fill.avg_price, stop),
                       entry_time=self.client.timestamp_ms(), entry_bar_time=bar_time,
                       highest_high=fill.avg_price, entry_client_id=fill.client_id,
                       cost_basis_quote=fill.quote_amount, fees_paid=fill.fees_quote)
        self.positions[symbol] = pos
        self.legs[symbol] = []
        log_event(self.log, "position_opened", symbol=symbol, qty=pos.qty, entry=round(entry_eff, 8),
                  stop=round(stop, 8), tp1=round(tp1, 8))
        self.notifier.send(f"BUY {symbol} qty={pos.qty:g} avg={fill.avg_price:.6g} SL={stop:.6g} TP1={tp1:.6g} "
                           f"biaya={fill.fees_quote:.4f}", "FILL")
        if self.broker.supports_exchange_stop and self.cfg.risk.use_exchange_stop:
            await self._place_stop(symbol, ref_price=fill.avg_price)

    # ================================================================== #
    # Stop / exit management
    # ================================================================== #
    async def _place_stop(self, symbol: str, ref_price: Optional[float] = None) -> None:
        pos = self.positions[symbol]
        rules = self.rules[symbol]
        price = ref_price or self.fresh_price(symbol) or pos.highest_high
        try:
            order = await self.broker.place_stop(rules, rules.floor_qty(pos.qty), pos.stop_price, price,
                                                 make_client_id("S", symbol, self.client.timestamp_ms()))
        except PrecisionError as exc:
            log_event(self.log, "stop_place_rejected", logging.WARNING, symbol=symbol, error=str(exc))
            if pos.stop_price >= price:
                # _exit_locked menolak bila simbol sedang 'busy' -> tidak ada rekursi exit
                await self._exit_locked(symbol, price, "stop_below_market")
            return
        except ExchangeError as exc:
            log_event(self.log, "stop_place_failed", logging.ERROR, symbol=symbol, error=str(exc))
            self.notifier.send(f"Gagal pasang stop {symbol}: {exc}. Soft-stop bot tetap aktif.", "WARN")
            return
        if order is not None:
            pos.stop_order_id = order.order_id
            pos.exchange_stop_price = float(order.stop_price) or pos.stop_price
            log_event(self.log, "stop_placed", symbol=symbol, order_id=order.order_id, stop=pos.exchange_stop_price)

    async def _cancel_stop(self, symbol: str) -> bool:
        """Batalkan stop bursa. Return True bila ternyata stop SUDAH terisi (posisi sudah ditutup)."""
        pos = self.positions.get(symbol)
        if pos is None or not pos.stop_order_id:
            return False
        rules = self.rules[symbol]
        order_id = pos.stop_order_id
        try:
            await self.broker.cancel(rules, order_id)
        except ExchangeError as exc:
            log_event(self.log, "stop_cancel_failed", logging.WARNING, symbol=symbol, error=str(exc))
        order = await self.broker.get_order(rules, order_id)
        pos.stop_order_id = None
        pos.exchange_stop_price = 0.0
        if order is not None and order.executed_qty > 0:
            self._apply_exchange_fill(symbol, order, "stop_loss_exchange")
            return symbol not in self.positions
        return False

    async def _sync_exchange_stop(self, symbol: str) -> None:
        """Naikkan stop di bursa bila stop lokal naik melebihi ``trail_min_step_pct``."""
        pos = self.positions.get(symbol)
        if pos is None or not (self.broker.supports_exchange_stop and self.cfg.risk.use_exchange_stop):
            return
        if pos.stop_order_id and pos.stop_price < pos.exchange_stop_price * (1 + self.cfg.risk.trail_min_step_pct):
            return
        async with self.order_lock:
            if await self._cancel_stop(symbol):
                return
            if symbol in self.positions:
                await self._place_stop(symbol)

    async def check_intrabar(self, symbol: str, price: float) -> None:
        """Monitor tick: soft stop, watchdog stop bursa, dan TP1 parsial."""
        pos = self.positions.get(symbol)
        if pos is None or symbol in self.busy:
            return
        if price <= pos.stop_price:
            exchange_protected = bool(pos.stop_order_id) and pos.exchange_stop_price >= pos.stop_price * (1 - 1e-9)
            if exchange_protected and price > pos.exchange_stop_price * (1 - 1.5 * self.cfg.risk.stop_limit_buffer):
                return  # biarkan stop bursa bekerja
            reason = "stop_watchdog" if exchange_protected else ("trailing_stop" if pos.tp1_done else "stop_loss")
            await self.exit_position(symbol, price, reason)
        elif not pos.tp1_done and price >= pos.tp1_price:
            await self.take_partial(symbol, price)

    async def exit_position(self, symbol: str, ref_price: float, reason: str) -> None:
        async with self.order_lock:
            await self._exit_locked(symbol, ref_price, reason)

    async def _exit_locked(self, symbol: str, ref_price: float, reason: str) -> None:
        if symbol not in self.positions or symbol in self.busy:
            return
        self.busy.add(symbol)
        try:
            if await self._cancel_stop(symbol):
                return
            pos = self.positions[symbol]
            rules = self.rules[symbol]
            qty = rules.floor_qty(pos.qty, market=True)
            if qty <= 0 or float(qty) * ref_price < float(rules.min_notional_for("MARKET")):
                log_event(self.log, "dust_close", logging.WARNING, symbol=symbol, qty=pos.qty)
                self._finalize(symbol, self.client.timestamp_ms(), f"{reason}_dust")
                return
            try:
                fill = await self.broker.sell(rules, qty, ref_price, make_client_id("X", symbol, self.client.timestamp_ms()))
            except (ExchangeError, PrecisionError) as exc:
                log_event(self.log, "exit_failed", logging.CRITICAL, symbol=symbol, error=str(exc))
                self.notifier.send(f"EXIT {symbol} GAGAL ({reason}): {exc}. Cek manual!", "CRITICAL")
                if symbol in self.positions and self.cfg.risk.use_exchange_stop:
                    await self._place_stop(symbol)
                return
            self._apply_sell(symbol, fill.filled_qty, fill.quote_amount, fill.avg_price, fill.fees_quote, reason)
        finally:
            self.busy.discard(symbol)
            self.save_state()

    async def take_partial(self, symbol: str, ref_price: float) -> None:
        async with self.order_lock:
            pos = self.positions.get(symbol)
            if pos is None or pos.tp1_done or symbol in self.busy:
                return
            rules = self.rules[symbol]
            part = rules.floor_qty(pos.initial_qty * self.cfg.strategy.tp1_fraction, market=True)
            remaining = rules.floor_qty(pos.qty - float(part))
            min_notional = float(rules.min_notional_for("MARKET"))
            if float(remaining) * ref_price < min_notional or remaining < rules.min_qty:
                await self._exit_locked(symbol, ref_price, "tp1_full")
                return
            if float(part) * ref_price < min_notional or part <= 0:
                pos.tp1_done = True
                pos.stop_price = max(pos.stop_price, self.strategy.breakeven_price(pos))
                log_event(self.log, "tp1_skip_partial", symbol=symbol, reason="part_below_min_notional")
                self.save_state()
                return
            self.busy.add(symbol)
            try:
                if await self._cancel_stop(symbol):
                    return
                try:
                    fill = await self.broker.sell(rules, part, ref_price,
                                                  make_client_id("T", symbol, self.client.timestamp_ms()))
                except (ExchangeError, PrecisionError) as exc:
                    log_event(self.log, "tp1_failed", logging.ERROR, symbol=symbol, error=str(exc))
                    self.notifier.send(f"TP1 {symbol} gagal: {exc}", "WARN")
                    if self.cfg.risk.use_exchange_stop:
                        await self._place_stop(symbol)
                    return
                pos.tp1_done = True
                self._apply_sell(symbol, fill.filled_qty, fill.quote_amount, fill.avg_price, fill.fees_quote, "tp1")
                if symbol in self.positions:
                    pos.stop_price = max(pos.stop_price, self.strategy.breakeven_price(pos))
                    if self.broker.supports_exchange_stop and self.cfg.risk.use_exchange_stop:
                        await self._place_stop(symbol, ref_price=ref_price)
            finally:
                self.busy.discard(symbol)
                self.save_state()

    def _apply_exchange_fill(self, symbol: str, order: OrderInfo, reason: str) -> None:
        """Catat eksekusi stop bursa (biaya diestimasi dari config; quote diff tidak tersedia async)."""
        executed = float(order.executed_qty)
        gross = float(order.executed_quote_qty) or executed * float(order.avg_price)
        fee = gross * (self.cfg.costs.taker_fee + self.cfg.costs.tax_rate_sell)
        self._apply_sell(symbol, executed, gross - fee, float(order.avg_price), fee, reason)

    def _apply_sell(self, symbol: str, qty: float, proceeds: float, avg_price: float, fees: float, reason: str) -> None:
        pos = self.positions.get(symbol)
        if pos is None:
            return
        qty = min(qty, pos.qty)
        pnl = proceeds - qty * pos.entry_price
        pos.qty -= qty
        pos.realized_pnl += pnl
        pos.fees_paid += fees
        now = self.client.timestamp_ms()
        self.legs.setdefault(symbol, []).append({"time": now, "qty": qty, "price": avg_price, "pnl": pnl, "reason": reason})
        level = "STOP" if "stop" in reason or "kill" in reason else "FILL"
        self.notifier.send(f"SELL {symbol} ({reason}) qty={qty:g} avg={avg_price:.6g} pnl={pnl:+.4f}", level)
        log_event(self.log, "sell_applied", symbol=symbol, reason=reason, qty=qty, avg=avg_price, pnl=round(pnl, 6))
        rules = self.rules[symbol]
        left = rules.floor_qty(pos.qty, market=True)
        if left <= 0 or left < rules.min_qty or float(left) * avg_price < float(rules.min_notional_for("MARKET")):
            self._finalize(symbol, now, reason)

    def _finalize(self, symbol: str, now: int, reason: str) -> None:
        pos = self.positions.pop(symbol, None)
        if pos is None:
            return
        legs = self.legs.pop(symbol, [])
        exit_qty = sum(l["qty"] for l in legs)
        exit_px = sum(l["qty"] * l["price"] for l in legs) / exit_qty if exit_qty else 0.0
        risk_amt = pos.initial_qty * pos.risk_per_unit
        record = TradeRecord(symbol, pos.entry_time, now, pos.entry_price, exit_px, pos.initial_qty, pos.realized_pnl,
                             pos.realized_pnl / risk_amt if risk_amt > 0 else 0.0, pos.fees_paid, reason, legs)
        self.store.append_trade(record.to_dict())
        log_event(self.log, "trade_closed", symbol=symbol, pnl=round(pos.realized_pnl, 6),
                  r=round(record.r_multiple, 3), reason=reason)
        self.notifier.send(f"CLOSED {symbol} pnl={pos.realized_pnl:+.4f} ({record.r_multiple:+.2f}R) via {reason}", "INFO")
        for ev in self.risk.record_trade_result(pos.realized_pnl, now):
            self.notifier.send(ev.message, "WARN")

    # ================================================================== #
    # Equity, risk loop, reconciliation
    # ================================================================== #
    async def compute_equity(self) -> float:
        """Quote total + nilai posisi yang dikelola bot (aset di luar bot tidak dihitung)."""
        balances = await self.broker.balances()
        quote = balances.get(self.cfg.runtime.quote_asset)
        equity = float(quote.total) if quote else 0.0
        for sym, pos in self.positions.items():
            price = self.last_price.get(sym, (pos.entry_price, 0.0))[0]
            equity += pos.qty * price
        return equity

    async def _handle_risk_events(self, events: list[RiskEvent]) -> None:
        for ev in events:
            self.notifier.send(ev.message, "KILL" if ev.kind in ("DAILY_KILL", "HARD_HALT") else "INFO")
            if ev.flatten:
                for sym in list(self.positions):
                    price = self.fresh_price(sym) or self.last_price.get(sym, (0.0, 0.0))[0]
                    if price > 0:
                        await self.exit_position(sym, price, f"kill_{ev.kind.lower()}")
        if events:
            self.save_state()

    async def _risk_loop(self) -> None:
        while not self.stop_event.is_set():
            try:
                if all(self.fresh_price(s) is not None for s in self.positions):
                    equity = await self.compute_equity()
                    await self._handle_risk_events(self.risk.update_equity(equity, self.client.timestamp_ms()))
                kill_exists = self.cfg.runtime.kill_file.exists()
                if kill_exists and not self.kill_file_notified:
                    self.notifier.send(f"Manual KILL file terdeteksi ({self.cfg.runtime.kill_file}); entry dihentikan.", "KILL")
                self.kill_file_notified = kill_exists
            except ExchangeError as exc:
                log_event(self.log, "risk_loop_error", logging.WARNING, error=str(exc))
            await self._sleep(self.cfg.runtime.risk_check_interval_s)

    async def _reconcile_loop(self) -> None:
        failures = 0
        while not self.stop_event.is_set():
            try:
                await asyncio.wait_for(self.reconcile_now.wait(), timeout=self.cfg.runtime.reconcile_interval_s)
            except asyncio.TimeoutError:
                pass
            self.reconcile_now.clear()
            if self.stop_event.is_set():
                break
            try:
                await self.reconcile()
                failures = 0
            except ExchangeError as exc:
                failures += 1
                log_event(self.log, "reconcile_error", logging.ERROR, error=str(exc), consecutive=failures)
                if failures == 3:
                    self.notifier.send(f"Rekonsiliasi gagal 3x berturut-turut: {exc}", "CRITICAL")

    async def reconcile(self) -> None:
        """Sinkronkan state lokal dengan bursa (order & saldo). Mencegah double-entry & posisi yatim."""
        async with self.order_lock:
            now = self.client.timestamp_ms()
            # 1) Entry yang statusnya belum pasti
            for sym, pend in list(self.pending_entries.items()):
                rules = self.rules[sym]
                order = await self.broker.find_order(rules, pend["client_id"])
                if order is None:
                    if now - int(pend["created_ms"]) > PENDING_ENTRY_TTL_MS:
                        log_event(self.log, "pending_entry_dropped", logging.WARNING, symbol=sym, client_id=pend["client_id"])
                        self.pending_entries.pop(sym)
                    continue
                if order.is_open:
                    try:
                        order = await self.client.cancel_order(order_id=order.order_id)
                    except ExchangeError:
                        continue
                self.pending_entries.pop(sym)
                if order.executed_qty > 0 and sym not in self.positions:
                    balances = await self.broker.balances()
                    base = balances.get(rules.base_asset)
                    net = min(order.executed_qty, base.total if base else Decimal(0))
                    quote_amt = float(order.executed_quote_qty) * (1 + self.cfg.costs.taker_fee + self.cfg.costs.tax_rate_buy)
                    fill = FillResult(sym, "BUY", float(order.orig_qty), float(net), float(order.avg_price), quote_amt,
                                      quote_amt - float(order.executed_quote_qty), order.order_id, order.client_id,
                                      order.status)
                    log_event(self.log, "pending_entry_adopted", logging.WARNING, symbol=sym, qty=str(net))
                    if fill.filled_qty > 0:
                        await self._open_position(sym, fill, float(pend["atr"]), int(pend["bar_time"]))

            balances = await self.broker.balances()
            # 2) Posisi vs stop order vs saldo
            for sym, pos in list(self.positions.items()):
                rules = self.rules[sym]
                if pos.stop_order_id:
                    order = await self.broker.get_order(rules, pos.stop_order_id)
                    if order is not None and order.is_final:
                        pos.stop_order_id = None
                        pos.exchange_stop_price = 0.0
                        if order.executed_qty > 0:
                            self._apply_exchange_fill(sym, order, "stop_loss_exchange")
                        else:
                            log_event(self.log, "stop_order_lost", logging.WARNING, symbol=sym, status=order.status)
                            self.notifier.send(f"Stop {sym} berstatus {order.status}; dipasang ulang.", "WARN")
                    if sym not in self.positions:
                        continue
                base = balances.get(rules.base_asset)
                held = float(base.total) if base else 0.0
                if held < pos.qty * 0.98:
                    log_event(self.log, "balance_mismatch", logging.ERROR, symbol=sym, local=pos.qty, exchange=held)
                    self.notifier.send(f"Saldo {rules.base_asset} ({held:g}) < posisi bot ({pos.qty:g}); disesuaikan.", "WARN")
                    pos.qty = float(rules.floor_qty(held))
                    price = self.last_price.get(sym, (pos.entry_price, 0.0))[0]
                    if pos.qty <= 0 or pos.qty * price < float(rules.min_notional_for("MARKET")):
                        self._finalize(sym, now, "external_close")
                        continue
                if not pos.stop_order_id and self.cfg.risk.use_exchange_stop and sym not in self.busy:
                    await self._place_stop(sym)

            # 3) Order yatim milik bot & saldo yang tidak dikelola
            for sym, rules in self.rules.items():
                known = {self.positions[sym].stop_order_id} if sym in self.positions else set()
                for order in await self.broker.open_orders(rules):
                    if order.client_id.startswith(CLIENT_PREFIX) and order.order_id not in known:
                        log_event(self.log, "orphan_order_cancel", logging.WARNING, symbol=sym, order_id=order.order_id)
                        try:
                            await self.broker.cancel(rules, order.order_id)
                        except ExchangeError as exc:
                            log_event(self.log, "orphan_cancel_failed", logging.WARNING, error=str(exc))
                base = balances.get(rules.base_asset)
                price = self.last_price.get(sym, (0.0, 0.0))[0]
                if sym not in self.positions and base and price > 0 and \
                        float(base.total) * price > float(rules.min_notional) and sym not in self.warned_unmanaged:
                    self.warned_unmanaged.add(sym)
                    self.notifier.send(f"Saldo {rules.base_asset} {base.total} tidak dikelola bot (tidak akan dijual).", "WARN")
            self.save_state()

    async def on_user_event(self, event: dict[str, Any]) -> None:
        """executionReport untuk order milik bot -> percepat rekonsiliasi."""
        if event.get("e") == "executionReport" and str(event.get("c", "")).startswith(CLIENT_PREFIX):
            log_event(self.log, "user_execution_report", symbol=event.get("s"), status=event.get("X"),
                      client_id=event.get("c"))
            if event.get("X") in ("FILLED", "PARTIALLY_FILLED", "CANCELED", "EXPIRED", "REJECTED"):
                self.reconcile_now.set()

    async def _time_sync_loop(self) -> None:
        while not self.stop_event.is_set():
            await self._sleep(self.cfg.runtime.time_sync_interval_s)
            try:
                await self.client.sync_time()
            except ExchangeError as exc:
                log_event(self.log, "time_sync_failed", logging.WARNING, error=str(exc))

    async def _heartbeat_loop(self) -> None:
        """Touch file heartbeat tiap 60 detik (dipakai healthcheck Docker) + log ringkas tiap 10 menit."""
        beat_file = self.cfg.runtime.state_dir / "heartbeat"
        tick = 0
        while not self.stop_event.is_set():
            await self._sleep(60)
            tick += 1
            stale = [s for s in self.rules if self.fresh_price(s) is None]
            if not stale:
                beat_file.write_text(str(self.client.timestamp_ms()))
            if tick % 10 != 0:
                continue
            try:
                equity = await self.compute_equity()
            except ExchangeError:
                equity = float("nan")
            log_event(self.log, "heartbeat", equity=round(equity, 4), positions=list(self.positions),
                      block=self.risk.entry_block_reason(self.client.timestamp_ms()), stale_feeds=stale,
                      prices={s: round(p, 8) for s, (p, _) in self.last_price.items()})

    async def _sleep(self, seconds: float) -> None:
        try:
            await asyncio.wait_for(self.stop_event.wait(), timeout=seconds)
        except asyncio.TimeoutError:
            pass


# ====================================================================== #
# CLI
# ====================================================================== #
def _cli_status(cfg: AppConfig) -> int:
    for mode in ("paper", "live"):
        path = cfg.runtime.state_dir / f"state_{mode}.json"
        if path.exists():
            print(f"--- {path} ---")
            print(json.dumps(json.loads(path.read_text()), indent=2))
    return 0


def _cli_reset_halt(cfg: AppConfig) -> int:
    store = StateStore(cfg.runtime.state_dir / f"state_{cfg.runtime.mode.value.lower()}.json",
                       cfg.runtime.state_dir / f"trades_{cfg.runtime.mode.value.lower()}.jsonl")
    state = store.load()
    risk = RiskManager(cfg.risk, cfg.costs, RiskState.from_dict(state.get("risk", {})))
    risk.reset_halt()
    risk.state.daily_kill = False
    state["risk"] = risk.state.to_dict()
    store.save(state)
    print(f"Hard halt & daily kill di-reset untuk mode {cfg.runtime.mode.value}")
    return 0


def main(argv: Optional[list[str]] = None) -> int:
    parser = argparse.ArgumentParser(description="TokoBot - Tokocrypto spot trading bot")
    parser.add_argument("--mode", choices=[m.value for m in Mode], default=None, help="override TRADING_MODE")
    parser.add_argument("--env", default=None, help="path file .env")
    parser.add_argument("--status", action="store_true", help="tampilkan state tersimpan")
    parser.add_argument("--reset-halt", action="store_true", help="reset hard halt & daily kill")
    args = parser.parse_args(argv)
    try:
        cfg = AppConfig.load(args.env, mode_override=args.mode)
    except ConfigError as exc:
        print(f"Konfigurasi tidak valid: {exc}", file=sys.stderr)
        return 2
    setup_logging(cfg.runtime.log_dir, cfg.runtime.log_level)
    if args.status:
        return _cli_status(cfg)
    if args.reset_halt:
        return _cli_reset_halt(cfg)
    if cfg.runtime.mode is Mode.BACKTEST:
        run_backtest_cli(cfg, cfg.runtime.symbols)
        return 0
    try:
        asyncio.run(TradingBot(cfg).run())
    except KeyboardInterrupt:
        pass
    except Exception as exc:  # noqa: BLE001 - exit code non-zero agar systemd/Docker restart
        logging.getLogger("tokobot").critical("bot_crashed", exc_info=exc)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
