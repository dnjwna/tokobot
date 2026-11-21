"""Risk manager: position sizing berbasis risiko tetap + kill-switch berlapis.

Lapisan proteksi:
1. **Per trade** — qty = (ekuitas * risk_per_trade) / risiko-per-unit, di mana
   risiko-per-unit sudah memasukkan slippage dan biaya beli+jual (fee + pajak).
   Dibatasi ``max_position_pct`` ekuitas dan kas tersedia, lalu dibulatkan ke
   ``stepSize`` dan dicek ``minQty``/``minNotional``.
2. **Portofolio** — maksimum posisi terbuka bersamaan, satu posisi per simbol.
3. **Daily kill-switch** — ekuitas (realized + unrealized) turun >= ``daily_max_drawdown``
   dari ekuitas awal hari (zona waktu ``day_reset_tz``) -> stop entry sampai hari berikutnya.
4. **Hard halt** — drawdown dari puncak >= ``max_total_drawdown`` -> stop permanen sampai
   di-reset manual (``python main.py --reset-halt``).
5. **Loss streak pause** — ``max_consecutive_losses`` kali rugi beruntun -> jeda N jam.
6. **Manual kill file** — file ``KILL_FILE`` ada -> stop entry.
"""

from __future__ import annotations

import logging
import math
from dataclasses import asdict, dataclass
from datetime import datetime
from decimal import Decimal
from pathlib import Path
from typing import Any, Optional
from zoneinfo import ZoneInfo

from config import CostConfig, RiskConfig
from exchange.tokocrypto_client import SymbolRules
from utils.logger import get_logger, log_event


@dataclass
class RiskState:
    """State risiko yang dipersist agar kill-switch bertahan saat restart."""

    day_key: str = ""
    day_start_equity: float = 0.0
    peak_equity: float = 0.0
    last_equity: float = 0.0
    daily_kill: bool = False
    hard_halt: bool = False
    halt_reason: str = ""
    consecutive_losses: int = 0
    pause_until_ms: int = 0

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "RiskState":
        known = {k: v for k, v in data.items() if k in cls.__dataclass_fields__}
        return cls(**known)


@dataclass(frozen=True)
class SizingResult:
    qty: Decimal
    notional: float
    risk_amount: float
    risk_per_unit: float
    reason: str = ""

    @property
    def ok(self) -> bool:
        return self.qty > 0


@dataclass(frozen=True)
class RiskEvent:
    """Event risiko yang perlu dinotifikasi (mis. kill-switch aktif)."""

    kind: str          # DAILY_KILL | HARD_HALT | DAY_RESET | LOSS_STREAK_PAUSE
    message: str
    flatten: bool = False


class RiskManager:
    """Penjaga modal. Tidak melakukan I/O jaringan; murni logika + state."""

    def __init__(self, cfg: RiskConfig, costs: CostConfig, state: Optional[RiskState] = None,
                 kill_file: Optional[Path] = None, circuit_breakers: bool = True) -> None:
        """``circuit_breakers=False`` hanya untuk riset backtest: daily kill, hard halt, dan
        loss-streak pause dimatikan agar edge strategi terukur di SELURUH periode data."""
        self.cfg = cfg
        self.circuit_breakers = circuit_breakers
        self.costs = costs
        self.state = state or RiskState()
        self.kill_file = kill_file
        self._tz = ZoneInfo(cfg.day_reset_tz)
        self._log = get_logger("risk")

    # ------------------------------------------------------------------ #
    # Ekuitas & kill-switch
    # ------------------------------------------------------------------ #
    def effective_equity(self, equity: float) -> float:
        """Terapkan ``capital_cap_quote`` (bila >0) agar bot tidak memakai seluruh akun."""
        if self.cfg.capital_cap_quote > 0:
            return min(equity, self.cfg.capital_cap_quote)
        return equity

    def _day_key(self, now_ms: int) -> str:
        return datetime.fromtimestamp(now_ms / 1000, tz=self._tz).strftime("%Y-%m-%d")

    def update_equity(self, equity: float, now_ms: int) -> list[RiskEvent]:
        """Perbarui ekuitas terkini; kembalikan event kill-switch/reset yang baru terjadi."""
        events: list[RiskEvent] = []
        if not math.isfinite(equity) or equity <= 0:
            log_event(self._log, "equity_invalid", logging.ERROR, equity=equity)
            return events
        s = self.state
        day = self._day_key(now_ms)
        if day != s.day_key:
            was_killed = s.daily_kill
            s.day_key, s.day_start_equity, s.daily_kill = day, equity, False
            if was_killed:
                events.append(RiskEvent("DAY_RESET", f"Hari baru {day}: daily kill-switch di-reset, ekuitas {equity:.2f}"))
            log_event(self._log, "day_reset", logging.DEBUG, day=day, day_start_equity=round(equity, 4))
        s.last_equity = equity
        s.peak_equity = max(s.peak_equity, equity)

        if not self.circuit_breakers:
            return events
        daily_dd = 1.0 - equity / s.day_start_equity if s.day_start_equity > 0 else 0.0
        total_dd = 1.0 - equity / s.peak_equity if s.peak_equity > 0 else 0.0
        if not s.daily_kill and daily_dd >= self.cfg.daily_max_drawdown:
            s.daily_kill = True
            msg = (f"DAILY KILL-SWITCH: drawdown harian {daily_dd:.2%} >= {self.cfg.daily_max_drawdown:.2%} "
                   f"(awal hari {s.day_start_equity:.2f} -> {equity:.2f}). Entry baru dihentikan.")
            log_event(self._log, "kill_switch_daily", logging.CRITICAL, daily_dd=round(daily_dd, 5))
            events.append(RiskEvent("DAILY_KILL", msg, flatten=self.cfg.kill_switch_flatten))
        if not s.hard_halt and total_dd >= self.cfg.max_total_drawdown:
            s.hard_halt = True
            s.halt_reason = f"total drawdown {total_dd:.2%} dari puncak {s.peak_equity:.2f}"
            msg = (f"HARD HALT: {s.halt_reason} >= {self.cfg.max_total_drawdown:.2%}. "
                   "Butuh reset manual (--reset-halt) setelah evaluasi.")
            log_event(self._log, "hard_halt", logging.CRITICAL, total_dd=round(total_dd, 5))
            events.append(RiskEvent("HARD_HALT", msg, flatten=self.cfg.kill_switch_flatten))
        return events

    def record_trade_result(self, pnl: float, now_ms: int) -> list[RiskEvent]:
        """Catat hasil trade tertutup untuk loss-streak guard."""
        events: list[RiskEvent] = []
        if not self.circuit_breakers:
            return events
        if pnl < 0:
            self.state.consecutive_losses += 1
            if self.state.consecutive_losses >= self.cfg.max_consecutive_losses:
                self.state.pause_until_ms = now_ms + int(self.cfg.loss_streak_pause_hours * 3600 * 1000)
                msg = (f"{self.state.consecutive_losses} loss beruntun: entry dijeda "
                       f"{self.cfg.loss_streak_pause_hours:.0f} jam")
                log_event(self._log, "loss_streak_pause", logging.WARNING, losses=self.state.consecutive_losses)
                events.append(RiskEvent("LOSS_STREAK_PAUSE", msg))
                self.state.consecutive_losses = 0
        else:
            self.state.consecutive_losses = 0
        return events

    def entry_block_reason(self, now_ms: int) -> Optional[str]:
        """Alasan entry diblokir, atau ``None`` bila boleh entry."""
        if self.state.hard_halt:
            return f"hard_halt: {self.state.halt_reason}"
        if self.state.daily_kill:
            return "daily_kill_switch"
        if now_ms < self.state.pause_until_ms:
            return "loss_streak_pause"
        if self.kill_file is not None and self.kill_file.exists():
            return f"manual_kill_file:{self.kill_file}"
        return None

    def can_open(self, symbol: str, open_symbols: set[str], now_ms: int) -> tuple[bool, str]:
        reason = self.entry_block_reason(now_ms)
        if reason:
            return False, reason
        if symbol in open_symbols:
            return False, "position_already_open"
        if len(open_symbols) >= self.cfg.max_open_positions:
            return False, f"max_open_positions({self.cfg.max_open_positions})"
        return True, ""

    def reset_halt(self) -> None:
        self.state.hard_halt = False
        self.state.halt_reason = ""
        self.state.peak_equity = self.state.last_equity
        log_event(self._log, "hard_halt_reset", logging.WARNING, peak_equity=self.state.peak_equity)

    # ------------------------------------------------------------------ #
    # Position sizing
    # ------------------------------------------------------------------ #
    def risk_per_unit(self, entry_price: float, stop_price: float) -> float:
        """Kerugian per unit bila stop kena, termasuk slippage dan biaya kedua sisi."""
        entry_eff = entry_price * (1.0 + self.costs.slippage_pct)
        stop_eff = stop_price * (1.0 - self.costs.slippage_pct)
        costs = entry_eff * (self.costs.taker_fee + self.costs.tax_rate_buy) + \
            stop_eff * (self.costs.taker_fee + self.costs.tax_rate_sell)
        return entry_eff - stop_eff + costs

    def size_position(self, equity: float, available_quote: float, entry_price: float, stop_price: float,
                      rules: Optional[SymbolRules] = None) -> SizingResult:
        """Hitung qty entry. ``rules=None`` (backtest tanpa aturan bursa) -> tanpa pembulatan."""
        equity = self.effective_equity(equity)
        if entry_price <= 0 or stop_price <= 0 or stop_price >= entry_price:
            return SizingResult(Decimal(0), 0.0, 0.0, 0.0, "invalid_stop")
        rpu = self.risk_per_unit(entry_price, stop_price)
        risk_amount = equity * self.cfg.risk_per_trade
        entry_eff = entry_price * (1.0 + self.costs.slippage_pct)
        buy_cost = 1.0 + self.costs.taker_fee + self.costs.tax_rate_buy
        qty_risk = risk_amount / rpu
        qty_cap = equity * self.cfg.max_position_pct / entry_eff
        qty_cash = max(available_quote, 0.0) * 0.995 / (entry_eff * buy_cost)
        raw_qty = min(qty_risk, qty_cap, qty_cash)
        binding = min((("risk", qty_risk), ("max_position_pct", qty_cap), ("cash", qty_cash)), key=lambda x: x[1])[0]

        if rules is not None:
            qty = rules.floor_qty(raw_qty)
            notional = qty * Decimal(str(entry_price))
            if qty <= 0 or (rules.min_qty > 0 and qty < rules.min_qty):
                return SizingResult(Decimal(0), 0.0, risk_amount, rpu, f"below_min_qty(binding={binding})")
            if rules.min_notional > 0 and notional < rules.min_notional:
                return SizingResult(Decimal(0), float(notional), risk_amount, rpu,
                                    f"below_min_notional {notional:.4f}<{rules.min_notional}(binding={binding})")
        else:
            qty = Decimal(str(raw_qty)) if raw_qty > 0 else Decimal(0)
            if qty <= 0:
                return SizingResult(Decimal(0), 0.0, risk_amount, rpu, f"zero_qty(binding={binding})")
        q = float(qty)
        return SizingResult(qty, q * entry_price, q * rpu, rpu, f"binding={binding}")
