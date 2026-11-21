"""Strategi Volatility Breakout (Donchian) + Market Regime Filter, long-only spot.

Alur keputusan per bar CLOSED (tidak pernah memakai bar yang sedang berjalan):

1. **Regime filter** (semua harus lolos, kalau tidak -> STANDBY):
   * Struktur tren: ``EMA_fast > EMA_slow``, ``close > EMA_slow``, slope ``EMA_fast`` positif.
   * Kekuatan tren: ``ADX >= adx_min`` dan (ADX naik vs ``k`` bar lalu ATAU ADX >= ``adx_strong``).
   * Anti-choppy: Kaufman Efficiency Ratio >= ``er_min``.
   * Volatilitas: ``atr_pct_min <= ATR/close <= atr_pct_max`` (terlalu sepi -> biaya
     mendominasi; terlalu liar -> gap risk & stop tidak reliabel).
2. **Trigger**: close menembus Donchian high ``N`` bar SEBELUMNYA, close di area atas
   range bar (anti false-wick), volume >= SMA volume * mult.
3. **Cost gate** (dua syarat):
   * jarak ke TP1 >= ``min_reward_cost_ratio`` x biaya round-trip (fee + pajak + slippage);
   * biaya round-trip / jarak stop <= ``max_cost_to_risk``. Ini yang menentukan berapa
     bagian dari 1R yang hilang ke biaya di SETIAP trade, menang atau kalah.

Exit (dikelola risk/execution layer memakai level dari kelas ini):
hard stop ``entry - sl_atr_mult*ATR``, TP1 parsial di ``tp1_r_multiple`` R lalu stop
ke break-even (+biaya), sisa posisi di-trail dengan Chandelier ``HH - trail_atr_mult*ATR``.
"""

from __future__ import annotations

import math
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Sequence

import numpy as np

from config import CostConfig, StrategyConfig
from core.models import Candle, Position
from data import indicators as ind


class Regime(str, Enum):
    WARMUP = "WARMUP"
    TRENDING_UP = "TRENDING_UP"
    DOWNTREND = "DOWNTREND"
    CHOPPY = "CHOPPY"
    LOW_VOL = "LOW_VOL"
    EXTREME_VOL = "EXTREME_VOL"


class Action(str, Enum):
    ENTER_LONG = "ENTER_LONG"
    STANDBY = "STANDBY"
    NONE = "NONE"


@dataclass
class Features:
    """Array fitur untuk satu seri candle (indeks sejajar dengan candle)."""

    open_time: np.ndarray
    open: np.ndarray
    high: np.ndarray
    low: np.ndarray
    close: np.ndarray
    volume: np.ndarray
    ema_fast: np.ndarray
    ema_slow: np.ndarray
    adx: np.ndarray
    atr: np.ndarray
    donchian_prev_high: np.ndarray
    er: np.ndarray
    vol_sma: np.ndarray
    close_loc: np.ndarray

    def __len__(self) -> int:
        return len(self.close)


@dataclass
class Decision:
    """Hasil evaluasi satu bar; dicatat penuh untuk audit filter."""

    symbol: str
    bar_time: int
    action: Action
    regime: Regime
    close: float
    atr: float = math.nan
    stop: float = math.nan
    tp1: float = math.nan
    reasons: list[str] = field(default_factory=list)
    metrics: dict[str, float] = field(default_factory=dict)

    def to_log(self) -> dict[str, Any]:
        return {"symbol": self.symbol, "bar_time": self.bar_time, "action": self.action.value,
                "regime": self.regime.value, "close": self.close, "stop": _r(self.stop), "tp1": _r(self.tp1),
                "reasons": ";".join(self.reasons), **{k: _r(v) for k, v in self.metrics.items()}}


def _r(x: float, nd: int = 6) -> float | None:
    return None if x is None or (isinstance(x, float) and math.isnan(x)) else round(float(x), nd)


class MomentumRegimeStrategy:
    """Implementasi strategi; stateless terhadap posisi (state dipegang layer lain)."""

    def __init__(self, cfg: StrategyConfig, costs: CostConfig) -> None:
        self.cfg = cfg
        self.costs = costs

    @property
    def warmup_bars(self) -> int:
        c = self.cfg
        return max(c.ema_slow + c.ema_slope_lookback, c.adx_period * 2 + c.adx_rising_lookback,
                   c.donchian_entry + 1, c.er_period + 1, c.volume_sma_period) + 1

    # ------------------------------------------------------------------ #
    def compute_features(self, candles: Sequence[Candle]) -> Features:
        """Hitung semua indikator. Hanya masukkan candle CLOSED."""
        c = self.cfg
        o = np.array([x.open for x in candles], dtype=np.float64)
        h = np.array([x.high for x in candles], dtype=np.float64)
        lo = np.array([x.low for x in candles], dtype=np.float64)
        cl = np.array([x.close for x in candles], dtype=np.float64)
        v = np.array([x.volume for x in candles], dtype=np.float64)
        t = np.array([x.open_time for x in candles], dtype=np.int64)
        adx_v, _, _ = ind.adx(h, lo, cl, c.adx_period)
        return Features(
            open_time=t, open=o, high=h, low=lo, close=cl, volume=v,
            ema_fast=ind.ema(cl, c.ema_fast), ema_slow=ind.ema(cl, c.ema_slow),
            adx=adx_v, atr=ind.atr(h, lo, cl, c.atr_period),
            donchian_prev_high=ind.shift(ind.rolling_max(h, c.donchian_entry), 1),
            er=ind.efficiency_ratio(cl, c.er_period), vol_sma=ind.sma(v, c.volume_sma_period),
            close_loc=ind.close_location(h, lo, cl),
        )

    # ------------------------------------------------------------------ #
    def regime_at(self, f: Features, i: int) -> tuple[Regime, list[str], dict[str, float]]:
        """Klasifikasi regime pasar pada bar ``i``."""
        c = self.cfg
        if i < self.warmup_bars or i >= len(f):
            return Regime.WARMUP, ["insufficient_history"], {}
        k = c.adx_rising_lookback
        vals = {
            "ema_fast": f.ema_fast[i], "ema_slow": f.ema_slow[i], "adx": f.adx[i], "adx_prev": f.adx[i - k],
            "atr": f.atr[i], "er": f.er[i], "ema_fast_prev": f.ema_fast[i - c.ema_slope_lookback],
        }
        if any(math.isnan(x) for x in vals.values()):
            return Regime.WARMUP, ["indicator_nan"], {}
        close = f.close[i]
        atr_pct = vals["atr"] / close
        metrics = {"adx": vals["adx"], "er": vals["er"], "atr_pct": atr_pct,
                   "ema_fast": vals["ema_fast"], "ema_slow": vals["ema_slow"]}
        if atr_pct > c.atr_pct_max:
            return Regime.EXTREME_VOL, [f"atr_pct {atr_pct:.4f} > {c.atr_pct_max}"], metrics
        if atr_pct < c.atr_pct_min:
            return Regime.LOW_VOL, [f"atr_pct {atr_pct:.4f} < {c.atr_pct_min}"], metrics
        reasons: list[str] = []
        if vals["ema_fast"] <= vals["ema_slow"]:
            reasons.append("ema_fast<=ema_slow")
        if close <= vals["ema_slow"]:
            reasons.append("close<=ema_slow")
        if vals["ema_fast"] <= vals["ema_fast_prev"]:
            reasons.append("ema_fast_slope<=0")
        if reasons:
            return Regime.DOWNTREND, reasons, metrics
        if vals["adx"] < c.adx_min:
            reasons.append(f"adx {vals['adx']:.1f} < {c.adx_min}")
        elif vals["adx"] <= vals["adx_prev"] and vals["adx"] < c.adx_strong:
            reasons.append("adx_not_rising")
        if vals["er"] < c.er_min:
            reasons.append(f"er {vals['er']:.2f} < {c.er_min}")
        if reasons:
            return Regime.CHOPPY, reasons, metrics
        return Regime.TRENDING_UP, [], metrics

    def signal_at(self, f: Features, i: int, symbol: str) -> Decision:
        """Evaluasi sinyal entry pada bar closed ``i``."""
        c = self.cfg
        regime, reasons, metrics = self.regime_at(f, i)
        close = float(f.close[i]) if 0 <= i < len(f) else math.nan
        bar_time = int(f.open_time[i]) if 0 <= i < len(f) else 0
        if regime is not Regime.TRENDING_UP:
            return Decision(symbol, bar_time, Action.STANDBY, regime, close, reasons=reasons, metrics=metrics)

        atr_v = float(f.atr[i])
        dc = float(f.donchian_prev_high[i])
        trig: list[str] = []
        if math.isnan(dc) or close <= dc:
            trig.append("no_breakout")
        if f.close_loc[i] < c.close_position_min:
            trig.append(f"weak_close {f.close_loc[i]:.2f}")
        if not math.isnan(f.vol_sma[i]) and f.volume[i] < f.vol_sma[i] * c.volume_mult:
            trig.append("low_volume")
        stop, tp1 = self.initial_levels(close, atr_v)
        reward_pct = (tp1 - close) / close
        metrics.update({"donchian_high": dc, "close_loc": float(f.close_loc[i]), "reward_pct": reward_pct,
                        "cost_rt_pct": self.costs.round_trip_pct})
        stop_pct = (close - stop) / close
        cost_to_risk = self.costs.round_trip_pct / stop_pct if stop_pct > 0 else math.inf
        metrics["cost_to_risk"] = cost_to_risk
        if reward_pct < c.min_reward_cost_ratio * self.costs.round_trip_pct:
            trig.append(f"edge<{c.min_reward_cost_ratio}x_cost")
        if cost_to_risk > c.max_cost_to_risk:
            trig.append(f"cost_to_risk {cost_to_risk:.2f}>{c.max_cost_to_risk}")
        if trig:
            return Decision(symbol, bar_time, Action.NONE, regime, close, atr_v, reasons=trig, metrics=metrics)
        return Decision(symbol, bar_time, Action.ENTER_LONG, regime, close, atr_v, stop, tp1,
                        reasons=["breakout_confirmed"], metrics=metrics)

    # ------------------------------------------------------------------ #
    def initial_levels(self, entry: float, atr_v: float) -> tuple[float, float]:
        """Stop & TP1 awal dari harga entry dan ATR."""
        stop = entry - self.cfg.sl_atr_mult * atr_v
        tp1 = entry + self.cfg.tp1_r_multiple * (entry - stop)
        return stop, tp1

    def rebase_levels(self, fill_price: float, atr_v: float) -> tuple[float, float]:
        """Hitung ulang level dari harga fill aktual (bukan close sinyal)."""
        return self.initial_levels(fill_price, atr_v)

    def breakeven_price(self, position: Position) -> float:
        """Harga jual minimum agar sisa posisi tidak rugi setelah biaya jual."""
        return position.entry_price * (1.0 + self.costs.exit_cost_pct)

    def trailing_stop(self, position: Position, f: Features, i: int) -> float:
        """Stop baru setelah bar ``i`` close (tidak pernah turun)."""
        atr_v = float(f.atr[i])
        new_stop = position.stop_price
        if not math.isnan(atr_v):
            chandelier = position.highest_high - self.cfg.trail_atr_mult * atr_v
            new_stop = max(new_stop, chandelier)
        if position.tp1_done:
            new_stop = max(new_stop, self.breakeven_price(position))
        return new_stop
