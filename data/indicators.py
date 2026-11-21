"""Indikator teknikal kausal (tanpa look-ahead) berbasis numpy.

Semua fungsi menerima array 1-D dan mengembalikan array dengan panjang sama;
nilai sebelum periode warmup berisi ``NaN``. Nilai pada indeks ``i`` hanya
bergantung pada data ``[0..i]`` sehingga aman dipakai untuk backtest maupun live.
"""

from __future__ import annotations

import numpy as np
from numpy.lib.stride_tricks import sliding_window_view

FloatArray = np.ndarray


def _as_float(x: FloatArray) -> FloatArray:
    return np.asarray(x, dtype=np.float64)


def sma(x: FloatArray, period: int) -> FloatArray:
    """Simple moving average."""
    x = _as_float(x)
    out = np.full_like(x, np.nan)
    if period <= 0 or len(x) < period:
        return out
    out[period - 1:] = sliding_window_view(x, period).mean(axis=1)
    return out


def ema(x: FloatArray, period: int) -> FloatArray:
    """Exponential moving average, di-seed dengan SMA ``period`` bar pertama."""
    x = _as_float(x)
    out = np.full_like(x, np.nan)
    if period <= 0 or len(x) < period:
        return out
    alpha = 2.0 / (period + 1.0)
    out[period - 1] = x[:period].mean()
    for i in range(period, len(x)):
        out[i] = alpha * x[i] + (1.0 - alpha) * out[i - 1]
    return out


def wilder_rma(x: FloatArray, period: int, start: int = 0) -> FloatArray:
    """Wilder's smoothing (RMA). ``start`` = indeks pertama data valid di ``x``."""
    x = _as_float(x)
    out = np.full_like(x, np.nan)
    first = start + period - 1
    if period <= 0 or len(x) <= first:
        return out
    out[first] = np.nanmean(x[start:first + 1])
    for i in range(first + 1, len(x)):
        out[i] = (out[i - 1] * (period - 1) + x[i]) / period
    return out


def true_range(high: FloatArray, low: FloatArray, close: FloatArray) -> FloatArray:
    high, low, close = _as_float(high), _as_float(low), _as_float(close)
    prev_close = np.concatenate(([np.nan], close[:-1]))
    tr = np.maximum(high - low, np.maximum(np.abs(high - prev_close), np.abs(low - prev_close)))
    tr[0] = high[0] - low[0]
    return tr


def atr(high: FloatArray, low: FloatArray, close: FloatArray, period: int) -> FloatArray:
    """Average True Range (Wilder)."""
    return wilder_rma(true_range(high, low, close), period)


def adx(high: FloatArray, low: FloatArray, close: FloatArray, period: int) -> tuple[FloatArray, FloatArray, FloatArray]:
    """Average Directional Index (Wilder). Return (adx, +DI, -DI)."""
    high, low, close = _as_float(high), _as_float(low), _as_float(close)
    n = len(close)
    nan = np.full(n, np.nan)
    if n < period * 2 + 1:
        return nan.copy(), nan.copy(), nan.copy()
    up = np.diff(high, prepend=np.nan)
    down = -np.diff(low, prepend=np.nan)
    plus_dm = np.where((up > down) & (up > 0), up, 0.0)
    minus_dm = np.where((down > up) & (down > 0), down, 0.0)
    plus_dm[0] = minus_dm[0] = np.nan
    tr = true_range(high, low, close)
    tr[0] = np.nan
    atr_w = wilder_rma(tr, period, start=1)
    plus_sm = wilder_rma(plus_dm, period, start=1)
    minus_sm = wilder_rma(minus_dm, period, start=1)
    with np.errstate(divide="ignore", invalid="ignore"):
        plus_di = 100.0 * plus_sm / atr_w
        minus_di = 100.0 * minus_sm / atr_w
        denom = plus_di + minus_di
        dx = np.where(denom > 0, 100.0 * np.abs(plus_di - minus_di) / denom, 0.0)
    dx[np.isnan(plus_di)] = np.nan
    adx_out = wilder_rma(dx, period, start=period)
    return adx_out, plus_di, minus_di


def rolling_max(x: FloatArray, period: int) -> FloatArray:
    """Maksimum bergulir inklusif bar ``i`` (window ``[i-period+1..i]``)."""
    x = _as_float(x)
    out = np.full_like(x, np.nan)
    if len(x) >= period > 0:
        out[period - 1:] = sliding_window_view(x, period).max(axis=1)
    return out


def rolling_min(x: FloatArray, period: int) -> FloatArray:
    x = _as_float(x)
    out = np.full_like(x, np.nan)
    if len(x) >= period > 0:
        out[period - 1:] = sliding_window_view(x, period).min(axis=1)
    return out


def shift(x: FloatArray, n: int = 1) -> FloatArray:
    """Geser ke kanan ``n`` bar (nilai bar sebelumnya), diisi NaN di awal."""
    x = _as_float(x)
    out = np.full_like(x, np.nan)
    if n < len(x):
        out[n:] = x[:-n] if n > 0 else x
    return out


def efficiency_ratio(close: FloatArray, period: int) -> FloatArray:
    """Kaufman Efficiency Ratio: |perubahan neto| / jumlah |perubahan| (0=choppy, 1=trend murni)."""
    close = _as_float(close)
    out = np.full_like(close, np.nan)
    if len(close) <= period:
        return out
    net = np.abs(close[period:] - close[:-period])
    abs_diff = np.abs(np.diff(close))
    path = sliding_window_view(abs_diff, period).sum(axis=1)
    with np.errstate(divide="ignore", invalid="ignore"):
        out[period:] = np.where(path > 0, net / path, 0.0)
    return out


def close_location(high: FloatArray, low: FloatArray, close: FloatArray) -> FloatArray:
    """Posisi close dalam range bar (0 = di low, 1 = di high). Filter false-wick breakout."""
    high, low, close = _as_float(high), _as_float(low), _as_float(close)
    rng = high - low
    with np.errstate(divide="ignore", invalid="ignore"):
        return np.where(rng > 0, (close - low) / rng, 0.5)
