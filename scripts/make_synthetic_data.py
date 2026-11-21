"""Generator data SINTETIS (regime-switching) untuk smoke test pipeline backtest.

PERINGATAN: hasil backtest di data ini TIDAK menunjukkan profitabilitas strategi di
pasar nyata. Gunanya hanya memastikan mesin backtest, filter regime, biaya, dan
risk manager berjalan benar. Untuk evaluasi riil pakai data historis Tokocrypto
(``python -m backtest.engine --download``) atau data.binance.vision.
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

import numpy as np

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from core.models import Candle  # noqa: E402
from data.history import csv_path, save_csv  # noqa: E402


def generate(n_bars: int, start_price: float, seed: int, interval_ms: int = 3_600_000,
             start_ms: int = 1_672_531_200_000) -> list[Candle]:
    """Bangkitkan OHLCV dengan blok regime: uptrend, downtrend, choppy (mean-reverting)."""
    rng = np.random.default_rng(seed)
    candles: list[Candle] = []
    price = start_price
    t = start_ms
    i = 0
    while i < n_bars:
        regime = str(rng.choice(["up", "down", "chop", "chop"], p=[0.3, 0.25, 0.25, 0.2]))
        length = int(rng.integers(150, 600))
        drift = {"up": 0.0009, "down": -0.0008, "chop": 0.0}[regime]
        vol = {"up": 0.007, "down": 0.009, "chop": 0.006}[regime]
        anchor = price
        for _ in range(min(length, n_bars - i)):
            ret = drift + vol * rng.standard_t(5) / np.sqrt(5 / 3)
            if regime == "chop":
                ret += -0.05 * np.log(price / anchor)
            open_ = price
            close = max(open_ * float(np.exp(ret)), 1e-6)
            wick = abs(rng.normal(0, vol * 0.6))
            high = max(open_, close) * (1 + wick)
            low = min(open_, close) * (1 - abs(rng.normal(0, vol * 0.6)))
            volume = float(rng.lognormal(3.0, 0.4) * (1.5 if abs(ret) > 1.5 * vol else 1.0))
            candles.append(Candle(t, open_, high, low, close, volume, t + interval_ms - 1, True))
            price = close
            t += interval_ms
            i += 1
    return candles


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--out", default="data/historical")
    parser.add_argument("--bars", type=int, default=12000)
    parser.add_argument("--interval", default="1h")
    args = parser.parse_args()
    for sym, px, seed in (("BTC_USDT", 30000.0, 7), ("ETH_USDT", 2000.0, 11)):
        n = save_csv(csv_path(Path(args.out), sym, args.interval), generate(args.bars, px, seed))
        print(f"{sym}: {n} bar sintetis -> {csv_path(Path(args.out), sym, args.interval)}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
