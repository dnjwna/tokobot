"""Sweep kecil parameter struktural (mode riset: biaya ON, circuit breaker OFF).

Tujuannya mencari WILAYAH parameter yang stabil, bukan satu titik terbaik. Setiap
konfigurasi tambahan yang diuji menaikkan peluang menemukan hasil bagus secara
kebetulan — jadi pilih berdasarkan in-sample, lalu lihat OOS SEKALI.

    python scripts/sweep.py
    python scripts/sweep.py --timeframes 4h,1d --sl 2,3 --ctr 0.15,0.25
Timeframe > 1h di-resample dari CSV 1h bila CSV timeframe itu tidak ada.
"""

from __future__ import annotations

import argparse
import csv
import logging
import sys
from dataclasses import replace
from itertools import product
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from backtest.engine import BARS_PER_YEAR, BacktestEngine, load_data, split_metrics  # noqa: E402
from config import AppConfig  # noqa: E402


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--timeframes", default="1h,4h,1d")
    parser.add_argument("--sl", default="2,3", help="SL_ATR_MULT")
    parser.add_argument("--ctr", default="0.25,0.5", help="MAX_COST_TO_RISK")
    parser.add_argument("--symbols", default=None)
    parser.add_argument("--out", default="backtest/results/sweep.csv")
    args = parser.parse_args()
    logging.getLogger("tokobot").setLevel(logging.ERROR)
    base = AppConfig.load(mode_override="BACKTEST")
    symbols = tuple(s.strip().upper() for s in args.symbols.split(",")) if args.symbols else base.runtime.symbols

    rows = []
    cache: dict[str, dict] = {}
    for tf, sl, ctr in product(args.timeframes.split(","), [float(x) for x in args.sl.split(",")],
                               [float(x) for x in args.ctr.split(",")]):
        cfg = replace(base, strategy=replace(base.strategy, timeframe=tf, sl_atr_mult=sl, max_cost_to_risk=ctr))
        if tf not in cache:
            cache[tf] = load_data(cfg, symbols)
        res = BacktestEngine(cfg, cache[tf], circuit_breakers=False).run()
        split = split_metrics(res, cfg.backtest.oos_split, cfg.backtest.initial_equity, BARS_PER_YEAR[tf])
        m, is_m, oos = res.metrics, split.get("in_sample", {}), split.get("out_of_sample", {})
        row = {"tf": tf, "sl_atr": sl, "max_ctr": ctr, "trades": m["trades"], "ret": m["total_return"],
               "maxdd": m["max_drawdown"], "pf": m["profit_factor"], "expR": m["expectancy_r"],
               "grossR": m["gross_expectancy_r"], "costR": m["cost_per_trade_r"], "stop%": m["median_stop_pct"],
               "is_trades": is_m.get("trades", 0), "is_expR": is_m.get("expectancy_r", 0),
               "oos_trades": oos.get("trades", 0), "oos_expR": oos.get("expectancy_r", 0),
               "n_ok": m["trades"] >= 30}  # < 30 trade: angka belum bermakna secara statistik
        rows.append(row)
        print(" ".join(f"{k}={v}" for k, v in row.items()), flush=True)

    out = Path(args.out)
    out.parent.mkdir(parents=True, exist_ok=True)
    with out.open("w", newline="") as fh:
        writer = csv.DictWriter(fh, fieldnames=list(rows[0]))
        writer.writeheader()
        writer.writerows(rows)
    print(f"\n{len(rows)} konfigurasi diuji -> {out}. Pilih dari kolom is_* dulu; oos_* hanya untuk konfirmasi.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
