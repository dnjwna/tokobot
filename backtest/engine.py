"""Backtest portofolio bar-by-bar memakai strategi & risk manager yang SAMA dengan live.

Model eksekusi (konservatif, tanpa look-ahead):
* Sinyal dievaluasi pada close bar ``t``; entry di OPEN bar ``t+1`` x (1 + slippage).
* Stop & TP1 dihitung ulang dari harga fill aktual (sama seperti live).
* Intrabar: gap di bawah stop -> keluar di open; low <= stop -> keluar di stop;
  bila stop dan TP1 sama-sama tersentuh dalam satu bar, diasumsikan STOP lebih dulu.
* Trailing stop hanya diperbarui setelah bar close dan berlaku mulai bar berikutnya.
* Biaya: taker fee + pajak (beli & jual) + slippage di setiap leg.
* Kill-switch harian / hard halt / loss streak dievaluasi pada mark-to-market tiap bar.

Jalankan: ``python main.py --mode BACKTEST`` atau ``python -m backtest.engine --help``.
"""

from __future__ import annotations

import argparse
import asyncio
import csv
import json
import logging
import math
import sys
from collections import Counter
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Optional

import numpy as np

if __package__ in (None, ""):
    sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from config import AppConfig, CostConfig
from core.models import Candle, Position, TradeRecord
from data.history import csv_path, filter_range, load_for_interval, parse_date_ms, save_csv
from risk.risk_manager import RiskManager, RiskState
from strategy.momentum_regime import Action, Features, MomentumRegimeStrategy

BARS_PER_YEAR = {"1m": 525600, "3m": 175200, "5m": 105120, "15m": 35040, "30m": 17520, "1h": 8760,
                 "2h": 4380, "4h": 2190, "6h": 1460, "8h": 1095, "12h": 730, "1d": 365}


@dataclass
class BacktestResult:
    trades: list[TradeRecord]
    equity_curve: list[tuple[int, float]]
    metrics: dict[str, Any]
    regime_counts: dict[str, int] = field(default_factory=dict)
    risk_events: list[dict[str, Any]] = field(default_factory=list)


class BacktestEngine:
    """Simulasi historis multi-simbol dengan satu akun kas bersama."""

    def __init__(self, cfg: AppConfig, data: dict[str, list[Candle]], costs: Optional[CostConfig] = None,
                 circuit_breakers: bool = True) -> None:
        """``circuit_breakers=False`` = mode riset: kill-switch/halt/pause dimatikan agar edge
        strategi terukur di seluruh periode (bukan berhenti di halt pertama)."""
        if not data:
            raise ValueError("Data backtest kosong")
        self.cfg = cfg
        self.costs = costs or cfg.costs
        self.data = data
        self.strategy = MomentumRegimeStrategy(cfg.strategy, self.costs)
        self.circuit_breakers = circuit_breakers
        self.risk = RiskManager(cfg.risk, self.costs, RiskState(), kill_file=None, circuit_breakers=circuit_breakers)

    # ------------------------------------------------------------------ #
    def run(self) -> BacktestResult:
        cfg, costs = self.cfg, self.costs
        features: dict[str, Features] = {s: self.strategy.compute_features(c) for s, c in self.data.items()}
        index: dict[str, dict[int, int]] = {s: {c.open_time: i for i, c in enumerate(cs)} for s, cs in self.data.items()}
        timeline = sorted({c.open_time for cs in self.data.values() for c in cs})

        cash = cfg.backtest.initial_equity
        positions: dict[str, Position] = {}
        pending: dict[str, Any] = {}
        last_close: dict[str, float] = {}
        trades: list[TradeRecord] = []
        legs: dict[str, list[dict[str, Any]]] = {}
        slip_cost: dict[str, float] = {}
        halted_at: Optional[int] = None
        equity_curve: list[tuple[int, float]] = []
        regimes: Counter[str] = Counter()
        risk_events: list[dict[str, Any]] = []
        sell_cost = costs.taker_fee + costs.tax_rate_sell
        buy_cost = costs.taker_fee + costs.tax_rate_buy

        def mark_equity() -> float:
            return cash + sum(p.qty * last_close.get(s, p.entry_price) for s, p in positions.items())

        def sell(symbol: str, qty: float, raw_px: float, t: int, reason: str) -> None:
            nonlocal cash
            pos = positions[symbol]
            qty = min(qty, pos.qty)
            px = raw_px * (1.0 - costs.slippage_pct)
            slip_cost[symbol] = slip_cost.get(symbol, 0.0) + qty * raw_px * costs.slippage_pct
            gross = qty * px
            fee = gross * sell_cost
            proceeds = gross - fee
            pnl = proceeds - qty * pos.entry_price
            cash += proceeds
            pos.qty -= qty
            pos.realized_pnl += pnl
            pos.fees_paid += fee
            legs.setdefault(symbol, []).append({"time": t, "qty": qty, "price": px, "pnl": pnl, "reason": reason})
            if pos.qty <= pos.initial_qty * 1e-9:
                symbol_legs = legs.pop(symbol, [])
                exit_qty = sum(l["qty"] for l in symbol_legs)
                exit_px = sum(l["qty"] * l["price"] for l in symbol_legs) / exit_qty if exit_qty else px
                risk_amt = pos.initial_qty * pos.risk_per_unit
                trades.append(TradeRecord(symbol, pos.entry_time, t, pos.entry_price, exit_px, pos.initial_qty,
                                          pos.realized_pnl, pos.realized_pnl / risk_amt if risk_amt > 0 else 0.0,
                                          pos.fees_paid, reason, symbol_legs, risk_amount=risk_amt,
                                          initial_stop=pos.initial_stop, slippage_cost=slip_cost.pop(symbol, 0.0)))
                for ev in self.risk.record_trade_result(pos.realized_pnl, t):
                    risk_events.append({"time": t, "kind": ev.kind, "message": ev.message})
                del positions[symbol]

        for t in timeline:
            # 1) Eksekusi pending entry di open, lalu kelola posisi intrabar
            for symbol, cs in self.data.items():
                i = index[symbol].get(t)
                if i is None:
                    continue
                bar = cs[i]
                f = features[symbol]
                dec = pending.pop(symbol, None)
                if dec is not None:
                    ok, _ = self.risk.can_open(symbol, set(positions), t)
                    if ok:
                        fill = bar.open * (1.0 + costs.slippage_pct)
                        stop, tp1 = self.strategy.rebase_levels(fill, dec.atr)
                        sizing = self.risk.size_position(mark_equity(), cash, bar.open, stop)
                        if sizing.ok:
                            qty = float(sizing.qty)
                            fee = qty * fill * buy_cost
                            slip_cost[symbol] = qty * bar.open * costs.slippage_pct
                            cash -= qty * fill + fee
                            positions[symbol] = Position(
                                symbol=symbol, entry_price=(qty * fill + fee) / qty, qty=qty, initial_qty=qty,
                                stop_price=stop, initial_stop=stop, tp1_price=tp1,
                                risk_per_unit=self.risk.risk_per_unit(bar.open, stop), entry_time=t,
                                entry_bar_time=dec.bar_time, highest_high=bar.open, cost_basis_quote=qty * fill + fee,
                                fees_paid=fee)
                pos = positions.get(symbol)
                if pos is not None:
                    if bar.open <= pos.stop_price:
                        sell(symbol, pos.qty, bar.open, t, "stop_gap")
                    elif bar.low <= pos.stop_price:
                        sell(symbol, pos.qty, pos.stop_price, t, "stop_loss" if not pos.tp1_done else "trailing_stop")
                    elif not pos.tp1_done and bar.high >= pos.tp1_price:
                        part = pos.initial_qty * cfg.strategy.tp1_fraction
                        pos.tp1_done = True
                        if part > 0:
                            sell(symbol, part, pos.tp1_price, t, "tp1")
                        if symbol in positions:
                            pos.stop_price = max(pos.stop_price, self.strategy.breakeven_price(pos))
                last_close[symbol] = bar.close
                pos = positions.get(symbol)
                if pos is not None:
                    pos.highest_high = max(pos.highest_high, bar.high)
                    pos.stop_price = self.strategy.trailing_stop(pos, f, i)
                    if bar.close <= pos.stop_price:
                        sell(symbol, pos.qty, bar.close, t, "trailing_close")

            # 2) Mark-to-market & kill-switch
            equity = mark_equity()
            for ev in self.risk.update_equity(equity, t):
                risk_events.append({"time": t, "kind": ev.kind, "message": ev.message})
                if ev.kind == "HARD_HALT" and halted_at is None:
                    halted_at = t
                if ev.flatten:
                    for sym in list(positions):
                        sell(sym, positions[sym].qty, last_close[sym], t, f"kill_{ev.kind.lower()}")
                    equity = mark_equity()
            equity_curve.append((t, equity))

            # 3) Sinyal di close bar -> pending untuk open bar berikutnya
            for symbol, cs in self.data.items():
                i = index[symbol].get(t)
                if i is None or symbol in positions:
                    continue
                dec = self.strategy.signal_at(features[symbol], i, symbol)
                regimes[dec.regime.value] += 1
                if dec.action is Action.ENTER_LONG and i + 1 < len(cs):
                    ok, _ = self.risk.can_open(symbol, set(positions) | set(pending), t)
                    if ok:
                        pending[symbol] = dec

        # Tutup posisi tersisa di close terakhir (agar metrik lengkap)
        if positions and timeline:
            t_end = timeline[-1]
            for sym in list(positions):
                sell(sym, positions[sym].qty, last_close[sym], t_end, "end_of_data")
            equity_curve[-1] = (t_end, cash)

        metrics = compute_metrics(trades, equity_curve, cfg.backtest.initial_equity,
                                  BARS_PER_YEAR.get(cfg.strategy.timeframe, 8760))
        metrics["buy_and_hold"] = {s: round(cs[-1].close / cs[0].open - 1.0, 4) for s, cs in self.data.items() if cs}
        metrics["circuit_breakers"] = self.circuit_breakers
        metrics["halted_at"] = _iso(halted_at) if halted_at else None
        metrics["data_start"] = _iso(timeline[0]) if timeline else None
        metrics["data_end"] = _iso(timeline[-1]) if timeline else None
        return BacktestResult(trades, equity_curve, metrics, dict(regimes), risk_events)


# --------------------------------------------------------------------------- #
# Metrik
# --------------------------------------------------------------------------- #
def _iso(ms: int) -> str:
    return datetime.fromtimestamp(ms / 1000, tz=timezone.utc).strftime("%Y-%m-%d %H:%M")


def _year(ms: int) -> int:
    return datetime.fromtimestamp(ms / 1000, tz=timezone.utc).year


def compute_metrics(trades: list[TradeRecord], equity_curve: list[tuple[int, float]], initial: float,
                    bars_per_year: int) -> dict[str, Any]:
    """Metrik performa standar + metrik berbasis R + diagnostik biaya + breakdown tahunan."""
    if not equity_curve:
        return {"trades": 0}
    eq = np.array([e for _, e in equity_curve], dtype=np.float64)
    rets = np.diff(eq) / eq[:-1] if len(eq) > 1 else np.array([0.0])
    peak = np.maximum.accumulate(eq)
    dd = 1.0 - eq / peak
    years = max((equity_curve[-1][0] - equity_curve[0][0]) / (365.25 * 24 * 3600 * 1000), 1e-9)
    total_ret = eq[-1] / initial - 1.0
    std = rets.std(ddof=1) if len(rets) > 1 else 0.0
    downside = rets[rets < 0]
    dstd = downside.std(ddof=1) if len(downside) > 1 else 0.0
    pnls = np.array([t.pnl for t in trades], dtype=np.float64)
    rs = np.array([t.r_multiple for t in trades], dtype=np.float64)
    wins, losses = pnls[pnls > 0], pnls[pnls <= 0]
    gross_win, gross_loss = wins.sum(), -losses.sum()
    hold_hours = [(t.exit_time - t.entry_time) / 3.6e6 for t in trades]
    exits = Counter(t.exit_reason for t in trades)
    with_risk = [t for t in trades if t.risk_amount > 0]
    cost_r = [(t.fees + t.slippage_cost) / t.risk_amount for t in with_risk]
    gross_r = [(t.pnl + t.fees + t.slippage_cost) / t.risk_amount for t in with_risk]
    stop_pct = [(t.entry_price - t.initial_stop) / t.entry_price for t in trades if t.initial_stop > 0]

    by_year: dict[int, dict[str, Any]] = {}
    for ts, e in equity_curve:
        y = by_year.setdefault(_year(ts), {"start_equity": e, "end_equity": e, "trades": 0, "pnl": 0.0, "r": []})
        y["end_equity"] = e
    for t in trades:
        y = by_year.setdefault(_year(t.exit_time), {"start_equity": 0.0, "end_equity": 0.0, "trades": 0,
                                                    "pnl": 0.0, "r": []})
        y["trades"] += 1
        y["pnl"] += t.pnl
        y["r"].append(t.r_multiple)
    yearly = {str(k): {"return": round(v["end_equity"] / v["start_equity"] - 1.0, 4) if v["start_equity"] else 0.0,
                       "trades": v["trades"], "pnl": round(v["pnl"], 2),
                       "expectancy_r": round(float(np.mean(v["r"])), 3) if v["r"] else 0.0}
              for k, v in sorted(by_year.items())}
    return {
        "initial_equity": round(initial, 2),
        "final_equity": round(float(eq[-1]), 2),
        "total_return": round(float(total_ret), 4),
        "cagr": round(float((eq[-1] / initial) ** (1 / years) - 1.0), 4) if eq[-1] > 0 else -1.0,
        "max_drawdown": round(float(dd.max()), 4),
        "sharpe": round(float(rets.mean() / std * math.sqrt(bars_per_year)), 3) if std > 0 else 0.0,
        "sortino": round(float(rets.mean() / dstd * math.sqrt(bars_per_year)), 3) if dstd > 0 else 0.0,
        "trades": len(trades),
        "win_rate": round(float(len(wins) / len(pnls)), 4) if len(pnls) else 0.0,
        "profit_factor": round(float(gross_win / gross_loss), 3) if gross_loss > 0 else (math.inf if gross_win > 0 else 0.0),
        "expectancy_r": round(float(rs.mean()), 4) if len(rs) else 0.0,
        "gross_expectancy_r": round(float(np.mean(gross_r)), 4) if gross_r else 0.0,
        "cost_per_trade_r": round(float(np.mean(cost_r)), 4) if cost_r else 0.0,
        "median_stop_pct": round(float(np.median(stop_pct)), 4) if stop_pct else 0.0,
        "avg_win": round(float(wins.mean()), 4) if len(wins) else 0.0,
        "avg_loss": round(float(losses.mean()), 4) if len(losses) else 0.0,
        "total_fees_and_tax": round(float(sum(t.fees for t in trades)), 4),
        "total_slippage": round(float(sum(t.slippage_cost for t in trades)), 4),
        "avg_hold_hours": round(float(np.mean(hold_hours)), 2) if hold_hours else 0.0,
        "exit_reasons": dict(exits),
        "years": round(years, 3),
        "by_year": yearly,
    }


def split_metrics(result: BacktestResult, split: float, initial: float, bars_per_year: int) -> dict[str, Any]:
    """Pisahkan metrik in-sample vs out-of-sample berdasarkan waktu (uji stabilitas)."""
    if not result.equity_curve or not 0 < split < 1:
        return {}
    t0, t1 = result.equity_curve[0][0], result.equity_curve[-1][0]
    cut = t0 + (t1 - t0) * split
    out: dict[str, Any] = {"cut": _iso(int(cut))}
    for name, cond in (("in_sample", lambda t: t < cut), ("out_of_sample", lambda t: t >= cut)):
        curve = [(t, e) for t, e in result.equity_curve if cond(t)]
        trades = [tr for tr in result.trades if cond(tr.entry_time)]
        if curve:
            out[name] = compute_metrics(trades, curve, curve[0][1], bars_per_year)
    return out


# --------------------------------------------------------------------------- #
# IO & CLI
# --------------------------------------------------------------------------- #
def write_results(result: BacktestResult, out_dir: Path, tag: str) -> Path:
    out_dir.mkdir(parents=True, exist_ok=True)
    with (out_dir / f"trades_{tag}.csv").open("w", newline="", encoding="utf-8") as fh:
        writer = csv.writer(fh)
        writer.writerow(["symbol", "entry_time", "exit_time", "entry_price", "exit_price", "qty", "pnl",
                         "r_multiple", "fees", "slippage", "risk_amount", "initial_stop", "exit_reason"])
        for t in result.trades:
            writer.writerow([t.symbol, _iso(t.entry_time), _iso(t.exit_time), f"{t.entry_price:.8f}",
                             f"{t.exit_price:.8f}", f"{t.qty:.8f}", f"{t.pnl:.6f}", f"{t.r_multiple:.4f}",
                             f"{t.fees:.6f}", f"{t.slippage_cost:.6f}", f"{t.risk_amount:.6f}",
                             f"{t.initial_stop:.8f}", t.exit_reason])
    with (out_dir / f"equity_{tag}.csv").open("w", newline="", encoding="utf-8") as fh:
        writer = csv.writer(fh)
        writer.writerow(["time", "equity"])
        writer.writerows(result.equity_curve)
    summary = out_dir / f"summary_{tag}.json"
    summary.write_text(json.dumps({"metrics": result.metrics, "regime_counts": result.regime_counts,
                                   "risk_events": result.risk_events}, indent=2, default=str), encoding="utf-8")
    return summary


def load_data(cfg: AppConfig, symbols: tuple[str, ...]) -> dict[str, list[Candle]]:
    """Muat CSV per simbol; timeframe > 1h di-resample otomatis dari CSV 1h bila perlu."""
    data: dict[str, list[Candle]] = {}
    for sym in symbols:
        candles = filter_range(load_for_interval(cfg.backtest.data_dir, sym, cfg.strategy.timeframe),
                               cfg.backtest.start, cfg.backtest.end)
        if len(candles) < 500:
            raise ValueError(f"{sym}: hanya {len(candles)} bar {cfg.strategy.timeframe}; butuh >= 500")
        data[sym] = candles
    return data


async def download(cfg: AppConfig, symbols: tuple[str, ...], start: str, end: str) -> None:
    """Unduh kline dari market data Tokocrypto (symbolType 1) ke CSV."""
    from exchange.tokocrypto_client import TokocryptoClient

    async with TokocryptoClient(cfg.exchange) as client:
        await client.sync_time()
        rules = await client.exchange_info()
        end_ms = parse_date_ms(end) if end else client.timestamp_ms()
        for sym in symbols:
            if sym not in rules:
                raise ValueError(f"Simbol {sym} tidak ditemukan di exchange info")
            candles = await client.klines_history(rules[sym], cfg.strategy.timeframe, parse_date_ms(start), end_ms)
            n = save_csv(csv_path(cfg.backtest.data_dir, sym, cfg.strategy.timeframe), candles)
            print(f"{sym}: {n} candle disimpan")


_CORE_KEYS = ("total_return", "cagr", "max_drawdown", "sharpe", "trades", "win_rate", "profit_factor",
              "expectancy_r", "gross_expectancy_r", "cost_per_trade_r", "median_stop_pct",
              "total_fees_and_tax", "total_slippage", "avg_hold_hours")


def print_report(research: BacktestResult, gross: Optional[BacktestResult], guarded: BacktestResult) -> None:
    m = research.metrics
    print(f"\nData: {m.get('data_start')} -> {m.get('data_end')} ({m.get('years')} thn)")
    print("\n=== [1] EDGE STRATEGI — biaya ON, circuit breaker OFF, seluruh periode ===")
    for key in _CORE_KEYS:
        print(f"  {key:<20} {m.get(key)}")
    print(f"  {'exit_reasons':<20} {m.get('exit_reasons')}")
    print(f"  {'buy_and_hold':<20} {m.get('buy_and_hold')}")
    total_bars = sum(research.regime_counts.values()) or 1
    print("  regime (bar tanpa posisi): " + ", ".join(
        f"{k}={v / total_bars:.0%}" for k, v in sorted(research.regime_counts.items())))
    print("  per tahun:")
    for year, y in m.get("by_year", {}).items():
        print(f"    {year}: return={y['return']:+.2%} trades={y['trades']:<3} pnl={y['pnl']:+.2f} expR={y['expectancy_r']:+.3f}")
    split = m.get("split", {})
    for name in ("in_sample", "out_of_sample"):
        mm = split.get(name)
        if mm:
            flag = "  <-- 0 trade, tidak bisa dinilai" if mm["trades"] == 0 else ""
            print(f"  [{name}] return={mm['total_return']:+.2%} maxDD={mm['max_drawdown']:.2%} trades={mm['trades']} "
                  f"PF={mm['profit_factor']} expR={mm['expectancy_r']}{flag}")
    if split.get("cut"):
        print(f"  (batas IS/OOS: {split['cut']})")
    if gross is not None:
        g = gross.metrics
        print("\n=== [2] TANPA BIAYA — circuit breaker OFF (sebanding dengan [1]) ===")
        print(f"  return={g['total_return']:+.2%} trades={g['trades']} PF={g['profit_factor']} expR={g['expectancy_r']} "
              f"| drag biaya vs [1] = {g['total_return'] - m['total_return']:+.2%}")
    gm = guarded.metrics
    print("\n=== [3] DENGAN RISK GUARD — perilaku live (daily kill, hard halt, loss-streak pause) ===")
    print(f"  return={gm['total_return']:+.2%} maxDD={gm['max_drawdown']:.2%} trades={gm['trades']} "
          f"PF={gm['profit_factor']} expR={gm['expectancy_r']} risk_events={len(guarded.risk_events)}")
    if gm.get("halted_at"):
        print(f"  !! HARD HALT pada {gm['halted_at']}: tidak ada entry setelah tanggal ini di [3]. "
              "Penilaian edge pakai [1].")
    print("\n=== Gate minimum sebelum PAPER (README §6) ===")
    oos = split.get("out_of_sample", {})
    checks = [
        ("trades >= 100", m["trades"] >= 100),
        ("profit_factor > 1.2", isinstance(m["profit_factor"], float) and m["profit_factor"] > 1.2),
        ("expectancy_r > 0.1", m["expectancy_r"] > 0.1),
        ("max_drawdown < 20%", m["max_drawdown"] < 0.20),
        ("OOS expectancy_r > 0 (>= 20 trade)", oos.get("trades", 0) >= 20 and oos.get("expectancy_r", 0) > 0),
    ]
    for label, ok in checks:
        print(f"  [{'LULUS' if ok else 'GAGAL'}] {label}")


def run_backtest_cli(cfg: AppConfig, symbols: tuple[str, ...], compare_costs: bool = True) -> BacktestResult:
    data = load_data(cfg, symbols)
    risk_log = logging.getLogger("tokobot.risk")
    previous = risk_log.level
    risk_log.setLevel(logging.ERROR)   # event risiko dicatat di summary JSON, bukan spam console
    try:
        research = BacktestEngine(cfg, data, circuit_breakers=False).run()
        gross = (BacktestEngine(cfg, data, costs=cfg.costs.zeroed(), circuit_breakers=False).run()
                 if compare_costs else None)
        guarded = BacktestEngine(cfg, data, circuit_breakers=True).run()
    finally:
        risk_log.setLevel(previous)
    bpy = BARS_PER_YEAR.get(cfg.strategy.timeframe, 8760)
    research.metrics["split"] = split_metrics(research, cfg.backtest.oos_split, cfg.backtest.initial_equity, bpy)
    tag = f"{'-'.join(symbols)}_{cfg.strategy.timeframe}"
    write_results(research, cfg.backtest.results_dir, f"{tag}_research")
    summary = write_results(guarded, cfg.backtest.results_dir, f"{tag}_guarded")
    print_report(research, gross, guarded)
    print(f"\nHasil disimpan: {summary.parent} (*_research.* dan *_guarded.*)")
    return research


def main(argv: Optional[list[str]] = None) -> int:
    parser = argparse.ArgumentParser(description="Backtest / unduh data TokoBot")
    parser.add_argument("--env", default=None, help="path file .env")
    parser.add_argument("--symbols", default=None, help="mis. BTC_USDT,ETH_USDT (default dari SYMBOLS)")
    parser.add_argument("--download", action="store_true", help="unduh kline dari Tokocrypto ke CSV")
    parser.add_argument("--start", default="2023-01-01")
    parser.add_argument("--end", default="")
    parser.add_argument("--no-compare-costs", action="store_true")
    args = parser.parse_args(argv)
    cfg = AppConfig.load(args.env, mode_override="BACKTEST")
    symbols = tuple(s.strip().upper() for s in args.symbols.split(",")) if args.symbols else cfg.runtime.symbols
    if args.download:
        asyncio.run(download(cfg, symbols, args.start, args.end))
        return 0
    run_backtest_cli(cfg, symbols, compare_costs=not args.no_compare_costs)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
