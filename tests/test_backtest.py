"""Test invariant backtest engine (bukan klaim profitabilitas)."""

from __future__ import annotations

import pytest

from backtest.engine import BacktestEngine
from config import AppConfig
from scripts.make_synthetic_data import generate


@pytest.fixture
def cfg(monkeypatch: pytest.MonkeyPatch) -> AppConfig:
    monkeypatch.setenv("TRADING_MODE", "BACKTEST")
    monkeypatch.setenv("SYMBOLS", "BTC_USDT,ETH_USDT")
    return AppConfig.load(env_file="/nonexistent.env")


@pytest.fixture
def data() -> dict:
    return {"BTC_USDT": generate(6000, 30000.0, seed=7), "ETH_USDT": generate(6000, 2000.0, seed=11)}


def test_accounting_is_consistent(cfg: AppConfig, data: dict) -> None:
    res = BacktestEngine(cfg, data).run()
    assert res.metrics["trades"] > 0
    final = res.equity_curve[-1][1]
    assert final == pytest.approx(cfg.backtest.initial_equity + sum(t.pnl for t in res.trades), rel=1e-9)


def test_costs_reduce_returns(cfg: AppConfig, data: dict) -> None:
    with_costs = BacktestEngine(cfg, data).run()
    no_costs = BacktestEngine(cfg, data, costs=cfg.costs.zeroed()).run()
    assert with_costs.metrics["total_fees_and_tax"] > 0
    assert no_costs.metrics["total_fees_and_tax"] == 0
    assert with_costs.metrics["final_equity"] < no_costs.metrics["final_equity"]


def test_risk_limits_respected(cfg: AppConfig, data: dict) -> None:
    res = BacktestEngine(cfg, data).run()
    worst_r = min(t.r_multiple for t in res.trades)
    assert worst_r > -1.6, "loss per trade jauh melebihi 1R (hanya gap ekstrem yang boleh)"
    # posisi bersamaan tidak melebihi batas
    events = sorted([(t.entry_time, 1) for t in res.trades] + [(t.exit_time, -1) for t in res.trades])
    open_n = peak = 0
    for _, d in events:
        open_n += d
        peak = max(peak, open_n)
    assert peak <= cfg.risk.max_open_positions


def test_regime_filter_keeps_bot_mostly_flat_in_bad_regimes(cfg: AppConfig, data: dict) -> None:
    res = BacktestEngine(cfg, data).run()
    counts = res.regime_counts
    standby = sum(v for k, v in counts.items() if k != "TRENDING_UP")
    assert standby > counts.get("TRENDING_UP", 0)


def test_research_mode_keeps_trading_after_drawdown(cfg: AppConfig, data: dict, monkeypatch: pytest.MonkeyPatch) -> None:
    """Regresi: hard halt di mode guarded menghentikan trading permanen; mode riset tidak."""
    from dataclasses import replace
    tight = replace(cfg, risk=replace(cfg.risk, max_total_drawdown=0.03, daily_max_drawdown=0.02),
                    strategy=replace(cfg.strategy, max_cost_to_risk=1.0))
    guarded = BacktestEngine(tight, data, circuit_breakers=True).run()
    research = BacktestEngine(tight, data, circuit_breakers=False).run()
    assert guarded.metrics["halted_at"] is not None
    assert research.metrics["halted_at"] is None and research.risk_events == []
    assert research.metrics["trades"] > guarded.metrics["trades"]


def test_cost_diagnostics_consistent(cfg: AppConfig, data: dict) -> None:
    m = BacktestEngine(cfg, data, circuit_breakers=False).run().metrics
    if m["trades"]:
        assert m["gross_expectancy_r"] == pytest.approx(m["expectancy_r"] + m["cost_per_trade_r"], abs=1e-3)
        assert sum(y["trades"] for y in m["by_year"].values()) == m["trades"]
