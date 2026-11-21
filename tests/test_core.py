"""Unit test: signing, precision, indikator, risk manager, strategi."""

from __future__ import annotations

import math
from decimal import Decimal
from pathlib import Path

import numpy as np
import pytest

from config import CostConfig, ExchangeConfig, RiskConfig, StrategyConfig
from core.models import Candle, Position
from data import indicators as ind
from exchange.tokocrypto_client import OrderInfo, PrecisionError, SymbolRules, TokocryptoClient, fmt_decimal
from risk.risk_manager import RiskManager, RiskState
from scripts.make_synthetic_data import generate
from strategy.momentum_regime import Action, MomentumRegimeStrategy, Regime

RULES_ITEM = {
    "type": 1, "symbol": "BTC_USDT", "baseAsset": "BTC", "quoteAsset": "USDT", "spotTradingEnable": 1,
    "orderTypes": [1, 2, 4],
    "filters": [
        {"filterType": "PRICE_FILTER", "minPrice": "0.01000000", "maxPrice": "1000000.00000000", "tickSize": "0.01000000"},
        {"filterType": "PERCENT_PRICE", "multiplierUp": 5, "multiplierDown": 0.2, "avgPriceMins": "5"},
        {"filterType": "LOT_SIZE", "minQty": "0.00001000", "maxQty": "9000.00000000", "stepSize": "0.00001000"},
        {"filterType": "NOTIONAL", "minNotional": "5.00000000", "applyToMarket": True},
    ],
}


@pytest.fixture
def rules() -> SymbolRules:
    return SymbolRules.from_api(RULES_ITEM)


# ------------------------------------------------------------------ signing
def test_hmac_signature_matches_reference_vector() -> None:
    """Vektor resmi skema MBX (HMAC SHA256 hex) yang juga dipakai Tokocrypto."""
    cfg = ExchangeConfig(api_key="k", api_secret="NhqPtmdSJYdKjVHjA7PZj4Mge3R5YNiP1e3UZjInClVN65XAbvqqM6A7H5fATj0j")
    client = TokocryptoClient(cfg)
    payload = ("symbol=LTCBTC&side=BUY&type=LIMIT&timeInForce=GTC&quantity=1&price=0.1"
               "&recvWindow=5000&timestamp=1499827319559")
    assert client.sign(payload) == "c8db56825ae71d6d79447849e617115f4a920fa2acdcab2b053c4b2838bd6b71"


# ---------------------------------------------------------------- precision
def test_rules_parse_and_enum_mapping(rules: SymbolRules) -> None:
    assert rules.market_symbol == "BTCUSDT"
    assert rules.tick_size == Decimal("0.01") and rules.step_size == Decimal("0.00001")
    assert rules.order_types == frozenset({"LIMIT", "MARKET", "STOP_LOSS_LIMIT"})
    assert rules.min_notional == Decimal("5")


def test_floor_qty_handles_float_noise(rules: SymbolRules) -> None:
    assert rules.floor_qty(0.0029999999999999996) == Decimal("0.00300")  # noise float di bawah kelipatan
    assert rules.floor_qty(0.123456789) == Decimal("0.12345")
    assert fmt_decimal(rules.floor_qty(0.1)) == "0.1"
    assert rules.floor_qty(-1) == 0


def test_round_price_direction(rules: SymbolRules) -> None:
    assert rules.round_price(100.019) == Decimal("100.01")
    assert rules.round_price(100.011, up=True) == Decimal("100.02")
    assert rules.round_price(100.01, up=True) == Decimal("100.01")


def test_validate_rejects_bad_orders(rules: SymbolRules) -> None:
    ref = Decimal("50000")
    with pytest.raises(PrecisionError, match="stepSize"):
        rules.validate("LIMIT", Decimal("0.000015"), Decimal("50000"), ref)
    with pytest.raises(PrecisionError, match="minNotional"):
        rules.validate("MARKET", Decimal("0.00001"), None, ref)
    with pytest.raises(PrecisionError, match="tickSize"):
        rules.validate("LIMIT", Decimal("0.001"), Decimal("50000.005"), ref)
    with pytest.raises(PrecisionError, match="multiplierUp"):
        rules.validate("LIMIT", Decimal("0.001"), Decimal("300000"), ref)
    with pytest.raises(PrecisionError, match="tidak didukung"):
        rules.validate("TAKE_PROFIT", Decimal("0.001"), None, ref)
    rules.validate("LIMIT", Decimal("0.001"), Decimal("50000.01"), ref)  # valid


def test_order_info_maps_numeric_enums() -> None:
    o = OrderInfo.from_api({"orderId": 5, "side": 1, "type": 4, "status": 2, "executedQty": "0.5",
                            "executedQuoteQty": "100", "price": "190"})
    assert (o.side, o.order_type, o.status, o.is_final) == ("SELL", "STOP_LOSS_LIMIT", "FILLED", True)
    assert o.avg_price == Decimal("200")


# --------------------------------------------------------------- indicators
def test_ema_seed_and_recursion() -> None:
    x = np.arange(1, 11, dtype=float)
    e = ind.ema(x, 3)
    assert math.isnan(e[1]) and e[2] == pytest.approx(2.0)
    assert e[3] == pytest.approx(0.5 * 4 + 0.5 * 2.0)


def test_atr_constant_range() -> None:
    h, lo, c = np.full(50, 11.0), np.full(50, 9.0), np.full(50, 10.0)
    assert ind.atr(h, lo, c, 14)[-1] == pytest.approx(2.0)


def test_efficiency_ratio_trend_vs_chop() -> None:
    trend = np.linspace(100, 120, 60)
    chop = 100 + np.tile([1.0, -1.0], 30)
    assert ind.efficiency_ratio(trend, 20)[-1] == pytest.approx(1.0)
    assert ind.efficiency_ratio(chop, 20)[-1] < 0.1


def test_adx_high_in_trend_low_in_chop() -> None:
    n = 300
    up = np.linspace(100, 200, n)
    adx_trend, pdi, mdi = ind.adx(up + 1, up - 1, up, 14)
    rng = np.random.default_rng(0)
    rw = 100 + np.cumsum(rng.normal(0, 0.5, n)) * 0 + rng.normal(0, 1, n)
    adx_chop, _, _ = ind.adx(rw + 1, rw - 1, rw, 14)
    assert adx_trend[-1] > 40 and pdi[-1] > mdi[-1]
    assert adx_chop[-1] < 30


def test_indicators_are_causal() -> None:
    """Nilai di indeks i tidak berubah bila data masa depan ditambahkan."""
    candles = generate(800, 100.0, seed=3)
    strat = MomentumRegimeStrategy(StrategyConfig(), CostConfig())
    full = strat.compute_features(candles)
    part = strat.compute_features(candles[:600])
    for name in ("ema_fast", "ema_slow", "adx", "atr", "donchian_prev_high", "er"):
        a, b = getattr(full, name)[:600], getattr(part, name)
        assert np.allclose(a, b, equal_nan=True), name


# ---------------------------------------------------------------- risk
def make_risk(**kw: object) -> RiskManager:
    return RiskManager(RiskConfig(**kw), CostConfig(), RiskState())  # type: ignore[arg-type]


def test_sizing_risk_includes_costs(rules: SymbolRules) -> None:
    rm = make_risk(max_position_pct=1.0)
    res = rm.size_position(1000.0, 1000.0, 100.0, 95.0, None)
    loss_at_stop = float(res.qty) * rm.risk_per_unit(100.0, 95.0)
    assert loss_at_stop == pytest.approx(10.0, rel=1e-6)
    assert rm.risk_per_unit(100.0, 95.0) > 5.0  # biaya + slippage menambah risiko per unit


def test_sizing_caps_and_min_notional(rules: SymbolRules) -> None:
    rm = make_risk(max_position_pct=0.30)
    res = rm.size_position(1000.0, 1000.0, 50000.0, 49900.0, rules)
    assert res.ok and "max_position_pct" in res.reason
    assert float(res.qty) * 50000 <= 300.0 + 1e-6
    tiny = rm.size_position(20.0, 20.0, 50000.0, 40000.0, rules)
    assert not tiny.ok and "min_notional" in tiny.reason
    assert not rm.size_position(1000, 1000, 100, 101, rules).ok


def test_daily_kill_switch_and_reset() -> None:
    rm = make_risk(daily_max_drawdown=0.03, max_total_drawdown=0.15, day_reset_tz="Asia/Jakarta")
    t0 = 1_700_000_000_000  # 2023-11-14 22:13 UTC = 05:13 WIB
    assert rm.update_equity(1000.0, t0) == []
    events = rm.update_equity(969.0, t0 + 60_000)
    assert [e.kind for e in events] == ["DAILY_KILL"]
    assert rm.can_open("BTC_USDT", set(), t0 + 120_000) == (False, "daily_kill_switch")
    nxt = rm.update_equity(969.0, t0 + 24 * 3600 * 1000)
    assert [e.kind for e in nxt] == ["DAY_RESET"]
    assert rm.can_open("BTC_USDT", set(), t0 + 24 * 3600 * 1000)[0]


def test_hard_halt_requires_manual_reset() -> None:
    rm = make_risk(daily_max_drawdown=0.5, max_total_drawdown=0.15)
    day = 24 * 3600 * 1000
    rm.update_equity(1000.0, 0)
    rm.update_equity(900.0, day)
    events = rm.update_equity(840.0, 2 * day)
    assert any(e.kind == "HARD_HALT" for e in events)
    assert not rm.can_open("X", set(), 10 * day)[0]
    rm.reset_halt()
    assert rm.can_open("X", set(), 10 * day)[0]


def test_loss_streak_pause_and_kill_file(tmp_path: Path) -> None:
    rm = RiskManager(RiskConfig(max_consecutive_losses=2, loss_streak_pause_hours=1), CostConfig(),
                     kill_file=tmp_path / "KILL")
    rm.record_trade_result(-1, 0)
    assert rm.record_trade_result(-1, 0)[0].kind == "LOSS_STREAK_PAUSE"
    assert rm.entry_block_reason(1000) == "loss_streak_pause"
    assert rm.entry_block_reason(3_600_001) is None
    (tmp_path / "KILL").touch()
    assert "manual_kill_file" in (rm.entry_block_reason(3_600_001) or "")


def test_max_open_positions() -> None:
    rm = make_risk(max_open_positions=1)
    assert rm.can_open("ETH_USDT", {"BTC_USDT"}, 0) == (False, "max_open_positions(1)")
    assert rm.can_open("BTC_USDT", {"BTC_USDT"}, 0) == (False, "position_already_open")


# ---------------------------------------------------------------- strategy
def _trend_breakout_series() -> list[Candle]:
    """Tren naik mulus panjang lalu konsolidasi singkat dan breakout bervolume."""
    candles: list[Candle] = []
    price, t = 100.0, 0
    for i in range(400):
        step = 1.006 if (i // 10) % 3 != 2 else 0.999  # naik bertahap dengan jeda kecil
        o, c = price, price * step
        candles.append(Candle(t, o, max(o, c) * 1.002, min(o, c) * 0.998, c, 100.0, t + 3_599_999))
        price, t = c, t + 3_600_000
    o = price
    c = max(x.high for x in candles[-20:]) * 1.01
    candles.append(Candle(t, o, c * 1.0005, o * 0.999, c, 300.0, t + 3_599_999))
    return candles


def test_strategy_enters_on_confirmed_trend_breakout() -> None:
    strat = MomentumRegimeStrategy(StrategyConfig(adx_min=20, er_min=0.2, max_cost_to_risk=1.0), CostConfig())
    candles = _trend_breakout_series()
    f = strat.compute_features(candles)
    dec = strat.signal_at(f, len(candles) - 1, "BTC_USDT")
    assert dec.regime is Regime.TRENDING_UP, dec.reasons
    assert dec.action is Action.ENTER_LONG, dec.reasons
    assert dec.stop < dec.close < dec.tp1


def test_strategy_standby_in_chop() -> None:
    strat = MomentumRegimeStrategy(StrategyConfig(), CostConfig())
    rng = np.random.default_rng(1)
    candles, t = [], 0
    for _ in range(500):
        c = 100 + rng.normal(0, 1.0)
        o = 100 + rng.normal(0, 1.0)
        candles.append(Candle(t, o, max(o, c) + 0.8, min(o, c) - 0.8, c, 100.0, t + 1))
        t += 3_600_000
    f = strat.compute_features(candles)
    actions = {strat.signal_at(f, i, "X").action for i in range(strat.warmup_bars, len(candles))}
    assert actions == {Action.STANDBY}


def test_cost_gate_blocks_thin_edge() -> None:
    expensive = CostConfig(taker_fee=0.01, tax_rate_buy=0.01, tax_rate_sell=0.01, slippage_pct=0.01)
    strat = MomentumRegimeStrategy(StrategyConfig(adx_min=20, er_min=0.2), expensive)
    candles = _trend_breakout_series()
    dec = strat.signal_at(strat.compute_features(candles), len(candles) - 1, "X")
    assert dec.action is Action.NONE and any("cost" in r for r in dec.reasons)


def test_trailing_stop_never_decreases_and_breakeven_after_tp1() -> None:
    strat = MomentumRegimeStrategy(StrategyConfig(), CostConfig())
    candles = _trend_breakout_series()
    f = strat.compute_features(candles)
    pos = Position("X", entry_price=100.0, qty=1, initial_qty=1, stop_price=98.0, initial_stop=98.0, tp1_price=103,
                   risk_per_unit=2.5, entry_time=0, entry_bar_time=0, highest_high=90.0, tp1_done=True)
    new = strat.trailing_stop(pos, f, len(candles) - 1)
    assert new >= strat.breakeven_price(pos) > 100.0


def test_cost_to_risk_gate_blocks_tight_stops() -> None:
    candles = _trend_breakout_series()
    tight = MomentumRegimeStrategy(StrategyConfig(adx_min=20, er_min=0.2, max_cost_to_risk=0.1), CostConfig())
    dec = tight.signal_at(tight.compute_features(candles), len(candles) - 1, "X")
    assert dec.action is Action.NONE and any("cost_to_risk" in r for r in dec.reasons)
    assert dec.metrics["cost_to_risk"] > 0.1


def test_resample_aligns_and_drops_incomplete_buckets() -> None:
    from data.history import resample
    hourly = generate(30, 100.0, seed=2, start_ms=1_672_531_200_000)  # mulai 00:00 UTC
    four = resample(hourly, "1h", "4h")
    assert len(four) == 7  # 30 jam -> 7 bucket penuh, sisa 2 jam dibuang
    first = four[0]
    assert first.open == hourly[0].open and first.close == hourly[3].close
    assert first.high == max(c.high for c in hourly[:4]) and first.low == min(c.low for c in hourly[:4])
    assert first.volume == pytest.approx(sum(c.volume for c in hourly[:4]))
    assert all(c.open_time % 14_400_000 == 0 for c in four)


def test_circuit_breakers_off_never_halts() -> None:
    rm = RiskManager(RiskConfig(), CostConfig(), circuit_breakers=False)
    rm.update_equity(1000.0, 0)
    assert rm.update_equity(500.0, 1000) == []
    assert rm.record_trade_result(-1, 0) == []
    assert rm.can_open("X", set(), 2000)[0]
