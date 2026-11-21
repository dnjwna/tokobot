"""Integration test LIVE & PAPER terhadap mock Tokocrypto (REST + WebSocket lokal)."""

from __future__ import annotations

import asyncio
import json
import time
from decimal import Decimal
from pathlib import Path

import pytest

from config import LIVE_CONFIRM_PHRASE, AppConfig
from core.models import Candle
from main import TradingBot
from scripts.make_synthetic_data import generate
from strategy.momentum_regime import Action, Decision, Regime
from tests.mock_exchange import API_KEY, API_SECRET, MockExchange, start_servers


@pytest.fixture
async def env(monkeypatch: pytest.MonkeyPatch, tmp_path: Path):
    candles = generate(1100, 30000.0, seed=5)
    ex = MockExchange(candles)
    runner, ws_server, http_port, ws_port = await start_servers(ex)
    base = f"http://127.0.0.1:{http_port}"
    for k, v in {
        "TOKO_REST_BASE": base, "TOKO_MARKET_BASE_TYPE1": base, "TOKO_WS_MARKET_TYPE1": f"ws://127.0.0.1:{ws_port}",
        "TOKO_API_KEY": API_KEY, "TOKO_API_SECRET": API_SECRET, "SYMBOLS": "BTC_USDT",
        "STATE_DIR": str(tmp_path / "state"), "LOG_DIR": str(tmp_path / "logs"), "KILL_FILE": str(tmp_path / "KILL"),
        "LIVE_CONFIRM": LIVE_CONFIRM_PHRASE, "RECONCILE_INTERVAL_S": "3600", "TOKO_MAX_RETRIES": "1",
    }.items():
        monkeypatch.setenv(k, v)
    yield ex, tmp_path
    ws_server.close()
    await runner.cleanup()


def _decision(bar_time: int, price: float) -> Decision:
    return Decision("BTC_USDT", bar_time, Action.ENTER_LONG, Regime.TRENDING_UP, price, atr=price * 0.01)


async def test_live_entry_stop_reconcile_and_unknown_state(env) -> None:
    ex, tmp = env
    cfg = AppConfig.load(env_file="/nonexistent.env", mode_override="LIVE")
    bot = TradingBot(cfg)
    await bot.setup()
    try:
        assert ex.bad_signatures == 0
        price = float(ex.price)

        # 1) Entry -> qty bersih (komisi dipotong di base) + stop di bursa
        await bot.enter("BTC_USDT", _decision(1, price))
        pos = bot.positions["BTC_USDT"]
        buy = [o for o in ex.orders.values() if o.side == 0][0]
        assert pos.qty == pytest.approx(float(buy.executed_qty * Decimal("0.999")))
        assert pos.stop_order_id is not None
        assert ex.balances["BTC"][1] == bot.rules["BTC_USDT"].floor_qty(pos.qty)
        equity0 = 1000.0
        loss_at_stop = pos.initial_qty * pos.risk_per_unit
        assert loss_at_stop <= equity0 * cfg.risk.risk_per_trade * 1.02

        # 2) Stop tereksekusi di bursa saat bot "tidak melihat" -> reconcile menutup posisi
        ex.trigger_stop(int(pos.stop_order_id), Decimal(str(pos.stop_price)))
        await bot.reconcile()
        assert "BTC_USDT" not in bot.positions
        trades = [json.loads(line) for line in (tmp / "state" / "trades_live.jsonl").read_text().splitlines()]
        assert len(trades) == 1 and trades[0]["pnl"] < 0 and trades[0]["exit_reason"] == "stop_loss_exchange"

        # 3) HTTP 502 saat create order -> status unknown -> ditemukan via clientId, TANPA double entry
        ex.fail_next_order_with_500 = True
        bot.last_price["BTC_USDT"] = (price, time.monotonic())
        await bot.enter("BTC_USDT", _decision(7_200_000, price))
        buys = [o for o in ex.orders.values() if o.side == 0]
        assert len(buys) == 2 and len({o.client_id for o in buys}) == 2
        assert "BTC_USDT" in bot.positions and bot.positions["BTC_USDT"].stop_order_id

        # 4) TP1 parsial: harga melewati TP1 -> jual sebagian, stop dipasang ulang >= break-even
        pos = bot.positions["BTC_USDT"]
        old_stop_id = pos.stop_order_id
        assert old_stop_id is not None
        ex.price = Decimal(str(round(pos.tp1_price * 1.01, 2)))
        await bot.check_intrabar("BTC_USDT", float(ex.price))
        pos = bot.positions["BTC_USDT"]
        assert pos.tp1_done and pos.qty < pos.initial_qty
        assert ex.orders[int(old_stop_id)].status == 3  # dibatalkan
        assert pos.stop_order_id is not None and pos.stop_order_id != old_stop_id
        assert pos.stop_price >= pos.entry_price
        assert ex.balances["BTC"][1] == bot.rules["BTC_USDT"].floor_qty(pos.qty)

        # 5) Reconcile idempoten: tidak membuat order baru bila semuanya sinkron
        n_orders = len(ex.orders)
        await bot.reconcile()
        assert len(ex.orders) == n_orders
        assert ex.bad_signatures == 0
    finally:
        await bot.client.close()


async def test_paper_mode_full_loop_over_websocket(env) -> None:
    ex, tmp = env
    cfg = AppConfig.load(env_file="/nonexistent.env", mode_override="PAPER")
    bot = TradingBot(cfg)
    task = asyncio.create_task(bot.run())
    for _ in range(100):
        await asyncio.sleep(0.05)
        if bot.candles.get("BTC_USDT"):
            break
    assert bot.candles["BTC_USDT"], "setup gagal"
    await asyncio.sleep(0.3)  # tunggu WS tersambung
    price = float(ex.price)
    bot.last_price["BTC_USDT"] = (price, time.monotonic())
    await bot.enter("BTC_USDT", _decision(3, price))
    pos = bot.positions["BTC_USDT"]
    assert pos.stop_order_id is None  # paper: soft stop

    last = bot.candles["BTC_USDT"][-1]
    tick = Candle(last.open_time + 3_600_000, price, price, pos.stop_price * 0.99, pos.stop_price * 0.995, 1.0,
                  last.close_time + 3_600_000, closed=False)
    ex.push_kline(tick)
    for _ in range(60):
        await asyncio.sleep(0.05)
        if "BTC_USDT" not in bot.positions:
            break
    assert "BTC_USDT" not in bot.positions, "soft stop via WS tidak tereksekusi"
    closed_bar = Candle(tick.open_time, tick.open, tick.high, tick.low, tick.close, 2.0, tick.close_time, closed=True)
    ex.push_kline(closed_bar)
    await asyncio.sleep(0.3)
    assert bot.candles["BTC_USDT"][-1].open_time == closed_bar.open_time

    bot.stop_event.set()
    await asyncio.wait_for(task, timeout=10)
    state = json.loads((tmp / "state" / "state_paper.json").read_text())
    assert state["positions"] == {}
    assert state["broker"]["balances"]["USDT"] < 1000.0  # rugi + biaya tercatat
    trades = (tmp / "state" / "trades_paper.jsonl").read_text().splitlines()
    assert len(trades) == 1 and json.loads(trades[0])["exit_reason"] == "stop_loss"
