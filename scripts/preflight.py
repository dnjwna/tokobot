"""Pre-flight check sebelum PAPER/LIVE: konektivitas, jam, aturan simbol, data, akun, fee.

    python scripts/preflight.py            # publik saja
    python scripts/preflight.py --private  # + cek akun (butuh API key)
"""

from __future__ import annotations

import argparse
import asyncio
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from config import AppConfig  # noqa: E402
from exchange.tokocrypto_client import SUPPORTED_SYMBOL_TYPES, ExchangeError, TokocryptoClient  # noqa: E402


async def run(private: bool) -> int:
    cfg = AppConfig.load(mode_override="PAPER")
    ok = True
    async with TokocryptoClient(cfg.exchange) as client:
        offset = await client.sync_time()
        print(f"[{'OK' if abs(offset) < 1000 else 'WARN'}] offset jam lokal vs server: {offset} ms")
        rules = await client.exchange_info()
        for sym in cfg.runtime.symbols:
            r = rules.get(sym)
            if r is None:
                print(f"[FAIL] {sym} tidak ditemukan")
                ok = False
                continue
            supported = r.symbol_type in SUPPORTED_SYMBOL_TYPES
            ok &= supported and r.spot_enabled
            print(f"[{'OK' if supported else 'FAIL'}] {sym} type={r.symbol_type} tick={r.tick_size} step={r.step_size} "
                  f"minNotional={r.min_notional} STOP_LOSS_LIMIT={'ya' if r.supports('STOP_LOSS_LIMIT') else 'TIDAK'}")
            if supported:
                bars = await client.klines(r, cfg.strategy.timeframe, limit=5)
                _, asks = await client.depth(r, limit=5)
                print(f"      kline terakhir close={bars[-1].close} closed={bars[-1].closed} best_ask={asks[0][0] if asks else '-'}")
        if private:
            balances = await client.account()
            quote = balances.get(cfg.runtime.quote_asset)
            print(f"[OK] akun terbaca: {cfg.runtime.quote_asset} free={quote.free if quote else 0}")
            comm = await client.account_commission()
            taker_raw = comm.get("takerCommission", 0)
            # Format bisa fraksi (0.0015 = 0,15%) atau gaya Binance (10 = 0,10%)
            taker = float(taker_raw) / 10_000 if taker_raw > 1 else float(taker_raw)
            maker_raw = comm.get("makerCommission", 0)
            maker = float(maker_raw) / 10_000 if maker_raw > 1 else float(maker_raw)
            status = "OK" if taker <= cfg.costs.taker_fee + 1e-12 else "WARN"
            print(f"[{status}] fee akun: maker={maker:.4%} taker={taker:.4%} | config FEE_MAKER={cfg.costs.maker_fee:.4%} "
                  f"FEE_TAKER={cfg.costs.taker_fee:.4%}")
            if status == "WARN":
                print("      -> fee akun lebih tinggi dari config: perbarui FEE_MAKER/FEE_TAKER di .env lalu ulang backtest")
            for sym in cfg.runtime.symbols:
                if sym not in rules:
                    continue
                trades = await client.my_trades(rules[sym], limit=20)
                if not trades:
                    print(f"      {sym}: belum ada riwayat trade -> taxRate BUY/SELL belum bisa diverifikasi")
                for t in trades[:6]:
                    side = "BUY " if t.get("isBuyer") in (True, 1, "true") else "SELL"
                    print(f"      trade {sym} {side}: commission={t.get('commission')} {t.get('commissionAsset')} "
                          f"taxRate={t.get('taxRate')} taxAmount={t.get('taxAmount')}")
    print("PRE-FLIGHT", "LULUS" if ok else "GAGAL")
    return 0 if ok else 1


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--private", action="store_true")
    args = parser.parse_args()
    try:
        return asyncio.run(run(args.private))
    except ExchangeError as exc:
        print(f"[FAIL] {exc}")
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
