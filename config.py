"""Konfigurasi terpusat untuk bot.

Semua nilai dibaca dari environment variable (atau file ``.env``) dan divalidasi
saat start-up. Tidak ada kredensial yang di-hardcode. Konfigurasi bersifat
immutable (``frozen=True``) agar tidak bisa berubah diam-diam saat runtime.
"""

from __future__ import annotations

import os
from dataclasses import dataclass, field
from enum import Enum
from pathlib import Path
from typing import Optional
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

from dotenv import load_dotenv

LIVE_CONFIRM_PHRASE = "I_UNDERSTAND_REAL_MONEY_RISK"
SUPPORTED_INTERVALS = ("1m", "3m", "5m", "15m", "30m", "1h", "2h", "4h", "6h", "8h", "12h", "1d")


class ConfigError(ValueError):
    """Dilempar bila konfigurasi tidak valid atau tidak lengkap."""


class Mode(str, Enum):
    """Mode eksekusi bot."""

    BACKTEST = "BACKTEST"
    PAPER = "PAPER"
    LIVE = "LIVE"


# --------------------------------------------------------------------------- #
# Helper pembaca environment
# --------------------------------------------------------------------------- #
def _env_str(name: str, default: Optional[str] = None, required: bool = False) -> str:
    value = os.getenv(name)
    if value is None or value.strip() == "":
        if required:
            raise ConfigError(f"Environment variable {name} wajib diisi")
        return default if default is not None else ""
    return value.strip()


def _env_float(name: str, default: float) -> float:
    raw = os.getenv(name)
    if raw is None or raw.strip() == "":
        return default
    try:
        return float(raw)
    except ValueError as exc:
        raise ConfigError(f"{name} harus berupa angka, didapat {raw!r}") from exc


def _env_int(name: str, default: int) -> int:
    raw = os.getenv(name)
    if raw is None or raw.strip() == "":
        return default
    try:
        return int(raw)
    except ValueError as exc:
        raise ConfigError(f"{name} harus berupa integer, didapat {raw!r}") from exc


def _env_bool(name: str, default: bool) -> bool:
    raw = os.getenv(name)
    if raw is None or raw.strip() == "":
        return default
    lowered = raw.strip().lower()
    if lowered in ("1", "true", "yes", "y", "on"):
        return True
    if lowered in ("0", "false", "no", "n", "off"):
        return False
    raise ConfigError(f"{name} harus boolean (true/false), didapat {raw!r}")


def _env_list(name: str, default: str) -> tuple[str, ...]:
    raw = _env_str(name, default)
    return tuple(item.strip().upper() for item in raw.split(",") if item.strip())


def _require_range(name: str, value: float, low: float, high: float) -> None:
    if not (low <= value <= high):
        raise ConfigError(f"{name}={value} di luar rentang yang diizinkan [{low}, {high}]")


# --------------------------------------------------------------------------- #
# Sub-konfigurasi
# --------------------------------------------------------------------------- #
@dataclass(frozen=True)
class ExchangeConfig:
    """Endpoint & kredensial Tokocrypto (lihat https://www.tokocrypto.com/apidocs/)."""

    api_key: str = field(repr=False)
    api_secret: str = field(repr=False)
    rest_base: str = "https://www.tokocrypto.com"
    market_base_type1: str = "https://www.tokocrypto.site"
    market_base_type3: str = "https://cloudme-toko.2meta.app"
    ws_market_type1: str = "wss://stream-cloud.tokocrypto.site"
    ws_market_type3: str = "wss://stream-toko.2meta.app"
    ws_api_url: str = "wss://ws-api.tokocrypto.site:443/ws-api/v3"
    recv_window_ms: int = 5000
    request_timeout_s: float = 10.0
    weight_limit_per_min: int = 600
    max_retries: int = 5

    @property
    def has_credentials(self) -> bool:
        return bool(self.api_key and self.api_secret)


@dataclass(frozen=True)
class CostConfig:
    """Model biaya transaksi.

    ``tax_rate_*`` default 0.21% mengikuti PPh Pasal 22 final PMK 50/2025 untuk
    PPMSE dalam negeri. Default dikenakan di kedua sisi (konservatif, karena
    pasangan kripto-kripto seperti BTC/USDT secara substansi adalah pertukaran
    dua aset kripto). Verifikasi dengan field ``taxRate`` di endpoint
    ``/open/v1/orders/trades`` akun Anda lalu sesuaikan.
    """

    maker_fee: float = 0.0015   # terverifikasi dari akun VIP0 via preflight (nov 2026)
    taker_fee: float = 0.0015
    tax_rate_buy: float = 0.0021
    tax_rate_sell: float = 0.0021
    slippage_pct: float = 0.0005

    @property
    def entry_cost_pct(self) -> float:
        """Biaya sisi beli (taker + pajak + slippage) dalam fraksi."""
        return self.taker_fee + self.tax_rate_buy + self.slippage_pct

    @property
    def exit_cost_pct(self) -> float:
        """Biaya sisi jual (taker + pajak + slippage) dalam fraksi."""
        return self.taker_fee + self.tax_rate_sell + self.slippage_pct

    @property
    def round_trip_pct(self) -> float:
        return self.entry_cost_pct + self.exit_cost_pct

    def zeroed(self) -> "CostConfig":
        """Salinan tanpa biaya, untuk membandingkan dampak biaya di backtest."""
        return CostConfig(0.0, 0.0, 0.0, 0.0, 0.0)


@dataclass(frozen=True)
class StrategyConfig:
    """Parameter strategi Volatility Breakout + Regime Filter."""

    timeframe: str = "4h"   # konfigurasi yang dievaluasi (lihat README §1)
    donchian_entry: int = 20
    ema_fast: int = 50
    ema_slow: int = 200
    ema_slope_lookback: int = 10
    adx_period: int = 14
    adx_min: float = 22.0
    adx_strong: float = 30.0
    adx_rising_lookback: int = 3
    atr_period: int = 14
    atr_pct_min: float = 0.004
    atr_pct_max: float = 0.06
    er_period: int = 20
    er_min: float = 0.30
    close_position_min: float = 0.60
    volume_sma_period: int = 20
    volume_mult: float = 1.0
    sl_atr_mult: float = 3.0
    tp1_r_multiple: float = 1.5
    tp1_fraction: float = 0.5
    trail_atr_mult: float = 3.0
    min_reward_cost_ratio: float = 3.0
    max_cost_to_risk: float = 0.25   # biaya round-trip / jarak stop; >batas -> sinyal dibuang
    history_bars: int = 1000


@dataclass(frozen=True)
class RiskConfig:
    """Parameter manajemen risiko & kill-switch."""

    risk_per_trade: float = 0.01
    max_open_positions: int = 2
    max_position_pct: float = 0.30
    daily_max_drawdown: float = 0.03
    max_total_drawdown: float = 0.15
    max_consecutive_losses: int = 4
    loss_streak_pause_hours: float = 24.0
    kill_switch_flatten: bool = False
    day_reset_tz: str = "Asia/Jakarta"
    use_exchange_stop: bool = True
    stop_limit_buffer: float = 0.005
    trail_min_step_pct: float = 0.002
    max_entry_slippage: float = 0.003
    capital_cap_quote: float = 0.0  # 0 = tanpa batas; >0 = batasi ekuitas yang dipakai bot


@dataclass(frozen=True)
class TelegramConfig:
    """Kredensial notifikasi Telegram (opsional)."""

    enabled: bool = False
    bot_token: str = field(default="", repr=False)
    chat_id: str = ""


@dataclass(frozen=True)
class BacktestConfig:
    """Parameter backtest."""

    data_dir: Path = Path("data/historical")
    results_dir: Path = Path("backtest/results")
    initial_equity: float = 1000.0
    start: str = ""
    end: str = ""
    oos_split: float = 0.7


@dataclass(frozen=True)
class RuntimeConfig:
    """Parameter runtime orchestrator."""

    mode: Mode = Mode.PAPER
    symbols: tuple[str, ...] = ("BTC_USDT", "ETH_USDT")
    quote_asset: str = "USDT"
    state_dir: Path = Path("state")
    log_dir: Path = Path("logs")
    log_level: str = "INFO"
    reconcile_interval_s: float = 30.0
    risk_check_interval_s: float = 5.0
    time_sync_interval_s: float = 1800.0
    ws_stale_timeout_s: float = 90.0
    max_price_age_s: float = 30.0
    paper_initial_balance: float = 1000.0
    use_user_stream: bool = False
    live_confirm: str = field(default="", repr=False)
    kill_file: Path = Path("state/KILL")


@dataclass(frozen=True)
class AppConfig:
    """Konfigurasi aplikasi lengkap."""

    runtime: RuntimeConfig
    exchange: ExchangeConfig
    costs: CostConfig
    strategy: StrategyConfig
    risk: RiskConfig
    telegram: TelegramConfig
    backtest: BacktestConfig

    @classmethod
    def load(cls, env_file: Optional[str] = None, mode_override: Optional[str] = None) -> "AppConfig":
        """Baca konfigurasi dari environment (dan ``.env`` bila ada), lalu validasi."""
        if env_file:
            load_dotenv(env_file, override=False)
        else:
            load_dotenv(override=False)

        mode_raw = (mode_override or _env_str("TRADING_MODE", "PAPER")).upper()
        try:
            mode = Mode(mode_raw)
        except ValueError as exc:
            raise ConfigError(f"TRADING_MODE harus BACKTEST/PAPER/LIVE, didapat {mode_raw!r}") from exc

        state_dir = Path(_env_str("STATE_DIR", "state"))
        runtime = RuntimeConfig(
            mode=mode,
            symbols=_env_list("SYMBOLS", "BTC_USDT,ETH_USDT"),
            quote_asset=_env_str("QUOTE_ASSET", "USDT").upper(),
            state_dir=state_dir,
            log_dir=Path(_env_str("LOG_DIR", "logs")),
            log_level=_env_str("LOG_LEVEL", "INFO").upper(),
            reconcile_interval_s=_env_float("RECONCILE_INTERVAL_S", 30.0),
            risk_check_interval_s=_env_float("RISK_CHECK_INTERVAL_S", 5.0),
            time_sync_interval_s=_env_float("TIME_SYNC_INTERVAL_S", 1800.0),
            ws_stale_timeout_s=_env_float("WS_STALE_TIMEOUT_S", 90.0),
            max_price_age_s=_env_float("MAX_PRICE_AGE_S", 30.0),
            paper_initial_balance=_env_float("PAPER_INITIAL_BALANCE", 1000.0),
            use_user_stream=_env_bool("USE_USER_STREAM", False),
            live_confirm=_env_str("LIVE_CONFIRM", ""),
            kill_file=Path(_env_str("KILL_FILE", str(state_dir / "KILL"))),
        )
        exchange = ExchangeConfig(
            api_key=_env_str("TOKO_API_KEY", ""),
            api_secret=_env_str("TOKO_API_SECRET", ""),
            rest_base=_env_str("TOKO_REST_BASE", ExchangeConfig.rest_base),
            market_base_type1=_env_str("TOKO_MARKET_BASE_TYPE1", ExchangeConfig.market_base_type1),
            market_base_type3=_env_str("TOKO_MARKET_BASE_TYPE3", ExchangeConfig.market_base_type3),
            ws_market_type1=_env_str("TOKO_WS_MARKET_TYPE1", ExchangeConfig.ws_market_type1),
            ws_market_type3=_env_str("TOKO_WS_MARKET_TYPE3", ExchangeConfig.ws_market_type3),
            ws_api_url=_env_str("TOKO_WS_API_URL", ExchangeConfig.ws_api_url),
            recv_window_ms=_env_int("TOKO_RECV_WINDOW_MS", 5000),
            request_timeout_s=_env_float("TOKO_REQUEST_TIMEOUT_S", 10.0),
            weight_limit_per_min=_env_int("TOKO_WEIGHT_LIMIT_PER_MIN", 600),
            max_retries=_env_int("TOKO_MAX_RETRIES", 5),
        )
        costs = CostConfig(
            maker_fee=_env_float("FEE_MAKER", 0.0015),
            taker_fee=_env_float("FEE_TAKER", 0.0015),
            tax_rate_buy=_env_float("TAX_RATE_BUY", 0.0021),
            tax_rate_sell=_env_float("TAX_RATE_SELL", 0.0021),
            slippage_pct=_env_float("SLIPPAGE_PCT", 0.0005),
        )
        strategy = StrategyConfig(
            timeframe=_env_str("TIMEFRAME", "4h"),
            donchian_entry=_env_int("DONCHIAN_ENTRY", 20),
            ema_fast=_env_int("EMA_FAST", 50),
            ema_slow=_env_int("EMA_SLOW", 200),
            ema_slope_lookback=_env_int("EMA_SLOPE_LOOKBACK", 10),
            adx_period=_env_int("ADX_PERIOD", 14),
            adx_min=_env_float("ADX_MIN", 22.0),
            adx_strong=_env_float("ADX_STRONG", 30.0),
            adx_rising_lookback=_env_int("ADX_RISING_LOOKBACK", 3),
            atr_period=_env_int("ATR_PERIOD", 14),
            atr_pct_min=_env_float("ATR_PCT_MIN", 0.004),
            atr_pct_max=_env_float("ATR_PCT_MAX", 0.06),
            er_period=_env_int("ER_PERIOD", 20),
            er_min=_env_float("ER_MIN", 0.30),
            close_position_min=_env_float("CLOSE_POSITION_MIN", 0.60),
            volume_sma_period=_env_int("VOLUME_SMA_PERIOD", 20),
            volume_mult=_env_float("VOLUME_MULT", 1.0),
            sl_atr_mult=_env_float("SL_ATR_MULT", 3.0),
            tp1_r_multiple=_env_float("TP1_R_MULTIPLE", 1.5),
            tp1_fraction=_env_float("TP1_FRACTION", 0.5),
            trail_atr_mult=_env_float("TRAIL_ATR_MULT", 3.0),
            min_reward_cost_ratio=_env_float("MIN_REWARD_COST_RATIO", 3.0),
            max_cost_to_risk=_env_float("MAX_COST_TO_RISK", 0.25),
            history_bars=_env_int("HISTORY_BARS", 1000),
        )
        risk = RiskConfig(
            risk_per_trade=_env_float("RISK_PER_TRADE", 0.01),
            max_open_positions=_env_int("MAX_OPEN_POSITIONS", 2),
            max_position_pct=_env_float("MAX_POSITION_PCT", 0.30),
            daily_max_drawdown=_env_float("DAILY_MAX_DRAWDOWN", 0.03),
            max_total_drawdown=_env_float("MAX_TOTAL_DRAWDOWN", 0.15),
            max_consecutive_losses=_env_int("MAX_CONSECUTIVE_LOSSES", 4),
            loss_streak_pause_hours=_env_float("LOSS_STREAK_PAUSE_HOURS", 24.0),
            kill_switch_flatten=_env_bool("KILL_SWITCH_FLATTEN", False),
            day_reset_tz=_env_str("DAY_RESET_TZ", "Asia/Jakarta"),
            use_exchange_stop=_env_bool("USE_EXCHANGE_STOP", True),
            stop_limit_buffer=_env_float("STOP_LIMIT_BUFFER", 0.005),
            trail_min_step_pct=_env_float("TRAIL_MIN_STEP_PCT", 0.002),
            max_entry_slippage=_env_float("MAX_ENTRY_SLIPPAGE", 0.003),
            capital_cap_quote=_env_float("CAPITAL_CAP_QUOTE", 0.0),
        )
        telegram = TelegramConfig(
            enabled=_env_bool("TELEGRAM_ENABLED", False),
            bot_token=_env_str("TELEGRAM_BOT_TOKEN", ""),
            chat_id=_env_str("TELEGRAM_CHAT_ID", ""),
        )
        backtest = BacktestConfig(
            data_dir=Path(_env_str("BACKTEST_DATA_DIR", "data/historical")),
            results_dir=Path(_env_str("BACKTEST_RESULTS_DIR", "backtest/results")),
            initial_equity=_env_float("BACKTEST_INITIAL_EQUITY", 1000.0),
            start=_env_str("BACKTEST_START", ""),
            end=_env_str("BACKTEST_END", ""),
            oos_split=_env_float("BACKTEST_OOS_SPLIT", 0.7),
        )
        cfg = cls(runtime, exchange, costs, strategy, risk, telegram, backtest)
        cfg.validate()
        return cfg

    def validate(self) -> None:
        """Validasi silang; lempar ``ConfigError`` bila ada yang tidak masuk akal."""
        rt, rk, st, co = self.runtime, self.risk, self.strategy, self.costs
        if not rt.symbols:
            raise ConfigError("SYMBOLS kosong")
        for sym in rt.symbols:
            if "_" not in sym:
                raise ConfigError(f"Format simbol REST Tokocrypto memakai underscore, mis. BTC_USDT (didapat {sym})")
            if not sym.endswith(f"_{rt.quote_asset}"):
                raise ConfigError(f"Simbol {sym} tidak ber-quote {rt.quote_asset}")
        if st.timeframe not in SUPPORTED_INTERVALS:
            raise ConfigError(f"TIMEFRAME {st.timeframe} tidak didukung")
        if st.ema_fast >= st.ema_slow:
            raise ConfigError("EMA_FAST harus < EMA_SLOW")
        if st.history_bars < st.ema_slow * 3 or st.history_bars > 1000:
            raise ConfigError("HISTORY_BARS harus >= 3x EMA_SLOW dan <= 1000 (limit klines API)")
        _require_range("RISK_PER_TRADE", rk.risk_per_trade, 0.0001, 0.05)
        _require_range("MAX_POSITION_PCT", rk.max_position_pct, 0.01, 1.0)
        _require_range("DAILY_MAX_DRAWDOWN", rk.daily_max_drawdown, 0.001, 0.5)
        _require_range("MAX_TOTAL_DRAWDOWN", rk.max_total_drawdown, rk.daily_max_drawdown, 0.9)
        _require_range("TP1_FRACTION", st.tp1_fraction, 0.0, 1.0)
        _require_range("MAX_COST_TO_RISK", st.max_cost_to_risk, 0.01, 5.0)
        _require_range("STOP_LIMIT_BUFFER", rk.stop_limit_buffer, 0.0005, 0.05)
        _require_range("MAX_ENTRY_SLIPPAGE", rk.max_entry_slippage, 0.0001, 0.05)
        for name, value in (("FEE_MAKER", co.maker_fee), ("FEE_TAKER", co.taker_fee),
                            ("TAX_RATE_BUY", co.tax_rate_buy), ("TAX_RATE_SELL", co.tax_rate_sell),
                            ("SLIPPAGE_PCT", co.slippage_pct)):
            _require_range(name, value, 0.0, 0.02)
        if rk.max_open_positions < 1:
            raise ConfigError("MAX_OPEN_POSITIONS minimal 1")
        try:
            ZoneInfo(rk.day_reset_tz)
        except ZoneInfoNotFoundError as exc:
            raise ConfigError(f"DAY_RESET_TZ tidak dikenal: {rk.day_reset_tz}") from exc
        if self.telegram.enabled and not (self.telegram.bot_token and self.telegram.chat_id):
            raise ConfigError("TELEGRAM_ENABLED=true tetapi TELEGRAM_BOT_TOKEN/TELEGRAM_CHAT_ID kosong")
        if rt.mode is Mode.LIVE:
            if not self.exchange.has_credentials:
                raise ConfigError("Mode LIVE membutuhkan TOKO_API_KEY dan TOKO_API_SECRET")
            if rt.live_confirm != LIVE_CONFIRM_PHRASE:
                raise ConfigError(f"Mode LIVE membutuhkan LIVE_CONFIRM={LIVE_CONFIRM_PHRASE} (pengaman uang riil)")
