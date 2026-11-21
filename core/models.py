"""Model data bersama (candle, posisi, trade, hasil fill)."""

from __future__ import annotations

from dataclasses import asdict, dataclass, field
from typing import Any, Optional


@dataclass(slots=True)
class Candle:
    """Satu bar OHLCV. Waktu dalam milidetik epoch (UTC)."""

    open_time: int
    open: float
    high: float
    low: float
    close: float
    volume: float
    close_time: int
    closed: bool = True

    @classmethod
    def from_rest(cls, row: list[Any]) -> "Candle":
        """Parse baris kline REST: [openTime, o, h, l, c, v, closeTime, ...]."""
        return cls(
            open_time=int(row[0]),
            open=float(row[1]),
            high=float(row[2]),
            low=float(row[3]),
            close=float(row[4]),
            volume=float(row[5]),
            close_time=int(row[6]),
            closed=True,
        )

    @classmethod
    def from_ws(cls, k: dict[str, Any]) -> "Candle":
        """Parse payload ``k`` dari stream ``<symbol>@kline_<interval>``."""
        return cls(
            open_time=int(k["t"]),
            open=float(k["o"]),
            high=float(k["h"]),
            low=float(k["l"]),
            close=float(k["c"]),
            volume=float(k["v"]),
            close_time=int(k["T"]),
            closed=bool(k["x"]),
        )


@dataclass
class Position:
    """Posisi long spot yang dikelola bot."""

    symbol: str
    entry_price: float          # harga rata-rata efektif (termasuk biaya beli) per unit
    qty: float                  # kuantitas base yang masih dipegang (bersih setelah komisi)
    initial_qty: float
    stop_price: float
    initial_stop: float
    tp1_price: float
    risk_per_unit: float
    entry_time: int             # ms
    entry_bar_time: int         # open_time bar sinyal
    highest_high: float
    tp1_done: bool = False
    stop_order_id: Optional[str] = None
    exchange_stop_price: float = 0.0   # trigger stop yang aktif di bursa (0 = tidak ada)
    entry_client_id: str = ""
    cost_basis_quote: float = 0.0   # total quote yang dikeluarkan untuk qty awal
    realized_pnl: float = 0.0
    fees_paid: float = 0.0

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "Position":
        return cls(**data)

    def unrealized_pnl(self, price: float) -> float:
        return (price - self.entry_price) * self.qty


@dataclass
class TradeRecord:
    """Ringkasan trade yang sudah ditutup penuh."""

    symbol: str
    entry_time: int
    exit_time: int
    entry_price: float
    exit_price: float
    qty: float
    pnl: float
    r_multiple: float
    fees: float
    exit_reason: str
    legs: list[dict[str, Any]] = field(default_factory=list)
    risk_amount: float = 0.0        # 1R dalam quote (initial_qty x risk_per_unit)
    initial_stop: float = 0.0
    slippage_cost: float = 0.0      # estimasi biaya slippage (backtest)

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass
class FillResult:
    """Hasil eksekusi order (paper atau live)."""

    symbol: str
    side: str
    requested_qty: float
    filled_qty: float           # qty bersih yang benar-benar diterima/dijual
    avg_price: float            # harga rata-rata eksekusi bursa
    quote_amount: float         # buy: quote keluar (termasuk biaya); sell: quote diterima bersih
    fees_quote: float
    order_id: str = ""
    client_id: str = ""
    status: str = "FILLED"

    @property
    def is_filled(self) -> bool:
        return self.filled_qty > 0
