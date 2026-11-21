"""Penyimpanan & pengunduhan data historis kline (CSV)."""

from __future__ import annotations

import csv
from datetime import datetime, timezone
from pathlib import Path
from typing import Iterable

from core.models import Candle

CSV_HEADER = ["open_time", "open", "high", "low", "close", "volume", "close_time"]


def parse_date_ms(text: str) -> int:
    """'2024-01-01' atau ISO datetime -> epoch ms UTC."""
    dt = datetime.fromisoformat(text)
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return int(dt.timestamp() * 1000)


def csv_path(data_dir: Path, symbol: str, interval: str) -> Path:
    return data_dir / f"{symbol}_{interval}.csv"


def save_csv(path: Path, candles: Iterable[Candle]) -> int:
    """Tulis candle ke CSV (atomic). Return jumlah baris."""
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(".tmp")
    n = 0
    with tmp.open("w", newline="", encoding="utf-8") as fh:
        writer = csv.writer(fh)
        writer.writerow(CSV_HEADER)
        for c in candles:
            writer.writerow([c.open_time, repr(c.open), repr(c.high), repr(c.low), repr(c.close),
                             repr(c.volume), c.close_time])
            n += 1
    tmp.replace(path)
    return n


def load_csv(path: Path) -> list[Candle]:
    """Baca CSV milik bot ATAU format mentah Binance/Tokocrypto (12 kolom, tanpa header).

    Timestamp mikrodetik (data.binance.vision spot sejak 2025) otomatis dikonversi ke ms.
    Data diurutkan, duplikat dibuang, dan dicek monoton.
    """
    if not path.exists():
        raise FileNotFoundError(f"File data tidak ditemukan: {path}")
    rows: list[Candle] = []
    with path.open("r", encoding="utf-8") as fh:
        reader = csv.reader(fh)
        for line_no, row in enumerate(reader, start=1):
            if not row or not row[0].strip():
                continue
            if not row[0].strip().lstrip("-").isdigit():
                continue  # header
            try:
                open_time, close_time = int(row[0]), int(row[6])
                if open_time > 10 ** 14:
                    open_time //= 1000
                    close_time //= 1000
                rows.append(Candle(open_time, float(row[1]), float(row[2]), float(row[3]), float(row[4]),
                                   float(row[5]), close_time, True))
            except (IndexError, ValueError) as exc:
                raise ValueError(f"{path}:{line_no} baris tidak valid: {row}") from exc
    rows.sort(key=lambda c: c.open_time)
    dedup: list[Candle] = []
    for c in rows:
        if dedup and c.open_time == dedup[-1].open_time:
            dedup[-1] = c
            continue
        if c.high < c.low or c.close <= 0:
            raise ValueError(f"{path}: OHLC tidak konsisten pada {c.open_time}")
        dedup.append(c)
    return dedup


def filter_range(candles: list[Candle], start: str = "", end: str = "") -> list[Candle]:
    start_ms = parse_date_ms(start) if start else None
    end_ms = parse_date_ms(end) if end else None
    return [c for c in candles if (start_ms is None or c.open_time >= start_ms)
            and (end_ms is None or c.open_time < end_ms)]


INTERVAL_MS = {"1m": 60_000, "3m": 180_000, "5m": 300_000, "15m": 900_000, "30m": 1_800_000, "1h": 3_600_000,
               "2h": 7_200_000, "4h": 14_400_000, "6h": 21_600_000, "8h": 28_800_000, "12h": 43_200_000,
               "1d": 86_400_000}


def resample(candles: list[Candle], src_interval: str, dst_interval: str) -> list[Candle]:
    """Agregasi candle ke timeframe lebih besar (bucket sejajar UTC, seperti kline bursa).

    Bucket yang tidak lengkap (ada bar sumber hilang, termasuk bucket terakhir yang belum
    selesai) dibuang agar tidak ada bar "palsu" dengan range terlalu kecil.
    """
    src_ms, dst_ms = INTERVAL_MS[src_interval], INTERVAL_MS[dst_interval]
    if dst_ms <= src_ms or dst_ms % src_ms != 0:
        raise ValueError(f"Tidak bisa resample {src_interval} -> {dst_interval}")
    per_bucket = dst_ms // src_ms
    out: list[Candle] = []
    bucket: list[Candle] = []
    current = None
    for c in candles:
        key = c.open_time // dst_ms
        if key != current:
            if bucket and len(bucket) == per_bucket:
                out.append(_merge(bucket, dst_ms))
            bucket, current = [], key
        bucket.append(c)
    if bucket and len(bucket) == per_bucket:
        out.append(_merge(bucket, dst_ms))
    return out


def _merge(bucket: list[Candle], dst_ms: int) -> Candle:
    start = bucket[0].open_time // dst_ms * dst_ms
    return Candle(start, bucket[0].open, max(c.high for c in bucket), min(c.low for c in bucket),
                  bucket[-1].close, sum(c.volume for c in bucket), start + dst_ms - 1, True)


def load_for_interval(data_dir: Path, symbol: str, interval: str, fallback_src: str = "1h") -> list[Candle]:
    """Muat CSV ``interval``; bila tidak ada, resample dari CSV ``fallback_src``."""
    direct = csv_path(data_dir, symbol, interval)
    if direct.exists():
        return load_csv(direct)
    src = csv_path(data_dir, symbol, fallback_src)
    if INTERVAL_MS[interval] > INTERVAL_MS[fallback_src] and src.exists():
        return resample(load_csv(src), fallback_src, interval)
    raise FileNotFoundError(f"Tidak ada {direct} maupun {src} untuk di-resample")
