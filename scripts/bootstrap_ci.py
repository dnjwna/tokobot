"""Seberapa yakin kita bahwa expectancy > 0? Bootstrap atas trade hasil backtest.

    python scripts/bootstrap_ci.py <trades_research.csv>
    python scripts/bootstrap_ci.py a.csv b.csv --cluster month      # gabungkan beberapa run
    python scripts/bootstrap_ci.py <csv> --n-configs 12              # koreksi Bonferroni

``--cluster none``  : resample per trade (asumsi tiap trade independen -> CI terlalu sempit
                      bila trade bergerombol di tren yang sama).
``--cluster month`` : resample per BULAN entry (block bootstrap). Trade yang masuk di bulan
                      yang sama ikut terambil bersama, sehingga korelasi antar-aset dalam satu
                      fase tren ikut diperhitungkan. Lebih jujur untuk portofolio kripto.
"""

from __future__ import annotations

import argparse
import csv
import sys
from collections import defaultdict
from pathlib import Path

import numpy as np


def cluster_bootstrap(r: np.ndarray, keys: list[str], n_boot: int, alpha: float,
                      seed: int) -> tuple[float, float, float, int]:
    """Bootstrap rasio (sum R / jumlah trade) dengan resample cluster. Return (lo, hi, P(mean<=0), n_cluster)."""
    sums: dict[str, float] = defaultdict(float)
    counts: dict[str, int] = defaultdict(int)
    for value, key in zip(r, keys):
        sums[key] += float(value)
        counts[key] += 1
    s = np.array(list(sums.values()))
    c = np.array([counts[k] for k in sums])
    rng = np.random.default_rng(seed)
    idx = rng.integers(0, len(s), size=(n_boot, len(s)))
    means = s[idx].sum(axis=1) / c[idx].sum(axis=1)
    lo, hi = np.quantile(means, [alpha / 2, 1 - alpha / 2])
    return float(lo), float(hi), float((means <= 0).mean()), len(s)


def load_rows(paths: list[Path]) -> list[dict[str, str]]:
    rows: list[dict[str, str]] = []
    for path in paths:
        if not path.exists():
            raise FileNotFoundError(path)
        with path.open() as fh:
            rows.extend(csv.DictReader(fh))
    return rows


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("csv", type=Path, nargs="+")
    parser.add_argument("--n-boot", type=int, default=20000)
    parser.add_argument("--n-configs", type=int, default=1, help="jumlah konfigurasi yang diuji (Bonferroni)")
    parser.add_argument("--cluster", choices=["none", "month"], default="none")
    parser.add_argument("--seed", type=int, default=0)
    args = parser.parse_args()
    try:
        rows = load_rows(args.csv)
    except FileNotFoundError as exc:
        print(f"File tidak ada: {exc}", file=sys.stderr)
        return 2
    if len(rows) < 5:
        print(f"Hanya {len(rows)} trade; terlalu sedikit untuk inferensi apa pun.")
        return 1
    alpha = 0.05 / max(args.n_configs, 1)
    groups: dict[str, list[tuple[float, str]]] = defaultdict(list)
    for i, row in enumerate(rows):
        r = float(row["r_multiple"])
        key = row["entry_time"][:7] if args.cluster == "month" else f"t{i}"
        groups["SEMUA"].append((r, key))
        groups[row["symbol"]].append((r, key))
        groups[f"tahun {row['exit_time'][:4]}"].append((r, key))
    print(f"CI {(1 - alpha):.1%} | bootstrap {args.n_boot}x | cluster={args.cluster} | n_configs={args.n_configs} "
          f"| file={len(args.csv)}")
    print(f"{'kelompok':<14}{'n':>5}{'cluster':>8}{'mean R':>9}{'std R':>8}{'CI bawah':>10}{'CI atas':>9}{'P(mean<=0)':>12}")
    for name, items in sorted(groups.items(), key=lambda kv: (kv[0] != "SEMUA", kv[0])):
        r = np.array([x for x, _ in items])
        keys = [k for _, k in items]
        if len(r) < 5:
            print(f"{name:<14}{len(r):>5}{'':>8}{r.mean():>9.3f}{'':>8}{'(n<5)':>10}")
            continue
        lo, hi, p_le0, n_cl = cluster_bootstrap(r, keys, args.n_boot, alpha, args.seed)
        print(f"{name:<14}{len(r):>5}{n_cl:>8}{r.mean():>9.3f}{r.std(ddof=1):>8.2f}{lo:>10.3f}{hi:>9.3f}{p_le0:>12.3f}")
    r_all = np.array([x for x, _ in groups["SEMUA"]])
    sd = r_all.std(ddof=1)
    need = int(np.ceil((1.96 * sd / 0.15) ** 2))
    print(f"\nTrade independen yang dibutuhkan agar mean R = 0.15 terbedakan dari 0 (95%, std {sd:.2f}): ~{need}")
    if args.cluster == "none":
        print("Catatan: cluster=none mengasumsikan trade independen. Bandingkan dengan --cluster month.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
