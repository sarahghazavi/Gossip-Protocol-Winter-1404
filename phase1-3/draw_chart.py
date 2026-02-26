#!/usr/bin/env python3
import argparse
import csv
import os
from collections import defaultdict

import matplotlib.pyplot as plt


def to_int(x):
    return int(str(x).strip())


def to_float(x):
    s = str(x).strip()
    if s == "" or s.lower() == "none":
        return None
    return float(s)


def read_summary_csv(path):
    with open(path, "r", encoding="utf-8", errors="ignore") as f:
        rows = list(csv.DictReader(f))

    required = {"config", "N", "conv_mean_ms", "over_mean"}
    if not rows or not required.issubset(rows[0].keys()):
        raise ValueError(
            "Invalid summary CSV format. Required columns: "
            "config,N,conv_mean_ms,over_mean"
        )

    out = []
    for r in rows:
        out.append({
            "config": r["config"],
            "N": to_int(r["N"]),
            "conv_mean_ms": to_float(r["conv_mean_ms"]),
            "over_mean": to_float(r["over_mean"]),
        })
    return out


def plot_metric(rows, metric_key, y_label, title, out_path):
    by_cfg = defaultdict(list)
    for r in rows:
        y = r[metric_key]
        if y is None:
            continue
        by_cfg[r["config"]].append((r["N"], y))

    plt.figure(figsize=(8, 5))
    for cfg, pts in sorted(by_cfg.items()):
        pts = sorted(pts, key=lambda x: x[0])
        xs = [p[0] for p in pts]
        ys = [p[1] for p in pts]
        plt.plot(xs, ys, marker="o", linewidth=2, label=cfg)

    plt.xlabel("N")
    plt.ylabel(y_label)
    plt.title(title)
    plt.grid(True, alpha=0.3)
    plt.legend()
    plt.tight_layout()
    plt.savefig(out_path, dpi=150)
    plt.close()


def main():
    ap = argparse.ArgumentParser(description="Plot phase3 charts from summary_results.csv")
    ap.add_argument("--summary-csv", required=True, help="Path to summary_results.csv")
    ap.add_argument("--out-dir", default="plots", help="Output directory")
    ap.add_argument("--prefix", default="phase3", help="Output filename prefix")
    args = ap.parse_args()

    rows = read_summary_csv(args.summary_csv)
    os.makedirs(args.out_dir, exist_ok=True)

    conv_path = os.path.join(args.out_dir, f"{args.prefix}_convergence_vs_n.png")
    over_path = os.path.join(args.out_dir, f"{args.prefix}_overhead_vs_n.png")

    plot_metric(
        rows,
        metric_key="conv_mean_ms",
        y_label="Convergence Time (ms)",
        title="Convergence Time vs N",
        out_path=conv_path,
    )

    plot_metric(
        rows,
        metric_key="over_mean",
        y_label="Message Overhead",
        title="Message Overhead vs N",
        out_path=over_path,
    )

    print("Done.")
    print(conv_path)
    print(over_path)


if __name__ == "__main__":
    main()
