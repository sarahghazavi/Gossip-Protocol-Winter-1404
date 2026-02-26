#!/usr/bin/env python3
import json
import argparse
import matplotlib.pyplot as plt


parser = argparse.ArgumentParser()
parser.add_argument("--input", type=str, default="pow_bench.json")
parser.add_argument("--output", type=str, default="pow_time_plot.png")
args = parser.parse_args()

with open(args.input, "r", encoding="utf-8") as f:
    data = json.load(f)
ks = []
means = []
mins = []
maxs = []

for r in data["results"]:
    ks.append(r["pow_k"])
    means.append(r["time_mean_s"])
    mins.append(r["time_min_s"])
    maxs.append(r["time_max_s"])

plt.figure(figsize=(8, 5))
plt.plot(ks, means, marker="o", linewidth=2)
plt.fill_between(ks, mins, maxs, alpha=0.2)

plt.xlabel("pow_k (Difficulty)")
plt.ylabel("Time to compute PoW (seconds)")
plt.title("PoW Computation Time vs Difficulty (pow_k)")
plt.grid(True)

plt.tight_layout()
plt.savefig(args.output, dpi=300)
print(f"Saved plot to {args.output}")

