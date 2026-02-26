import argparse
import hashlib
import json
import time
import uuid
from statistics import mean, pstdev

def pow_digest(node_id: str, nonce: int) -> str:
    s = f"{node_id}|{nonce}".encode("utf-8")
    return hashlib.sha256(s).hexdigest()

def compute_pow(node_id: str, k: int, start_nonce: int = 0):
    prefix = "0" * k
    nonce = start_nonce
    t0 = time.perf_counter()
    tries = 0
    while True:
        h = pow_digest(node_id, nonce)
        tries += 1
        if h.startswith(prefix):
            dt = time.perf_counter() - t0
            return {
                "nonce": nonce,
                "digest_hex": h,
                "time_s": dt,
                "tries": tries,
            }
        nonce += 1

def bench_one_k(k: int, trials: int, base_nonce: int):
    times = []
    tries = []
    samples = []
    for i in range(trials):
        node_id = str(uuid.uuid4())
        res = compute_pow(node_id, k, start_nonce=base_nonce + i * 100000)
        times.append(res["time_s"])
        tries.append(res["tries"])
        samples.append({
            "trial": i,
            "node_id": node_id,
            **res
        })

    return {
        "pow_k": k,
        "trials": trials,
        "time_mean_s": mean(times),
        "time_std_s": pstdev(times) if trials >= 2 else 0.0,
        "time_min_s": min(times),
        "time_max_s": max(times),
        "tries_mean": mean(tries),
        "tries_min": min(tries),
        "tries_max": max(tries),
        "samples": samples[:3],
    }

ap = argparse.ArgumentParser()
ap.add_argument("--ks", type=str, default="3,4,5", help="comma-separated pow_k values")
ap.add_argument("--trials", type=int, default=10)
ap.add_argument("--base-nonce", type=int, default=0)
ap.add_argument("--out", type=str, default="pow_bench.json")
args = ap.parse_args()

ks = [int(x.strip()) for x in args.ks.split(",") if x.strip()]
report = {
    "machine_note": "Run on my laptop/PC (fill CPU model in report)",
    "timestamp": time.time(),
    "results": [],
}

for k in ks:
    r = bench_one_k(k, args.trials, args.base_nonce)
    report["results"].append(r)
    print(f"pow_k={k}  mean={r['time_mean_s']:.4f}s  min={r['time_min_s']:.4f}s  max={r['time_max_s']:.4f}s  tries_mean={r['tries_mean']:.1f}")

with open(args.out, "w", encoding="utf-8") as f:
    json.dump(report, f, indent=2)
print(f"\nSaved: {args.out}")

