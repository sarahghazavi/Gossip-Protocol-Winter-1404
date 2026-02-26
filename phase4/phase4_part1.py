#!/usr/bin/env python3
import argparse
import csv
import json
import math
import os
import signal
import socket
import statistics
import subprocess
import time
import uuid
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, List, Optional, Tuple

import matplotlib.pyplot as plt

MSG_TYPES_FOR_OVERHEAD = {
    "GOSSIP", "HELLO", "GET_PEERS", "PEERS_LIST", "PING", "PONG", "IHAVE", "IWANT"
}

Addr = Tuple[str, int]


@dataclass
class RunConfig:
    mode: str              # "push" or "hybrid"
    fanout: int
    ttl: int
    peer_limit: int
    ping_interval: float
    peer_timeout: float
    get_peers_interval: float
    get_peers_fanout: int
    pull_interval: float
    ihave_max_ids: int
    pull_fanout: int
    pow_k: int = 0


def parse_addr(s: str) -> Addr:
    ip, port = s.split(":")
    return ip, int(port)


def now_ms() -> int:
    return int(time.time() * 1000)


def start_node(
    node_file: str,
    port: int,
    cfg: RunConfig,
    seed: int,
    log_path: Path,
    bootstrap: Optional[str],
) -> Tuple[subprocess.Popen, object]:
    cmd = [
        "python3", node_file,
        "--port", str(port),
        "--fanout", str(cfg.fanout),
        "--ttl", str(cfg.ttl),
        "--peer-limit", str(cfg.peer_limit),
        "--ping-interval", str(cfg.ping_interval),
        "--peer-timeout", str(cfg.peer_timeout),
        "--seed", str(seed),
        "--get-peers-interval", str(cfg.get_peers_interval),
        "--get-peers-fanout", str(cfg.get_peers_fanout),
        "--mode", cfg.mode,
        "--pull-interval", str(cfg.pull_interval),
        "--ihave-max-ids", str(cfg.ihave_max_ids),
        "--pull-fanout", str(cfg.pull_fanout),
        "--pow-k", str(cfg.pow_k),
    ]
    if bootstrap:
        cmd += ["--bootstrap", bootstrap]

    f = log_path.open("w", encoding="utf-8")
    p = subprocess.Popen(
        cmd,
        stdout=f,
        stderr=subprocess.STDOUT,
        stdin=subprocess.DEVNULL,
        text=True,
        bufsize=1,
        preexec_fn=os.setsid if os.name != "nt" else None
    )
    return p, f


def stop_all(procs: List[Tuple[subprocess.Popen, object]]) -> None:
    for p, f in procs:
        try:
            if p.poll() is None:
                if os.name == "nt":
                    p.terminate()
                else:
                    os.killpg(os.getpgid(p.pid), signal.SIGINT)
        except Exception:
            pass

    time.sleep(0.8)

    for p, f in procs:
        try:
            if p.poll() is None:
                if os.name == "nt":
                    p.kill()
                else:
                    os.killpg(os.getpgid(p.pid), signal.SIGKILL)
        except Exception:
            pass
        try:
            f.flush()
            f.close()
        except Exception:
            pass


def inject_gossip(msg_id: str, dest_port: int, ttl: int) -> int:
    t0 = now_ms()
    msg = {
        "version": 1,
        "msg_id": msg_id,
        "msg_type": "GOSSIP",
        "sender_id": "injector",
        "sender_addr": "127.0.0.1:9999",
        "timestamp_ms": t0,
        "ttl": int(ttl),
        "payload": {
            "topic": "phase4",
            "data": "measurement",
            "origin_id": "injector",
            "origin_timestamp_ms": t0
        }
    }
    s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    s.sendto(json.dumps(msg).encode("utf-8"), ("127.0.0.1", dest_port))
    s.close()
    return t0


def read_jsonl(path: Path) -> List[dict]:
    out = []
    if not path.exists():
        return out
    for line in path.read_text(encoding="utf-8", errors="ignore").splitlines():
        line = line.strip()
        if not line:
            continue
        try:
            out.append(json.loads(line))
        except Exception:
            continue
    return out


def peer_counts(run_dir: Path) -> List[int]:
    """تخمین اتصال شبکه: تعداد peer_added برای هر نود."""
    counts = []
    for lf in sorted(run_dir.glob("n*.log")):
        ev = read_jsonl(lf)
        c = 0
        for e in ev:
            if e.get("event") == "peer_added":
                c += 1
        counts.append(c)
    return counts


def has_enough_connectivity(run_dir: Path, N: int, min_peers_per_node: int, ratio_ok: float) -> bool:
    """اگر حداقل ratio_ok از نودها حداقل min_peers_per_node همسایه داشته باشند."""
    counts = peer_counts(run_dir)
    if len(counts) < N:
        return False
    good = sum(1 for c in counts if c >= min_peers_per_node)
    return (good / N) >= ratio_ok


def parse_progress(run_dir: Path, n: int, msg_id: str, t0_ms: int):
    """Polling: اگر به 95% رسیدیم t95 و overhead را بده."""
    first_delivery_by_node: Dict[str, int] = {}
    send_events: List[Tuple[int, str]] = []

    for lf in sorted(run_dir.glob("n*.log")):
        evs = read_jsonl(lf)
        for o in evs:
            ts = o.get("ts_ms")
            if not isinstance(ts, int):
                continue

            if o.get("event") == "gossip_delivered" and o.get("msg_id") == msg_id:
                nid = o.get("node_id") or lf.name
                if nid not in first_delivery_by_node or ts < first_delivery_by_node[nid]:
                    first_delivery_by_node[nid] = ts

            if o.get("event") == "send":
                mt = o.get("msg_type")
                if mt in MSG_TYPES_FOR_OVERHEAD:
                    send_events.append((ts, mt))

    delivered = len(first_delivery_by_node)
    threshold = math.ceil(0.95 * n)

    if delivered < threshold:
        return {"done": False, "delivered_nodes": delivered, "threshold_95": threshold}

    sorted_ts = sorted(first_delivery_by_node.values())
    t95_ms = sorted_ts[threshold - 1]
    conv_ms = t95_ms - t0_ms
    overhead = sum(1 for ts, _ in send_events if t0_ms <= ts <= t95_ms)

    return {
        "done": True,
        "delivered_nodes": delivered,
        "threshold_95": threshold,
        "t95_ms": t95_ms,
        "convergence_ms": conv_ms,
        "overhead": overhead,
    }


def run_one(
    node_file: str,
    base_port: int,
    N: int,
    seed: int,
    cfg: RunConfig,
    run_dir: Path,
    warmup_initial_s: float,
    warmup_step_s: float,
    warmup_max_s: float,
    warmup_retry: int,
    min_peers_per_node: int,
    ratio_ok: float,
    delivery_poll_s: float,
    delivery_max_s: float,
) -> Dict:
    run_dir.mkdir(parents=True, exist_ok=True)

    procs: List[Tuple[subprocess.Popen, object]] = []
    try:
        p0, f0 = start_node(node_file, base_port, cfg, seed, run_dir / "n0.log", bootstrap=None)
        procs.append((p0, f0))

        boot = f"127.0.0.1:{base_port}"
        for i in range(1, N):
            port = base_port + i
            pi, fi = start_node(node_file, port, cfg, seed + i, run_dir / f"n{i}.log", bootstrap=boot)
            procs.append((pi, fi))

        time.sleep(0.5)
        if not all(p.poll() is None for p, _ in procs):
            return {"ok": False, "reason": "node_crash_early"}
        waited = 0.0
        attempt = 0

        while True:
            time.sleep(warmup_initial_s)
            waited += warmup_initial_s

            while waited < warmup_max_s:
                if has_enough_connectivity(run_dir, N, min_peers_per_node, ratio_ok):
                    break
                time.sleep(warmup_step_s)
                waited += warmup_step_s

            if has_enough_connectivity(run_dir, N, min_peers_per_node, ratio_ok):
                break

            attempt += 1
            if attempt > warmup_retry:
                break  
            time.sleep(warmup_step_s * 2)
            waited += warmup_step_s * 2

        mid = str(uuid.uuid4())
        t0 = inject_gossip(mid, base_port, cfg.ttl)

        t_start = time.time()
        last_del = 0
        last_need = math.ceil(0.95 * N)

        while True:
            prog = parse_progress(run_dir, N, mid, t0)

            if prog["done"]:
                return {
                    "ok": True,
                    "status": "ok",
                    "mode": cfg.mode,
                    "N": N,
                    "seed": seed,
                    "msg_id": mid,
                    "warmup_wait_s": waited,
                    "delivered_nodes": prog["delivered_nodes"],
                    "threshold_95": prog["threshold_95"],
                    "reached_95": True,
                    "t0_ms": t0,
                    "t95_ms": prog["t95_ms"],
                    "convergence_ms": prog["convergence_ms"],
                    "overhead": prog["overhead"],
                }

            last_del = prog["delivered_nodes"]
            last_need = prog["threshold_95"]

            if time.time() - t_start > delivery_max_s:
                return {
                    "ok": False,
                    "status": "timeout",
                    "reason": f"timeout_before_t95_delivered_{last_del}_need_{last_need}",
                    "mode": cfg.mode,
                    "N": N,
                    "seed": seed,
                    "msg_id": mid,
                    "warmup_wait_s": waited,
                    "delivered_nodes": last_del,
                    "threshold_95": last_need,
                    "reached_95": False,
                    "t0_ms": t0,
                    "t95_ms": None,
                    "convergence_ms": None,
                    "overhead": None,
                }

            time.sleep(delivery_poll_s)

    finally:
        stop_all(procs)
        time.sleep(0.3)


def summarize(rows: List[Dict]) -> List[Dict]:
    grouped: Dict[Tuple[str, int], List[Dict]] = {}
    for r in rows:
        key = (r["mode"], r["N"])
        grouped.setdefault(key, []).append(r)

    out = []
    for (mode, n), vals in sorted(grouped.items(), key=lambda x: (x[0][0], x[0][1])):
        succ = [
            v for v in vals
            if v.get("reached_95")
            and v.get("convergence_ms") is not None
            and v.get("overhead") is not None
        ]

        conv = [v["convergence_ms"] for v in succ if isinstance(v.get("convergence_ms"), (int, float)) and v["convergence_ms"] > 0]
        ov = [v["overhead"] for v in succ if isinstance(v.get("overhead"), (int, float)) and v["overhead"] > 0]

        pass_rate = sum(1 for v in vals if v.get("reached_95")) / len(vals)
        success_runs = len(succ)

        out.append({
            "mode": mode,
            "N": n,
            "runs": len(vals),
            "success_runs": success_runs,
            "pass_rate_95": pass_rate,
            "conv_mean_ms": statistics.mean(conv) if conv else None,
            "conv_std_ms": statistics.pstdev(conv) if len(conv) > 1 else (0.0 if conv else None),
            "over_mean": statistics.mean(ov) if ov else None,
            "over_std": statistics.pstdev(ov) if len(ov) > 1 else (0.0 if ov else None),
        })
    return out


def write_csv(path: Path, rows: List[Dict], fieldnames: List[str]) -> None:
    with path.open("w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=fieldnames)
        w.writeheader()
        for r in rows:
            w.writerow(r)


def plot(summary_rows: List[Dict], out_dir: Path):
    modes = ["push", "hybrid"]
    Ns = sorted(set(r["N"] for r in summary_rows))

    offset = 0.8  
    plt.figure()

    for mode in modes:
        xs, ys, yerr = [], [], []
        shift = -offset if mode == "push" else offset

        for N in Ns:
            r = next((x for x in summary_rows if x["mode"] == mode and x["N"] == N), None)
            if not r or r["conv_mean_ms"] is None:
                continue

            mu = r["conv_mean_ms"] / 1000.0
            sd = (r["conv_std_ms"] or 0.0) / 1000.0

            xs.append(N + shift)
            ys.append(mu)
            yerr.append(sd)

        if xs:
            plt.errorbar(xs, ys, yerr=yerr, fmt="o", linestyle="none", capsize=5, label=mode)
            ymax = max(ys) if ys else 1.0
            for x, y, sd in zip(xs, ys, yerr):
                plt.text(x, y + 0.03 * ymax, f"{y:.2f}±{sd:.2f}", ha="center", fontsize=8)

    plt.xticks(Ns)
    plt.xlabel("N")
    plt.ylabel("Convergence Time to 95% (s)")
    plt.title("Convergence: Push vs Hybrid (mean±std)")
    plt.legend()
    p1 = out_dir / "convergence_push_vs_hybrid.png"
    plt.savefig(p1, dpi=160, bbox_inches="tight")
    plt.close()

    plt.figure()

    for mode in modes:
        xs, ys, yerr = [], [], []
        shift = -offset if mode == "push" else offset

        for N in Ns:
            r = next((x for x in summary_rows if x["mode"] == mode and x["N"] == N), None)
            if not r or r["over_mean"] is None:
                continue

            mu = r["over_mean"]
            sd = (r["over_std"] or 0.0)

            xs.append(N + shift)
            ys.append(mu)
            yerr.append(sd)

        if xs:
            plt.errorbar(xs, ys, yerr=yerr, fmt="o", linestyle="none", capsize=5, label=mode)
            ymax = max(ys) if ys else 1.0
            for x, y, sd in zip(xs, ys, yerr):
                plt.text(x, y + 0.03 * ymax, f"{y:.0f}±{sd:.0f}", ha="center", fontsize=8)

    plt.xticks(Ns)
    plt.xlabel("N")
    plt.ylabel("Message Overhead (#send events) until 95%")
    plt.title("Overhead: Push vs Hybrid (mean±std)")
    plt.legend()
    p2 = out_dir / "overhead_push_vs_hybrid.png"
    plt.savefig(p2, dpi=160, bbox_inches="tight")
    plt.close()

    return p1, p2


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--node-file", type=str, default="node.py")
    ap.add_argument("--out", type=str, default="results_phase4_compare")
    ap.add_argument("--ns", type=str, default="5,10,20,50,100")
    ap.add_argument("--seeds", type=str, default="42,43,44,45,46")
    ap.add_argument("--base-port", type=int, default=12000)

    ap.add_argument("--warmup-initial-s", type=float, default=2.0)
    ap.add_argument("--warmup-step-s", type=float, default=2.0)
    ap.add_argument("--warmup-max-s", type=float, default=45.0)
    ap.add_argument("--warmup-retry", type=int, default=2)        
    ap.add_argument("--min-peers-per-node", type=int, default=3)
    ap.add_argument("--ratio-ok", type=float, default=0.7)

    ap.add_argument("--delivery-poll-s", type=float, default=0.25)
    ap.add_argument("--delivery-max-s", type=float, default=90.0)

    args = ap.parse_args()

    ns = [int(x.strip()) for x in args.ns.split(",") if x.strip()]
    seeds = [int(x.strip()) for x in args.seeds.split(",") if x.strip()]

    out_dir = Path(args.out)
    out_dir.mkdir(parents=True, exist_ok=True)

    common = dict(
        fanout=5,
        ttl=14,
        peer_limit=120,
        ping_interval=1.0,
        peer_timeout=10.0,
        get_peers_interval=0.7,
        get_peers_fanout=4,
        pull_interval=2.0,
        ihave_max_ids=64,
        pull_fanout=4,
        pow_k=0,
    )

    cfg_push = RunConfig(mode="push", **common)
    cfg_hybrid = RunConfig(mode="hybrid", **common)
    all_cfgs = [cfg_push, cfg_hybrid]

    raw_rows: List[Dict] = []

    for cfg in all_cfgs:
        for N in ns:
            for i, sd in enumerate(seeds):
                mode_off = 0 if cfg.mode == "push" else 500
                base_port = args.base_port + mode_off + (i * 200) + (N * 5)

                run_tag = f"{cfg.mode}_N{N}_seed{sd}"
                run_dir = out_dir / "runs" / run_tag

                print(f"[RUN] mode={cfg.mode} N={N} seed={sd} base_port={base_port}", flush=True)

                res = run_one(
                    node_file=args.node_file,
                    base_port=base_port,
                    N=N,
                    seed=sd,
                    cfg=cfg,
                    run_dir=run_dir,
                    warmup_initial_s=args.warmup_initial_s,
                    warmup_step_s=args.warmup_step_s,
                    warmup_max_s=args.warmup_max_s,
                    warmup_retry=args.warmup_retry,
                    min_peers_per_node=args.min_peers_per_node,
                    ratio_ok=args.ratio_ok,
                    delivery_poll_s=args.delivery_poll_s,
                    delivery_max_s=args.delivery_max_s,
                )
                raw_rows.append(res)

                if res.get("ok"):
                    print(f"  OK  warmup={res['warmup_wait_s']:.1f}s conv={res['convergence_ms']/1000:.3f}s overhead={res['overhead']}", flush=True)
                else:
                    print(f"  FAIL reason={res.get('reason')}", flush=True)

    raw_fields = [
        "ok", "status", "reason",
        "mode", "N", "seed", "msg_id",
        "warmup_wait_s",
        "delivered_nodes", "threshold_95", "reached_95",
        "t0_ms", "t95_ms", "convergence_ms", "overhead",
    ]
    write_csv(out_dir / "raw_results.csv", raw_rows, raw_fields)

    summary_rows = summarize(raw_rows)
    sum_fields = [
        "mode", "N", "runs", "success_runs", "pass_rate_95",
        "conv_mean_ms", "conv_std_ms",
        "over_mean", "over_std"
    ]
    write_csv(out_dir / "summary_results.csv", summary_rows, sum_fields)

    p1, p2 = plot(summary_rows, out_dir)

    print("\nDone.")
    print(f"Raw:     {out_dir / 'raw_results.csv'}")
    print(f"Summary: {out_dir / 'summary_results.csv'}")
    print(f"Plots:   {p1} , {p2}")


if __name__ == "__main__":
    main()