#!/usr/bin/env python3
import argparse
import csv
import json
import math
import socket
import statistics
import subprocess
import time
import uuid
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, List, Optional, Tuple

MSG_TYPES_FOR_OVERHEAD = {"GOSSIP", "HELLO", "GET_PEERS", "PEERS_LIST", "PING", "PONG"}


@dataclass
class Config:
    name: str
    fanout: int
    ttl: int
    peer_limit: int
    ping_interval: float
    peer_timeout: float


def wait_by_n(n: int) -> Tuple[int, int]:
    if n <= 10:
        return 25, 8
    if n <= 20:
        return 35, 12
    return 50, 18


def start_node(node_file: str, port: int, cfg: Config, seed: int, log_path: Path, bootstrap: Optional[str]) -> subprocess.Popen:
    cmd = [
        "python3", node_file,
        "--port", str(port),
        "--fanout", str(cfg.fanout),
        "--ttl", str(cfg.ttl),
        "--peer-limit", str(cfg.peer_limit),
        "--ping-interval", str(cfg.ping_interval),
        "--peer-timeout", str(cfg.peer_timeout),
        "--seed", str(seed),
    ]
    if bootstrap:
        cmd += ["--bootstrap", bootstrap]

    f = log_path.open("w", encoding="utf-8")
    p = subprocess.Popen(cmd, stdout=f, stderr=subprocess.STDOUT, stdin=subprocess.DEVNULL)
    return p


def stop_all(procs: List[subprocess.Popen]) -> None:
    for p in procs:
        if p.poll() is None:
            p.terminate()
    time.sleep(1.0)
    for p in procs:
        if p.poll() is None:
            p.kill()


def inject_gossip(mid: str, bootstrap_port: int, ttl: int) -> int:
    t0 = int(time.time() * 1000)
    msg = {
        "version": 1,
        "msg_id": mid,
        "msg_type": "GOSSIP",
        "sender_id": "injector",
        "sender_addr": "127.0.0.1:9999",
        "timestamp_ms": t0,
        "ttl": ttl,
        "payload": {
            "topic": "phase3",
            "data": "phase3-measurement",
            "origin_id": "injector",
            "origin_timestamp_ms": t0
        }
    }
    s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    s.sendto(json.dumps(msg).encode("utf-8"), ("127.0.0.1", bootstrap_port))
    s.close()
    return t0


def parse_run(run_dir: Path, n: int, msg_id: str, t0_ms: int) -> Dict:
    first_delivery_by_node: Dict[str, int] = {}
    send_events: List[Tuple[int, str]] = []

    for lf in sorted(run_dir.glob("n*.log")):
        with lf.open("r", encoding="utf-8", errors="ignore") as f:
            for line in f:
                try:
                    o = json.loads(line)
                except Exception:
                    continue

                ev = o.get("event")
                ts = o.get("ts_ms")
                if not isinstance(ts, int):
                    continue

                if ev == "gossip_delivered" and o.get("msg_id") == msg_id:
                    nid = o.get("node_id") or lf.name
                    if nid not in first_delivery_by_node or ts < first_delivery_by_node[nid]:
                        first_delivery_by_node[nid] = ts

                if ev == "send":
                    mt = o.get("msg_type")
                    if mt in MSG_TYPES_FOR_OVERHEAD:
                        send_events.append((ts, mt))

    delivered = len(first_delivery_by_node)
    threshold = math.ceil(0.95 * n)
    reached_95 = delivered >= threshold

    t95_ms = None
    conv_ms = None
    overhead = None

    if reached_95:
        sorted_ts = sorted(first_delivery_by_node.values())
        t95_ms = sorted_ts[threshold - 1]
        conv_ms = t95_ms - t0_ms
        overhead = sum(1 for ts, _ in send_events if t0_ms <= ts <= t95_ms)

    return {
        "delivered_nodes": delivered,
        "delivery_ratio": delivered / n,
        "threshold_95": threshold,
        "reached_95": reached_95,
        "t0_ms": t0_ms,
        "t95_ms": t95_ms,
        "convergence_ms": conv_ms,
        "overhead": overhead,
    }


def summarize(rows: List[Dict]) -> List[Dict]:
    grouped: Dict[Tuple[str, int], List[Dict]] = {}
    for r in rows:
        key = (r["config"], r["N"])
        grouped.setdefault(key, []).append(r)

    out = []
    for (cfg, n), vals in sorted(grouped.items(), key=lambda x: (x[0][0], x[0][1])):
        conv = [v["convergence_ms"] for v in vals if v["convergence_ms"] is not None]
        ov = [v["overhead"] for v in vals if v["overhead"] is not None]
        pass_rate = sum(1 for v in vals if v["reached_95"]) / len(vals)

        out.append({
            "config": cfg,
            "N": n,
            "runs": len(vals),
            "pass_rate_95": pass_rate,
            "conv_mean_ms": statistics.mean(conv) if conv else None,
            "conv_std_ms": statistics.pstdev(conv) if len(conv) > 1 else 0.0 if conv else None,
            "over_mean": statistics.mean(ov) if ov else None,
            "over_std": statistics.pstdev(ov) if len(ov) > 1 else 0.0 if ov else None,
        })
    return out


def write_csv(path: Path, rows: List[Dict], fieldnames: List[str]) -> None:
    with path.open("w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=fieldnames)
        w.writeheader()
        for r in rows:
            w.writerow(r)

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--node-file", type=str, default="node.py")
    ap.add_argument("--base-port", type=int, default=9000)
    ap.add_argument("--ns", type=str, default="10,20,50")
    ap.add_argument("--seeds", type=str, default="42,43,44,45,46")
    ap.add_argument("--out", type=str, default="results_phase3/phase3")
    args = ap.parse_args()

    ns = [int(x.strip()) for x in args.ns.split(",") if x.strip()]
    seeds = [int(x.strip()) for x in args.seeds.split(",") if x.strip()]
    out_dir = Path(args.out)
    out_dir.mkdir(parents=True, exist_ok=True)

    configs = [
        Config("base", fanout=3, ttl=8, peer_limit=20, ping_interval=2.0, peer_timeout=6.0),
        Config("fanout5", fanout=5, ttl=8, peer_limit=20, ping_interval=2.0, peer_timeout=6.0),
        Config("ttl12", fanout=3, ttl=12, peer_limit=20, ping_interval=2.0, peer_timeout=6.0),
        Config("peerlimit10_timeout10", fanout=3, ttl=8, peer_limit=10, ping_interval=2.0, peer_timeout=10.0),
    ]

    raw_rows: List[Dict] = []

    for cfg in configs:
        for n in ns:
            for sd in seeds:
                run_tag = f"{cfg.name}_N{n}_seed{sd}"
                run_dir = out_dir / "runs" / run_tag
                run_dir.mkdir(parents=True, exist_ok=True)

                subprocess.run(["pkill", "-f", f"python3 {args.node_file}"], check=False, stdout=subprocess.DEVNULL,
                               stderr=subprocess.DEVNULL)
                time.sleep(1)

                procs = []
                try:
                    procs.append(start_node(args.node_file, args.base_port, cfg, sd, run_dir / "n0.log", None))
                    for i in range(1, n):
                        port = args.base_port + i
                        procs.append(start_node(args.node_file, port, cfg, sd + i,
                                                run_dir / f"n{i}.log", f"127.0.0.1:{args.base_port}"))

                    time.sleep(2)
                    alive = all(p.poll() is None for p in procs)
                    if not alive:
                        raw_rows.append({
                            "config": cfg.name, "N": n, "seed": sd, "status": "node_crash",
                            "delivered_nodes": 0, "delivery_ratio": 0.0, "reached_95": False,
                            "convergence_ms": None, "overhead": None
                        })
                        continue

                    wb, wp = wait_by_n(n)
                    time.sleep(wb)

                    mid = str(uuid.uuid4())
                    t0 = inject_gossip(mid, args.base_port, cfg.ttl)
                    time.sleep(wp)

                    metrics = parse_run(run_dir, n, mid, t0)
                    raw_rows.append({
                        "config": cfg.name,
                        "N": n,
                        "seed": sd,
                        "status": "ok",
                        "msg_id": mid,
                        **metrics
                    })
                finally:
                    stop_all(procs)
                    time.sleep(1)

    raw_fields = [
        "config", "N", "seed", "status", "msg_id",
        "delivered_nodes", "delivery_ratio", "threshold_95", "reached_95",
        "t0_ms", "t95_ms", "convergence_ms", "overhead"
    ]
    write_csv(out_dir / "raw_results.csv", raw_rows, raw_fields)

    summary_rows = summarize(raw_rows)
    sum_fields = [
        "config", "N", "runs", "pass_rate_95",
        "conv_mean_ms", "conv_std_ms",
        "over_mean", "over_std"
    ]
    write_csv(out_dir / "summary_results.csv", summary_rows, sum_fields)

    print("Done.")
    print(f"Raw: {out_dir / 'raw_results.csv'}")
    print(f"Summary: {out_dir / 'summary_results.csv'}")


if __name__ == "__main__":
    main()
