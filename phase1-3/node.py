import argparse
import asyncio
import json
import sys
import time
import uuid
from dataclasses import dataclass
from collections import OrderedDict
from typing import Dict, Tuple, Optional, List
import random

Addr = Tuple[str, int]

def now_ms() -> int:
    return int(time.time() * 1000)

def parse_addr(s: str) -> Addr:
    ip, port = s.split(":")
    return ip, int(port)

def addr_to_str(a: Addr) -> str:
    return f"{a[0]}:{a[1]}"

@dataclass
class PeerState:
    peer_id: Optional[str]
    addr: Addr
    last_seen_ms: int
    fail_count: int = 0

class SeenLRU:
    def __init__(self, capacity: int = 50000):
        self.capacity = capacity
        self.od = OrderedDict()

    def __contains__(self, k: str) -> bool:
        return k in self.od

    def add(self, k: str, ts_ms: int) -> None:
        if k in self.od:
            return
        self.od[k] = ts_ms
        self.od.move_to_end(k)
        if len(self.od) > self.capacity:
            self.od.popitem(last=False)

class NodeProtocol(asyncio.DatagramProtocol):
    def __init__(self, node: "GossipNode"):
        self.node = node

    def connection_made(self, transport):
        self.node.transport = transport

    def datagram_received(self, data: bytes, addr: Addr):
        asyncio.create_task(self.node.on_datagram(data, addr))

class GossipNode:
    def __init__(self, args):
        self.cfg = args
        self.node_id = str(uuid.uuid4())
        self.self_addr: Addr = ("127.0.0.1", args.port)

        self.transport = None
        self.peers: Dict[Addr, PeerState] = {}
        self.seen = SeenLRU(capacity=50000)
        self.store: Dict[str, dict] = {}

        self.rng = random.Random(args.seed)
        self.protocol = NodeProtocol(self)

        self.pending_pings: Dict[str, Tuple[Addr, int]] = {}
        self.pending_by_addr: Dict[Addr, str] = {}

    def log(self, event: str, **fields):
        rec = {
            "ts_ms": now_ms(),
            "node_id": self.node_id,
            "self": addr_to_str(self.self_addr),
            "event": event,
            **fields,
        }
        print(json.dumps(rec, ensure_ascii=False), flush=True)

    def pack(self, msg_type: str, payload: dict, ttl: int, msg_id: Optional[str] = None) -> bytes:
        mid = msg_id or str(uuid.uuid4())
        msg = {
            "version": 1,
            "msg_id": mid,
            "msg_type": msg_type,
            "sender_id": self.node_id,
            "sender_addr": addr_to_str(self.self_addr),
            "timestamp_ms": now_ms(),
            "ttl": ttl,
            "payload": payload,
        }
        return json.dumps(msg).encode("utf-8")

    def remove_peer(self, addr: Addr, reason: str):
        removed = self.peers.pop(addr, None)
        if removed:
            pid = self.pending_by_addr.pop(addr, None)
            if pid:
                self.pending_pings.pop(pid, None)
            self.log("peer_removed", reason=reason, peer=addr_to_str(addr))

    def peer_add(self, addr: Addr, peer_id: Optional[str]):
        if addr in self.peers:
            ps = self.peers[addr]
            ps.peer_id = peer_id or ps.peer_id
            ps.last_seen_ms = now_ms()
            return

        if len(self.peers) >= self.cfg.peer_limit:
            stale_addr = min(self.peers.values(), key=lambda p: p.last_seen_ms).addr
            self.remove_peer(stale_addr, "peer_limit")

        self.peers[addr] = PeerState(peer_id=peer_id, addr=addr, last_seen_ms=now_ms(), fail_count=0)
        self.log("peer_added", peer=addr_to_str(addr), peer_id=peer_id)

    def peer_touch(self, addr: Addr, peer_id: Optional[str] = None):
        if addr not in self.peers:
            self.peer_add(addr, peer_id)
        else:
            ps = self.peers[addr]
            ps.last_seen_ms = now_ms()
            ps.fail_count = 0
            if peer_id and not ps.peer_id:
                ps.peer_id = peer_id

    def pick_peers(self, k: int, exclude: Optional[Addr] = None) -> List[Addr]:
        addrs = list(self.peers.keys())
        if exclude and exclude in addrs:
            addrs.remove(exclude)
        if not addrs:
            return []
        k = min(k, len(addrs))
        self.rng.shuffle(addrs)
        return addrs[:k]

    async def sendto(self, msg_bytes: bytes, to_addr: Addr, meta: dict):
        try:
            self.transport.sendto(msg_bytes, to_addr)
            self.log("send", to=addr_to_str(to_addr), **meta)
        except Exception as e:
            self.log("send_error", to=addr_to_str(to_addr), err=str(e), **meta)

    async def on_datagram(self, data: bytes, addr: Addr):
        try:
            s = data.decode("utf-8", errors="strict")
            msg = json.loads(s)
        except Exception:
            self.log("recv_bad_json", from_addr=addr_to_str(addr))
            return

        mtype = msg.get("msg_type")
        mid = msg.get("msg_id")
        sender_id = msg.get("sender_id")
        ttl = msg.get("ttl")

        if not isinstance(mid, str) or not mid:
            self.log("recv_bad_msg_id", from_addr=addr_to_str(addr), msg_type=str(mtype))
            return

        if mid in self.seen:
            self.log("recv_duplicate", from_addr=addr_to_str(addr), msg_type=str(mtype), msg_id=mid)
            return
        self.seen.add(mid, now_ms())

        self.log("recv", from_addr=addr_to_str(addr), msg_type=mtype, msg_id=mid)

        if isinstance(sender_id, str):
            self.peer_touch(addr, sender_id)

        if mtype == "HELLO":
            await self.handle_hello(msg, addr)
        elif mtype == "GET_PEERS":
            await self.handle_get_peers(msg, addr)
        elif mtype == "PEERS_LIST":
            await self.handle_peers_list(msg)
        elif mtype == "PING":
            await self.handle_ping(msg, addr)
        elif mtype == "PONG":
            await self.handle_pong(msg, addr)
        elif mtype == "GOSSIP":
            if not isinstance(ttl, int):
                return
            await self.handle_gossip(msg, addr)
        else:
            self.log("recv_unknown_type", from_addr=addr_to_str(addr), msg_type=str(mtype))

    async def handle_hello(self, msg: dict, addr: Addr):
        return

    async def handle_get_peers(self, msg: dict, addr: Addr):
        payload = msg.get("payload", {}) or {}
        max_peers = payload.get("max_peers", self.cfg.peer_limit)
        try:
            max_peers = int(max_peers)
        except Exception:
            max_peers = self.cfg.peer_limit

        candidates = [a for a in self.peers.keys() if a != addr and a != self.self_addr]
        if candidates:
            k = min(max_peers, len(candidates))
            chosen = self.rng.sample(candidates, k=k)
        else:
            chosen = []

        peers_list = []
        for a in chosen:
            ps = self.peers.get(a)
            if not ps:
                continue
            peers_list.append({"node_id": ps.peer_id or "", "addr": addr_to_str(a)})

        out = self.pack("PEERS_LIST", {"peers": peers_list}, ttl=1)
        out_mid = json.loads(out.decode("utf-8")).get("msg_id", "")
        await self.sendto(out, addr, {"msg_type": "PEERS_LIST", "msg_id": out_mid})

    async def handle_peers_list(self, msg: dict):
        payload = msg.get("payload", {}) or {}
        peers = payload.get("peers", []) or []
        for p in peers:
            try:
                a = parse_addr(p["addr"])
                pid = p.get("node_id") or None
                if a != self.self_addr:
                    self.peer_add(a, pid)
            except Exception:
                continue

    async def handle_ping(self, msg: dict, addr: Addr):
        payload = msg.get("payload", {}) or {}
        pong_payload = {"ping_id": payload.get("ping_id"), "seq": payload.get("seq")}
        out = self.pack("PONG", pong_payload, ttl=1)
        out_mid = json.loads(out.decode("utf-8")).get("msg_id", "")
        await self.sendto(out, addr, {"msg_type": "PONG", "msg_id": out_mid})

    async def handle_pong(self, msg: dict, addr: Addr):
        payload = msg.get("payload", {}) or {}
        ping_id = payload.get("ping_id")
        if not isinstance(ping_id, str):
            return

        info = self.pending_pings.get(ping_id)
        if not info:
            return

        peer_addr, sent_ts = info
        if peer_addr != addr:
            self.log("pong_addr_mismatch", expected=addr_to_str(peer_addr), got=addr_to_str(addr), ping_id=ping_id)
            return

        self.pending_pings.pop(ping_id, None)
        self.pending_by_addr.pop(addr, None)
        self.log("ping_ok", peer=addr_to_str(addr), rtt_ms=max(0, now_ms() - sent_ts))

    async def handle_gossip(self, msg: dict, from_addr: Addr):
        mid = msg.get("msg_id")
        ttl = msg.get("ttl")

        if not isinstance(mid, str):
            return

        self.store[mid] = msg
        self.log("gossip_delivered", msg_id=mid, ttl=ttl, origin=msg.get("payload", {}).get("origin_id", ""))

        new_ttl = int(ttl) - 1
        if new_ttl <= 0:
            self.log("gossip_drop_ttl0", msg_id=mid)
            return

        fwd = dict(msg)
        fwd["ttl"] = new_ttl
        fwd["sender_id"] = self.node_id
        fwd["sender_addr"] = addr_to_str(self.self_addr)
        fwd["timestamp_ms"] = now_ms()
        out = json.dumps(fwd).encode("utf-8")

        targets = self.pick_peers(self.cfg.fanout, exclude=from_addr)
        if not targets:
            targets = self.pick_peers(self.cfg.fanout, exclude=None)
        self.log("gossip_forwarded", msg_id=mid, ttl=new_ttl, fanout=len(targets), targets=[addr_to_str(t) for t in targets])
        for t in targets:
            await self.sendto(out, t, {"msg_type": "GOSSIP", "msg_id": mid, "ttl": new_ttl})

    async def bootstrap(self):
        if not self.cfg.bootstrap:
            return
        b = parse_addr(self.cfg.bootstrap)

        hello = self.pack("HELLO", {"capabilities": ["udp", "json"]}, ttl=1)
        hello_mid = json.loads(hello.decode("utf-8")).get("msg_id", "")
        await self.sendto(hello, b, {"msg_type": "HELLO", "msg_id": hello_mid})

        getp = self.pack("GET_PEERS", {"max_peers": self.cfg.peer_limit}, ttl=1)
        getp_mid = json.loads(getp.decode("utf-8")).get("msg_id", "")
        await self.sendto(getp, b, {"msg_type": "GET_PEERS", "msg_id": getp_mid})

    async def discovery_loop(self):
        while True:
            await asyncio.sleep(self.cfg.get_peers_interval)
            if not self.peers:
                continue
            targets = self.pick_peers(min(self.cfg.get_peers_fanout, len(self.peers)))
            for t in targets:
                out = self.pack("GET_PEERS", {"max_peers": self.cfg.peer_limit}, ttl=1)
                out_mid = json.loads(out.decode("utf-8")).get("msg_id", "")
                await self.sendto(out, t, {"msg_type": "GET_PEERS", "msg_id": out_mid})

    async def ping_loop(self):
        while True:
            await asyncio.sleep(self.cfg.ping_interval)
            if self.peers:
                targets = self.pick_peers(min(3, len(self.peers)))
                for t in targets:
                    if t in self.pending_by_addr:
                        continue
                    ping_id = str(uuid.uuid4())
                    seq = self.rng.randint(1, 1_000_000)
                    out = self.pack("PING", {"ping_id": ping_id, "seq": seq}, ttl=1)
                    out_mid = json.loads(out.decode("utf-8")).get("msg_id", "")
                    self.pending_pings[ping_id] = (t, now_ms())
                    self.pending_by_addr[t] = ping_id
                    await self.sendto(out, t, {"msg_type": "PING", "msg_id": out_mid})

            deadline = int(self.cfg.peer_timeout * 1000)
            now = now_ms()
            expired = []
            for pid, (a, ts) in list(self.pending_pings.items()):
                if now - ts > deadline:
                    expired.append((pid, a))

            for pid, a in expired:
                self.pending_pings.pop(pid, None)
                if self.pending_by_addr.get(a) == pid:
                    self.pending_by_addr.pop(a, None)
                ps = self.peers.get(a)
                if not ps:
                    continue
                ps.fail_count += 1
                self.log("peer_suspect", peer=addr_to_str(a), fail_count=ps.fail_count, reason="ping_timeout")
                if ps.fail_count >= 3:
                    self.remove_peer(a, "timeout")

    async def stdin_loop(self):
        while True:
            line = await asyncio.to_thread(sys.stdin.readline)
            if not line:
                await asyncio.sleep(0.1)
                continue
            line = line.strip()
            if not line:
                continue

            if line == "peers":
                self.log("peers_dump", peers=[addr_to_str(a) for a in self.peers.keys()])
                continue

            if line.startswith("gossip "):
                parts = line.split(" ", 2)
                topic = parts[1] if len(parts) >= 2 else "chat"
                data = parts[2] if len(parts) >= 3 else ""
            else:
                topic = "chat"
                data = line

            mid = str(uuid.uuid4())
            payload = {
                "topic": topic,
                "data": data,
                "origin_id": self.node_id,
                "origin_timestamp_ms": now_ms(),
            }
            msg = {
                "version": 1,
                "msg_id": mid,
                "msg_type": "GOSSIP",
                "sender_id": self.node_id,
                "sender_addr": addr_to_str(self.self_addr),
                "timestamp_ms": now_ms(),
                "ttl": self.cfg.ttl,
                "payload": payload,
            }

            self.seen.add(mid, now_ms())
            self.store[mid] = msg
            self.log("gossip_created", msg_id=mid, ttl=self.cfg.ttl, topic=topic)

            out = json.dumps(msg).encode("utf-8")
            targets = self.pick_peers(self.cfg.fanout)
            self.log("gossip_forwarded", msg_id=mid, ttl=self.cfg.ttl, fanout=len(targets), targets=[addr_to_str(t) for t in targets])
            for t in targets:
                await self.sendto(out, t, {"msg_type": "GOSSIP", "msg_id": mid, "ttl": self.cfg.ttl})

async def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--port", type=int, required=True)
    ap.add_argument("--bootstrap", type=str, default="")
    ap.add_argument("--fanout", type=int, default=3)
    ap.add_argument("--ttl", type=int, default=8)
    ap.add_argument("--peer-limit", dest="peer_limit", type=int, default=20)
    ap.add_argument("--ping-interval", dest="ping_interval", type=float, default=2.0)
    ap.add_argument("--peer-timeout", dest="peer_timeout", type=float, default=6.0)
    ap.add_argument("--seed", type=int, default=42)
    ap.add_argument("--get-peers-interval", dest="get_peers_interval", type=float, default=5.0)
    ap.add_argument("--get-peers-fanout", dest="get_peers_fanout", type=int, default=2)

    args = ap.parse_args()

    node = GossipNode(args)
    loop = asyncio.get_running_loop()
    transport, _ = await loop.create_datagram_endpoint(
        lambda: node.protocol,
        local_addr=("127.0.0.1", args.port)
    )

    node.log("node_started", port=args.port, bootstrap=args.bootstrap)

    await node.bootstrap()

    tasks = [
        asyncio.create_task(node.ping_loop()),
        asyncio.create_task(node.discovery_loop()),
        asyncio.create_task(node.stdin_loop()),
    ]
    try:
        await asyncio.gather(*tasks)
    finally:
        transport.close()

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        pass
