#!/usr/bin/env bash
set -euo pipefail

NODE_FILE="node.py"
BOOTSTRAP_PORT=9000
START_PORT=9001
NODES=10

FANOUT=3
TTL=8
PEER_LIMIT=20
PING_INTERVAL=2
PEER_TIMEOUT=6
SEED_BASE=42

RUN_ID="$(date +%Y%m%d_%H%M%S)"
LOG_DIR="logs/phase2_exact_${RUN_ID}"
PID_FILE="${LOG_DIR}/pids.txt"

mkdir -p "${LOG_DIR}"
: > "${PID_FILE}"

cleanup() {
  if [[ -f "${PID_FILE}" ]]; then
    while read -r pid; do
      [[ -n "${pid:-}" ]] && kill "$pid" 2>/dev/null || true
    done < "${PID_FILE}"
  fi
}
trap cleanup EXIT INT TERM

if [[ ! -f "${NODE_FILE}" ]]; then
  echo "ERROR: ${NODE_FILE} not found."
  exit 1
fi

pkill -f "python3 ${NODE_FILE}" 2>/dev/null || true
sleep 1

echo "[1/5] Starting ${NODES} nodes..."

# node0: bootstrap on 9000
nohup python3 "${NODE_FILE}" \
  --port "${BOOTSTRAP_PORT}" \
  --fanout "${FANOUT}" \
  --ttl "${TTL}" \
  --peer-limit "${PEER_LIMIT}" \
  --ping-interval "${PING_INTERVAL}" \
  --peer-timeout "${PEER_TIMEOUT}" \
  --seed "${SEED_BASE}" \
  > "${LOG_DIR}/n0.log" 2>&1 < /dev/null &
echo $! >> "${PID_FILE}"

# node1..node9 on 9001..9009
for i in $(seq 1 9); do
  port=$((START_PORT + i - 1))
  seed=$((SEED_BASE + i))
  nohup python3 "${NODE_FILE}" \
    --port "${port}" \
    --bootstrap "127.0.0.1:${BOOTSTRAP_PORT}" \
    --fanout "${FANOUT}" \
    --ttl "${TTL}" \
    --peer-limit "${PEER_LIMIT}" \
    --ping-interval "${PING_INTERVAL}" \
    --peer-timeout "${PEER_TIMEOUT}" \
    --seed "${seed}" \
    > "${LOG_DIR}/n${i}.log" 2>&1 < /dev/null &
  echo $! >> "${PID_FILE}"
done

sleep 2
idx=0
while read -r pid; do
  if ! kill -0 "$pid" 2>/dev/null; then
    echo "ERROR: node died early. log=${LOG_DIR}/n${idx}.log"
    sed -n '1,120p' "${LOG_DIR}/n${idx}.log" || true
    exit 1
  fi
  idx=$((idx+1))
done < "${PID_FILE}"

echo "[2/5] Waiting for bootstrap/discovery..."
sleep 25

echo "[3/5] Injecting one GOSSIP message..."
MID="$(python3 -c 'import uuid; print(uuid.uuid4())')"
export MID

python3 - <<'PY'
import os, json, socket, time
mid = os.environ["MID"]
msg = {
    "version": 1,
    "msg_id": mid,
    "msg_type": "GOSSIP",
    "sender_id": "injector",
    "sender_addr": "127.0.0.1:9999",
    "timestamp_ms": int(time.time()*1000),
    "ttl": 8,
    "payload": {
        "topic": "phase2",
        "data": "check >= 9 deliveries",
        "origin_id": "injector",
        "origin_timestamp_ms": int(time.time()*1000),
    },
}
s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
s.sendto(json.dumps(msg).encode("utf-8"), ("127.0.0.1", 9000))
s.close()
print("MID =", mid)
PY

echo "[4/5] Waiting for propagation..."
sleep 8

echo "[5/5] Analyzing logs..."
python3 - <<'PY'
import os, glob, json

mid = os.environ["MID"]
log_dir = sorted(glob.glob("logs/phase2_exact_*"))[-1]

received_nodes = set()
per_log = {}

for fn in glob.glob(f"{log_dir}/n*.log"):
    cnt = 0
    with open(fn, "r", encoding="utf-8", errors="ignore") as f:
        for line in f:
            try:
                o = json.loads(line)
            except Exception:
                continue
            if o.get("event") == "gossip_delivered" and o.get("msg_id") == mid:
                cnt += 1
                received_nodes.add(o.get("node_id") or fn)
    per_log[fn] = cnt

print(f"delivered_nodes_count = {len(received_nodes)}")
for k in sorted(per_log):
    print(f"{k}: {per_log[k]}")

if len(received_nodes) >= 9:
    print("PASS")
else:
    print("FAIL")
PY

echo "Logs: ${LOG_DIR}"
