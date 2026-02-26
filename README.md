# Gossip Protocol (UDP) - P2P Dissemination & Analysis

**Design and Implementation of a UDP-based Gossip Protocol**

> Developed by *Sara Ghazavi*
> Sharif University of Technology — Winter 1404

---

## 📝 Description

This repository contains a **UDP-based Gossip protocol implementation** for **peer discovery** and **scalable information dissemination** in a P2P network. Messages are encoded as **JSON over UDP**, and the design follows standard gossip ideas (random fanout forwarding + TTL + seen-set deduplication) to provide **high reachability with low overhead**. 

The project includes:
- A runnable node implementation (`node.py`) using **asyncio** for non-blocking networking and periodic tasks.
- A measurement pipeline to compute **convergence time** (time to reach ~95% of nodes) and **message overhead** from logs.
- A **hybrid push/pull** extension using **IHAVE/IWANT** to reduce redundant transfers. :contentReference[oaicite:3]{index=3}

---

## ⚙️ Protocol Overview

### Message format (JSON over UDP)
Each packet follows a common header (version, msg_id, msg_type, sender info, timestamp, ttl) plus a payload. :contentReference[oaicite:4]{index=4}

### Core message types
- **HELLO**: introduce a node and its capabilities
- **PEERS_GET / LIST_PEERS**: discovery & neighbor list exchange
- **PING / PONG**: periodic liveness checking + failure handling
- **GOSSIP**: broadcast application data using **fanout** forwarding and **TTL** decay
- *(Hybrid mode)* **IHAVE / IWANT**: pull missing message IDs and fetch only what’s needed :contentReference[oaicite:9]{index=9}

### Reliability mechanisms
- **Set-Seen** (bounded / LRU-style) to ignore duplicates and prevent infinite rebroadcast
- **TTL** decrement per hop to stop infinite spreading
- **PeerList management** with limits, timeouts, and replacement policies

---

## 🧩 Implementation Notes

### Node runtime (`node.py`)
- UDP datagram endpoint + event-driven processing using `asyncio`
- Periodic loops for:
  - peer discovery (requesting peers)
  - liveness checks (PING/PONG)
  - optional stdin-driven message injection

### Configurable parameters
The following are exposed via CLI to support experimentation: **fanout**, **ttl**, **peer-limit**, **ping-interval**, **peer-timeout**, plus discovery knobs.

---

## 📊 Evaluation (Convergence & Overhead)

The analysis pipeline runs multiple experiments across different network sizes and seeds, injects a GOSSIP message, and extracts:
- **Convergence time** (≈ time until ~95% nodes receive the message)
- **Overhead** (total sent messages including control/data until that point)

---

## 🚀 Quick Start

### Run a single node
```bash
python3 node.py --port 9000 --fanout 3 --ttl 8 --peer-limit 20 --ping-interval 2 --peer-timeout 6 --seed 42
```

### To join via a seed/bootstrap node:
```bash
python3 node.py --port 9001 --bootstrap 127.0.0.1:9000
```
(Parameters mirror the course requirements and the provided node CLI)

---

## 🛠 Technical Stack

- **Language:** Python
- **Networking:** UDP sockets (asyncio datagram protocol)
- **Encoding:** JSON messages
- **Data structures:** bounded Seen-set (LRU), peer state dataclass

---

## 👩‍💻 Author

**Sara Ghazavi**
Sharif University of Technology
Course: Computer Networks – Winter 1404
