# VoidRush Telemetry Gateway & Chaos Resilience Example

A high-throughput, cross-protocol telemetry ingestion and broadcast gateway demonstrating **Eiffel + SCOOP Concurrency + Edge-Triggered C Reactor + Erlang/OTP Supervision**.

---

## 1. Architectural Highlights

```
                                 ┌──────────────────────────────────┐
                                 │   Caddy / HAProxy Edge Proxy     │
                                 │    (TLS & PROXY Protocol v2)     │
                                 └─────────────────┬────────────────┘
                                                   │ :4343
                                                   ▼
┌───────────────────────────────────────────────────────────────────────────────────────────────┐
│ VoidRush Engine & Root Supervisor Plane                                                       │
│                                                                                               │
│  ┌─────────────────────────┐  ┌─────────────────────────┐  ┌────────────────────────────────┐ │
│  │ VR_WORKER 0 (SCOOP)     │  │ VR_WORKER 1 (SCOOP)     │  │ VR_BROKER (SCOOP)              │ │
│  │ - Conns 0..M (HTTP/WS)  │  │ - Conns M+1..2M (SSE)   │  │ - Cross-Worker Fan-Out         │ │
│  │ - Supervised rescue     │  │ - Generational FDs      │  │ - Stateless Room Router        │ │
│  └────────────┬────────────┘  └────────────┬────────────┘  └───────────────▲────────────────┘ │
│               │                            │                               │                  │
│               └────────────────────────────┼───────────────────────────────┘                  │
│                                            ▼                                                  │
│                         ┌────────────────────────────────────┐                                │
│                         │ C Native Edge-Triggered epoll Loop │                                │
│                         │ - Lock-Free Atomic Heartbeats      │                                │
│                         │ - fd_owner[65536] Kernel Purging   │                                │
│                         │ - Generational FD Reuse Protection │                                │
│                         └────────────────────────────────────┘                                │
└───────────────────────────────────────────────────────────────────────────────────────────────┘
```

1. **Tri-Protocol Convergence Without External Brokers:**
   - **Ingest (`HTTP/1.1 POST`):** Pipelined keep-alive sensor payloads parsed with zero-allocation pointer slices.
   - **Interactive Control (`WebSocket RFC 6455`):** Full-duplex connection for client telecommands.
   - **Spectator / Dashboard Stream (`WHATWG SSE`):** Unidirectional event stream featuring 15s comment heartbeats (`: keepalive`) and automatic client resumption via `Last-Event-ID`.
2. **Deterministic OTP Supervision & Fault Isolation:**
   - **Connection-Level `rescue`:** Single socket errors or payload corruptions are caught by the worker's disciplined `rescue` clause. The bad descriptor is closed and `vr_supervisor_isolated_faults_total` is incremented. **Neighboring connections on the same worker remain completely unharmed.**
   - **Worker Crash & Kernel FD Purging:** If an entire worker shard crashes, `VR_SUPERVISOR` executes `vr_reactor_purge_worker_fds(handle, shard_id)`. All orphaned sockets are closed with clean TCP `FIN/RST`, preventing Linux kernel FD leaks. A fresh `VR_WORKER` is hot-swapped into the running broker and reactor within $\le 5\text{ ms}$.
   - **Zero-Downtime Broker Replacement:** `VR_BROKER` is stateless with respect to room memberships. On broker crash, the supervisor creates a new broker instance and hot-swaps worker references. **0 client connections are dropped and 0 room subscriptions are lost.**
   - **OTP Restart Intensity Limiter:** Enforces max 3 child restarts in 30 seconds.

---

## 2. Wire Protocol Contracts

| Endpoint | Protocol | Method & Wire Request | Engine & Broker Behavior | Response / Wire Delivery |
| :--- | :--- | :--- | :--- | :--- |
| `/healthz` | HTTP/1.1 | `GET /healthz HTTP/1.1` | Fast-path slice response; zero heap allocations. | `HTTP/1.1 200 OK\r\nContent-Length: 2\r\n\r\nOK` |
| `/metrics` | HTTP/1.1 | `GET /metrics HTTP/1.1` | Dumps lock-free atomic Prometheus counters from native C control block. | `200 OK` (Prometheus text format) |
| `/events` | WHATWG SSE | `GET /events?room=42 HTTP/1.1`<br>`Accept: text/event-stream`<br>`Last-Event-ID: 100` | Registers `(fd, gen)` in worker `sse_rooms[42]`. Arms 15s heartbeats. | `200 OK`<br>`retry: 3000\n\n`<br>`: keepalive\n\n` |
| `/ws` | RFC 6455 WS | Upgrade handshake, then client frame:<br>`PUB 42 {"sensor":"temp","val":24.5}` | Unmasks frame; fans out ref-counted slice across all worker shards. | Frame delivered to all WS & SSE room 42 members |
| `/events` | HTTP/1.1 | `POST /events HTTP/1.1`<br>`Content-Length: 32`<br><br>`{"room":42,"alert":"overheat"}` | Fast-path ingest. Broker fans out slice across all worker shards. | `HTTP/1.1 202 Accepted\r\n\r\n` |
| `/debug/fault` | HTTP/1.1 | `POST /debug/fault?type=conn` | Injects fault in current socket turn. Handled by connection `rescue`. | Socket cleanly reset; metrics counter incremented |
| `/debug/fault` | HTTP/1.1 | `POST /debug/fault?type=worker&id=0` | Triggers worker shard failure. Supervisor purges FDs and hot-swaps worker. | Target shard FDs closed; other 3 workers unaffected |
| `/debug/fault` | HTTP/1.1 | `POST /debug/fault?type=broker` | Signals broker failure. Supervisor performs zero-downtime hot-swap. | All connections remain open; pub/sub resumes |

---

## 3. 60-Second Copy-Paste Terminal Walkthrough

Open four terminal windows to test the gateway:

### Terminal 1: Subscribe as an SSE Spectator
```bash
curl -N -H "Accept: text/event-stream" -H "Last-Event-ID: 100" \
  http://localhost:4343/events?room=42
```
*Expected initial output:*
```http
HTTP/1.1 200 OK
Content-Type: text/event-stream
Cache-Control: no-cache
Connection: keep-alive
retry: 3000

: keepalive
```

---

### Terminal 2: Connect Interactive WebSocket Producer
```bash
websocat ws://localhost:4343/ws
```
Send a telemetry event to Room 42:
```text
PUB 42 {"sensor":"turbine_rpm","val":8450}
```
*Observation in Terminal 1 (SSE Spectator receives live frame):*
```text
id: 101
data: {"sensor":"turbine_rpm","val":8450}

```

---

### Terminal 3: High-Rate HTTP Ingest
```bash
curl -X POST http://localhost:4343/events \
  -H "Content-Type: application/json" \
  -d '{"room":42,"sensor":"coolant_temp","val":78.3}'
```
*Observation in Terminal 1:*
```text
id: 102
data: {"room":42,"sensor":"coolant_temp","val":78.3}

```

---

### Terminal 4: Inject Chaos & Inspect OTP Supervisor Recovery

#### A. Connection-Level Rescue:
```bash
curl -X POST "http://localhost:4343/debug/fault?type=conn"
```
*The fault is isolated within the connection's `rescue` clause. Terminal 1 and Terminal 2 stay connected.*

#### B. Worker Shard Crash & Hot-Swap:
```bash
curl -X POST "http://localhost:4343/debug/fault?type=worker&id=0"
```
*Worker 0 FDs are purged cleanly. Supervisor hot-swaps Worker 0 in $\le 5\text{ ms}$.*

#### C. Inspect Prometheus Supervisor Counters:
```bash
curl -s http://localhost:4343/metrics | grep -E "vr_(connections|restarts|faults|events)"
```
*Expected output:*
```text
vr_connections_active 2
vr_supervisor_restarts_total 1
vr_supervisor_isolated_faults_total 1
vr_sse_events_out_total 2
```

---

## 4. Deploying via Docker

VoidRush provides a complete Dockerized build and runtime environment using Debian Trixie with EiffelStudio and GCC.

### Option A: Using `docker compose` (Recommended)

Start the telemetry example gateway:
```bash
cd docker
docker compose up example
```

To run with Caddy reverse proxy providing automatic TLS and PROXY v2 header injection:
```bash
docker compose up example caddy-gate
```

---

### Option B: Using the PowerShell Build Helper (Windows)

```powershell
cd docker
# Build and run the Telemetry Example on port 4343:
.\build.ps1 -Example

# Or run automated tests:
.\build.ps1 -Test
```

---

### Option C: Using the Bash Build Helper (Linux / macOS / WSL)

```bash
cd docker
# Build and run the Telemetry Example:
./build.sh --example

# Or run automated tests:
./build.sh --test
```

---

## 5. Automated Verification Scripts

Two automated test scripts are included in `example/scripts/`:

* **Linux / macOS:**
  ```bash
  chmod +x example/scripts/test_walkthrough.sh
  ./example/scripts/test_walkthrough.sh
  ```
* **Windows (PowerShell):**
  ```powershell
  .\example\scripts\test_walkthrough.ps1
  ```
