# VoidRush

A **high-concurrency networking backend for Eiffel** that combines:

- A **thin C reactor** using Linux `epoll` (with `kqueue` and `IOCP` planned for future multi-platform expansion) for efficient edge-triggered non-blocking I/O.
- **SCOOP** for safe, concurrent application logic and connection handlers (asynchronous separate calls, Design by Contract, no data races).

## Architecture

```
Clients ──HTTPS──► [SSL/TLS Gate] ──plain TCP/HTTP──► [Our Solution]
   (nginx, HAProxy, Caddy…)              Thin C reactor + SCOOP handlers
```

- TLS is terminated externally. Our solution itself contains **no SSL/TLS code**.
- The C layer only handles readiness notification and non-blocking sockets.
- SCOOP processors own the real work (protocol handling, business logic, session state).

## Goals

- Support tens of thousands of concurrent connections efficiently.
- Keep the Eiffel side clean and safe thanks to SCOOP.
- Stay simple and fast by offloading cryptography to a battle-tested front-end proxy.
- Serve as a high-performance foundation for HTTP servers, WebSocket services, custom protocol backends, reverse proxies, game servers, etc.

## Project Structure & Eiffel Clusters

The source code is organized into dedicated Eiffel clusters by architectural responsibility:

```text
src/
├── kernel/                  -- Root control plane & OTP supervision
│   ├── vr_engine.e          -- Top-level engine facade
│   ├── vr_supervisor.e      -- Root supervisor (ordered lifecycle, health checks, hot-swap)
│   ├── vr_child_spec.e      -- Child specification, lifecycle states, restart policies
│   └── vr_restart_intensity.e -- Sliding-window rate limiter (3 in 30s)
│
├── reactor/                 -- Native OS I/O & epoll event dispatch
│   ├── vr_reactor.e         -- Listening socket & event demultiplexer
│   ├── vr_reactor_bridge.e  -- Low-level C FFI external bindings
│   ├── vr_reactor_constants.e -- Bitmasks, opcodes, states, timeouts
│   ├── vr_reactor_event_buffer.e -- Direct memory-indexed epoll event buffer
│   └── vr_metrics.e         -- 136-byte native control block telemetry snapshot
│
├── worker/                  -- Partitioned connection execution
│   ├── vr_worker.e          -- SCOOP worker processor (isolated rescues, fair turns)
│   └── vr_connection.e      -- Non-separate connection state machine & ring buffers
│
├── broker/                  -- Pub/Sub & messaging hub
│   └── vr_broker.e          -- Cross-worker broadcast fan-out (WebSocket & SSE rooms)
│
├── protocol/                -- Protocol parsers, codecs, and frames
│   ├── http/
│   │   ├── vr_http_parser.e -- Incremental streaming HTTP/1.1 header scanner
│   │   ├── vr_http_slice.e  -- Zero-copy string slice utility
│   │   └── vr_proxy_v2.e    -- HAProxy PROXY v2 binary preface decoder
│   ├── websocket/
│   │   ├── vr_ws_parser.e   -- RFC 6455 frame parser & unmasking
│   │   └── vr_ws_constants.e-- WS opcodes, close codes, handshake keys
│   └── sse/
│       └── vr_sse_constants.e -- SSE field directives & protocol constants
│
└── app/                     -- Application entry points
    ├── vr_application.e     -- Production standalone application root
    └── vr_test_app.e        -- Automated verification test harness
```

## Project Milestones

| Phase | Milestone | Description | Status |
| :---: | :--- | :--- | :---: |
| **1** | **Native C Reactor & Epoll Bridge** | Edge-triggered epoll, event ring buffers, generational FD tracking, sub-millisecond wakes | **Done** |
| **2** | **SCOOP Concurrency Architecture** | Processor boundaries, zero traitor guarantees, typed pointer FFI | **Done** |
| **3** | **Non-Blocking TCP Engine** | Bidirectional TCP echo, flow-control quotas, half-close clean FIN draining | **Done** |
| **4** | **High-Performance HTTP/1.1 Engine** | Zero-copy header slice scanner, keep-alive pipelining, HAProxy PROXY v2 preface decoding | **Done** |
| **5** | **RFC 6455 WebSockets & Pub/Sub** | Handshake upgrade, SIMD-aligned frame masking, partitioned multi-worker room broadcasting | **Done** |
| **6** | **Server-Sent Events & Cross-Protocol** | Zero-alloc SSE codec, Last-Event-ID catchup, WS $\leftrightarrow$ SSE room bridge | **Done** |
| **7** | **OTP Supervision & Resilient Control Plane** | Lock-free C heartbeats, atomic fault flags, sliding-window limiter, worker shard purge, hot-swap | **Done** |
