# VoidRush

A **high-concurrency networking backend for Eiffel** that combines:

- A **thin C reactor** using modern OS facilities (`epoll` on Linux, `kqueue` on BSD/macOS, `IOCP` on Windows) for efficient non-blocking I/O.
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
