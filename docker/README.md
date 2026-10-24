# VoidRush — Linux Docker Build & Test Guide

This environment provides a reproducible Linux container for compiling and testing **VoidRush** (SCOOP actor network engine with native C `epoll` reactor bridge), verifying event-loop correctness, non-blocking I/O, and running leak-detection harnesses under Linux.

Inspired by the Eiffel containerization pattern in `tui_dev/docker`.

---

## Architectural Overview

VoidRush's primary engine runs on Linux to leverage kernel primitives such as `epoll(7)` (Edge-Triggered + `EPOLLONESHOT`), `eventfd(2)`, and `accept4(2)`. Developing on a Windows host requires a containerized Linux build and runtime.

```
┌─────────────────────────────────────────────────────────────┐
│  Host Machine (Windows / Linux / macOS)                     │
│                                                             │
│  VoidRush/ (Source Code) ─────volume──────► /opt/eiffel/src/ │
│  docker/_build/ ─────────────volume──────► /opt/eiffel/build│
│  voidrush_eifgens_linux ─────volume──────► .../src/EIFGENs  │
└─────────────────────────────────────────────────────────────┘
                              │
                              ▼
┌─────────────────────────────────────────────────────────────┐
│  Container: voidrush/tool-eiffel-compilation                │
│  OS: Debian 13 (trixie)                                     │
│  Runtime: EiffelStudio 26.09 + GCC + glibc + Valgrind       │
│                                                             │
│  Modes:                                                     │
│    ├─ test        ► Runs native C tests & Eiffel test suite │
│    ├─ c-test      ► Runs native C epoll unit tests          │
│    ├─ valgrind    ► Runs C tests under Valgrind (no leaks)  │
│    ├─ server      ► Runs VoidRush server on port 8080       │
│    ├─ compile     ► Compiles Linux binaries to _build/      │
│    └─ shell       ► Interactive /bin/bash shell             │
└─────────────────────────────────────────────────────────────┘
```

### Why Isolated `EIFGENs` Volume?
When pair-programming on Windows, the host directory `VoidRush/EIFGENs` (if EiffelStudio is run locally) contains Windows PE/COFF `.obj` and `.lib` binaries. Mounting the isolated Docker volume `voidrush_eifgens_linux` over `/opt/eiffel/src/EIFGENs` inside the container guarantees:
1. Linux compilation uses its own ELF-64 objects and cache.
2. Windows host directory is never polluted or corrupted.
3. Subsequent Linux builds inside the container remain fast and incremental.

---

## Prerequisites

- [Docker Desktop](https://www.docker.com/products/docker-desktop/) (configured with WSL2 backend on Windows) or Docker Engine on Linux.
- Docker daemon running.

### EiffelStudio version

The image installs **EiffelStudio 26.09 (rev 99088)**. The version is pinned by the `ISE_MAJOR_MINOR_*` / `ISE_BUILD_*` defaults in `images/tools/eiffel-compilation/files/bin/install.sh`.

Debian 13 (trixie) is used as base to provide glibc 2.38+ required by EiffelStudio 26.09.

After changing the compiler version, rebuild the image and clean old Linux compilation caches:
- PowerShell: `.\docker\build.ps1 -Reset`
- Bash: `./docker/build.sh --reset`

---

## Directory Structure

```
docker/
├── README.md                 ← This documentation
├── build.ps1                 ← Build & run script (PowerShell)
├── build.sh                  ← Build & run script (Bash)
├── clean_all.ps1             ← Cleans build directory & Linux volume
├── clean_all.sh              ← Cleans build directory & Linux volume (Bash)
├── docker-compose.yml        ← Compose service definitions
├── _build/                   ← Compiled Linux binaries output (git-ignored)
└── images/
    └── tools/
        └── eiffel-compilation/
            ├── dockerfile    ← Debian Trixie + GCC + EiffelStudio + Valgrind
            └── files/
                └── bin/
                    ├── install.sh       ← EiffelStudio installer
                    ├── get_eiffel_env   ← Environment configuration (setup.rc)
                    └── main             ← Container entrypoint
```

---

## Quick Start Commands

### 1. Run Automated Test Suite (Default)
Compiles and runs the native C reactor tests, and compiles/executes the Eiffel test suite if `voidrush.ecf` is present:

**Windows PowerShell:**
```powershell
.\docker\build.ps1
# Or explicitly:
.\docker\build.ps1 -Test
```

**Linux / macOS Bash:**
```bash
./docker/build.sh
# Or explicitly:
./docker/build.sh --test
```

### 2. Run Native C epoll Reactor Tests
Builds and runs the standalone C test harness in `native/`:

**Windows PowerShell:**
```powershell
.\docker\build.ps1 -CTest
```

**Linux / macOS Bash:**
```bash
./docker/build.sh --c-test
```

### 3. Check for File Descriptor & Memory Leaks (Valgrind)
Runs the native C test harness under Valgrind with `--leak-check=full --track-fds=yes`:

**Windows PowerShell:**
```powershell
.\docker\build.ps1 -Valgrind
```

**Linux / macOS Bash:**
```bash
./docker/build.sh --valgrind
```

### 4. Run VoidRush Server
Starts the VoidRush server with port 8080 forwarded:

**Windows PowerShell:**
```powershell
.\docker\build.ps1 -Server
```

**Linux / macOS Bash:**
```bash
./docker/build.sh --server
```

### 5. Interactive Bash Shell
Enters an interactive bash shell with full EiffelStudio tools (`ec`, `ecb`, `eiffel`), GCC, Valgrind, and netcat:

**Windows PowerShell:**
```powershell
.\docker\build.ps1 -Shell
```

**Linux / macOS Bash:**
```bash
./docker/build.sh --shell
```

### 6. Compile Only
Compiles native C and Eiffel targets into `docker/_build/` without launching them:

**Windows PowerShell:**
```powershell
.\docker\build.ps1 -CompileOnly
```

**Linux / macOS Bash:**
```bash
./docker/build.sh --compile-only
```

---

## Docker Compose Alternative

You can also manage services via `docker compose`:

```bash
cd docker

# Run tests
docker compose run --rm test

# Run C native tests
docker compose run --rm c-test

# Run Valgrind leak checker
docker compose run --rm valgrind

# Run server (port 8080 published)
docker compose up server

# Open shell
docker compose run --rm shell

# Stop and clean up containers
docker compose down
```

---

## Cleaning Artifacts

To remove compiled Linux binaries and wipe the Linux `EIFGENs` cache volume:

**Windows PowerShell:**
```powershell
.\docker\clean_all.ps1
```

**Linux / macOS Bash:**
```bash
./docker/clean_all.sh
```

To force a full rebuild of the Docker image without cache:
```powershell
.\docker\build.ps1 -Reset -NoCache
```
```bash
./docker/build.sh --reset --no-cache
```
