#!/bin/bash
set -e

# Git Bash / MSYS on Windows rewrites arguments that look like POSIX paths, which
# breaks the "-v host:/opt/eiffel/..." mounts (the container then reports an
# invalid ECF). Disable that conversion; it has no effect on Linux or macOS.
export MSYS_NO_PATHCONV=1
export MSYS2_ARG_CONV_EXCL="*"

RESET=0
SHELL_MODE=0
NO_CACHE=0
SERVER_MODE=0
C_TEST=0
VALGRIND=0
COMPILE_ONLY=0

# Parse arguments
while [[ "$#" -gt 0 ]]; do
    case $1 in
        --reset|-reset|-r) RESET=1 ;;
        --shell|-shell|-s) SHELL_MODE=1 ;;
        --no-cache|-no-cache) NO_CACHE=1 ;;
        --server|-server) SERVER_MODE=1 ;;
        --c-test|-c-test|-ct) C_TEST=1 ;;
        --valgrind|-valgrind|-v) VALGRIND=1 ;;
        --compile-only|-compile-only|-c) COMPILE_ONLY=1 ;;
        --test|-test|-t) ;; # Default
        *) echo "Unknown parameter passed: $1"; exit 1 ;;
    esac
    shift
done

IMAGE_NAME="voidrush/tool-eiffel-compilation"
VOLUME_NAME="voidrush_eifgens_linux"

SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
BUILD_DIR="$SCRIPT_DIR/_build"
SRC_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"

echo "============================================================"
echo "  VoidRush Linux Docker Build & Test Environment"
echo "============================================================"
echo "Build Directory: $BUILD_DIR"
echo "Source Root:     $SRC_ROOT"

build_compiler_image() {
    echo "Building VoidRush compilation tool image: $IMAGE_NAME..."
    cd "$SCRIPT_DIR/images/tools/eiffel-compilation"
    
    opts=""
    if [ "$NO_CACHE" -eq 1 ]; then
        opts="$opts --no-cache"
    fi
    
    docker build $opts -t "$IMAGE_NAME" .
    echo "Compiler image built successfully."
}

ensure_compiler_image() {
    if ! docker inspect "$IMAGE_NAME" &> /dev/null || [ "$RESET" -eq 1 ]; then
        build_compiler_image
    else
        echo "Compiler image $IMAGE_NAME is ready."
    fi
}

if [ "$RESET" -eq 1 ]; then
    echo "Reset requested: Cleaning build artifacts and volume..."
    rm -rf "$BUILD_DIR"
    docker volume rm -f "$VOLUME_NAME" 2>/dev/null || true
fi

ensure_compiler_image
mkdir -p "$BUILD_DIR"

if [ "$SHELL_MODE" -eq 1 ]; then
    echo "Launching interactive bash shell in Linux container..."
    docker run --rm -it \
        -p "8080:8080" \
        -v "$BUILD_DIR:/opt/eiffel/build:rw" \
        -v "$SRC_ROOT:/opt/eiffel/src:rw" \
        -v "$VOLUME_NAME:/opt/eiffel/src/EIFGENs" \
        "$IMAGE_NAME" /bin/bash
    exit 0
fi

if [ "$SERVER_MODE" -eq 1 ]; then
    echo "Launching VoidRush Server in container..."
    docker run --rm -it \
        -p "8080:8080" \
        -v "$BUILD_DIR:/opt/eiffel/build:rw" \
        -v "$SRC_ROOT:/opt/eiffel/src:rw" \
        -v "$VOLUME_NAME:/opt/eiffel/src/EIFGENs" \
        "$IMAGE_NAME" server
    exit 0
fi

if [ "$C_TEST" -eq 1 ]; then
    echo "Running VoidRush native C tests in container..."
    docker run --rm \
        -v "$BUILD_DIR:/opt/eiffel/build:rw" \
        -v "$SRC_ROOT:/opt/eiffel/src:rw" \
        -v "$VOLUME_NAME:/opt/eiffel/src/EIFGENs" \
        "$IMAGE_NAME" c-test
    exit $?
fi

if [ "$VALGRIND" -eq 1 ]; then
    echo "Running Valgrind on VoidRush native C tests in container..."
    docker run --rm \
        -v "$BUILD_DIR:/opt/eiffel/build:rw" \
        -v "$SRC_ROOT:/opt/eiffel/src:rw" \
        -v "$VOLUME_NAME:/opt/eiffel/src/EIFGENs" \
        "$IMAGE_NAME" valgrind-c
    exit $?
fi

if [ "$COMPILE_ONLY" -eq 1 ]; then
    echo "Compiling targets inside container..."
    docker run --rm \
        -v "$BUILD_DIR:/opt/eiffel/build:rw" \
        -v "$SRC_ROOT:/opt/eiffel/src:rw" \
        -v "$VOLUME_NAME:/opt/eiffel/src/EIFGENs" \
        "$IMAGE_NAME" compile
    exit 0
fi

# Default action: run test suite
echo "Running VoidRush test suite on Linux..."
docker run --rm \
    -v "$BUILD_DIR:/opt/eiffel/build:rw" \
    -v "$SRC_ROOT:/opt/eiffel/src:rw" \
    -v "$VOLUME_NAME:/opt/eiffel/src/EIFGENs" \
    "$IMAGE_NAME" test

echo "All Linux container tasks completed successfully."
