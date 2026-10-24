#!/bin/bash
set -e

SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
BUILD_DIR="$SCRIPT_DIR/_build"

echo "Cleaning up VoidRush Docker build artifacts..."

# Remove local build directory
if [ -d "$BUILD_DIR" ]; then
    echo "Removing $BUILD_DIR..."
    rm -rf "$BUILD_DIR"
fi

# Remove Linux compilation volume
echo "Removing Linux EIFGENs compilation cache volume..."
docker volume rm -f voidrush_eifgens_linux 2>/dev/null || true

# Stop compose if running
echo "Stopping Docker Compose containers..."
cd "$SCRIPT_DIR"
docker compose down 2>/dev/null || true

echo "Clean completed successfully."
