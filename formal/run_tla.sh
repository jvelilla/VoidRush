#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT_DIR="$(cd "${SCRIPT_DIR}/.." && pwd)"
TLA_JAR="${ROOT_DIR}/plan/tla/tla2tools.jar"

if [ ! -f "${TLA_JAR}" ]; then
  echo "Downloading tla2tools.jar..."
  curl -L -o "${TLA_JAR}" https://github.com/tlaplus/tlaplus/releases/download/v1.8.0/tla2tools.jar
fi

PROFILE="${1:-ci}"

if [ "${PROFILE}" = "deep" ]; then
  echo "Running deep state space exploration with VR_Reactor_Concurrency.cfg..."
  CFG="${SCRIPT_DIR}/tla/VR_Reactor_Concurrency.cfg"
else
  echo "Running fast CI verification with VR_Reactor_Concurrency_CI.cfg..."
  CFG="${SCRIPT_DIR}/tla/VR_Reactor_Concurrency_CI.cfg"
fi

java -XX:+UseParallelGC -cp "${TLA_JAR}" tlc2.TLC \
  -deadlock \
  -workers auto \
  -config "${CFG}" \
  "${SCRIPT_DIR}/tla/VR_Reactor_Concurrency.tla"
