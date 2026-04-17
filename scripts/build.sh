#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
BUILD_DIR="${ROOT_DIR}/build"
BIN_DIR="${BUILD_DIR}/bin"
BIN_PATH="${BIN_DIR}/trackengine"

mkdir -p "${BIN_DIR}"

cd "${ROOT_DIR}"

c++ \
  -std=c++20 \
  -O3 \
  -DNDEBUG \
  -Wall \
  -Wextra \
  -Wpedantic \
  -pthread \
  -I"${ROOT_DIR}/include" \
  -I"${ROOT_DIR}/third_party" \
  "${ROOT_DIR}/src/main.cpp" \
  "${ROOT_DIR}/src/engine_service.cpp" \
  "${ROOT_DIR}/src/schedule_store.cpp" \
  "${ROOT_DIR}/src/raptor.cpp" \
  "${ROOT_DIR}/src/csa.cpp" \
  "${ROOT_DIR}/src/logger.cpp" \
  -lsqlite3 \
  -o "${BIN_PATH}"

echo "${BIN_PATH}"
