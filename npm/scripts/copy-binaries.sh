#!/bin/bash
# Copies the built level_pivot.so to the appropriate npm platform package
# Usage: ./copy-binaries.sh [build_dir]
#   build_dir: Path to CMake build directory (default: ../../build)

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
NPM_DIR="$(dirname "${SCRIPT_DIR}")"
PROJECT_ROOT="$(dirname "${NPM_DIR}")"
BUILD_DIR="${1:-${PROJECT_ROOT}/build}"

detect_platform() {
    local os arch
    case "$(uname -s)" in
        Linux)  os="linux" ;;
        Darwin) os="darwin" ;;
        MINGW*|MSYS*|CYGWIN*) os="windows" ;;
        *)      echo "Unsupported OS: $(uname -s)" >&2; exit 1 ;;
    esac
    case "$(uname -m)" in
        x86_64|amd64)  arch="x64" ;;
        aarch64|arm64) arch="arm64" ;;
        *)             echo "Unsupported arch: $(uname -m)" >&2; exit 1 ;;
    esac
    echo "${os}-${arch}"
}

PLATFORM=$(detect_platform)
PKG_DIR="${NPM_DIR}/packages/level-pivot-${PLATFORM}"

echo "Platform: ${PLATFORM}"
echo "Build directory: ${BUILD_DIR}"
echo "Target package: ${PKG_DIR}"

# Find the .so file in various possible locations
SO_FILE=""
for path in \
    "${BUILD_DIR}/level_pivot.so" \
    "${BUILD_DIR}/Release/level_pivot.so" \
    "${BUILD_DIR}/release/level_pivot.so" \
    "${BUILD_DIR}/lib/level_pivot.so"
do
    if [[ -f "${path}" ]]; then
        SO_FILE="${path}"
        break
    fi
done

if [[ -z "${SO_FILE}" ]]; then
    echo "Error: Could not find level_pivot.so in ${BUILD_DIR}" >&2
    echo "Build the project first with: cmake --build build" >&2
    exit 1
fi

# Check if target package directory exists
if [[ ! -d "${PKG_DIR}" ]]; then
    echo "Error: Platform package directory does not exist: ${PKG_DIR}" >&2
    echo "Create it first or use a supported platform." >&2
    exit 1
fi

# Copy the binary
mkdir -p "${PKG_DIR}/binaries"
cp "${SO_FILE}" "${PKG_DIR}/binaries/level_pivot.so"
echo "Copied ${SO_FILE} -> ${PKG_DIR}/binaries/level_pivot.so"

# Copy SQL files if they don't exist
if [[ ! -f "${PKG_DIR}/sql/level_pivot.control" ]]; then
    mkdir -p "${PKG_DIR}/sql"
    cp "${PROJECT_ROOT}/sql/level_pivot.control" "${PKG_DIR}/sql/"
    cp "${PROJECT_ROOT}/sql/level_pivot--1.0.sql" "${PKG_DIR}/sql/"
    echo "Copied SQL extension files"
fi

echo "Done! Binary package is ready at ${PKG_DIR}"
