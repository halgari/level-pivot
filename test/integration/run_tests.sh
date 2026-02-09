#!/bin/bash
# Main entry point for integration tests
# Usage: ./run_tests.sh [BUILD_DIR]

set -e

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PROJECT_DIR="$(cd "${SCRIPT_DIR}/../.." && pwd)"
BUILD_DIR="${BUILD_DIR:-${1:-${PROJECT_DIR}/build}}"
SQL_DIR="${PROJECT_DIR}/sql"

# Source the embedded postgres functions
source "${SCRIPT_DIR}/embedded_postgres.sh"

# Setup trap for cleanup
trap cleanup EXIT

echo "========================================"
echo "level_pivot Integration Tests"
echo "========================================"
echo ""
echo "Build directory: ${BUILD_DIR}"
echo "Project directory: ${PROJECT_DIR}"
echo ""

# Setup embedded PostgreSQL
echo "Setting up embedded PostgreSQL..."
setup_embedded_postgres

echo ""
echo "PostgreSQL directory: ${PG_DIR}"
echo "Data directory: ${DATA_DIR}"
echo "Port: ${PG_PORT}"
echo ""

# Install the extension
install_extension "${PG_DIR}" "${BUILD_DIR}" "${SQL_DIR}"

# Set library path for psql
if [[ "$(uname -s)" == "Darwin" ]]; then
    export DYLD_LIBRARY_PATH="${PG_DIR}/lib:${DYLD_LIBRARY_PATH:-}"
else
    export LD_LIBRARY_PATH="${PG_DIR}/lib:${LD_LIBRARY_PATH:-}"
fi

# Run tests
run_test() {
    local test_file="$1"
    local test_name
    test_name=$(basename "${test_file}" .sql)

    echo "----------------------------------------"
    echo "Running: ${test_name}"
    echo "----------------------------------------"

    if run_psql "${PG_DIR}" "${DATA_DIR}" "${PG_PORT}" "test_db" -f "${test_file}"; then
        echo "[PASS] ${test_name}"
        return 0
    else
        echo "[FAIL] ${test_name}"
        return 1
    fi
}

FAILED=0

# Run setup
run_test "${SCRIPT_DIR}/setup.sql" || FAILED=1

if [[ $FAILED -eq 0 ]]; then
    # Run test files
    run_test "${SCRIPT_DIR}/test_select.sql" || FAILED=1
    run_test "${SCRIPT_DIR}/test_modify.sql" || FAILED=1

    # Run WriteBatch tests if present
    if [[ -f "${SCRIPT_DIR}/test_write_batch.sql" ]]; then
        run_test "${SCRIPT_DIR}/test_write_batch.sql" || FAILED=1
    fi

    # Run raw table mode tests
    if [[ -f "${SCRIPT_DIR}/test_raw_validation.sql" ]]; then
        run_test "${SCRIPT_DIR}/test_raw_validation.sql" || FAILED=1
    fi
    if [[ -f "${SCRIPT_DIR}/test_raw.sql" ]]; then
        run_test "${SCRIPT_DIR}/test_raw.sql" || FAILED=1
    fi

    # Run NOTIFY tests
    if [[ -f "${SCRIPT_DIR}/test_notify.sql" ]]; then
        run_test "${SCRIPT_DIR}/test_notify.sql" || FAILED=1
    fi

    # Run cleanup
    run_test "${SCRIPT_DIR}/cleanup.sql" || FAILED=1
fi

echo ""
echo "========================================"
if [[ $FAILED -eq 0 ]]; then
    echo "All integration tests PASSED"
    echo "========================================"
    exit 0
else
    echo "Some integration tests FAILED"
    echo "========================================"
    exit 1
fi
