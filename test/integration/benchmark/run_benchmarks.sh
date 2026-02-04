#!/bin/bash
# Main entry point for integration benchmarks
# Usage: ./run_benchmarks.sh [BUILD_DIR]
# Environment:
#   SIZES       - Comma-separated list of dataset sizes (default: 1000,10000,100000)
#   ITERATIONS  - Number of iterations per benchmark (default: 3)
#   BENCHMARKS  - Comma-separated list of benchmarks to run (default: all)

set -e

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PROJECT_DIR="$(cd "${SCRIPT_DIR}/../../.." && pwd)"
BUILD_DIR="${BUILD_DIR:-${1:-${PROJECT_DIR}/build}}"
SQL_DIR="${PROJECT_DIR}/sql"

# Configuration
SIZES="${SIZES:-1000,10000,100000}"
ITERATIONS="${ITERATIONS:-3}"
BENCHMARKS="${BENCHMARKS:-full_scan,filtered_scan,prefix_scan,single_insert,batch_insert,update_same_identity,update_new_identity,delete_single,delete_batch}"

# Source the embedded postgres functions
source "${SCRIPT_DIR}/../embedded_postgres.sh"

# Source benchmark utilities
source "${SCRIPT_DIR}/bench_utils.sh"

# Setup trap for cleanup
trap cleanup EXIT

echo "========================================"
echo "level_pivot Integration Benchmarks"
echo "========================================"
echo ""
echo "Build directory: ${BUILD_DIR}"
echo "Sizes: ${SIZES}"
echo "Iterations: ${ITERATIONS}"
echo "Benchmarks: ${BENCHMARKS}"
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

# Create benchmark database
echo "Creating benchmark database..."
run_psql "${PG_DIR}" "${DATA_DIR}" "${PG_PORT}" "postgres" -c "CREATE DATABASE bench_db" 2>/dev/null || true

# Run setup SQL
echo "Setting up benchmark tables..."
run_psql "${PG_DIR}" "${DATA_DIR}" "${PG_PORT}" "bench_db" -f "${SCRIPT_DIR}/bench_setup.sql"

# Initialize CSV output
CSV_FILE=$(init_csv_output)
echo ""
echo "Results will be written to: ${CSV_FILE}"
echo ""

# Convert comma-separated lists to arrays
IFS=',' read -ra SIZE_ARRAY <<< "${SIZES}"
IFS=',' read -ra BENCHMARK_ARRAY <<< "${BENCHMARKS}"

# Source all benchmark scripts
for benchmark in "${BENCHMARK_ARRAY[@]}"; do
    benchmark_script="${SCRIPT_DIR}/benchmarks/${benchmark}.sh"
    if [[ -f "${benchmark_script}" ]]; then
        source "${benchmark_script}"
    else
        echo "WARNING: Benchmark script not found: ${benchmark_script}" >&2
    fi
done

# Run benchmarks
echo "========================================"
echo "Running Benchmarks"
echo "========================================"

for size in "${SIZE_ARRAY[@]}"; do
    echo ""
    echo "----------------------------------------"
    echo "Dataset size: ${size} rows"
    echo "----------------------------------------"

    # Generate data for this size
    echo "Generating test data..."
    BENCH_SIZE="${size}" run_psql "${PG_DIR}" "${DATA_DIR}" "${PG_PORT}" "bench_db" \
        -v bench_size="${size}" \
        -f "${SCRIPT_DIR}/bench_data_gen.sql"

    for benchmark in "${BENCHMARK_ARRAY[@]}"; do
        echo ""
        echo "Running: ${benchmark}"

        for iteration in $(seq 1 "${ITERATIONS}"); do
            duration_ms=$(run_benchmark "${benchmark}" "${size}" "${iteration}")

            if [[ "${duration_ms}" == "-1" ]]; then
                echo "  [${iteration}/${ITERATIONS}] ERROR"
            else
                print_progress "${benchmark}" "${size}" "${iteration}" "${ITERATIONS}" "${duration_ms}"
            fi
        done
    done

    # Cleanup between sizes
    echo ""
    echo "Cleaning up..."
    run_psql "${PG_DIR}" "${DATA_DIR}" "${PG_PORT}" "bench_db" -f "${SCRIPT_DIR}/bench_cleanup.sql"
done

# Generate summary
generate_summary

echo ""
echo "========================================"
echo "Benchmarks Complete"
echo "========================================"
echo ""
echo "Results saved to: ${CSV_FILE}"
