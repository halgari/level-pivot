#!/bin/bash
# Utility functions for benchmarking
# Sources by run_benchmarks.sh and individual benchmark scripts

# CSV output file (set by init_csv_output)
CSV_FILE=""

# Initialize CSV output with header
# Usage: init_csv_output [output_dir]
init_csv_output() {
    local output_dir="${1:-${SCRIPT_DIR}/results}"
    mkdir -p "${output_dir}"

    local timestamp
    timestamp=$(date +%Y%m%d_%H%M%S)
    CSV_FILE="${output_dir}/benchmark_results_${timestamp}.csv"

    echo "benchmark,size,iteration,duration_ms,rows_affected,keys_per_sec" > "${CSV_FILE}"
    echo "${CSV_FILE}"
}

# Get current time in nanoseconds (or milliseconds on systems without nanosecond support)
get_time_ns() {
    if date +%N >/dev/null 2>&1 && [[ "$(date +%N)" != "N" ]]; then
        # Linux with nanosecond support
        echo $(($(date +%s%N)))
    else
        # macOS or systems without nanosecond support - use perl for milliseconds
        perl -MTime::HiRes=time -e 'printf "%.0f\n", time * 1000000000'
    fi
}

# Time a SQL command and return duration in milliseconds
# Usage: time_sql_cmd "SQL COMMAND"
# Returns: duration_ms,rows_affected
time_sql_cmd() {
    local sql="$1"
    local start_ns end_ns duration_ms
    local output row_count

    start_ns=$(get_time_ns)

    # Run SQL and capture output
    output=$(run_psql "${PG_DIR}" "${DATA_DIR}" "${PG_PORT}" "bench_db" -t -A -c "${sql}" 2>&1)
    local status=$?

    end_ns=$(get_time_ns)

    # Calculate duration in milliseconds (with decimals) using awk for portability
    duration_ms=$(awk "BEGIN {printf \"%.3f\", (${end_ns} - ${start_ns}) / 1000000}")

    # Extract row count from output if it's a count query
    if [[ "${sql}" =~ SELECT.*COUNT ]]; then
        row_count="${output}"
    elif [[ "${output}" =~ ^[0-9]+$ ]]; then
        row_count="${output}"
    else
        # For INSERT/UPDATE/DELETE, try to get affected rows
        # psql outputs like "INSERT 0 5" or "UPDATE 5" or "DELETE 5"
        row_count=$(echo "${output}" | grep -oE '[0-9]+$' | tail -1)
        if [[ -z "${row_count}" ]]; then
            row_count=0
        fi
    fi

    if [[ ${status} -ne 0 ]]; then
        echo "ERROR: ${output}" >&2
        echo "-1,0"
        return 1
    fi

    echo "${duration_ms},${row_count}"
}

# Time a SQL file and return duration in milliseconds
# Usage: time_sql_file "path/to/file.sql"
# Returns: duration_ms,rows_affected
time_sql_file() {
    local sql_file="$1"
    local start_ns end_ns duration_ms
    local output

    start_ns=$(get_time_ns)

    # Run SQL file
    output=$(run_psql "${PG_DIR}" "${DATA_DIR}" "${PG_PORT}" "bench_db" -f "${sql_file}" 2>&1)
    local status=$?

    end_ns=$(get_time_ns)

    duration_ms=$(awk "BEGIN {printf \"%.3f\", (${end_ns} - ${start_ns}) / 1000000}")

    if [[ ${status} -ne 0 ]]; then
        echo "ERROR: ${output}" >&2
        echo "-1,0"
        return 1
    fi

    echo "${duration_ms},0"
}

# Record a benchmark result to CSV
# Usage: record_result benchmark_name size iteration duration_ms rows_affected
record_result() {
    local benchmark="$1"
    local size="$2"
    local iteration="$3"
    local duration_ms="$4"
    local rows_affected="$5"

    # Calculate keys per second (avoid division by zero) using awk for portability
    local keys_per_sec
    if [[ "${duration_ms}" != "0" && "${duration_ms}" != "-1" && -n "${duration_ms}" ]]; then
        keys_per_sec=$(awk "BEGIN {printf \"%.2f\", ${rows_affected} * 1000 / ${duration_ms}}" 2>/dev/null || echo "0")
    else
        keys_per_sec="0"
    fi

    echo "${benchmark},${size},${iteration},${duration_ms},${rows_affected},${keys_per_sec}" >> "${CSV_FILE}"
}

# Run a benchmark with setup and teardown
# Usage: run_benchmark benchmark_name size iteration
# Expects functions: setup_<benchmark>, run_<benchmark>, teardown_<benchmark>
run_benchmark() {
    local benchmark="$1"
    local size="$2"
    local iteration="$3"

    # Run setup if defined
    local setup_fn="setup_${benchmark}"
    if declare -f "${setup_fn}" >/dev/null 2>&1; then
        "${setup_fn}" "${size}" || return 1
    fi

    # Run the benchmark
    local run_fn="run_${benchmark}"
    if ! declare -f "${run_fn}" >/dev/null 2>&1; then
        echo "ERROR: run_${benchmark} not defined" >&2
        return 1
    fi

    local result
    result=$("${run_fn}" "${size}")
    local status=$?

    # Run teardown if defined
    local teardown_fn="teardown_${benchmark}"
    if declare -f "${teardown_fn}" >/dev/null 2>&1; then
        "${teardown_fn}" "${size}" || true
    fi

    if [[ ${status} -ne 0 ]]; then
        return 1
    fi

    # Parse result (duration_ms,rows_affected)
    local duration_ms rows_affected
    duration_ms=$(echo "${result}" | cut -d',' -f1)
    rows_affected=$(echo "${result}" | cut -d',' -f2)

    record_result "${benchmark}" "${size}" "${iteration}" "${duration_ms}" "${rows_affected}"

    # Return result for display
    echo "${duration_ms}"
}

# Generate summary statistics from CSV
# Usage: generate_summary
generate_summary() {
    echo ""
    echo "========================================"
    echo "Benchmark Summary"
    echo "========================================"
    echo ""
    printf "%-25s %8s %10s %10s %10s %12s\n" "Benchmark" "Size" "Min(ms)" "Max(ms)" "Avg(ms)" "Keys/sec"
    printf "%-25s %8s %10s %10s %10s %12s\n" "-------------------------" "--------" "----------" "----------" "----------" "------------"

    # Skip header, group by benchmark and size, calculate statistics
    tail -n +2 "${CSV_FILE}" | \
    awk -F',' '
    {
        key = $1 "," $2
        if (!(key in count)) {
            count[key] = 0
            sum[key] = 0
            min[key] = 999999999
            max[key] = 0
            kps_sum[key] = 0
            order[++n] = key
        }
        count[key]++
        sum[key] += $4
        kps_sum[key] += $6
        if ($4 < min[key]) min[key] = $4
        if ($4 > max[key]) max[key] = $4
    }
    END {
        for (i = 1; i <= n; i++) {
            key = order[i]
            split(key, parts, ",")
            benchmark = parts[1]
            size = parts[2]
            avg = sum[key] / count[key]
            kps_avg = kps_sum[key] / count[key]
            printf "%-25s %8d %10.1f %10.1f %10.1f %12.1f\n", benchmark, size, min[key], max[key], avg, kps_avg
        }
    }
    '
}

# Print progress indicator
print_progress() {
    local benchmark="$1"
    local size="$2"
    local iteration="$3"
    local total_iterations="$4"
    local duration_ms="$5"

    printf "  [%d/%d] %s @ %d rows: %.1f ms\n" \
        "${iteration}" "${total_iterations}" "${benchmark}" "${size}" "${duration_ms}"
}
