#!/bin/bash
# Full table scan benchmark
# Tests key parsing overhead on a complete table scan

# Run benchmark: SELECT COUNT(*) from all rows
# Returns: duration_ms,rows_affected
run_full_scan() {
    local size="$1"
    time_sql_cmd "SELECT COUNT(*) FROM bench_users"
}
