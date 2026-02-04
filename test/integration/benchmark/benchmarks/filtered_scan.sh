#!/bin/bash
# Filtered scan benchmark
# Tests WHERE clause pushdown to prefix scan on first identity column

# Run benchmark: SELECT with filter on tenant (first identity column)
# This should use prefix scanning rather than full table scan
# Returns: duration_ms,rows_affected
run_filtered_scan() {
    local size="$1"
    time_sql_cmd "SELECT COUNT(*) FROM bench_users WHERE tenant = 'tenant_0'"
}
