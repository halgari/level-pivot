#!/bin/bash
# Multi-level prefix scan benchmark
# Tests WHERE clause pushdown with multiple identity columns

# Run benchmark: SELECT with filter on tenant and service (first two identity columns)
# This should use efficient prefix scanning on both levels
# Returns: duration_ms,rows_affected
run_prefix_scan() {
    local size="$1"
    time_sql_cmd "SELECT COUNT(*) FROM bench_metrics WHERE tenant = 'tenant_0' AND service = 'service_0'"
}
