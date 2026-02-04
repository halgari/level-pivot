#!/bin/bash
# Update same identity benchmark
# Tests incremental attribute updates (no key change)

# Run benchmark: Update only attribute columns for 100 rows
# This should be efficient as it only updates values, not keys
# Returns: duration_ms,rows_affected
run_update_same_identity() {
    local size="$1"

    # Update status attribute for users in tenant_0, limited to 100
    # This only changes attribute values, identity columns stay the same
    time_sql_cmd "UPDATE bench_users SET status = 'updated', email = email || '.updated' WHERE tenant = 'tenant_0' AND user_id IN (SELECT user_id FROM bench_users WHERE tenant = 'tenant_0' LIMIT 100)"
}
