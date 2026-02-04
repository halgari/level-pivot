#!/bin/bash
# Update with new identity benchmark
# Tests key scan + rewrite overhead when identity columns change

# Setup: create rows specifically for identity update testing
setup_update_new_identity() {
    local size="$1"
    # Create 100 users in a special tenant for identity update testing
    run_psql "${PG_DIR}" "${DATA_DIR}" "${PG_PORT}" "bench_db" -c \
        "INSERT INTO bench_users (tenant, user_id, name, email, status) SELECT 'tenant_identity_test', 'id_update_' || i, 'Name ' || i, 'email' || i || '@test.com', 'active' FROM generate_series(1, 100) AS i" \
        >/dev/null 2>&1 || true
}

# Run benchmark: Update identity column (tenant) which requires key rewrite
# Returns: duration_ms,rows_affected
run_update_new_identity() {
    local size="$1"

    # Update tenant (identity column) which requires deleting old keys and inserting new ones
    time_sql_cmd "UPDATE bench_users SET tenant = 'tenant_identity_updated' WHERE tenant = 'tenant_identity_test'"
}

# Teardown: clean up test rows
teardown_update_new_identity() {
    local size="$1"
    run_psql "${PG_DIR}" "${DATA_DIR}" "${PG_PORT}" "bench_db" -c \
        "DELETE FROM bench_users WHERE tenant IN ('tenant_identity_test', 'tenant_identity_updated')" \
        >/dev/null 2>&1 || true
}
