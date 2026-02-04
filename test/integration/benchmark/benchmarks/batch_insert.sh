#!/bin/bash
# Batch INSERT benchmark
# Tests WriteBatch efficiency with multi-row inserts

# Setup: ensure events table is ready
setup_batch_insert() {
    local size="$1"
    # Clean events table before batch insert test
    run_psql "${PG_DIR}" "${DATA_DIR}" "${PG_PORT}" "bench_db" -c \
        "DELETE FROM bench_events" >/dev/null 2>&1 || true
}

# Run benchmark: Insert 1000 rows in a single batch
# Returns: duration_ms,rows_affected
run_batch_insert() {
    local size="$1"

    # Generate a batch INSERT with 1000 rows using generate_series
    time_sql_cmd "INSERT INTO bench_events (event_id, event_type, payload) SELECT 'batch_' || i, 'batch_test', 'payload_' || i FROM generate_series(1, 1000) AS i"
}

# Teardown: clean up inserted rows
teardown_batch_insert() {
    local size="$1"
    run_psql "${PG_DIR}" "${DATA_DIR}" "${PG_PORT}" "bench_db" -c \
        "DELETE FROM bench_events WHERE event_id LIKE 'batch_%'" >/dev/null 2>&1 || true
}
