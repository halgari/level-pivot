#!/bin/bash
# Batch DELETE benchmark
# Tests batch deletion efficiency

# Setup: create rows specifically for batch delete testing
setup_delete_batch() {
    local size="$1"
    # Create 1000 events for batch delete testing
    run_psql "${PG_DIR}" "${DATA_DIR}" "${PG_PORT}" "bench_db" -c \
        "INSERT INTO bench_events (event_id, event_type, payload) SELECT 'delete_batch_' || i, 'batch_delete_test', 'payload_' || i FROM generate_series(1, 1000) AS i" \
        >/dev/null 2>&1 || true
}

# Run benchmark: Delete 1000 rows in a single operation
# Returns: duration_ms,rows_affected
run_delete_batch() {
    local size="$1"

    # Delete all batch test events in one query
    time_sql_cmd "DELETE FROM bench_events WHERE event_id LIKE 'delete_batch_%'"
}
