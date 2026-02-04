#!/bin/bash
# Single DELETE benchmark
# Tests key scan for deletion of individual rows

# Setup: create rows specifically for delete testing
setup_delete_single() {
    local size="$1"
    # Create 100 events for single delete testing
    run_psql "${PG_DIR}" "${DATA_DIR}" "${PG_PORT}" "bench_db" -c \
        "INSERT INTO bench_events (event_id, event_type, payload) SELECT 'delete_single_' || i, 'delete_test', 'payload_' || i FROM generate_series(1, 100) AS i" \
        >/dev/null 2>&1 || true
}

# Run benchmark: Delete 100 rows one at a time
# Returns: duration_ms,rows_affected
run_delete_single() {
    local size="$1"
    local start_ns end_ns duration_ms

    start_ns=$(get_time_ns)

    # Delete 100 single rows
    for i in $(seq 1 100); do
        run_psql "${PG_DIR}" "${DATA_DIR}" "${PG_PORT}" "bench_db" -c \
            "DELETE FROM bench_events WHERE event_id = 'delete_single_${i}'" \
            >/dev/null 2>&1
    done

    end_ns=$(get_time_ns)
    duration_ms=$(awk "BEGIN {printf \"%.3f\", (${end_ns} - ${start_ns}) / 1000000}")

    echo "${duration_ms},100"
}
