#!/bin/bash
# Single-row INSERT benchmark
# Tests per-row write overhead

# Setup: ensure events table is ready
setup_single_insert() {
    local size="$1"
    # Clean events table before insert test
    run_psql "${PG_DIR}" "${DATA_DIR}" "${PG_PORT}" "bench_db" -c \
        "DELETE FROM bench_events" >/dev/null 2>&1 || true
}

# Run benchmark: Insert 100 single rows
# Returns: duration_ms,rows_affected
run_single_insert() {
    local size="$1"
    local start_ns end_ns duration_ms

    start_ns=$(get_time_ns)

    # Insert 100 single rows
    for i in $(seq 1 100); do
        run_psql "${PG_DIR}" "${DATA_DIR}" "${PG_PORT}" "bench_db" -c \
            "INSERT INTO bench_events (event_id, event_type, payload) VALUES ('single_${i}', 'test', 'payload_${i}')" \
            >/dev/null 2>&1
    done

    end_ns=$(get_time_ns)
    duration_ms=$(awk "BEGIN {printf \"%.3f\", (${end_ns} - ${start_ns}) / 1000000}")

    echo "${duration_ms},100"
}

# Teardown: clean up inserted rows
teardown_single_insert() {
    local size="$1"
    run_psql "${PG_DIR}" "${DATA_DIR}" "${PG_PORT}" "bench_db" -c \
        "DELETE FROM bench_events WHERE event_id LIKE 'single_%'" >/dev/null 2>&1 || true
}
