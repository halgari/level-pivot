-- Benchmark table setup for level_pivot FDW
-- Creates foreign tables used by benchmark scripts

-- Create extension
CREATE EXTENSION IF NOT EXISTS level_pivot;

-- Create test server for benchmarks
CREATE SERVER IF NOT EXISTS bench_leveldb
    FOREIGN DATA WRAPPER level_pivot
    OPTIONS (
        db_path '/tmp/level_pivot_bench',
        read_only 'false',
        create_if_missing 'true'
    );

-- Users table (2 identity columns, 3 attrs)
-- Used for filtered scans and updates
CREATE FOREIGN TABLE IF NOT EXISTS bench_users (
    tenant      TEXT,
    user_id     TEXT,
    name        TEXT,
    email       TEXT,
    status      TEXT
)
SERVER bench_leveldb
OPTIONS (
    key_pattern 'users##{tenant}##{user_id}##{attr}'
);

-- Metrics table (3 identity columns, 2 attrs)
-- Used for prefix scan benchmarks
CREATE FOREIGN TABLE IF NOT EXISTS bench_metrics (
    tenant      TEXT,
    service     TEXT,
    metric_id   TEXT,
    value       TEXT,
    timestamp   TEXT
)
SERVER bench_leveldb
OPTIONS (
    key_pattern 'metrics##{tenant}##{service}##{metric_id}##{attr}'
);

-- Events table (1 identity column, 2 attrs)
-- Used for bulk insert/delete operations
CREATE FOREIGN TABLE IF NOT EXISTS bench_events (
    event_id    TEXT,
    event_type  TEXT,
    payload     TEXT
)
SERVER bench_leveldb
OPTIONS (
    key_pattern 'events##{event_id}##{attr}'
);

SELECT 'Benchmark tables created successfully' AS status;
