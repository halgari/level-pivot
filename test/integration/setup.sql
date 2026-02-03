-- Integration test setup for level_pivot FDW
-- Run this script to set up test tables

-- Create extension
CREATE EXTENSION IF NOT EXISTS level_pivot;

-- Create test server
CREATE SERVER IF NOT EXISTS test_leveldb
    FOREIGN DATA WRAPPER level_pivot
    OPTIONS (
        db_path '/tmp/level_pivot_test',
        read_only 'false',
        create_if_missing 'true'
    );

-- Create foreign table with classic pattern
CREATE FOREIGN TABLE IF NOT EXISTS users (
    group_name  TEXT,
    id          TEXT,
    name        TEXT,
    email       TEXT,
    created_at  TEXT
)
SERVER test_leveldb
OPTIONS (
    key_pattern 'users##{group_name}##{id}##{attr}'
);

-- Create foreign table with mixed delimiters
CREATE FOREIGN TABLE IF NOT EXISTS sales (
    arg         TEXT,
    sub_arg     TEXT,
    revenue     TEXT,
    count       TEXT,
    margin      TEXT
)
SERVER test_leveldb
OPTIONS (
    key_pattern 'this###{arg}__{sub_arg}##pat##{attr}'
);

-- Create foreign table with different delimiters
CREATE FOREIGN TABLE IF NOT EXISTS metrics (
    tenant      TEXT,
    env         TEXT,
    service     TEXT,
    requests    TEXT,
    latency_p99 TEXT,
    error_rate  TEXT
)
SERVER test_leveldb
OPTIONS (
    key_pattern '{tenant}:{env}/{service}/{attr}'
);

-- Output success message
SELECT 'Integration test tables created successfully' AS status;
