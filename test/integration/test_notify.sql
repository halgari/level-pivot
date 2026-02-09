-- Integration tests for NOTIFY functionality
-- Tests that INSERT/UPDATE/DELETE trigger notifications on FDW tables
--
-- Note: PostgreSQL's NOTIFY is async and delivered at transaction commit.
-- These tests verify the code path executes without error.
-- Full async notification testing requires multiple connections.

\echo '=== NOTIFY Functionality Tests ==='

-- Setup: Clean state
DELETE FROM users WHERE group_name = 'notify_test';

-- Test 1: Basic INSERT notification
\echo '--- Test 1: INSERT triggers notification path ---'
LISTEN public_users_changed;
INSERT INTO users (group_name, id, name) VALUES ('notify_test', 'n1', 'Alice');
-- Notification queued, will be delivered on commit
\echo 'INSERT completed - notification queued'

-- Test 2: UPDATE notification
\echo '--- Test 2: UPDATE triggers notification path ---'
UPDATE users SET name = 'Alice Updated' WHERE group_name = 'notify_test' AND id = 'n1';
\echo 'UPDATE completed - notification queued'

-- Test 3: DELETE notification
\echo '--- Test 3: DELETE triggers notification path ---'
DELETE FROM users WHERE group_name = 'notify_test' AND id = 'n1';
\echo 'DELETE completed - notification queued'

-- Test 4: Batch INSERT (single notification per statement)
\echo '--- Test 4: Batch INSERT triggers notification path ---'
INSERT INTO users (group_name, id, name) VALUES
    ('notify_test', 'n2', 'Bob'),
    ('notify_test', 'n3', 'Charlie');
\echo 'Batch INSERT completed - notification queued'

-- Cleanup pivot table test data
DELETE FROM users WHERE group_name = 'notify_test';
UNLISTEN public_users_changed;

-- Test 5: Raw table notification
\echo '--- Test 5: Raw table INSERT triggers notification path ---'

-- Ensure raw_test table exists
DROP FOREIGN TABLE IF EXISTS raw_test;
CREATE FOREIGN TABLE raw_test (
    key   TEXT,
    value TEXT
)
SERVER test_leveldb
OPTIONS (table_mode 'raw');

LISTEN public_raw_test_changed;
INSERT INTO raw_test (key, value) VALUES ('notify_key_1', 'notify_value_1');
\echo 'Raw INSERT completed - notification queued'

-- Test 6: Raw table UPDATE notification
\echo '--- Test 6: Raw table UPDATE triggers notification path ---'
UPDATE raw_test SET value = 'updated_value' WHERE key = 'notify_key_1';
\echo 'Raw UPDATE completed - notification queued'

-- Test 7: Raw table DELETE notification
\echo '--- Test 7: Raw table DELETE triggers notification path ---'
DELETE FROM raw_test WHERE key = 'notify_key_1';
\echo 'Raw DELETE completed - notification queued'

UNLISTEN public_raw_test_changed;

\echo ''
\echo '=== NOTIFY Tests Completed Successfully ==='
\echo 'Note: Actual notification delivery requires checking via LISTEN in separate connection'
