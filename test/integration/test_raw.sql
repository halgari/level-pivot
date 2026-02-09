-- Integration tests for raw table mode
-- Tests basic CRUD operations and predicate pushdown

\echo '=== Raw Table Mode Tests ==='

-- Create raw foreign table
DROP FOREIGN TABLE IF EXISTS raw_test;
CREATE FOREIGN TABLE raw_test (
    key   TEXT,
    value TEXT
)
SERVER test_leveldb
OPTIONS (table_mode 'raw');

\echo 'Created raw_test table'

-- Test INSERT
\echo '--- Testing INSERT ---'
INSERT INTO raw_test VALUES ('raw:001', 'value1');
INSERT INTO raw_test VALUES ('raw:002', 'value2');
INSERT INTO raw_test VALUES ('raw:003', 'value3');
INSERT INTO raw_test VALUES ('raw:010', 'value10');
INSERT INTO raw_test VALUES ('raw:020', 'value20');
INSERT INTO raw_test VALUES ('other:key', 'other_value');

\echo 'Inserted 6 rows'

-- Test SELECT all
\echo '--- Testing SELECT * ---'
SELECT * FROM raw_test WHERE key LIKE 'raw:%' ORDER BY key;

-- Test exact match (should use seek)
\echo '--- Testing exact match (key = value) ---'
SELECT * FROM raw_test WHERE key = 'raw:002';

-- Test range query with >= and <
\echo '--- Testing range query (>= and <) ---'
SELECT * FROM raw_test WHERE key >= 'raw:002' AND key < 'raw:010' ORDER BY key;

-- Test range query with > and <=
\echo '--- Testing range query (> and <=) ---'
SELECT * FROM raw_test WHERE key > 'raw:001' AND key <= 'raw:003' ORDER BY key;

-- Test prefix-style range query
\echo '--- Testing prefix scan pattern ---'
SELECT * FROM raw_test WHERE key >= 'raw:' AND key < 'raw:\xFF' ORDER BY key;

-- Test EXPLAIN shows key bounds
\echo '--- Testing EXPLAIN output ---'
EXPLAIN (COSTS OFF) SELECT * FROM raw_test WHERE key = 'raw:002';
EXPLAIN (COSTS OFF) SELECT * FROM raw_test WHERE key >= 'raw:' AND key < 'raw:\xFF';

-- Test UPDATE
\echo '--- Testing UPDATE ---'
UPDATE raw_test SET value = 'updated_value2' WHERE key = 'raw:002';
SELECT * FROM raw_test WHERE key = 'raw:002';

-- Test DELETE
\echo '--- Testing DELETE ---'
DELETE FROM raw_test WHERE key = 'raw:003';
SELECT * FROM raw_test WHERE key = 'raw:003';

-- Verify row was deleted
\echo '--- Verifying deletion ---'
SELECT COUNT(*) AS count_after_delete FROM raw_test WHERE key LIKE 'raw:%';

-- Clean up
\echo '--- Cleanup ---'
DELETE FROM raw_test WHERE key LIKE 'raw:%';
DELETE FROM raw_test WHERE key = 'other:key';

SELECT COUNT(*) AS final_count FROM raw_test;

\echo '=== Raw Table Mode Tests Complete ==='
