-- Cleanup integration test resources

-- Drop foreign tables
DROP FOREIGN TABLE IF EXISTS users;
DROP FOREIGN TABLE IF EXISTS sales;
DROP FOREIGN TABLE IF EXISTS metrics;

-- Drop server
DROP SERVER IF EXISTS test_leveldb CASCADE;

-- Note: Extension is not dropped to allow re-running tests

SELECT 'Integration test cleanup completed' AS status;
