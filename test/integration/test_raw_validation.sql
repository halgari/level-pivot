-- Integration tests for raw table mode validation
-- Tests that option validation works correctly

\echo '=== Raw Table Validation Tests ==='

-- Test: raw mode should NOT allow key_pattern
\echo '--- Test: raw mode rejects key_pattern ---'
DO $$
BEGIN
    EXECUTE '
        CREATE FOREIGN TABLE raw_with_pattern (
            key   TEXT,
            value TEXT
        )
        SERVER test_leveldb
        OPTIONS (table_mode ''raw'', key_pattern ''test##{id}##{attr}'')
    ';
    RAISE EXCEPTION 'Expected error was not raised';
EXCEPTION
    WHEN fdw_invalid_option_name THEN
        RAISE NOTICE 'Correctly rejected: raw mode with key_pattern';
END $$;

-- Test: pivot mode (default) requires key_pattern
\echo '--- Test: pivot mode requires key_pattern ---'
DO $$
BEGIN
    EXECUTE '
        CREATE FOREIGN TABLE pivot_no_pattern (
            id   TEXT,
            name TEXT
        )
        SERVER test_leveldb
    ';
    RAISE EXCEPTION 'Expected error was not raised';
EXCEPTION
    WHEN fdw_option_name_not_found THEN
        RAISE NOTICE 'Correctly rejected: pivot mode without key_pattern';
END $$;

-- Test: explicit pivot mode requires key_pattern
\echo '--- Test: explicit pivot mode requires key_pattern ---'
DO $$
BEGIN
    EXECUTE '
        CREATE FOREIGN TABLE explicit_pivot_no_pattern (
            id   TEXT,
            name TEXT
        )
        SERVER test_leveldb
        OPTIONS (table_mode ''pivot'')
    ';
    RAISE EXCEPTION 'Expected error was not raised';
EXCEPTION
    WHEN fdw_option_name_not_found THEN
        RAISE NOTICE 'Correctly rejected: explicit pivot mode without key_pattern';
END $$;

-- Test: invalid table_mode value
\echo '--- Test: invalid table_mode value ---'
DO $$
BEGIN
    EXECUTE '
        CREATE FOREIGN TABLE invalid_mode (
            key   TEXT,
            value TEXT
        )
        SERVER test_leveldb
        OPTIONS (table_mode ''invalid'')
    ';
    RAISE EXCEPTION 'Expected error was not raised';
EXCEPTION
    WHEN fdw_invalid_attribute_value THEN
        RAISE NOTICE 'Correctly rejected: invalid table_mode value';
END $$;

-- Test: valid raw mode without key_pattern succeeds
\echo '--- Test: valid raw mode succeeds ---'
DROP FOREIGN TABLE IF EXISTS valid_raw_test;
CREATE FOREIGN TABLE valid_raw_test (
    key   TEXT,
    value TEXT
)
SERVER test_leveldb
OPTIONS (table_mode 'raw');
\echo 'Successfully created raw table'
DROP FOREIGN TABLE valid_raw_test;

-- Test: valid pivot mode with key_pattern succeeds
\echo '--- Test: valid pivot mode succeeds ---'
DROP FOREIGN TABLE IF EXISTS valid_pivot_test;
CREATE FOREIGN TABLE valid_pivot_test (
    id   TEXT,
    name TEXT,
    email TEXT
)
SERVER test_leveldb
OPTIONS (key_pattern 'test##{id}##{attr}');
\echo 'Successfully created pivot table'
DROP FOREIGN TABLE valid_pivot_test;

\echo '=== Raw Table Validation Tests Complete ==='
