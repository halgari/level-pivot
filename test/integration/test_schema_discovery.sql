-- Integration tests for schema discovery (IMPORT FOREIGN SCHEMA)
-- Tests the ability to discover table structure from existing LevelDB data

\echo '=== Schema Discovery Tests ==='

-- First, populate some data using raw mode so we have known data to discover
DROP FOREIGN TABLE IF EXISTS discovery_raw;
CREATE FOREIGN TABLE discovery_raw (
    key   TEXT,
    value TEXT
)
SERVER test_leveldb
OPTIONS (table_mode 'raw');

\echo '--- Populating test data ---'

-- Insert data with a consistent pattern: discover##{tenant}##{id}##{attr}
INSERT INTO discovery_raw VALUES ('discover##acme##user001##name', 'Alice');
INSERT INTO discovery_raw VALUES ('discover##acme##user001##email', 'alice@acme.com');
INSERT INTO discovery_raw VALUES ('discover##acme##user001##department', 'Engineering');
INSERT INTO discovery_raw VALUES ('discover##acme##user002##name', 'Bob');
INSERT INTO discovery_raw VALUES ('discover##acme##user002##email', 'bob@acme.com');
INSERT INTO discovery_raw VALUES ('discover##acme##user002##department', 'Sales');
INSERT INTO discovery_raw VALUES ('discover##globex##user003##name', 'Charlie');
INSERT INTO discovery_raw VALUES ('discover##globex##user003##email', 'charlie@globex.com');

\echo 'Inserted 8 test keys'

-- Verify data was inserted
\echo '--- Verifying test data ---'
SELECT COUNT(*) AS key_count FROM discovery_raw WHERE key LIKE 'discover##%';

-- Test IMPORT FOREIGN SCHEMA
-- This should analyze the keys and generate a CREATE FOREIGN TABLE statement
\echo '--- Testing IMPORT FOREIGN SCHEMA ---'

-- Create a target schema for imported tables
DROP SCHEMA IF EXISTS discovered CASCADE;
CREATE SCHEMA discovered;

-- Import foreign schema - this invokes levelPivotImportForeignSchema
-- The FDW will scan keys, infer the pattern, and generate table DDL
IMPORT FOREIGN SCHEMA leveldb_schema
FROM SERVER test_leveldb
INTO discovered;

-- List tables in discovered schema
\echo '--- Tables created by IMPORT FOREIGN SCHEMA ---'
SELECT table_name
FROM information_schema.tables
WHERE table_schema = 'discovered';

-- If a table was created, show its columns
\echo '--- Columns of discovered table ---'
SELECT table_name, column_name, data_type
FROM information_schema.columns
WHERE table_schema = 'discovered'
ORDER BY table_name, ordinal_position;

-- Try to query the discovered table (if it exists)
\echo '--- Querying discovered data ---'
DO $$
DECLARE
    tbl_name TEXT;
    row_count INTEGER;
BEGIN
    -- Find first table in discovered schema
    SELECT table_name INTO tbl_name
    FROM information_schema.tables
    WHERE table_schema = 'discovered'
    LIMIT 1;

    IF tbl_name IS NOT NULL THEN
        EXECUTE format('SELECT COUNT(*) FROM discovered.%I', tbl_name) INTO row_count;
        RAISE NOTICE 'Table discovered.% has % rows', tbl_name, row_count;
    ELSE
        RAISE NOTICE 'No tables were created by IMPORT FOREIGN SCHEMA';
    END IF;
END $$;

-- Clean up test data
\echo '--- Cleanup ---'
DELETE FROM discovery_raw WHERE key LIKE 'discover##%';

-- Drop discovered schema
DROP SCHEMA IF EXISTS discovered CASCADE;

\echo '=== Schema Discovery Tests Complete ==='
