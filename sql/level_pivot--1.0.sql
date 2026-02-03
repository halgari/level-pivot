-- level_pivot Foreign Data Wrapper for LevelDB
-- Exposes LevelDB key-value data as relational tables with pivot semantics

-- complain if script is sourced in psql, rather than via CREATE EXTENSION
\echo Use "CREATE EXTENSION level_pivot" to load this file. \quit

-- Create the FDW handler function
CREATE FUNCTION level_pivot_fdw_handler()
RETURNS fdw_handler
AS 'MODULE_PATHNAME'
LANGUAGE C STRICT;

-- Create the FDW validator function
CREATE FUNCTION level_pivot_fdw_validator(text[], oid)
RETURNS void
AS 'MODULE_PATHNAME'
LANGUAGE C STRICT;

-- Create the Foreign Data Wrapper
CREATE FOREIGN DATA WRAPPER level_pivot
    HANDLER level_pivot_fdw_handler
    VALIDATOR level_pivot_fdw_validator;

-- Grant usage to public (users still need CREATE SERVER privileges)
GRANT USAGE ON FOREIGN DATA WRAPPER level_pivot TO PUBLIC;

COMMENT ON FOREIGN DATA WRAPPER level_pivot IS
'Foreign Data Wrapper for LevelDB with pivot semantics.

Transforms LevelDB keys into relational rows where key segments become
identity columns and the final segment (attr) pivots into columns.

SERVER OPTIONS:
  db_path          - Path to LevelDB database (required)
  read_only        - Open in read-only mode (default: true)
  create_if_missing - Create DB if not exists (default: false)
  block_cache_size  - LRU cache size in bytes (default: 8MB)
  write_buffer_size - Write buffer size in bytes (default: 4MB)

FOREIGN TABLE OPTIONS:
  key_pattern      - Key pattern with {name} placeholders (required)
                     Example: ''users##{group}##{id}##{attr}''
  prefix_filter    - Optional prefix to filter keys

EXAMPLE:
  CREATE SERVER my_leveldb
      FOREIGN DATA WRAPPER level_pivot
      OPTIONS (db_path ''/path/to/db'', read_only ''false'');

  CREATE FOREIGN TABLE users (
      group_name  TEXT,
      id          TEXT,
      name        TEXT,
      email       TEXT
  )
  SERVER my_leveldb
  OPTIONS (key_pattern ''users##{group_name}##{id}##{attr}'');

  SELECT * FROM users WHERE group_name = ''admins'';
';
