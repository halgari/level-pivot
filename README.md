# level_pivot

A PostgreSQL Foreign Data Wrapper (FDW) that exposes LevelDB key-value data as relational tables using pivot semantics.

## Overview

LevelDB is a fast key-value store—nothing more than string keys mapped to string values. However, applications commonly impose structure on their keys by encoding multiple fields with delimiters, creating a pseudo-hierarchical organization within a flat namespace.

`level_pivot` recognizes these patterns and transforms them into relational tables, "pivoting" multiple LevelDB entries that share a common identity into columns of a single row.

### The Problem

Applications often encode structure into LevelDB keys:

```
users##admins##user001##name   = "Alice"
users##admins##user001##email  = "alice@example.com"
users##admins##user001##role   = "admin"
users##admins##user002##name   = "Bob"
users##admins##user002##email  = "bob@example.com"
```

Querying this data requires iteration and manual parsing.

### The Solution

`level_pivot` lets you define a key pattern that describes your key structure, then query it as a normal SQL table:

```sql
SELECT * FROM users WHERE group_name = 'admins';
```

```
group_name | id      | name  | email             | role
-----------+---------+-------+-------------------+-------
admins     | user001 | Alice | alice@example.com | admin
admins     | user002 | Bob   | bob@example.com   | NULL
```

## How It Works

### Key Patterns

A key pattern defines how LevelDB keys map to table columns using placeholders:

```
users##{group_name}##{id}##{attr}
```

- **Literals** (`users##`): Fixed text that must match exactly
- **Captures** (`{group_name}`, `{id}`): Variable segments that become identity columns
- **Attr** (`{attr}`): Special marker—the value after this becomes the column name

### Pivoting

Given the pattern `users##{group_name}##{id}##{attr}`:

1. The FDW scans LevelDB keys matching the pattern prefix
2. Keys are parsed to extract identity values (`group_name`, `id`) and the attr name
3. Keys sharing the same identity are grouped into a single row
4. Each unique attr value becomes a column with the LevelDB value as its cell value

```
LevelDB Key                        → Row Identity      + Column
─────────────────────────────────────────────────────────────────
users##admins##user001##name       → (admins, user001) + name="Alice"
users##admins##user001##email      → (admins, user001) + email="alice@..."
users##admins##user001##role       → (admins, user001) + role="admin"
```

All three keys become one row with columns: `group_name`, `id`, `name`, `email`, `role`.

## Features

- **Full CRUD support**: SELECT, INSERT, UPDATE, DELETE operations
- **Prefix optimization**: Identity column filters use LevelDB prefix scans
- **Connection pooling**: Connections are cached per PostgreSQL server
- **Flexible patterns**: Supports multiple delimiter styles (`##`, `__`, `/`, `:`)
- **Type conversion**: Maps LevelDB string values to PostgreSQL types (TEXT, INTEGER, BOOLEAN, JSONB, etc.)
- **Raw table mode**: Direct key-value access without pattern parsing
- **Change notifications**: Automatic NOTIFY on table modifications
- **Schema discovery**: Import foreign schema from existing LevelDB data
- **Cross-platform**: Builds on Linux, macOS, and Windows

## Installation

### Prerequisites

- PostgreSQL 12+ with development headers
- CMake 3.16+
- C++17 compatible compiler
- LevelDB library

### Building from Source

```bash
# Clone the repository
git clone https://github.com/halgari/level-pivot.git
cd level-pivot

# Configure with CMake (using vcpkg for dependencies)
cmake --preset=default

# Build
cmake --build build

# Install (requires appropriate permissions)
sudo cmake --install build
```

### Enabling the Extension

```sql
CREATE EXTENSION level_pivot;
```

## Usage

### Basic Setup

```sql
-- Create a server pointing to your LevelDB database
CREATE SERVER my_leveldb
    FOREIGN DATA WRAPPER level_pivot
    OPTIONS (
        db_path '/path/to/leveldb',
        read_only 'false',
        create_if_missing 'true'
    );

-- Create a foreign table with a key pattern
CREATE FOREIGN TABLE users (
    group_name  TEXT,    -- identity column (from {group_name})
    id          TEXT,    -- identity column (from {id})
    name        TEXT,    -- attr column
    email       TEXT,    -- attr column
    created_at  TEXT     -- attr column
)
SERVER my_leveldb
OPTIONS (key_pattern 'users##{group_name}##{id}##{attr}');
```

### Querying Data

```sql
-- Select all rows
SELECT * FROM users;

-- Filter by identity columns (uses prefix scan)
SELECT * FROM users WHERE group_name = 'admins';

-- Select specific columns
SELECT group_name, id, email FROM users WHERE id = 'user001';
```

### Inserting Data

```sql
-- Creates LevelDB keys for each non-null attr column
INSERT INTO users (group_name, id, name, email)
VALUES ('admins', 'user003', 'Charlie', 'charlie@example.com');

-- This creates two LevelDB entries:
--   users##admins##user003##name  = "Charlie"
--   users##admins##user003##email = "charlie@example.com"
```

### Updating Data

```sql
-- Update attr values
UPDATE users
SET email = 'newemail@example.com', name = 'Charles'
WHERE group_name = 'admins' AND id = 'user003';

-- Setting an attr to NULL deletes the corresponding key
UPDATE users SET email = NULL WHERE id = 'user003';
```

### Deleting Data

```sql
-- Removes all attr keys for matching identity
DELETE FROM users WHERE group_name = 'admins' AND id = 'user003';
```

### Raw Table Mode

For direct key-value access without pattern parsing:

```sql
CREATE FOREIGN TABLE raw_kv (
    key   TEXT,
    value TEXT
)
SERVER my_leveldb
OPTIONS (table_mode 'raw');

-- Insert key-value pairs
INSERT INTO raw_kv VALUES ('mykey', 'myvalue');

-- Supports range queries
SELECT * FROM raw_kv WHERE key >= 'prefix:' AND key < 'prefix:\xFF';

-- Point lookups
SELECT value FROM raw_kv WHERE key = 'mykey';
```

### Change Notifications

Tables automatically send PostgreSQL NOTIFY on modifications:

```sql
-- Listen for changes
LISTEN public_users_changed;

-- Any INSERT/UPDATE/DELETE triggers notification
INSERT INTO users VALUES ('group1', 'id1', 'Alice');
-- Notification received: public_users_changed
```

Channel format: `{schema}_{table}_changed` (truncated to 63 chars)

### Schema Discovery

Automatically discover table structure from existing LevelDB data:

```sql
IMPORT FOREIGN SCHEMA leveldb_schema
FROM SERVER my_leveldb
INTO public;
```

This analyzes existing keys, infers a pattern, discovers attribute names, and generates the CREATE FOREIGN TABLE statement.

## Configuration

### Server Options

| Option | Default | Description |
|--------|---------|-------------|
| `db_path` | (required) | Path to the LevelDB database directory |
| `read_only` | `true` | Open database in read-only mode |
| `create_if_missing` | `false` | Create database if it doesn't exist |
| `block_cache_size` | `8388608` | LRU block cache size in bytes (8MB, supports K/M/G suffixes) |
| `write_buffer_size` | `4194304` | Write buffer size in bytes (4MB, supports K/M/G suffixes) |
| `use_write_batch` | `true` | Use atomic WriteBatch for modifications |

### Table Options

| Option | Default | Description |
|--------|---------|-------------|
| `key_pattern` | (required for pivot) | Key pattern with `{name}` placeholders |
| `table_mode` | `pivot` | Table mode: `pivot` (pattern-based) or `raw` (direct key-value) |
| `prefix_filter` | (none) | Optional prefix to filter keys (raw mode) |

## Key Pattern Syntax

### Supported Delimiters

Patterns can use any consistent delimiter:

```sql
-- Double hash
OPTIONS (key_pattern 'users##{group}##{id}##{attr}')

-- Double underscore
OPTIONS (key_pattern 'data__{tenant}__{key}__{attr}')

-- Slash (URL-style)
OPTIONS (key_pattern '{tenant}/{env}/{service}/{attr}')

-- Colon
OPTIONS (key_pattern '{namespace}:{collection}:{id}:{attr}')

-- Mixed delimiters
OPTIONS (key_pattern 'prefix##{arg}__{sub}##data##{attr}')
```

### Rules

1. Pattern must contain exactly one `{attr}` placeholder
2. Capture names must be valid identifiers (alphanumeric + underscore)
3. Two variable segments cannot be adjacent without a delimiter
4. Identity columns in the table must match capture names in the pattern

### Column Mapping

- **Identity columns**: Must match capture names in the pattern (order doesn't matter)
- **Attr columns**: Can be any name—they become the `{attr}` value in generated keys
- **All columns are TEXT**: Values are stored as strings in LevelDB

## Architecture

```
┌─────────────────────────────────────────────────────────────┐
│                     PostgreSQL                              │
│  ┌─────────────────────────────────────────────────────┐   │
│  │              Foreign Data Wrapper API                │   │
│  └─────────────────────────────────────────────────────┘   │
│                           │                                 │
│  ┌─────────────────────────────────────────────────────┐   │
│  │              level_pivot Extension                   │   │
│  │  ┌─────────────┐  ┌─────────────┐  ┌─────────────┐  │   │
│  │  │ KeyPattern  │  │ Projection  │  │PivotScanner │  │   │
│  │  │   Parser    │  │   Builder   │  │             │  │   │
│  │  └─────────────┘  └─────────────┘  └─────────────┘  │   │
│  │  ┌─────────────┐  ┌─────────────┐  ┌─────────────┐  │   │
│  │  │   Writer    │  │    Type     │  │ Connection  │  │   │
│  │  │             │  │  Converter  │  │   Manager   │  │   │
│  │  └─────────────┘  └─────────────┘  └─────────────┘  │   │
│  └─────────────────────────────────────────────────────┘   │
└─────────────────────────────────────────────────────────────┘
                           │
                           ▼
                    ┌─────────────┐
                    │   LevelDB   │
                    └─────────────┘
```

### Core Components

| Component | File | Purpose |
|-----------|------|---------|
| **KeyPattern** | `key_pattern.hpp/cpp` | Parses key pattern strings into segments |
| **KeyParser** | `key_parser.hpp/cpp` | Matches LevelDB keys against patterns, extracts values |
| **Projection** | `projection.hpp/cpp` | Maps table columns to pattern captures and attrs |
| **PivotScanner** | `pivot_scanner.hpp/cpp` | Iterates LevelDB and assembles pivoted rows |
| **Writer** | `writer.hpp/cpp` | Handles INSERT, UPDATE, DELETE operations |
| **ConnectionManager** | `connection_manager.hpp/cpp` | Pools LevelDB connections per server |
| **TypeConverter** | `type_converter.hpp/cpp` | Converts between PostgreSQL and string types |

## Testing

### Unit Tests

```bash
# Build and run unit tests
cmake --build build --target test_unit
ctest --test-dir build
```

### Integration Tests

Integration tests require a running PostgreSQL instance:

```bash
# Setup test environment
psql -f test/integration/setup.sql

# Run tests
psql -f test/integration/test_select.sql
psql -f test/integration/test_modify.sql

# Cleanup
psql -f test/integration/cleanup.sql
```

## Performance Features

- **SIMD Optimization**: AVX2/SSE2 accelerated delimiter detection with automatic scalar fallback
- **Zero-Copy Parsing**: Uses `string_view` to avoid allocations during key parsing
- **Filter Pushdown**: WHERE clauses on identity columns use LevelDB prefix scans
- **Link-Time Optimization**: Release builds use LTO for cross-module optimization
- **Connection Pooling**: LevelDB connections cached per PostgreSQL server
- **Atomic Batch Writes**: Multiple modifications batched into single atomic write

## Performance Considerations

- **Prefix filtering**: Queries filtering on identity columns (in pattern order) use efficient LevelDB prefix scans
- **Full scans**: Queries without identity filters scan all matching keys
- **Connection caching**: LevelDB connections are reused across queries to the same server
- **Block cache**: Tune `block_cache_size` for read-heavy workloads

## Limitations

- All values stored as strings (type conversion happens at read/write time)
- No support for transactions spanning multiple rows
- Pattern must contain exactly one `{attr}` segment
- Identity columns cannot be NULL
