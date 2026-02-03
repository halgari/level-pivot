## level_pivot: PostgreSQL FDW for LevelDB

### What It Does

`level_pivot` exposes LevelDB key-value data as PostgreSQL tables. It transforms hierarchical keys into relational rows using a "pivot" semantic—multiple LevelDB keys sharing the same identity become columns in a single row.

### Key Pattern Syntax

Patterns define how LevelDB keys map to table columns:

```
users##{group_name}##{id}##{attr}
```

- **Literals**: Fixed text like `users##` that must match exactly
- **Captures** `{name}`: Variable segments that become identity columns
- **Attr** `{attr}`: Special marker—values after this become column names

**Supported delimiters**: `##`, `__`, `/`, `:`, or any consistent separator

### How Pivoting Works

Given this pattern: `users##{group_name}##{id}##{attr}`

These LevelDB keys:
```
users##admins##user001##name  = "Alice"
users##admins##user001##email = "alice@example.com"
```

Become one row:
```
group_name | id      | name  | email
-----------+---------+-------+------------------
admins     | user001 | Alice | alice@example.com
```

### Setup

```sql
-- Load extension
CREATE EXTENSION level_pivot;

-- Create server pointing to LevelDB database
CREATE SERVER my_leveldb
    FOREIGN DATA WRAPPER level_pivot
    OPTIONS (
        db_path '/path/to/leveldb',
        read_only 'false',          -- 'true' for read-only access
        create_if_missing 'true'    -- create DB if not exists
    );

-- Create foreign table
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

### Queries

```sql
-- Select all
SELECT * FROM users;

-- Filter by identity columns
SELECT * FROM users WHERE group_name = 'admins';

-- Insert (creates LevelDB keys for each non-null attr)
INSERT INTO users (group_name, id, name, email)
VALUES ('admins', 'user001', 'Alice', 'alice@example.com');

-- Update (modifies attr values)
UPDATE users SET email = 'new@example.com'
WHERE group_name = 'admins' AND id = 'user001';

-- Delete (removes all attr keys for matching identity)
DELETE FROM users WHERE group_name = 'admins' AND id = 'user001';
```

### Column Rules

1. **Identity columns** must match capture names in the pattern (order doesn't matter)
2. **Attr columns** can be any name—they become the `{attr}` value in keys
3. All columns are TEXT type (values stored as strings in LevelDB)
4. NULL attr values = key doesn't exist (UPDATE to NULL deletes the key)

### Pattern Examples

```sql
-- Simple with double-hash delimiter
OPTIONS (key_pattern 'users##{group}##{id}##{attr}')

-- Mixed delimiters
OPTIONS (key_pattern 'prefix###{arg}__{sub}##data##{attr}')

-- Slash/colon delimiters (metrics style)
OPTIONS (key_pattern '{tenant}:{env}/{service}/{attr}')
```

### Server Options

| Option | Default | Description |
|--------|---------|-------------|
| `db_path` | required | Path to LevelDB database |
| `read_only` | `true` | Open in read-only mode |
| `create_if_missing` | `false` | Create DB if not exists |
| `block_cache_size` | 8MB | LRU cache size |
| `write_buffer_size` | 4MB | Write buffer size |

### Constraints

- Pattern must contain exactly one `{attr}` placeholder
- Capture names must be valid identifiers (alphanumeric + underscore)
- No consecutive variable segments without delimiter between them
- Identity columns must be provided for INSERT/UPDATE/DELETE
