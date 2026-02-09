# @halgari/level-pivot

Embedded PostgreSQL with LevelDB foreign data wrapper. Query LevelDB databases using SQL - perfect for Electron and desktop apps.

## Features

- **Embedded PostgreSQL**: No external database installation required
- **LevelDB as SQL tables**: Query LevelDB key-value data using full SQL
- **Pivot semantics**: Transform key segments into relational columns
- **Desktop-ready**: Perfect for Electron apps and CLI tools
- **TypeScript support**: Full type definitions included

## Installation

```bash
npm install @halgari/level-pivot
```

The appropriate platform-specific binary package will be installed automatically.

## Quick Start

```typescript
import { LevelPivotPostgres } from '@halgari/level-pivot';

// Create and start embedded PostgreSQL with level_pivot extension
const pg = new LevelPivotPostgres({
  databaseDir: './my-app-data',
  port: 0,  // Auto-select free port
  persistent: true
});

await pg.initialise();
await pg.start();

// Connect to a LevelDB database
await pg.createLevelDbServer('mydata', {
  dbPath: '/path/to/leveldb',
  createIfMissing: true,
  readOnly: false
});

// Map LevelDB keys to a SQL table
// Keys like: users##admins##user123##name -> "Alice"
//            users##admins##user123##email -> "alice@example.com"
await pg.createForeignTable('users', 'mydata', {
  keyPattern: 'users##{group}##{id}##{attr}',
  columns: {
    group: 'TEXT',
    id: 'TEXT',
    name: 'TEXT',
    email: 'TEXT'
  }
});

// Query using standard pg client
const client = pg.getClient();
await client.connect();

const result = await client.query(
  'SELECT * FROM users WHERE group = $1',
  ['admins']
);
console.log(result.rows);
// [{ group: 'admins', id: 'user123', name: 'Alice', email: 'alice@example.com' }]

await client.end();
await pg.stop();
```

## API Reference

### `LevelPivotPostgres`

Main class that manages the embedded PostgreSQL instance with level_pivot extension.

#### Constructor Options

```typescript
interface LevelPivotPostgresConfig {
  databaseDir: string;     // Where to store PostgreSQL data
  port?: number;           // Port to listen on (0 = auto-select)
  persistent?: boolean;    // Keep data between restarts (default: true)
  user?: string;          // PostgreSQL user (default: 'postgres')
  database?: string;      // Default database (default: 'postgres')
}
```

#### Methods

- `initialise()` - Initialize PostgreSQL and install the extension
- `start()` - Start the PostgreSQL server
- `stop()` - Stop the PostgreSQL server
- `getClient(database?)` - Get a `pg.Client` for queries
- `createLevelDbServer(name, options)` - Create a LevelDB foreign server
- `createForeignTable(name, server, options)` - Create a foreign table
- `dropForeignTable(name, ifExists?)` - Drop a foreign table
- `dropLevelDbServer(name, options?)` - Drop a foreign server
- `ensureExtension(client?)` - Ensure the extension is created

### LevelDB Server Options

```typescript
interface LevelDbServerOptions {
  dbPath: string;           // Path to LevelDB database
  readOnly?: boolean;       // Open read-only (default: true)
  createIfMissing?: boolean; // Create if not exists (default: false)
  blockCacheSize?: number;   // LRU cache size in bytes
  writeBufferSize?: number;  // Write buffer size in bytes
}
```

### Foreign Table Options

```typescript
interface ForeignTableOptions {
  keyPattern: string;  // Key pattern with {name} placeholders
  columns: Record<string, string | ColumnDefinition>;
  prefixFilter?: string;  // Optional key prefix filter
}
```

## Key Patterns

The `keyPattern` defines how LevelDB keys map to table columns:

- `{name}` placeholders become identity columns
- The special `{attr}` placeholder determines which column receives the value
- Use `##` as the delimiter between segments

Example: `users##{group}##{id}##{attr}`

For a key `users##admins##user123##email` with value `alice@example.com`:
- `group` = `admins`
- `id` = `user123`
- `email` = `alice@example.com`

## Platform Support

| Platform | Architecture | Status |
|----------|-------------|--------|
| Linux | x64 | Supported |
| Linux | arm64 | Planned |
| macOS | arm64 | Planned |
| macOS | x64 | Planned |
| Windows | x64 | Planned |

## License

AGPL-3.0-only
