/**
 * Configuration options for LevelPivotPostgres
 */
export interface LevelPivotPostgresConfig {
  /**
   * Directory where PostgreSQL data will be stored.
   * If persistent is true, data survives restarts.
   */
  databaseDir: string;

  /**
   * Port to run PostgreSQL on.
   * Use 0 for auto-selection of a free port.
   * @default 0
   */
  port?: number;

  /**
   * Whether to persist data between restarts.
   * If false, data directory is cleaned up on stop.
   * @default true
   */
  persistent?: boolean;

  /**
   * PostgreSQL user name.
   * @default 'postgres'
   */
  user?: string;

  /**
   * Default database name to create and use.
   * @default 'postgres'
   */
  database?: string;
}

/**
 * Options for creating a LevelDB server (CREATE SERVER)
 */
export interface LevelDbServerOptions {
  /**
   * Path to the LevelDB database directory.
   */
  dbPath: string;

  /**
   * Open the database in read-only mode.
   * @default true
   */
  readOnly?: boolean;

  /**
   * Create the database if it doesn't exist.
   * @default false
   */
  createIfMissing?: boolean;

  /**
   * LRU block cache size in bytes.
   * @default 8MB
   */
  blockCacheSize?: number;

  /**
   * Write buffer size in bytes.
   * @default 4MB
   */
  writeBufferSize?: number;
}

/**
 * Column definition for a foreign table
 */
export interface ColumnDefinition {
  /**
   * SQL type for the column (e.g., 'TEXT', 'INTEGER', 'BOOLEAN')
   */
  type: string;

  /**
   * Whether this column can contain NULL values.
   * @default true
   */
  nullable?: boolean;
}

/**
 * Options for creating a foreign table (CREATE FOREIGN TABLE)
 */
export interface ForeignTableOptions {
  /**
   * Key pattern with {name} placeholders.
   * Example: 'users##{group}##{id}##{attr}'
   * The {attr} placeholder is special - it defines which key segment
   * determines the column name for the value.
   */
  keyPattern: string;

  /**
   * Column definitions. Keys are column names, values are SQL types or ColumnDefinition objects.
   * Example: { group: 'TEXT', id: 'TEXT', name: 'TEXT', email: 'TEXT' }
   */
  columns: Record<string, string | ColumnDefinition>;

  /**
   * Optional prefix to filter keys during scans.
   */
  prefixFilter?: string;
}

/**
 * Result of extension installation
 */
export interface ExtensionPaths {
  /**
   * Path to the level_pivot.so shared library
   */
  libraryPath: string;

  /**
   * Path to the SQL extension files directory
   */
  sqlPath: string;
}

/**
 * Platform package export interface
 */
export interface PlatformPackage {
  /**
   * Absolute path to level_pivot.so
   */
  libraryPath: string;

  /**
   * Absolute path to SQL files directory
   */
  sqlPath: string;
}
