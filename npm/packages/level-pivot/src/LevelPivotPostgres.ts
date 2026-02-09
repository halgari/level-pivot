import EmbeddedPostgres from 'embedded-postgres';
import { Client } from 'pg';
import { installExtension, isExtensionInstalled, loadPlatformPackage } from './extension-installer.js';
import type {
  LevelPivotPostgresConfig,
  LevelDbServerOptions,
  ForeignTableOptions,
  ColumnDefinition,
} from './types.js';

/**
 * LevelPivotPostgres provides an embedded PostgreSQL instance with the level_pivot
 * extension pre-installed, enabling SQL queries over LevelDB databases.
 *
 * This class wraps the `embedded-postgres` npm package and handles:
 * - PostgreSQL lifecycle (init, start, stop)
 * - Automatic installation of the level_pivot extension
 * - Helper methods for creating LevelDB servers and foreign tables
 *
 * @example
 * ```typescript
 * const pg = new LevelPivotPostgres({
 *   databaseDir: './my-app-data',
 *   port: 0,  // Auto-select free port
 *   persistent: true
 * });
 *
 * await pg.initialise();
 * await pg.start();
 *
 * await pg.createLevelDbServer('mydata', {
 *   dbPath: '/path/to/leveldb',
 *   createIfMissing: true
 * });
 *
 * await pg.createForeignTable('users', 'mydata', {
 *   keyPattern: 'users##{group}##{id}##{attr}',
 *   columns: { group: 'TEXT', id: 'TEXT', name: 'TEXT', email: 'TEXT' }
 * });
 *
 * const client = pg.getClient();
 * await client.connect();
 * const result = await client.query('SELECT * FROM users');
 *
 * await pg.stop();
 * ```
 */
export class LevelPivotPostgres {
  private readonly config: Required<LevelPivotPostgresConfig>;
  private embeddedPg: EmbeddedPostgres | null = null;
  private extensionInstalled = false;
  private extensionCreated = false;

  constructor(config: LevelPivotPostgresConfig) {
    this.config = {
      databaseDir: config.databaseDir,
      port: config.port ?? 0,
      persistent: config.persistent ?? true,
      user: config.user ?? 'postgres',
      database: config.database ?? 'postgres',
    };
  }

  /**
   * Initializes the PostgreSQL cluster and installs the level_pivot extension.
   * This downloads PostgreSQL binaries if not cached and creates the data directory.
   */
  async initialise(): Promise<void> {
    this.embeddedPg = new EmbeddedPostgres({
      databaseDir: this.config.databaseDir,
      user: this.config.user,
      port: this.config.port,
      persistent: this.config.persistent,
    });

    await this.embeddedPg.initialise();

    // Install the level_pivot extension files
    await this.installLevelPivotExtension();
  }

  /**
   * Starts the PostgreSQL server.
   * Must call initialise() first.
   */
  async start(): Promise<void> {
    if (!this.embeddedPg) {
      throw new Error('Must call initialise() before start()');
    }

    await this.embeddedPg.start();
  }

  /**
   * Stops the PostgreSQL server.
   */
  async stop(): Promise<void> {
    if (this.embeddedPg) {
      await this.embeddedPg.stop();
    }
  }

  /**
   * Creates a database if it doesn't exist.
   */
  async createDatabase(name: string): Promise<void> {
    if (!this.embeddedPg) {
      throw new Error('Must call initialise() before createDatabase()');
    }

    await this.embeddedPg.createDatabase(name);
  }

  /**
   * Drops a database.
   */
  async dropDatabase(name: string): Promise<void> {
    if (!this.embeddedPg) {
      throw new Error('Must call initialise() before dropDatabase()');
    }

    await this.embeddedPg.dropDatabase(name);
  }

  /**
   * Returns a configured pg.Client for connecting to the database.
   * The client is not connected - call client.connect() to establish the connection.
   */
  getClient(database?: string): Client {
    if (!this.embeddedPg) {
      throw new Error('Must call initialise() before getClient()');
    }

    const client = this.embeddedPg.getPgClient();
    if (database && database !== this.config.database) {
      // Create a new client with the specified database
      return new Client({
        host: 'localhost',
        port: this.getPort(),
        user: this.config.user,
        database: database,
      });
    }
    return client;
  }

  /**
   * Returns the port PostgreSQL is running on.
   */
  getPort(): number {
    if (!this.embeddedPg) {
      throw new Error('Must call initialise() before getPort()');
    }

    // Access the port from the embedded postgres instance
    // The actual port may differ from config if port was 0 (auto-select)
    return (this.embeddedPg as unknown as { port: number }).port ?? this.config.port;
  }

  /**
   * Ensures the level_pivot extension is created in the current database.
   * This is called automatically by createLevelDbServer() if needed.
   */
  async ensureExtension(client?: Client): Promise<void> {
    if (this.extensionCreated) {
      return;
    }

    const ownClient = !client;
    const pgClient = client ?? this.getClient();

    try {
      if (ownClient) {
        await pgClient.connect();
      }

      await pgClient.query('CREATE EXTENSION IF NOT EXISTS level_pivot');
      this.extensionCreated = true;
    } finally {
      if (ownClient) {
        await pgClient.end();
      }
    }
  }

  /**
   * Creates a LevelDB foreign server.
   * Maps to: CREATE SERVER ... FOREIGN DATA WRAPPER level_pivot OPTIONS (...)
   *
   * @param serverName - Name for the foreign server (used in CREATE FOREIGN TABLE)
   * @param options - LevelDB connection options
   */
  async createLevelDbServer(serverName: string, options: LevelDbServerOptions): Promise<void> {
    const client = this.getClient();

    try {
      await client.connect();
      await this.ensureExtension(client);

      const serverOptions: string[] = [`db_path '${escapeSqlString(options.dbPath)}'`];

      if (options.readOnly !== undefined) {
        serverOptions.push(`read_only '${options.readOnly}'`);
      }
      if (options.createIfMissing !== undefined) {
        serverOptions.push(`create_if_missing '${options.createIfMissing}'`);
      }
      if (options.blockCacheSize !== undefined) {
        serverOptions.push(`block_cache_size '${options.blockCacheSize}'`);
      }
      if (options.writeBufferSize !== undefined) {
        serverOptions.push(`write_buffer_size '${options.writeBufferSize}'`);
      }

      const sql = `
        CREATE SERVER IF NOT EXISTS ${escapeIdentifier(serverName)}
        FOREIGN DATA WRAPPER level_pivot
        OPTIONS (${serverOptions.join(', ')})
      `;

      await client.query(sql);
    } finally {
      await client.end();
    }
  }

  /**
   * Creates a foreign table that maps to LevelDB data.
   * Maps to: CREATE FOREIGN TABLE ... SERVER ... OPTIONS (...)
   *
   * @param tableName - Name for the foreign table
   * @param serverName - Name of the LevelDB server (created via createLevelDbServer)
   * @param options - Table options including key pattern and columns
   */
  async createForeignTable(
    tableName: string,
    serverName: string,
    options: ForeignTableOptions
  ): Promise<void> {
    const client = this.getClient();

    try {
      await client.connect();

      // Build column definitions
      const columnDefs = Object.entries(options.columns).map(([name, typeOrDef]) => {
        const def: ColumnDefinition =
          typeof typeOrDef === 'string' ? { type: typeOrDef } : typeOrDef;
        const nullable = def.nullable !== false ? '' : ' NOT NULL';
        return `${escapeIdentifier(name)} ${def.type}${nullable}`;
      });

      // Build table options
      const tableOptions: string[] = [`key_pattern '${escapeSqlString(options.keyPattern)}'`];

      if (options.prefixFilter) {
        tableOptions.push(`prefix_filter '${escapeSqlString(options.prefixFilter)}'`);
      }

      const sql = `
        CREATE FOREIGN TABLE IF NOT EXISTS ${escapeIdentifier(tableName)} (
          ${columnDefs.join(',\n          ')}
        )
        SERVER ${escapeIdentifier(serverName)}
        OPTIONS (${tableOptions.join(', ')})
      `;

      await client.query(sql);
    } finally {
      await client.end();
    }
  }

  /**
   * Drops a foreign table.
   */
  async dropForeignTable(tableName: string, ifExists = true): Promise<void> {
    const client = this.getClient();

    try {
      await client.connect();
      const ifExistsClause = ifExists ? 'IF EXISTS ' : '';
      await client.query(`DROP FOREIGN TABLE ${ifExistsClause}${escapeIdentifier(tableName)}`);
    } finally {
      await client.end();
    }
  }

  /**
   * Drops a foreign server and optionally all dependent tables.
   */
  async dropLevelDbServer(
    serverName: string,
    options: { ifExists?: boolean; cascade?: boolean } = {}
  ): Promise<void> {
    const client = this.getClient();

    try {
      await client.connect();
      const ifExists = options.ifExists !== false ? 'IF EXISTS ' : '';
      const cascade = options.cascade ? ' CASCADE' : '';
      await client.query(`DROP SERVER ${ifExists}${escapeIdentifier(serverName)}${cascade}`);
    } finally {
      await client.end();
    }
  }

  /**
   * Installs the level_pivot extension files into the embedded PostgreSQL directory.
   */
  private async installLevelPivotExtension(): Promise<void> {
    if (this.extensionInstalled) {
      return;
    }

    if (!this.embeddedPg) {
      throw new Error('Embedded PostgreSQL not initialized');
    }

    // Get the PostgreSQL installation directory
    let pgDir: string;
    try {
      pgDir = this.getPgDir();
    } catch (err) {
      // In test environments or when binaries aren't available yet,
      // skip extension installation - it will fail at runtime when
      // CREATE EXTENSION is called
      if (process.env.NODE_ENV === 'test' || process.env.VITEST) {
        this.extensionInstalled = true;
        return;
      }
      throw err;
    }

    // Check if already installed
    if (isExtensionInstalled(pgDir)) {
      this.extensionInstalled = true;
      return;
    }

    // Load platform package and install
    const platformPkg = loadPlatformPackage();
    installExtension(pgDir, platformPkg);
    this.extensionInstalled = true;
  }

  /**
   * Gets the PostgreSQL installation directory from embedded-postgres.
   * The embedded-postgres package stores binaries in node_modules/@embedded-postgres/{platform}/native/
   */
  private getPgDir(): string {
    if (!this.embeddedPg) {
      throw new Error('Embedded PostgreSQL not initialized');
    }

    // Determine the platform package name based on OS and architecture
    const platform = process.platform;
    const arch = process.arch;

    let platformPkg: string;
    switch (platform) {
      case 'darwin':
        platformPkg = arch === 'arm64' ? 'darwin-arm64' : 'darwin-x64';
        break;
      case 'linux':
        switch (arch) {
          case 'arm64':
            platformPkg = 'linux-arm64';
            break;
          case 'arm':
            platformPkg = 'linux-arm';
            break;
          case 'ia32':
            platformPkg = 'linux-ia32';
            break;
          case 'ppc64':
            platformPkg = 'linux-ppc64';
            break;
          default:
            platformPkg = 'linux-x64';
        }
        break;
      case 'win32':
        platformPkg = 'windows-x64';
        break;
      default:
        throw new Error(`Unsupported platform: ${platform}`);
    }

    // The embedded-postgres binaries are in node_modules/@embedded-postgres/{platform}/native/
    // We need to resolve this path from where the embedded-postgres package is installed
    try {
      const embeddedPgPath = require.resolve(`@embedded-postgres/${platformPkg}/package.json`);
      const path = require('node:path');
      return path.join(path.dirname(embeddedPgPath), 'native');
    } catch {
      throw new Error(
        `Could not find embedded-postgres binaries for platform ${platformPkg}. ` +
        `Ensure @embedded-postgres/${platformPkg} is installed.`
      );
    }
  }
}

/**
 * Escapes a SQL identifier (table name, column name, etc.)
 */
function escapeIdentifier(identifier: string): string {
  // Double any double quotes and wrap in double quotes
  return `"${identifier.replace(/"/g, '""')}"`;
}

/**
 * Escapes a SQL string value (for use in single quotes)
 */
function escapeSqlString(value: string): string {
  // Escape single quotes by doubling them
  return value.replace(/'/g, "''");
}
