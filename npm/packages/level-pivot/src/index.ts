/**
 * @halgari/level-pivot
 *
 * Embedded PostgreSQL with LevelDB foreign data wrapper.
 * Query LevelDB databases using SQL - perfect for Electron and desktop apps.
 *
 * @example
 * ```typescript
 * import { LevelPivotPostgres } from '@halgari/level-pivot';
 *
 * const pg = new LevelPivotPostgres({
 *   databaseDir: './my-app-data',
 *   port: 0,  // Auto-select free port
 *   persistent: true
 * });
 *
 * await pg.initialise();
 * await pg.start();
 *
 * // Create a LevelDB server connection
 * await pg.createLevelDbServer('mydata', {
 *   dbPath: '/path/to/leveldb',
 *   createIfMissing: true,
 *   readOnly: false
 * });
 *
 * // Create a foreign table mapping LevelDB keys to columns
 * await pg.createForeignTable('users', 'mydata', {
 *   keyPattern: 'users##{group}##{id}##{attr}',
 *   columns: { group: 'TEXT', id: 'TEXT', name: 'TEXT', email: 'TEXT' }
 * });
 *
 * // Query using standard pg client
 * const client = pg.getClient();
 * await client.connect();
 * const result = await client.query('SELECT * FROM users WHERE group = $1', ['admins']);
 * console.log(result.rows);
 * await client.end();
 *
 * await pg.stop();
 * ```
 *
 * @packageDocumentation
 */

export { LevelPivotPostgres } from './LevelPivotPostgres.js';

export type {
  LevelPivotPostgresConfig,
  LevelDbServerOptions,
  ForeignTableOptions,
  ColumnDefinition,
  ExtensionPaths,
  PlatformPackage,
} from './types.js';

export {
  detectPlatformPackage,
  loadPlatformPackage,
  installExtension,
  isExtensionInstalled,
} from './extension-installer.js';
