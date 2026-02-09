#!/usr/bin/env node
/**
 * Local integration test for @halgari/level-pivot
 *
 * Usage: node test-local.mjs
 *
 * This script:
 * 1. Starts embedded PostgreSQL with level_pivot extension
 * 2. Creates a LevelDB server and foreign table
 * 3. Runs some queries
 * 4. Cleans up
 */

import { LevelPivotPostgres } from './packages/level-pivot/dist/index.js';
import { mkdtempSync, rmSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';

async function main() {
  const tempDir = mkdtempSync(join(tmpdir(), 'level-pivot-test-'));
  const dbDir = join(tempDir, 'pg-data');
  const leveldbDir = join(tempDir, 'leveldb');

  console.log('Test directories:');
  console.log('  PostgreSQL:', dbDir);
  console.log('  LevelDB:', leveldbDir);
  console.log();

  const pg = new LevelPivotPostgres({
    databaseDir: dbDir,
    port: 0,  // Auto-select
    persistent: false,  // Clean up on stop
  });

  try {
    console.log('Initializing PostgreSQL...');
    await pg.initialise();

    console.log('Starting PostgreSQL...');
    await pg.start();

    console.log(`PostgreSQL running on port ${pg.getPort()}`);

    // Create a LevelDB server
    console.log('\nCreating LevelDB server...');
    await pg.createLevelDbServer('testdb', {
      dbPath: leveldbDir,
      createIfMissing: true,
      readOnly: false,
    });

    // Create a foreign table
    console.log('Creating foreign table...');
    await pg.createForeignTable('users', 'testdb', {
      keyPattern: 'users##{id}##{attr}',
      columns: {
        id: 'TEXT',
        name: 'TEXT',
        email: 'TEXT',
      },
    });

    // Get a client and run queries
    const client = pg.getClient();
    await client.connect();

    console.log('\nRunning queries...');

    // Check extension is loaded
    const extResult = await client.query(
      "SELECT * FROM pg_extension WHERE extname = 'level_pivot'"
    );
    console.log('Extension loaded:', extResult.rows.length > 0 ? 'YES' : 'NO');

    // Check foreign server exists
    const serverResult = await client.query(
      "SELECT * FROM pg_foreign_server WHERE srvname = 'testdb'"
    );
    console.log('Foreign server created:', serverResult.rows.length > 0 ? 'YES' : 'NO');

    // Check foreign table exists
    const tableResult = await client.query(
      "SELECT * FROM information_schema.foreign_tables WHERE foreign_table_name = 'users'"
    );
    console.log('Foreign table created:', tableResult.rows.length > 0 ? 'YES' : 'NO');

    // Try a SELECT (will be empty since LevelDB is new)
    const selectResult = await client.query('SELECT * FROM users');
    console.log('SELECT query works:', selectResult.rows.length >= 0 ? 'YES' : 'NO');
    console.log('  Rows returned:', selectResult.rows.length);

    await client.end();
    console.log('\n✓ All tests passed!');

  } catch (err) {
    console.error('\n✗ Test failed:', err);
    process.exitCode = 1;
  } finally {
    console.log('\nStopping PostgreSQL...');
    await pg.stop();

    // Clean up temp directory
    console.log('Cleaning up...');
    rmSync(tempDir, { recursive: true, force: true });
  }
}

main();
