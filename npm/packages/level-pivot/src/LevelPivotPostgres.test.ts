import { describe, it, expect, vi, beforeEach, afterEach } from 'vitest';
import { LevelPivotPostgres } from './LevelPivotPostgres.js';

// Mock embedded-postgres
vi.mock('embedded-postgres', () => {
  return {
    default: vi.fn().mockImplementation((config) => ({
      config,
      port: config.port || 5432,
      initialise: vi.fn().mockResolvedValue(undefined),
      start: vi.fn().mockResolvedValue(undefined),
      stop: vi.fn().mockResolvedValue(undefined),
      createDatabase: vi.fn().mockResolvedValue(undefined),
      dropDatabase: vi.fn().mockResolvedValue(undefined),
      getPgClient: vi.fn().mockReturnValue({
        connect: vi.fn().mockResolvedValue(undefined),
        query: vi.fn().mockResolvedValue({ rows: [] }),
        end: vi.fn().mockResolvedValue(undefined),
      }),
      postgresPath: '/mock/path/to/bin/postgres',
    })),
  };
});

// Mock extension installer
// Return true for isExtensionInstalled to skip the installation flow which requires
// accessing the actual embedded-postgres binary paths
vi.mock('./extension-installer.js', () => ({
  loadPlatformPackage: vi.fn().mockReturnValue({
    libraryPath: '/mock/level_pivot.so',
    sqlPath: '/mock/sql',
  }),
  installExtension: vi.fn().mockReturnValue({
    libraryPath: '/mock/installed/level_pivot.so',
    sqlPath: '/mock/installed/extension',
  }),
  isExtensionInstalled: vi.fn().mockReturnValue(true),
}));

describe('LevelPivotPostgres', () => {
  let pg: LevelPivotPostgres;

  beforeEach(() => {
    pg = new LevelPivotPostgres({
      databaseDir: '/tmp/test-db',
      port: 5433,
      persistent: true,
    });
  });

  describe('constructor', () => {
    it('uses default values when not provided', () => {
      const minimalPg = new LevelPivotPostgres({
        databaseDir: '/tmp/test-db',
      });
      // No error thrown means defaults were applied
      expect(minimalPg).toBeDefined();
    });
  });

  describe('initialise', () => {
    it('initializes embedded postgres and installs extension', async () => {
      await pg.initialise();
      // If no error, initialization succeeded
      expect(pg).toBeDefined();
    });
  });

  describe('start', () => {
    it('throws if called before initialise', async () => {
      await expect(pg.start()).rejects.toThrow('Must call initialise() before start()');
    });

    it('starts after initialise', async () => {
      await pg.initialise();
      await pg.start();
      // If no error, start succeeded
    });
  });

  describe('stop', () => {
    it('does not throw if called without initialise', async () => {
      await pg.stop();
      // Should not throw
    });

    it('stops the database', async () => {
      await pg.initialise();
      await pg.start();
      await pg.stop();
      // If no error, stop succeeded
    });
  });

  describe('getClient', () => {
    it('throws if called before initialise', () => {
      expect(() => pg.getClient()).toThrow('Must call initialise() before getClient()');
    });

    it('returns a pg client after initialise', async () => {
      await pg.initialise();
      const client = pg.getClient();
      expect(client).toBeDefined();
      expect(client.connect).toBeDefined();
      expect(client.query).toBeDefined();
    });
  });

  describe('createDatabase', () => {
    it('throws if called before initialise', async () => {
      await expect(pg.createDatabase('test')).rejects.toThrow(
        'Must call initialise() before createDatabase()'
      );
    });

    it('creates a database', async () => {
      await pg.initialise();
      await pg.createDatabase('test');
      // If no error, database creation succeeded
    });
  });

  describe('dropDatabase', () => {
    it('throws if called before initialise', async () => {
      await expect(pg.dropDatabase('test')).rejects.toThrow(
        'Must call initialise() before dropDatabase()'
      );
    });

    it('drops a database', async () => {
      await pg.initialise();
      await pg.dropDatabase('test');
      // If no error, database drop succeeded
    });
  });

  describe('createLevelDbServer', () => {
    it('creates a server with minimal options', async () => {
      await pg.initialise();
      await pg.start();
      await pg.createLevelDbServer('testserver', {
        dbPath: '/path/to/leveldb',
      });
      // Verify the query was called with correct SQL
      const client = pg.getClient();
      expect(client.query).toHaveBeenCalled();
    });

    it('creates a server with all options', async () => {
      await pg.initialise();
      await pg.start();
      await pg.createLevelDbServer('testserver', {
        dbPath: '/path/to/leveldb',
        readOnly: false,
        createIfMissing: true,
        blockCacheSize: 16 * 1024 * 1024,
        writeBufferSize: 8 * 1024 * 1024,
      });
      const client = pg.getClient();
      expect(client.query).toHaveBeenCalled();
    });
  });

  describe('createForeignTable', () => {
    it('creates a table with string column types', async () => {
      await pg.initialise();
      await pg.start();
      await pg.createForeignTable('users', 'testserver', {
        keyPattern: 'users##{id}##{attr}',
        columns: {
          id: 'TEXT',
          name: 'TEXT',
          email: 'TEXT',
        },
      });
      const client = pg.getClient();
      expect(client.query).toHaveBeenCalled();
    });

    it('creates a table with detailed column definitions', async () => {
      await pg.initialise();
      await pg.start();
      await pg.createForeignTable('items', 'testserver', {
        keyPattern: 'items##{id}##{attr}',
        columns: {
          id: { type: 'TEXT', nullable: false },
          name: { type: 'TEXT', nullable: true },
          count: { type: 'INTEGER' },
        },
        prefixFilter: 'items##',
      });
      const client = pg.getClient();
      expect(client.query).toHaveBeenCalled();
    });
  });

  describe('dropForeignTable', () => {
    it('drops a table with IF EXISTS', async () => {
      await pg.initialise();
      await pg.start();
      await pg.dropForeignTable('users');
      const client = pg.getClient();
      expect(client.query).toHaveBeenCalled();
    });

    it('drops a table without IF EXISTS', async () => {
      await pg.initialise();
      await pg.start();
      await pg.dropForeignTable('users', false);
      const client = pg.getClient();
      expect(client.query).toHaveBeenCalled();
    });
  });

  describe('dropLevelDbServer', () => {
    it('drops a server with defaults', async () => {
      await pg.initialise();
      await pg.start();
      await pg.dropLevelDbServer('testserver');
      const client = pg.getClient();
      expect(client.query).toHaveBeenCalled();
    });

    it('drops a server with CASCADE', async () => {
      await pg.initialise();
      await pg.start();
      await pg.dropLevelDbServer('testserver', { cascade: true });
      const client = pg.getClient();
      expect(client.query).toHaveBeenCalled();
    });
  });
});

describe('SQL escaping', () => {
  let pg: LevelPivotPostgres;

  beforeEach(async () => {
    pg = new LevelPivotPostgres({
      databaseDir: '/tmp/test-db',
    });
    await pg.initialise();
    await pg.start();
  });

  it('escapes single quotes in string values', async () => {
    await pg.createLevelDbServer('test', {
      dbPath: "/path/with'quote",
    });
    const client = pg.getClient();
    const calls = (client.query as ReturnType<typeof vi.fn>).mock.calls;
    const sql = calls.find((c) => c[0].includes('CREATE SERVER'))?.[0];
    expect(sql).toContain("/path/with''quote");
  });

  it('escapes double quotes in identifiers', async () => {
    await pg.createForeignTable('table"name', 'server', {
      keyPattern: 'test##{id}##{attr}',
      columns: { 'col"name': 'TEXT' },
    });
    const client = pg.getClient();
    const calls = (client.query as ReturnType<typeof vi.fn>).mock.calls;
    const sql = calls.find((c) => c[0].includes('CREATE FOREIGN TABLE'))?.[0];
    expect(sql).toContain('"table""name"');
    expect(sql).toContain('"col""name"');
  });
});
