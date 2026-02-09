import { describe, it, expect, vi, beforeEach, afterEach } from 'vitest';
import { existsSync, mkdirSync, copyFileSync } from 'node:fs';
import { join } from 'node:path';
import { tmpdir } from 'node:os';
import {
  detectPlatformPackage,
  installExtension,
  isExtensionInstalled,
} from './extension-installer.js';
import type { PlatformPackage } from './types.js';

describe('detectPlatformPackage', () => {
  const originalPlatform = process.platform;
  const originalArch = process.arch;

  afterEach(() => {
    Object.defineProperty(process, 'platform', { value: originalPlatform });
    Object.defineProperty(process, 'arch', { value: originalArch });
  });

  it('returns linux-x64 for Linux x86_64', () => {
    Object.defineProperty(process, 'platform', { value: 'linux' });
    Object.defineProperty(process, 'arch', { value: 'x64' });
    expect(detectPlatformPackage()).toBe('@halgari/level-pivot-linux-x64');
  });

  it('returns linux-arm64 for Linux ARM64', () => {
    Object.defineProperty(process, 'platform', { value: 'linux' });
    Object.defineProperty(process, 'arch', { value: 'arm64' });
    expect(detectPlatformPackage()).toBe('@halgari/level-pivot-linux-arm64');
  });

  it('returns darwin-arm64 for macOS ARM64', () => {
    Object.defineProperty(process, 'platform', { value: 'darwin' });
    Object.defineProperty(process, 'arch', { value: 'arm64' });
    expect(detectPlatformPackage()).toBe('@halgari/level-pivot-darwin-arm64');
  });

  it('returns darwin-x64 for macOS x86_64', () => {
    Object.defineProperty(process, 'platform', { value: 'darwin' });
    Object.defineProperty(process, 'arch', { value: 'x64' });
    expect(detectPlatformPackage()).toBe('@halgari/level-pivot-darwin-x64');
  });

  it('returns windows-x64 for Windows x86_64', () => {
    Object.defineProperty(process, 'platform', { value: 'win32' });
    Object.defineProperty(process, 'arch', { value: 'x64' });
    expect(detectPlatformPackage()).toBe('@halgari/level-pivot-windows-x64');
  });

  it('throws for unsupported platform', () => {
    Object.defineProperty(process, 'platform', { value: 'freebsd' });
    Object.defineProperty(process, 'arch', { value: 'x64' });
    expect(() => detectPlatformPackage()).toThrow('Unsupported platform: freebsd');
  });

  it('throws for unsupported architecture', () => {
    Object.defineProperty(process, 'platform', { value: 'linux' });
    Object.defineProperty(process, 'arch', { value: 'ia32' });
    expect(() => detectPlatformPackage()).toThrow('Unsupported architecture: ia32');
  });
});

describe('installExtension', () => {
  let testDir: string;
  let pgDir: string;
  let mockPlatformPkg: PlatformPackage;

  beforeEach(() => {
    // Create a temporary test directory structure
    testDir = join(tmpdir(), `level-pivot-test-${Date.now()}`);
    pgDir = join(testDir, 'pg');

    // Create mock platform package files
    const srcDir = join(testDir, 'src');
    mkdirSync(join(srcDir, 'sql'), { recursive: true });

    // Create mock .so file
    const fs = require('node:fs');
    fs.writeFileSync(join(srcDir, 'level_pivot.so'), 'mock shared library');
    fs.writeFileSync(join(srcDir, 'sql', 'level_pivot.control'), 'mock control');
    fs.writeFileSync(join(srcDir, 'sql', 'level_pivot--1.0.sql'), 'mock sql');

    mockPlatformPkg = {
      libraryPath: join(srcDir, 'level_pivot.so'),
      sqlPath: join(srcDir, 'sql'),
    };
  });

  afterEach(() => {
    // Clean up test directory
    const fs = require('node:fs');
    fs.rmSync(testDir, { recursive: true, force: true });
  });

  it('creates target directories if they do not exist', () => {
    installExtension(pgDir, mockPlatformPkg);

    expect(existsSync(join(pgDir, 'lib', 'postgresql'))).toBe(true);
    expect(existsSync(join(pgDir, 'share', 'postgresql', 'extension'))).toBe(true);
  });

  it('copies shared library to lib/postgresql', () => {
    installExtension(pgDir, mockPlatformPkg);

    const libPath = join(pgDir, 'lib', 'postgresql', 'level_pivot.so');
    expect(existsSync(libPath)).toBe(true);
  });

  it('copies SQL files to share/postgresql/extension', () => {
    installExtension(pgDir, mockPlatformPkg);

    const extDir = join(pgDir, 'share', 'postgresql', 'extension');
    expect(existsSync(join(extDir, 'level_pivot.control'))).toBe(true);
    expect(existsSync(join(extDir, 'level_pivot--1.0.sql'))).toBe(true);
  });

  it('returns correct paths after installation', () => {
    const result = installExtension(pgDir, mockPlatformPkg);

    expect(result.libraryPath).toBe(join(pgDir, 'lib', 'postgresql', 'level_pivot.so'));
    expect(result.sqlPath).toBe(join(pgDir, 'share', 'postgresql', 'extension'));
  });

  it('throws if library file does not exist', () => {
    const badPkg: PlatformPackage = {
      libraryPath: '/nonexistent/level_pivot.so',
      sqlPath: mockPlatformPkg.sqlPath,
    };

    expect(() => installExtension(pgDir, badPkg)).toThrow(
      'Extension library not found'
    );
  });

  it('throws if control file does not exist', () => {
    const fs = require('node:fs');
    fs.unlinkSync(join(mockPlatformPkg.sqlPath, 'level_pivot.control'));

    expect(() => installExtension(pgDir, mockPlatformPkg)).toThrow(
      'Extension control file not found'
    );
  });

  it('throws if SQL file does not exist', () => {
    const fs = require('node:fs');
    fs.unlinkSync(join(mockPlatformPkg.sqlPath, 'level_pivot--1.0.sql'));

    expect(() => installExtension(pgDir, mockPlatformPkg)).toThrow(
      'Extension SQL file not found'
    );
  });
});

describe('isExtensionInstalled', () => {
  let testDir: string;
  let pgDir: string;

  beforeEach(() => {
    testDir = join(tmpdir(), `level-pivot-test-${Date.now()}`);
    pgDir = join(testDir, 'pg');
  });

  afterEach(() => {
    const fs = require('node:fs');
    fs.rmSync(testDir, { recursive: true, force: true });
  });

  it('returns false when nothing is installed', () => {
    mkdirSync(pgDir, { recursive: true });
    expect(isExtensionInstalled(pgDir)).toBe(false);
  });

  it('returns false when only some files exist', () => {
    const fs = require('node:fs');
    const libDir = join(pgDir, 'lib', 'postgresql');
    mkdirSync(libDir, { recursive: true });
    fs.writeFileSync(join(libDir, 'level_pivot.so'), 'mock');

    expect(isExtensionInstalled(pgDir)).toBe(false);
  });

  it('returns true when all files exist', () => {
    const fs = require('node:fs');
    const libDir = join(pgDir, 'lib', 'postgresql');
    const extDir = join(pgDir, 'share', 'postgresql', 'extension');

    mkdirSync(libDir, { recursive: true });
    mkdirSync(extDir, { recursive: true });

    fs.writeFileSync(join(libDir, 'level_pivot.so'), 'mock');
    fs.writeFileSync(join(extDir, 'level_pivot.control'), 'mock');
    fs.writeFileSync(join(extDir, 'level_pivot--1.0.sql'), 'mock');

    expect(isExtensionInstalled(pgDir)).toBe(true);
  });
});
