import { copyFileSync, existsSync, mkdirSync } from 'node:fs';
import { join, dirname } from 'node:path';
import type { PlatformPackage, ExtensionPaths } from './types.js';

/**
 * Detects the current platform and returns the corresponding package name.
 */
export function detectPlatformPackage(): string {
  const platform = process.platform;
  const arch = process.arch;

  let platformStr: string;
  let archStr: string;

  switch (platform) {
    case 'linux':
      platformStr = 'linux';
      break;
    case 'darwin':
      platformStr = 'darwin';
      break;
    case 'win32':
      platformStr = 'windows';
      break;
    default:
      throw new Error(`Unsupported platform: ${platform}`);
  }

  switch (arch) {
    case 'x64':
      archStr = 'x64';
      break;
    case 'arm64':
      archStr = 'arm64';
      break;
    default:
      throw new Error(`Unsupported architecture: ${arch}`);
  }

  return `@halgari/level-pivot-${platformStr}-${archStr}`;
}

/**
 * Loads the platform-specific binary package and returns paths to the extension files.
 */
export function loadPlatformPackage(): PlatformPackage {
  const packageName = detectPlatformPackage();

  try {
    // Dynamic require to load the platform package
    // eslint-disable-next-line @typescript-eslint/no-require-imports
    const platformPkg = require(packageName) as PlatformPackage;

    if (!platformPkg.libraryPath || !platformPkg.sqlPath) {
      throw new Error(`Invalid platform package ${packageName}: missing libraryPath or sqlPath`);
    }

    if (!existsSync(platformPkg.libraryPath)) {
      throw new Error(`Extension library not found at ${platformPkg.libraryPath}`);
    }

    return platformPkg;
  } catch (err) {
    if ((err as NodeJS.ErrnoException).code === 'MODULE_NOT_FOUND') {
      throw new Error(
        `Platform package ${packageName} not found. ` +
        `Please install it: npm install ${packageName}`
      );
    }
    throw err;
  }
}

/**
 * Installs the level_pivot extension into the embedded PostgreSQL directory.
 *
 * This mirrors the logic from embedded_postgres.sh:189-218:
 * 1. Copy level_pivot.so to ${PG_DIR}/lib/postgresql/
 * 2. Copy level_pivot.control and level_pivot--1.0.sql to ${PG_DIR}/share/postgresql/extension/
 *
 * @param pgDir - The PostgreSQL installation directory (from embedded-postgres)
 * @param platformPkg - The loaded platform package with extension paths
 * @returns The paths where files were installed
 */
export function installExtension(pgDir: string, platformPkg?: PlatformPackage): ExtensionPaths {
  const pkg = platformPkg ?? loadPlatformPackage();

  // Validate source files exist
  if (!existsSync(pkg.libraryPath)) {
    throw new Error(`Extension library not found at ${pkg.libraryPath}`);
  }

  // Target directories in the embedded PostgreSQL installation
  const libDir = join(pgDir, 'lib', 'postgresql');
  const extDir = join(pgDir, 'share', 'postgresql', 'extension');

  // Ensure target directories exist
  mkdirSync(libDir, { recursive: true });
  mkdirSync(extDir, { recursive: true });

  // Copy shared library
  const libDest = join(libDir, 'level_pivot.so');
  copyFileSync(pkg.libraryPath, libDest);

  // Copy SQL extension files
  const controlSrc = join(pkg.sqlPath, 'level_pivot.control');
  const sqlSrc = join(pkg.sqlPath, 'level_pivot--1.0.sql');

  if (!existsSync(controlSrc)) {
    throw new Error(`Extension control file not found at ${controlSrc}`);
  }
  if (!existsSync(sqlSrc)) {
    throw new Error(`Extension SQL file not found at ${sqlSrc}`);
  }

  copyFileSync(controlSrc, join(extDir, 'level_pivot.control'));
  copyFileSync(sqlSrc, join(extDir, 'level_pivot--1.0.sql'));

  return {
    libraryPath: libDest,
    sqlPath: extDir,
  };
}

/**
 * Checks if the extension is already installed in the PostgreSQL directory.
 */
export function isExtensionInstalled(pgDir: string): boolean {
  const libPath = join(pgDir, 'lib', 'postgresql', 'level_pivot.so');
  const controlPath = join(pgDir, 'share', 'postgresql', 'extension', 'level_pivot.control');
  const sqlPath = join(pgDir, 'share', 'postgresql', 'extension', 'level_pivot--1.0.sql');

  return existsSync(libPath) && existsSync(controlPath) && existsSync(sqlPath);
}
