/**
 * @halgari/level-pivot-linux-x64
 *
 * Platform-specific binaries for level_pivot PostgreSQL extension.
 * This package is automatically selected via optionalDependencies.
 */

const path = require('path');

module.exports = {
  /**
   * Absolute path to the level_pivot.so shared library
   */
  libraryPath: path.join(__dirname, 'binaries', 'level_pivot.so'),

  /**
   * Absolute path to the SQL extension files directory
   */
  sqlPath: path.join(__dirname, 'sql'),
};
