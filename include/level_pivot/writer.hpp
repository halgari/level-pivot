#pragma once

#include "level_pivot/projection.hpp"
#include "level_pivot/connection_manager.hpp"
#include "level_pivot/type_converter.hpp"
#include <vector>
#include <unordered_map>
#include <unordered_set>

// Forward declarations for PostgreSQL types
extern "C" {
    typedef uintptr_t Datum;
}

namespace level_pivot {

/**
 * Write operation result
 */
struct WriteResult {
    size_t keys_written = 0;
    size_t keys_deleted = 0;
};

/**
 * Handles INSERT, UPDATE, DELETE operations on pivoted tables
 *
 * Translates row-level operations to LevelDB Put/Delete calls:
 *   - INSERT: Creates Put for each non-null attr column
 *   - UPDATE: Updates changed attr keys, deletes keys set to NULL
 *   - DELETE: Removes all attr keys matching the identity
 */
class Writer {
public:
    /**
     * Create a writer for the given projection and connection
     *
     * @param projection Table projection
     * @param connection LevelDB connection (must be writable)
     */
    Writer(const Projection& projection,
           std::shared_ptr<LevelDBConnection> connection);

    /**
     * Insert a new row
     *
     * Creates LevelDB keys for each non-null attr column.
     *
     * @param values Array of Datum values (indexed by column attnum - 1)
     * @param nulls Array of null flags
     * @return Write result
     */
    WriteResult insert(Datum* values, bool* nulls);

    /**
     * Update an existing row
     *
     * @param old_values Original row values (for identity columns)
     * @param old_nulls Original null flags
     * @param new_values New row values
     * @param new_nulls New null flags
     * @return Write result
     */
    WriteResult update(Datum* old_values, bool* old_nulls,
                       Datum* new_values, bool* new_nulls);

    /**
     * Delete a row
     *
     * Removes all attr keys matching the identity columns.
     *
     * @param values Array of Datum values
     * @param nulls Array of null flags
     * @return Write result
     */
    WriteResult remove(Datum* values, bool* nulls);

    /**
     * Delete a row by identity values
     *
     * Removes all attr keys matching the given identity.
     *
     * @param identity_values Values for identity columns (in pattern order)
     * @return Write result
     */
    WriteResult remove_by_identity(const std::vector<std::string>& identity_values);

private:
    const Projection& projection_;
    std::shared_ptr<LevelDBConnection> connection_;

    // Extract identity column values from Datum array
    std::vector<std::string> extract_identity(Datum* values, bool* nulls) const;

    // Extract attr column values from Datum array
    // Returns map of attr_name -> value (only non-null values)
    std::unordered_map<std::string, std::string> extract_attrs(
        Datum* values, bool* nulls) const;

    // Get set of attr names that are null
    std::unordered_set<std::string> get_null_attrs(bool* nulls) const;

    // Find all existing keys for an identity
    std::vector<std::string> find_keys_for_identity(
        const std::vector<std::string>& identity_values) const;
};

} // namespace level_pivot
