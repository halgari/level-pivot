#pragma once

#include "level_pivot/connection_manager.hpp"
#include "level_pivot/error.hpp"
#include <string>
#include <memory>

namespace level_pivot {

/**
 * Write operation result for raw tables
 */
struct RawWriteResult {
    size_t keys_written = 0;
    size_t keys_deleted = 0;
};

/**
 * Handles INSERT, UPDATE, DELETE operations on raw (non-pivoted) tables
 *
 * Operations are simple key-value mappings:
 *   - INSERT: Put(key, value)
 *   - UPDATE: Put(key, new_value) - key is immutable
 *   - DELETE: Delete(key)
 */
class RawWriter {
public:
    /**
     * Create a writer for the given connection
     *
     * @param connection LevelDB connection (must be writable)
     * @param use_batch Whether to buffer writes in a batch
     */
    RawWriter(std::shared_ptr<LevelDBConnection> connection, bool use_batch);

    /**
     * Insert a key-value pair
     *
     * @param key The key
     * @param value The value
     * @return Write result
     */
    RawWriteResult insert(const std::string& key, const std::string& value);

    /**
     * Update the value for an existing key
     *
     * Note: The key itself is immutable. This just overwrites the value.
     *
     * @param key The key
     * @param new_value The new value
     * @return Write result
     */
    RawWriteResult update(const std::string& key, const std::string& new_value);

    /**
     * Delete a key
     *
     * @param key The key to delete
     * @return Write result
     */
    RawWriteResult remove(const std::string& key);

    /**
     * Check if this writer is using batched mode
     */
    bool is_batched() const;

    /**
     * Commit the batch (only valid in batched mode)
     */
    void commit_batch();

    /**
     * Discard the batch without writing (only valid in batched mode)
     */
    void discard_batch();

    /**
     * Get the number of pending operations in the batch
     */
    size_t pending_count() const;

private:
    std::shared_ptr<LevelDBConnection> connection_;
    std::unique_ptr<LevelDBWriteBatch> batch_;

    void do_put(const std::string& key, const std::string& value);
    void do_del(const std::string& key);
};

} // namespace level_pivot
