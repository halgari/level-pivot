/**
 * raw_writer.cpp - Direct key-value writes for raw table mode
 *
 * RawWriter handles INSERT, UPDATE, DELETE for raw foreign tables.
 * Unlike the pivot Writer, there's no pattern transformation - keys
 * and values are written directly to LevelDB.
 *
 * Supports optional WriteBatch for atomic multi-row operations.
 * Without batching, each operation is written immediately (faster for
 * single rows but no atomicity across multiple operations).
 */

#include "level_pivot/raw_writer.hpp"

namespace level_pivot {

/**
 * Creates a writer with optional batching support.
 * When use_batch=true, operations accumulate until commit_batch().
 * This provides atomicity: either all operations succeed or none.
 */
RawWriter::RawWriter(std::shared_ptr<LevelDBConnection> connection, bool use_batch)
    : connection_(std::move(connection)), batch_(nullptr) {

    if (connection_->is_read_only()) {
        throw LevelDBError("Cannot create writer for read-only connection");
    }

    if (use_batch) {
        batch_ = std::make_unique<LevelDBWriteBatch>(connection_->create_batch());
    }
}

/**
 * INSERT: Creates a new key-value pair.
 * In LevelDB, this is an upsert - it will overwrite if the key exists.
 * PostgreSQL's FDW layer doesn't check for duplicates.
 */
RawWriteResult RawWriter::insert(const std::string& key, const std::string& value) {
    RawWriteResult result;
    do_put(key, value);
    result.keys_written = 1;
    return result;
}

/**
 * UPDATE: Replaces the value for an existing key.
 * In LevelDB this is identical to insert - just a put operation.
 * The key is preserved; only the value changes.
 */
RawWriteResult RawWriter::update(const std::string& key, const std::string& new_value) {
    RawWriteResult result;
    do_put(key, new_value);
    result.keys_written = 1;
    return result;
}

/**
 * DELETE: Removes a key-value pair.
 * LevelDB delete is idempotent - deleting a non-existent key is a no-op.
 */
RawWriteResult RawWriter::remove(const std::string& key) {
    RawWriteResult result;
    do_del(key);
    result.keys_deleted = 1;
    return result;
}

/**
 * Internal: routes put to batch or direct write based on mode.
 * Batch mode accumulates in memory; direct mode writes immediately.
 */
void RawWriter::do_put(const std::string& key, const std::string& value) {
    if (batch_) {
        batch_->put(key, value);
    } else {
        connection_->put(key, value);
    }
}

/**
 * Internal: routes delete to batch or direct write based on mode.
 */
void RawWriter::do_del(const std::string& key) {
    if (batch_) {
        batch_->del(key);
    } else {
        connection_->del(key);
    }
}

bool RawWriter::is_batched() const {
    return batch_ != nullptr;
}

void RawWriter::commit_batch() {
    if (batch_) {
        batch_->commit();
    }
}

void RawWriter::discard_batch() {
    if (batch_) {
        batch_->discard();
    }
}

size_t RawWriter::pending_count() const {
    return batch_ ? batch_->pending_count() : 0;
}

} // namespace level_pivot
