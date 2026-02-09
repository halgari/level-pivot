/**
 * connection_manager.cpp - LevelDB connection management and pooling
 *
 * This module provides the interface between level_pivot and LevelDB:
 *   - LevelDBIterator: Sequential access to key-value pairs
 *   - LevelDBWriteBatch: Atomic multi-operation transactions
 *   - LevelDBConnection: Single database connection with get/put/del
 *   - ConnectionManager: Singleton that pools connections per PostgreSQL server
 *
 * Connection pooling is important because LevelDB only allows one process
 * to open a database at a time. By caching connections per server OID,
 * multiple foreign tables pointing to the same LevelDB can share a connection.
 */

#include "level_pivot/connection_manager.hpp"
#include <leveldb/db.h>
#include <leveldb/cache.h>
#include <leveldb/options.h>
#include <leveldb/iterator.h>
#include <leveldb/write_batch.h>
#include <string_view>

namespace level_pivot {

// LevelDBIterator implementation

/**
 * Creates an iterator with caching enabled.
 * fill_cache=true populates the block cache as we scan, which helps
 * subsequent queries that access the same blocks.
 */
LevelDBIterator::LevelDBIterator(leveldb::DB* db) {
    leveldb::ReadOptions options;
    options.fill_cache = true;
    iter_.reset(db->NewIterator(options));
}

LevelDBIterator::~LevelDBIterator() = default;

LevelDBIterator::LevelDBIterator(LevelDBIterator&& other) noexcept
    : iter_(std::move(other.iter_)) {}

LevelDBIterator& LevelDBIterator::operator=(LevelDBIterator&& other) noexcept {
    iter_ = std::move(other.iter_);
    return *this;
}

void LevelDBIterator::seek(const std::string& key) {
    iter_->Seek(key);
}

void LevelDBIterator::seek_to_first() {
    iter_->SeekToFirst();
}

void LevelDBIterator::next() {
    iter_->Next();
}

bool LevelDBIterator::valid() const {
    return iter_->Valid();
}

std::string LevelDBIterator::key() const {
    return iter_->key().ToString();
}

std::string LevelDBIterator::value() const {
    return iter_->value().ToString();
}

std::string_view LevelDBIterator::key_view() const {
    auto s = iter_->key();
    return std::string_view(s.data(), s.size());
}

/**
 * Returns value as string_view for zero-copy access.
 * IMPORTANT: This view is only valid until next(), so callers must
 * copy data they need to keep before advancing the iterator.
 */
std::string_view LevelDBIterator::value_view() const {
    auto s = iter_->value();
    return std::string_view(s.data(), s.size());
}

// LevelDBWriteBatch implementation

/**
 * WriteBatch accumulates operations in memory until commit().
 * This is crucial for atomicity - either all operations succeed
 * or none do. Also improves performance by reducing disk syncs.
 */
LevelDBWriteBatch::LevelDBWriteBatch(LevelDBConnection* connection)
    : connection_(connection), batch_(std::make_unique<leveldb::WriteBatch>()) {}

LevelDBWriteBatch::~LevelDBWriteBatch() {
    if (!committed_) {
        discard();
    }
}

LevelDBWriteBatch::LevelDBWriteBatch(LevelDBWriteBatch&& other) noexcept
    : connection_(other.connection_),
      batch_(std::move(other.batch_)),
      pending_count_(other.pending_count_),
      committed_(other.committed_) {
    other.connection_ = nullptr;
    other.pending_count_ = 0;
    other.committed_ = true;  // Prevent moved-from object from discarding
}

LevelDBWriteBatch& LevelDBWriteBatch::operator=(LevelDBWriteBatch&& other) noexcept {
    if (this != &other) {
        if (!committed_) {
            discard();
        }
        connection_ = other.connection_;
        batch_ = std::move(other.batch_);
        pending_count_ = other.pending_count_;
        committed_ = other.committed_;
        other.connection_ = nullptr;
        other.pending_count_ = 0;
        other.committed_ = true;
    }
    return *this;
}

void LevelDBWriteBatch::put(const std::string& key, const std::string& value) {
    batch_->Put(key, value);
    ++pending_count_;
}

void LevelDBWriteBatch::del(const std::string& key) {
    batch_->Delete(key);
    ++pending_count_;
}

/**
 * Applies all accumulated operations atomically to LevelDB.
 * sync=false means we don't wait for fsync - the OS buffer cache
 * provides durability for most crash scenarios. For critical data,
 * consider enabling sync or using external durability guarantees.
 */
void LevelDBWriteBatch::commit() {
    if (committed_) {
        return;
    }

    if (connection_ && batch_ && pending_count_ > 0) {
        leveldb::WriteOptions options;
        options.sync = false;

        leveldb::Status status = connection_->raw()->Write(options, batch_.get());
        if (!status.ok()) {
            throw LevelDBError("WriteBatch commit failed: " + status.ToString());
        }
    }

    committed_ = true;
    pending_count_ = 0;
}

void LevelDBWriteBatch::discard() {
    if (batch_) {
        batch_->Clear();
    }
    pending_count_ = 0;
    committed_ = true;
}

size_t LevelDBWriteBatch::pending_count() const {
    return pending_count_;
}

bool LevelDBWriteBatch::has_pending() const {
    return pending_count_ > 0;
}

// LevelDBConnection implementation

/**
 * Opens a LevelDB database with the specified options.
 *
 * block_cache: LRU cache for frequently accessed data blocks.
 *   Default 8MB is good for most workloads; increase for read-heavy.
 *
 * write_buffer_size: Memory used for buffering writes before flushing.
 *   Larger values improve write throughput but increase memory usage.
 *
 * Note: LevelDB doesn't support true read-only mode, so we track it
 * ourselves and reject writes if read_only=true.
 */
LevelDBConnection::LevelDBConnection(const ConnectionOptions& options)
    : path_(options.db_path), read_only_(options.read_only) {

    leveldb::Options db_options;
    db_options.create_if_missing = options.create_if_missing;
    db_options.write_buffer_size = options.write_buffer_size;

    if (options.block_cache_size > 0) {
        db_options.block_cache = leveldb::NewLRUCache(options.block_cache_size);
    }

    leveldb::Status status = leveldb::DB::Open(db_options, path_, &db_);

    if (!status.ok()) {
        throw LevelDBError("Failed to open LevelDB at '" + path_ + "': " +
                          status.ToString());
    }
}

LevelDBConnection::~LevelDBConnection() {
    delete db_;
}

std::optional<std::string> LevelDBConnection::get(const std::string& key) {
    std::string value;
    leveldb::ReadOptions options;

    leveldb::Status status = db_->Get(options, key, &value);

    if (status.IsNotFound()) {
        return std::nullopt;
    }

    if (!status.ok()) {
        throw LevelDBError("Get failed for key '" + key + "': " + status.ToString());
    }

    return value;
}

void LevelDBConnection::put(const std::string& key, const std::string& value) {
    check_write_allowed();

    leveldb::WriteOptions options;
    options.sync = false;  // Don't fsync each write for performance

    leveldb::Status status = db_->Put(options, key, value);

    if (!status.ok()) {
        throw LevelDBError("Put failed for key '" + key + "': " + status.ToString());
    }
}

void LevelDBConnection::del(const std::string& key) {
    check_write_allowed();

    leveldb::WriteOptions options;
    options.sync = false;

    leveldb::Status status = db_->Delete(options, key);

    if (!status.ok()) {
        throw LevelDBError("Delete failed for key '" + key + "': " + status.ToString());
    }
}

LevelDBIterator LevelDBConnection::iterator() {
    return LevelDBIterator(db_);
}

LevelDBWriteBatch LevelDBConnection::create_batch() {
    check_write_allowed();
    return LevelDBWriteBatch(this);
}

void LevelDBConnection::check_write_allowed() {
    if (read_only_) {
        throw LevelDBError("Cannot write to read-only connection");
    }
}

// ConnectionManager implementation

/**
 * Singleton pattern - one ConnectionManager per PostgreSQL backend process.
 * Using static local variable ensures thread-safe initialization (C++11+).
 */
ConnectionManager& ConnectionManager::instance() {
    static ConnectionManager manager;
    return manager;
}

ConnectionManager::~ConnectionManager() {
    close_all();
}

/**
 * Returns a connection for the given server, creating one if needed.
 * Connections are keyed by server_oid, so all foreign tables using
 * the same server share one LevelDB connection. This is required
 * because LevelDB locks the database directory.
 *
 * Mutex protects against concurrent connection creation in
 * parallel query execution scenarios.
 */
std::shared_ptr<LevelDBConnection> ConnectionManager::get_connection(
    unsigned int server_oid,
    const ConnectionOptions& options) {

    std::lock_guard<std::mutex> lock(mutex_);

    auto it = connections_.find(server_oid);
    if (it != connections_.end()) {
        return it->second;
    }

    auto conn = std::make_shared<LevelDBConnection>(options);
    connections_[server_oid] = conn;
    return conn;
}

void ConnectionManager::close_connection(unsigned int server_oid) {
    std::lock_guard<std::mutex> lock(mutex_);
    connections_.erase(server_oid);
}

void ConnectionManager::close_all() {
    std::lock_guard<std::mutex> lock(mutex_);
    connections_.clear();
}

size_t ConnectionManager::connection_count() const {
    std::lock_guard<std::mutex> lock(mutex_);
    return connections_.size();
}

} // namespace level_pivot
