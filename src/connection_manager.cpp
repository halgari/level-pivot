#include "level_pivot/connection_manager.hpp"
#include <leveldb/db.h>
#include <leveldb/cache.h>
#include <leveldb/options.h>
#include <leveldb/iterator.h>
#include <leveldb/write_batch.h>

namespace level_pivot {

// LevelDBIterator implementation

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

// LevelDBConnection implementation

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

void LevelDBConnection::check_write_allowed() {
    if (read_only_) {
        throw LevelDBError("Cannot write to read-only connection");
    }
}

// ConnectionManager implementation

ConnectionManager& ConnectionManager::instance() {
    static ConnectionManager manager;
    return manager;
}

ConnectionManager::~ConnectionManager() {
    close_all();
}

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
