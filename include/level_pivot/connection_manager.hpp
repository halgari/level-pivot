#pragma once

#include "level_pivot/error.hpp"
#include <string>
#include <string_view>
#include <memory>
#include <mutex>
#include <optional>
#include <unordered_map>

// Forward declarations
namespace leveldb {
    class DB;
    class Iterator;
    struct Options;
    struct ReadOptions;
    struct WriteOptions;
    class WriteBatch;
    class Status;
}

namespace level_pivot {

/**
 * Options for opening a LevelDB connection
 */
struct ConnectionOptions {
    std::string db_path;
    bool read_only = true;
    bool create_if_missing = false;
    size_t block_cache_size = 8 * 1024 * 1024;  // 8MB default
    size_t write_buffer_size = 4 * 1024 * 1024;  // 4MB default
    bool use_write_batch = true;  // Use WriteBatch for atomic operations
};

/**
 * RAII wrapper for LevelDB iterator
 */
class LevelDBIterator {
public:
    LevelDBIterator(leveldb::DB* db);
    ~LevelDBIterator();

    // Move-only
    LevelDBIterator(LevelDBIterator&& other) noexcept;
    LevelDBIterator& operator=(LevelDBIterator&& other) noexcept;
    LevelDBIterator(const LevelDBIterator&) = delete;
    LevelDBIterator& operator=(const LevelDBIterator&) = delete;

    void seek(const std::string& key);
    void seek_to_first();
    void next();
    bool valid() const;
    std::string key() const;
    std::string value() const;

    /**
     * Get the current key as a string_view (zero-copy)
     *
     * WARNING: The returned view is only valid until the iterator moves.
     * Use key() if you need the value to outlive iterator movement.
     */
    std::string_view key_view() const;

    /**
     * Get the current value as a string_view (zero-copy)
     *
     * WARNING: The returned view is only valid until the iterator moves.
     * Use value() if you need the value to outlive iterator movement.
     */
    std::string_view value_view() const;

private:
    std::unique_ptr<leveldb::Iterator> iter_;
};

// Forward declaration for LevelDBWriteBatch
class LevelDBWriteBatch;

/**
 * RAII wrapper for LevelDB connection
 */
class LevelDBConnection {
public:
    explicit LevelDBConnection(const ConnectionOptions& options);
    ~LevelDBConnection();

    // Non-copyable
    LevelDBConnection(const LevelDBConnection&) = delete;
    LevelDBConnection& operator=(const LevelDBConnection&) = delete;

    /**
     * Get a value by key
     * @return Value string, or std::nullopt if key not found
     */
    std::optional<std::string> get(const std::string& key);

    /**
     * Put a key-value pair
     */
    void put(const std::string& key, const std::string& value);

    /**
     * Delete a key
     */
    void del(const std::string& key);

    /**
     * Create an iterator for range scans
     */
    LevelDBIterator iterator();

    /**
     * Create a write batch for atomic operations
     */
    LevelDBWriteBatch create_batch();

    /**
     * Get the database path
     */
    const std::string& path() const { return path_; }

    /**
     * Check if the connection is read-only
     */
    bool is_read_only() const { return read_only_; }

    /**
     * Get raw DB pointer (for advanced use)
     */
    leveldb::DB* raw() { return db_; }

private:
    leveldb::DB* db_ = nullptr;
    std::string path_;
    bool read_only_;

    void check_write_allowed();
};

/**
 * RAII wrapper for LevelDB WriteBatch
 *
 * Buffers write operations and commits them atomically.
 * If not explicitly committed, the destructor discards pending writes.
 */
class LevelDBWriteBatch {
public:
    explicit LevelDBWriteBatch(LevelDBConnection* connection);
    ~LevelDBWriteBatch();  // Discards if not committed

    // Move-only
    LevelDBWriteBatch(LevelDBWriteBatch&& other) noexcept;
    LevelDBWriteBatch& operator=(LevelDBWriteBatch&& other) noexcept;
    LevelDBWriteBatch(const LevelDBWriteBatch&) = delete;
    LevelDBWriteBatch& operator=(const LevelDBWriteBatch&) = delete;

    /**
     * Add a put operation to the batch
     */
    void put(const std::string& key, const std::string& value);

    /**
     * Add a delete operation to the batch
     */
    void del(const std::string& key);

    /**
     * Commit all operations atomically
     */
    void commit();

    /**
     * Discard all pending operations without writing
     */
    void discard();

    /**
     * Get the number of pending operations
     */
    size_t pending_count() const;

    /**
     * Check if there are pending operations
     */
    bool has_pending() const;

private:
    LevelDBConnection* connection_;
    std::unique_ptr<leveldb::WriteBatch> batch_;
    size_t pending_count_ = 0;
    bool committed_ = false;
};

/**
 * Singleton manager for LevelDB connections
 *
 * Manages a pool of connections keyed by PostgreSQL SERVER OID.
 * Thread-safe for concurrent access.
 */
class ConnectionManager {
public:
    /**
     * Get the singleton instance
     */
    static ConnectionManager& instance();

    /**
     * Get or create a connection for a server
     *
     * @param server_oid PostgreSQL server OID
     * @param options Connection options (used only if connection doesn't exist)
     * @return Shared pointer to the connection
     */
    std::shared_ptr<LevelDBConnection> get_connection(
        unsigned int server_oid,
        const ConnectionOptions& options);

    /**
     * Close a connection for a server
     */
    void close_connection(unsigned int server_oid);

    /**
     * Close all connections
     */
    void close_all();

    /**
     * Get the number of active connections
     */
    size_t connection_count() const;

private:
    ConnectionManager() = default;
    ~ConnectionManager();

    // Non-copyable
    ConnectionManager(const ConnectionManager&) = delete;
    ConnectionManager& operator=(const ConnectionManager&) = delete;

    mutable std::mutex mutex_;
    std::unordered_map<unsigned int, std::shared_ptr<LevelDBConnection>> connections_;
};

} // namespace level_pivot
