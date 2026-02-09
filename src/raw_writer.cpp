#include "level_pivot/raw_writer.hpp"

namespace level_pivot {

RawWriter::RawWriter(std::shared_ptr<LevelDBConnection> connection, bool use_batch)
    : connection_(std::move(connection)), batch_(nullptr) {

    if (connection_->is_read_only()) {
        throw LevelDBError("Cannot create writer for read-only connection");
    }

    if (use_batch) {
        batch_ = std::make_unique<LevelDBWriteBatch>(connection_->create_batch());
    }
}

RawWriteResult RawWriter::insert(const std::string& key, const std::string& value) {
    RawWriteResult result;
    do_put(key, value);
    result.keys_written = 1;
    return result;
}

RawWriteResult RawWriter::update(const std::string& key, const std::string& new_value) {
    RawWriteResult result;
    do_put(key, new_value);
    result.keys_written = 1;
    return result;
}

RawWriteResult RawWriter::remove(const std::string& key) {
    RawWriteResult result;
    do_del(key);
    result.keys_deleted = 1;
    return result;
}

void RawWriter::do_put(const std::string& key, const std::string& value) {
    if (batch_) {
        batch_->put(key, value);
    } else {
        connection_->put(key, value);
    }
}

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
