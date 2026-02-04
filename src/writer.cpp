#include "level_pivot/writer.hpp"
#include <string_view>

namespace level_pivot {

namespace {
// Helper to compare owned identity with parsed views
bool identity_matches_views(
    const std::vector<std::string>& identity,
    const std::vector<std::string_view>& views) {
    if (identity.size() != views.size()) {
        return false;
    }
    for (size_t i = 0; i < identity.size(); ++i) {
        if (identity[i] != views[i]) {
            return false;
        }
    }
    return true;
}
} // anonymous namespace

Writer::Writer(const Projection& projection,
               std::shared_ptr<LevelDBConnection> connection)
    : projection_(projection), connection_(std::move(connection)), batch_(nullptr) {

    if (connection_->is_read_only()) {
        throw LevelDBError("Cannot create writer for read-only connection");
    }
}

Writer::Writer(const Projection& projection,
               std::shared_ptr<LevelDBConnection> connection,
               std::unique_ptr<LevelDBWriteBatch> batch)
    : projection_(projection), connection_(std::move(connection)), batch_(std::move(batch)) {

    if (connection_->is_read_only()) {
        throw LevelDBError("Cannot create writer for read-only connection");
    }
}

WriteResult Writer::insert(Datum* values, bool* nulls) {
    WriteResult result;

    // Extract identity values
    auto identity = extract_identity(values, nulls);

    // Check for null identity columns
    for (const auto& val : identity) {
        if (val.empty()) {
            throw LevelPivotError("Cannot insert row with NULL identity column");
        }
    }

    // Extract attr values and write keys (null attrs are ignored for INSERT)
    auto extracted = extract_all_attrs(values, nulls);

    for (const auto& [attr_name, attr_value] : extracted.values) {
        std::string key = projection_.parser().build(identity, attr_name);
        do_put(key, attr_value);
        ++result.keys_written;
    }

    return result;
}

WriteResult Writer::update(Datum* old_values, bool* old_nulls,
                           Datum* new_values, bool* new_nulls) {
    WriteResult result;

    // Extract identity from old row (used to locate existing keys)
    auto old_identity = extract_identity(old_values, old_nulls);

    // Extract identity from new row (for key construction)
    auto new_identity = extract_identity(new_values, new_nulls);

    // Check if identity columns changed
    bool identity_changed = (old_identity != new_identity);

    if (identity_changed) {
        // Identity changed - this is essentially DELETE + INSERT
        // First delete all old keys
        auto del_result = remove_by_identity(old_identity);
        result.keys_deleted = del_result.keys_deleted;

        // Then insert new keys
        auto ins_result = insert(new_values, new_nulls);
        result.keys_written = ins_result.keys_written;

        return result;
    }

    // Identity unchanged - update in place
    auto extracted = extract_all_attrs(new_values, new_nulls);

    // Write non-null attrs
    for (const auto& [attr_name, attr_value] : extracted.values) {
        std::string key = projection_.parser().build(new_identity, attr_name);
        do_put(key, attr_value);
        ++result.keys_written;
    }

    // Delete attrs that are now null
    for (const auto& attr_name : extracted.null_names) {
        std::string key = projection_.parser().build(new_identity, attr_name);
        do_del(key);
        ++result.keys_deleted;
    }

    return result;
}

WriteResult Writer::remove(Datum* values, bool* nulls) {
    auto identity = extract_identity(values, nulls);
    return remove_by_identity(identity);
}

WriteResult Writer::remove_by_identity(const std::vector<std::string>& identity_values) {
    WriteResult result;

    // Find and delete all keys with this identity
    auto keys = find_keys_for_identity(identity_values);

    for (const auto& key : keys) {
        do_del(key);
        ++result.keys_deleted;
    }

    return result;
}

std::vector<std::string> Writer::extract_identity(Datum* values, bool* nulls) const {
    std::vector<std::string> identity;
    identity.reserve(projection_.identity_columns().size());

    const auto& capture_names = projection_.parser().pattern().capture_names();

    for (const auto& cap_name : capture_names) {
        const auto* col = projection_.column(cap_name);
        if (!col) {
            throw LevelPivotError("Identity column not found: " + cap_name);
        }

        int idx = col->attnum - 1;  // attnum is 1-based
        if (nulls[idx]) {
            identity.push_back("");  // Empty string for NULL
        } else {
            identity.push_back(TypeConverter::datum_to_string(
                values[idx], col->type, false));
        }
    }

    return identity;
}

Writer::ExtractedAttrs Writer::extract_all_attrs(Datum* values, bool* nulls) const {
    ExtractedAttrs result;

    for (const auto* col : projection_.attr_columns()) {
        int idx = col->attnum - 1;
        if (nulls[idx]) {
            result.null_names.push_back(col->name);
        } else {
            result.values[col->name] = TypeConverter::datum_to_string(
                values[idx], col->type, false);
        }
    }

    return result;
}

std::vector<std::string> Writer::find_keys_for_identity(
    const std::vector<std::string>& identity_values) const {

    std::vector<std::string> keys;

    // Build prefix for this identity
    std::string prefix = projection_.parser().build_prefix(identity_values);

    // Scan for matching keys
    auto iter = connection_->iterator();

    if (prefix.empty()) {
        iter.seek_to_first();
    } else {
        iter.seek(prefix);
    }

    while (iter.valid()) {
        // Zero-copy: get key as string_view
        std::string_view key_sv = iter.key_view();

        // Check if still within prefix
        if (!prefix.empty() && (key_sv.size() < prefix.size() ||
            key_sv.substr(0, prefix.size()) != prefix)) {
            break;
        }

        // Zero-copy parse to check identity match
        auto parsed = projection_.parser().parse_view(key_sv);
        if (parsed && identity_matches_views(identity_values, parsed->capture_values)) {
            // Only materialize when we have a match
            keys.emplace_back(key_sv);
        }

        iter.next();
    }

    return keys;
}

void Writer::do_put(const std::string& key, const std::string& value) {
    if (batch_) {
        batch_->put(key, value);
    } else {
        connection_->put(key, value);
    }
}

void Writer::do_del(const std::string& key) {
    if (batch_) {
        batch_->del(key);
    } else {
        connection_->del(key);
    }
}

bool Writer::is_batched() const {
    return batch_ != nullptr;
}

void Writer::commit_batch() {
    if (batch_) {
        batch_->commit();
    }
}

void Writer::discard_batch() {
    if (batch_) {
        batch_->discard();
    }
}

size_t Writer::pending_count() const {
    return batch_ ? batch_->pending_count() : 0;
}

} // namespace level_pivot
