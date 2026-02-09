/**
 * writer.cpp - Handles INSERT, UPDATE, DELETE operations for pivot tables
 *
 * The writer converts SQL DML operations into LevelDB key-value operations:
 *   - INSERT: Creates one key per non-null attr column
 *   - UPDATE: Modifies existing keys, deletes newly-null attrs
 *   - DELETE: Removes all keys matching the row's identity
 *
 * Operations can be batched for atomicity and performance. When using a
 * WriteBatch, all operations are held in memory until commit_batch() is
 * called, then applied atomically to LevelDB.
 */

#include "level_pivot/writer.hpp"
#include <string_view>

namespace level_pivot {

namespace {
/**
 * Helper to compare owned strings with parsed string_views.
 * Used to verify identity matches when scanning for keys to delete.
 */
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

/**
 * INSERT creates one LevelDB key-value pair for each non-null attr column.
 * For example, INSERT INTO users (group, id, name, email) VALUES ('admins', 'u1', 'Alice', 'a@x.com')
 * creates two keys:
 *   users##admins##u1##name  = "Alice"
 *   users##admins##u1##email = "a@x.com"
 *
 * Identity columns (group, id) form the key prefix; attr columns become the {attr} suffix.
 */
WriteResult Writer::insert(Datum* values, bool* nulls) {
    WriteResult result;

    auto identity = extract_identity(values, nulls);

    // Identity columns cannot be NULL - they're part of the key
    for (const auto& val : identity) {
        if (val.empty()) {
            throw LevelPivotError("Cannot insert row with NULL identity column");
        }
    }

    // Extract non-null attrs and write a key for each one.
    // NULL attrs are simply not written - they'll read back as NULL.
    auto extracted = extract_all_attrs(values, nulls);

    for (const auto& [attr_name, attr_value] : extracted.values) {
        std::string key = projection_.parser().build(identity, attr_name);
        do_put(key, attr_value);
        ++result.keys_written;
    }

    return result;
}

/**
 * UPDATE handles both value changes and identity changes.
 *
 * If identity columns changed: This is effectively DELETE old + INSERT new,
 * because the keys are completely different.
 *
 * If identity unchanged: Update values in place. For attrs that became NULL,
 * delete those keys (NULL means "no key exists").
 */
WriteResult Writer::update(Datum* old_values, bool* old_nulls,
                           Datum* new_values, bool* new_nulls) {
    WriteResult result;

    auto old_identity = extract_identity(old_values, old_nulls);
    auto new_identity = extract_identity(new_values, new_nulls);

    bool identity_changed = (old_identity != new_identity);

    if (identity_changed) {
        // Identity changed - keys move to new location.
        // Delete all old keys, then create new ones.
        auto del_result = remove_by_identity(old_identity);
        result.keys_deleted = del_result.keys_deleted;

        auto ins_result = insert(new_values, new_nulls);
        result.keys_written = ins_result.keys_written;

        return result;
    }

    // Identity unchanged - update in place
    auto extracted = extract_all_attrs(new_values, new_nulls);

    // Write keys for non-null attrs (creates or updates)
    for (const auto& [attr_name, attr_value] : extracted.values) {
        std::string key = projection_.parser().build(new_identity, attr_name);
        do_put(key, attr_value);
        ++result.keys_written;
    }

    // Delete keys for attrs that are now NULL.
    // This is how "UPDATE ... SET email = NULL" works.
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

/**
 * DELETE removes all keys matching a given identity.
 * We can't just delete known attr columns because other attrs might exist
 * in LevelDB that aren't in our projection. So we scan for all matching keys.
 */
WriteResult Writer::remove_by_identity(const std::vector<std::string>& identity_values) {
    WriteResult result;

    // Scan LevelDB to find all keys with this identity
    auto keys = find_keys_for_identity(identity_values);

    for (const auto& key : keys) {
        do_del(key);
        ++result.keys_deleted;
    }

    return result;
}

/**
 * Extracts identity column values from a PostgreSQL tuple.
 * Values are extracted in capture order (as defined by the pattern),
 * not column order, because that's how keys are built.
 */
std::vector<std::string> Writer::extract_identity(Datum* values, bool* nulls) const {
    std::vector<std::string> identity;
    identity.reserve(projection_.identity_columns().size());

    const auto& capture_names = projection_.parser().pattern().capture_names();

    for (const auto& cap_name : capture_names) {
        const auto* col = projection_.column(cap_name);
        if (!col) {
            throw LevelPivotError("Identity column not found: " + cap_name);
        }

        // attnum is 1-based in PostgreSQL, array is 0-based
        int idx = col->attnum - 1;
        if (nulls[idx]) {
            identity.push_back("");  // Empty string represents NULL
        } else {
            identity.push_back(TypeConverter::datum_to_string(
                values[idx], col->type, false));
        }
    }

    return identity;
}

/**
 * Extracts all attr column values, separating non-null values from null names.
 * This split is needed because UPDATE treats them differently:
 * non-null values get written, null values cause key deletion.
 */
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

/**
 * Scans LevelDB to find all keys that have a given identity.
 * This is needed for DELETE to find all attr keys to remove.
 *
 * Uses prefix seeking for efficiency - we jump directly to the first
 * possible key with this identity, then scan until identity changes.
 */
std::vector<std::string> Writer::find_keys_for_identity(
    const std::vector<std::string>& identity_values) const {

    std::vector<std::string> keys;

    // Build prefix to seek efficiently
    std::string prefix = projection_.parser().build_prefix(identity_values);

    auto iter = connection_->iterator();

    if (prefix.empty()) {
        iter.seek_to_first();
    } else {
        iter.seek(prefix);
    }

    while (iter.valid()) {
        // Zero-copy key access
        std::string_view key_sv = iter.key_view();

        // Stop when we leave the prefix range
        if (!prefix.empty() && (key_sv.size() < prefix.size() ||
            key_sv.substr(0, prefix.size()) != prefix)) {
            break;
        }

        // Parse and verify identity matches (prefix might match unrelated keys)
        auto parsed = projection_.parser().parse_view(key_sv);
        if (parsed && identity_matches_views(identity_values, parsed->capture_values)) {
            // Only materialize string when we have a confirmed match
            keys.emplace_back(key_sv);
        }

        iter.next();
    }

    return keys;
}

/**
 * Writes a key-value pair, either to batch or directly to LevelDB.
 * When batched, operations are held in memory until commit_batch().
 */
void Writer::do_put(const std::string& key, const std::string& value) {
    if (batch_) {
        batch_->put(key, value);
    } else {
        connection_->put(key, value);
    }
}

/**
 * Deletes a key, either from batch or directly from LevelDB.
 */
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

/**
 * Commits all batched operations atomically.
 * If any operation fails, none are applied.
 */
void Writer::commit_batch() {
    if (batch_) {
        batch_->commit();
    }
}

/**
 * Discards all batched operations without applying them.
 * Used for rollback on error.
 */
void Writer::discard_batch() {
    if (batch_) {
        batch_->discard();
    }
}

size_t Writer::pending_count() const {
    return batch_ ? batch_->pending_count() : 0;
}

} // namespace level_pivot
