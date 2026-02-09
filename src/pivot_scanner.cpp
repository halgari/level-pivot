/**
 * pivot_scanner.cpp - Scans LevelDB and assembles pivoted rows
 *
 * The core of the "pivot" operation: multiple LevelDB keys with the same
 * identity values are combined into a single SQL row. For example:
 *
 *   LevelDB keys:
 *     users##admins##user001##name  = "Alice"
 *     users##admins##user001##email = "alice@example.com"
 *     users##admins##user002##name  = "Bob"
 *
 *   Produces SQL rows:
 *     (group='admins', id='user001', name='Alice', email='alice@example.com')
 *     (group='admins', id='user002', name='Bob', email=NULL)
 *
 * The scanner maintains state across next_row() calls, accumulating attrs
 * until the identity changes, then emitting a complete row.
 */

#include "level_pivot/pivot_scanner.hpp"

namespace level_pivot {

PivotScanner::PivotScanner(const Projection& projection,
                           std::shared_ptr<LevelDBConnection> connection)
    : projection_(projection), connection_(std::move(connection)) {}

void PivotScanner::begin_scan() {
    begin_scan({});
}

/**
 * Initializes the scan with optional prefix filter values.
 * If prefix_values contains ["admins"], we seek directly to "users##admins##"
 * instead of scanning from the start, making filtered queries O(matching keys)
 * instead of O(all keys).
 */
void PivotScanner::begin_scan(const std::vector<std::string>& prefix_values) {
    stats_ = Stats{};
    current_identity_.reset();
    current_attrs_.clear();

    // Build prefix from provided filter values for efficient seeking
    prefix_ = projection_.parser().build_prefix(prefix_values);

    iterator_ = std::make_unique<LevelDBIterator>(connection_->iterator());

    if (prefix_.empty()) {
        iterator_->seek_to_first();
    } else {
        iterator_->seek(prefix_);
    }
}

/**
 * Returns the next pivoted row, or nullopt when exhausted.
 *
 * The state machine works as follows:
 *   1. Read keys sequentially from LevelDB
 *   2. Parse each key to extract identity and attr
 *   3. If identity matches current row, accumulate attr value
 *   4. If identity changes, emit accumulated row and start new one
 *   5. At end of iteration, emit any remaining accumulated row
 *
 * This streaming approach means we never load all keys into memory -
 * we only hold one row's worth of attrs at a time.
 */
std::optional<PivotRow> PivotScanner::next_row() {
    while (iterator_ && iterator_->valid()) {
        // Zero-copy: get key as string_view to avoid allocation
        std::string_view key_sv = iterator_->key_view();

        // Stop scanning when we leave the prefix range.
        // LevelDB iteration is sorted, so all matching keys are contiguous.
        if (!is_within_prefix_view(key_sv)) {
            if (current_identity_.has_value()) {
                return emit_current_row();
            }
            return std::nullopt;
        }

        ++stats_.keys_scanned;

        // Parse the key to extract identity values and attr name.
        // Keys that don't match the pattern are skipped (e.g., other tables' data).
        auto parsed = projection_.parser().parse_view(key_sv);
        if (!parsed) {
            ++stats_.keys_skipped;
            iterator_->next();
            continue;
        }

        if (!current_identity_.has_value()) {
            // First key - start accumulating a new row
            current_identity_ = materialize_identity(parsed->capture_values);
            current_attrs_.clear();
        } else if (!identity_matches(*current_identity_, parsed->capture_values)) {
            // Identity changed - emit the completed row and start a new one.
            // We return immediately here to yield the row to the caller.
            auto row = emit_current_row();
            current_identity_ = materialize_identity(parsed->capture_values);
            current_attrs_.clear();

            // Don't lose this key's attr - add it to the new row
            std::string_view attr_name = parsed->attr_name;
            if (projection_.has_attr(attr_name)) {
                current_attrs_[std::string(attr_name)] = std::string(iterator_->value_view());
            }

            iterator_->next();
            return row;
        }

        // Same identity - accumulate this attr into the current row.
        // Only accumulate attrs that are in our projection (table columns).
        std::string_view attr_name = parsed->attr_name;
        if (projection_.has_attr(attr_name)) {
            current_attrs_[std::string(attr_name)] = std::string(iterator_->value_view());
        }

        iterator_->next();
    }

    // End of iteration - emit any remaining accumulated row
    if (current_identity_.has_value()) {
        return emit_current_row();
    }

    return std::nullopt;
}

void PivotScanner::rescan() {
    begin_scan({});
}

void PivotScanner::end_scan() {
    iterator_.reset();
    current_identity_.reset();
    current_attrs_.clear();
}

bool PivotScanner::is_within_prefix(const std::string& key) const {
    if (prefix_.empty()) {
        return true;
    }
    return key.compare(0, prefix_.size(), prefix_) == 0;
}

bool PivotScanner::is_within_prefix_view(std::string_view key) const {
    if (prefix_.empty()) {
        return true;
    }
    if (key.size() < prefix_.size()) {
        return false;
    }
    return key.substr(0, prefix_.size()) == prefix_;
}

/**
 * Converts string_views (pointing into LevelDB's memory) into owned strings.
 * We must do this before calling iterator_->next() because LevelDB may
 * invalidate the previous key's memory.
 */
std::vector<std::string> PivotScanner::materialize_identity(
    const std::vector<std::string_view>& views) {
    std::vector<std::string> result;
    result.reserve(views.size());
    for (const auto& sv : views) {
        result.emplace_back(sv);
    }
    return result;
}

/**
 * Compares owned identity strings with parsed string_views without allocation.
 * This is called for every key to detect row boundaries.
 */
bool PivotScanner::identity_matches(
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

/**
 * Packages accumulated identity and attrs into a PivotRow and resets state.
 * Uses std::move to transfer ownership efficiently.
 */
std::optional<PivotRow> PivotScanner::emit_current_row() {
    if (!current_identity_.has_value()) {
        return std::nullopt;
    }

    PivotRow row;
    row.identity_values = std::move(*current_identity_);
    row.attr_values = std::move(current_attrs_);

    ++stats_.rows_returned;

    current_identity_.reset();
    current_attrs_.clear();

    return row;
}

/**
 * DatumBuilder converts PivotRows into PostgreSQL Datum arrays for tuple building.
 *
 * The challenge is mapping between two orderings:
 *   - PivotRow::identity_values are ordered by pattern capture order
 *   - PostgreSQL tuple expects values in column attnum order
 *
 * We use pre-computed column_to_identity_index for O(1) mapping.
 */
void DatumBuilder::build_datums(const PivotRow& row,
                                const Projection& projection,
                                Datum* values,
                                bool* nulls) {
    const auto& columns = projection.columns();

    for (size_t i = 0; i < columns.size(); ++i) {
        const auto& col = columns[i];

        if (col.is_identity) {
            // Identity column: look up value by pre-computed index into identity_values.
            // This avoids name-based lookup for every cell.
            int identity_idx = projection.column_to_identity_index(i);
            if (identity_idx >= 0 &&
                static_cast<size_t>(identity_idx) < row.identity_values.size()) {
                values[i] = TypeConverter::string_to_datum(
                    row.identity_values[identity_idx], col.type, nulls[i]);
            } else {
                nulls[i] = true;
                values[i] = (Datum)0;
            }
        } else {
            // Attr column: look up by column name in attr_values map.
            // Missing attrs are NULL (that attr key didn't exist in LevelDB).
            auto it = row.attr_values.find(col.name);
            if (it != row.attr_values.end()) {
                values[i] = TypeConverter::string_to_datum(it->second, col.type, nulls[i]);
            } else {
                nulls[i] = true;
                values[i] = (Datum)0;
            }
        }
    }
}

} // namespace level_pivot
