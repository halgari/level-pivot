#include "level_pivot/pivot_scanner.hpp"

namespace level_pivot {

PivotScanner::PivotScanner(const Projection& projection,
                           std::shared_ptr<LevelDBConnection> connection)
    : projection_(projection), connection_(std::move(connection)) {}

void PivotScanner::begin_scan() {
    begin_scan({});
}

void PivotScanner::begin_scan(const std::vector<std::string>& prefix_values) {
    stats_ = Stats{};
    current_identity_.reset();
    current_attrs_.clear();

    // Build prefix for seeking
    prefix_ = projection_.parser().build_prefix(prefix_values);

    // Create iterator and seek to prefix
    iterator_ = std::make_unique<LevelDBIterator>(connection_->iterator());

    if (prefix_.empty()) {
        iterator_->seek_to_first();
    } else {
        iterator_->seek(prefix_);
    }
}

std::optional<PivotRow> PivotScanner::next_row() {
    while (iterator_ && iterator_->valid()) {
        // Zero-copy: get key as string_view
        std::string_view key_sv = iterator_->key_view();

        // Check if we're still within the prefix
        if (!is_within_prefix_view(key_sv)) {
            // Emit any accumulated row before stopping
            if (current_identity_.has_value()) {
                return emit_current_row();
            }
            return std::nullopt;
        }

        ++stats_.keys_scanned;

        // Zero-copy parse using string_view
        auto parsed = projection_.parser().parse_view(key_sv);
        if (!parsed) {
            ++stats_.keys_skipped;
            iterator_->next();
            continue;
        }

        // Check if this is the same row or a new one
        if (!current_identity_.has_value()) {
            // First row - materialize identity strings
            current_identity_ = materialize_identity(parsed->capture_values);
            current_attrs_.clear();
        } else if (!identity_matches(*current_identity_, parsed->capture_values)) {
            // Identity changed - emit the previous row and start a new one
            auto row = emit_current_row();
            current_identity_ = materialize_identity(parsed->capture_values);
            current_attrs_.clear();

            // Accumulate this key into the new row
            std::string attr_name(parsed->attr_name);
            if (projection_.has_attr(attr_name)) {
                current_attrs_[attr_name] = std::string(iterator_->value_view());
            }

            iterator_->next();
            return row;
        }

        // Accumulate this key into the current row
        std::string attr_name(parsed->attr_name);
        if (projection_.has_attr(attr_name)) {
            current_attrs_[attr_name] = std::string(iterator_->value_view());
        }

        iterator_->next();
    }

    // End of iteration - emit any remaining row
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

std::vector<std::string> PivotScanner::materialize_identity(
    const std::vector<std::string_view>& views) {
    std::vector<std::string> result;
    result.reserve(views.size());
    for (const auto& sv : views) {
        result.emplace_back(sv);
    }
    return result;
}

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

// DatumBuilder implementation

void DatumBuilder::build_datums(const PivotRow& row,
                                const Projection& projection,
                                Datum* values,
                                bool* nulls) {
    const auto& columns = projection.columns();

    for (size_t i = 0; i < columns.size(); ++i) {
        const auto& col = columns[i];

        if (col.is_identity) {
            // Identity column - use O(1) lookup via pre-computed index
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
            // Attr column - look up in attr_values
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
