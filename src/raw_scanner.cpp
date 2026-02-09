#include "level_pivot/raw_scanner.hpp"

namespace level_pivot {

// RawScanBounds implementation

std::string RawScanBounds::seek_start() const {
    if (exact_key.has_value()) {
        return *exact_key;
    }
    if (lower_bound.has_value()) {
        return *lower_bound;
    }
    return "";  // Start from beginning
}

bool RawScanBounds::is_within_bounds(std::string_view key) const {
    // Exact match case
    if (exact_key.has_value()) {
        return key == *exact_key;
    }

    // Lower bound check
    if (lower_bound.has_value()) {
        int cmp = key.compare(*lower_bound);
        if (lower_inclusive) {
            if (cmp < 0) return false;  // key < lower
        } else {
            if (cmp <= 0) return false;  // key <= lower
        }
    }

    // Upper bound check
    if (upper_bound.has_value()) {
        int cmp = key.compare(*upper_bound);
        if (upper_inclusive) {
            if (cmp > 0) return false;  // key > upper
        } else {
            if (cmp >= 0) return false;  // key >= upper
        }
    }

    return true;
}

bool RawScanBounds::is_past_upper_bound(std::string_view key) const {
    // No upper bound - never past
    if (!upper_bound.has_value()) {
        return false;
    }

    // Exact match - past if key > exact_key
    if (exact_key.has_value()) {
        return key > *exact_key;
    }

    int cmp = key.compare(*upper_bound);
    if (upper_inclusive) {
        return cmp > 0;  // Past if key > upper
    } else {
        return cmp >= 0;  // Past if key >= upper
    }
}

// RawScanner implementation

RawScanner::RawScanner(std::shared_ptr<LevelDBConnection> connection)
    : connection_(std::move(connection)) {}

void RawScanner::begin_scan(const RawScanBounds& bounds) {
    stats_ = Stats{};
    bounds_ = bounds;
    exact_match_returned_ = false;

    iterator_ = std::make_unique<LevelDBIterator>(connection_->iterator());

    std::string seek_key = bounds_.seek_start();
    if (seek_key.empty()) {
        iterator_->seek_to_first();
    } else {
        iterator_->seek(seek_key);
    }

    // For non-inclusive lower bound, skip if we landed exactly on the bound
    if (iterator_->valid() && bounds_.lower_bound.has_value() &&
        !bounds_.lower_inclusive) {
        if (iterator_->key_view() == *bounds_.lower_bound) {
            iterator_->next();
        }
    }
}

std::optional<RawRow> RawScanner::next_row() {
    // Exact match query - return at most one row
    if (bounds_.is_exact_match()) {
        if (exact_match_returned_) {
            return std::nullopt;
        }
        exact_match_returned_ = true;

        if (iterator_ && iterator_->valid()) {
            std::string_view key_sv = iterator_->key_view();
            if (key_sv == *bounds_.exact_key) {
                ++stats_.keys_scanned;
                return RawRow{
                    std::string(key_sv),
                    std::string(iterator_->value_view())
                };
            }
        }
        return std::nullopt;
    }

    // Range or unbounded scan
    while (iterator_ && iterator_->valid()) {
        std::string_view key_sv = iterator_->key_view();

        // Check for early termination
        if (bounds_.is_past_upper_bound(key_sv)) {
            return std::nullopt;
        }

        ++stats_.keys_scanned;

        // Check if within bounds (handles lower bound)
        if (bounds_.is_within_bounds(key_sv)) {
            RawRow row{
                std::string(key_sv),
                std::string(iterator_->value_view())
            };
            iterator_->next();
            return row;
        }

        iterator_->next();
    }

    return std::nullopt;
}

void RawScanner::rescan() {
    begin_scan(bounds_);
}

void RawScanner::end_scan() {
    iterator_.reset();
}

} // namespace level_pivot
