/**
 * raw_scanner.cpp - Direct key-value access without pivot transformation
 *
 * RawScanner provides simple key-value iteration for "raw" table mode,
 * bypassing the pattern matching and pivoting logic. Use cases:
 *   - Debugging: inspect actual LevelDB contents
 *   - Migration: bulk export/import of key-value data
 *   - Generic access: when pivot semantics aren't needed
 *
 * Supports efficient range queries via RawScanBounds:
 *   - exact_key: single key lookup (WHERE key = 'foo')
 *   - lower/upper bounds: range scans (WHERE key >= 'a' AND key < 'b')
 *
 * LevelDB's sorted key order makes range scans efficient - we seek
 * to the start and iterate until we exit the range.
 */

#include "level_pivot/raw_scanner.hpp"

namespace level_pivot {

// RawScanBounds implementation

/**
 * Determines where to position the LevelDB iterator at scan start.
 * For exact match, seek directly to that key.
 * For range scans, seek to lower bound (or beginning if unbounded).
 */
std::string RawScanBounds::seek_start() const {
    if (exact_key.has_value()) {
        return *exact_key;
    }
    if (lower_bound.has_value()) {
        return *lower_bound;
    }
    return "";  // Start from beginning
}

/**
 * Tests if a key falls within the specified bounds.
 * Handles inclusive vs exclusive bounds for both lower and upper.
 * Uses string comparison since LevelDB keys are lexicographically ordered.
 */
bool RawScanBounds::is_within_bounds(std::string_view key) const {
    // Exact match case - simplest test
    if (exact_key.has_value()) {
        return key == *exact_key;
    }

    // Lower bound: key must be >= (inclusive) or > (exclusive) lower
    if (lower_bound.has_value()) {
        int cmp = key.compare(*lower_bound);
        if (lower_inclusive) {
            if (cmp < 0) return false;
        } else {
            if (cmp <= 0) return false;
        }
    }

    // Upper bound: key must be <= (inclusive) or < (exclusive) upper
    if (upper_bound.has_value()) {
        int cmp = key.compare(*upper_bound);
        if (upper_inclusive) {
            if (cmp > 0) return false;
        } else {
            if (cmp >= 0) return false;
        }
    }

    return true;
}

/**
 * Checks if iteration should stop because we've passed the upper bound.
 * This is an optimization - since LevelDB keys are sorted, once we
 * see a key past the upper bound, all subsequent keys will also be
 * past it, so we can stop early.
 */
bool RawScanBounds::is_past_upper_bound(std::string_view key) const {
    if (!upper_bound.has_value()) {
        return false;
    }

    if (exact_key.has_value()) {
        return key > *exact_key;
    }

    int cmp = key.compare(*upper_bound);
    if (upper_inclusive) {
        return cmp > 0;
    } else {
        return cmp >= 0;
    }
}

// RawScanner implementation

RawScanner::RawScanner(std::shared_ptr<LevelDBConnection> connection)
    : connection_(std::move(connection)) {}

/**
 * Initializes iteration with optional bounds from WHERE clause pushdown.
 *
 * The seek operation is O(log N) in LevelDB, then iteration is O(1) per key.
 * This makes range queries efficient even on large databases.
 *
 * For exclusive lower bounds (key > X), we seek to X then skip it if found.
 */
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

    // Handle exclusive lower bound: if we landed exactly on it, skip
    if (iterator_->valid() && bounds_.lower_bound.has_value() &&
        !bounds_.lower_inclusive) {
        if (iterator_->key_view() == *bounds_.lower_bound) {
            iterator_->next();
        }
    }
}

/**
 * Returns the next key-value pair, or nullopt when exhausted.
 *
 * Exact match queries (WHERE key = 'X') return at most one row.
 * Range queries iterate until upper bound is exceeded.
 *
 * We copy key and value into RawRow because LevelDB's iterator
 * invalidates previous data on next().
 */
std::optional<RawRow> RawScanner::next_row() {
    // Exact match: O(log N) seek + single key check
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

    // Range or unbounded scan: iterate until exhausted or past upper bound
    while (iterator_ && iterator_->valid()) {
        std::string_view key_sv = iterator_->key_view();

        // Early termination: sorted keys mean we're done
        if (bounds_.is_past_upper_bound(key_sv)) {
            return std::nullopt;
        }

        ++stats_.keys_scanned;

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
