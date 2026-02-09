#pragma once

#include "level_pivot/connection_manager.hpp"
#include <vector>
#include <optional>
#include <memory>
#include <string>
#include <string_view>

namespace level_pivot {

/**
 * Bounds for a raw LevelDB scan
 *
 * Supports exact matches, range queries, and unbounded scans.
 */
struct RawScanBounds {
    std::optional<std::string> exact_key;     // key = 'x'
    std::optional<std::string> lower_bound;   // key > 'x' or key >= 'x'
    std::optional<std::string> upper_bound;   // key < 'y' or key <= 'y'
    bool lower_inclusive = true;
    bool upper_inclusive = false;

    /**
     * Get the key to seek to when starting the scan
     */
    std::string seek_start() const;

    /**
     * Check if a key is within the bounds
     */
    bool is_within_bounds(std::string_view key) const;

    /**
     * Check if a key is past the upper bound (used for early termination)
     */
    bool is_past_upper_bound(std::string_view key) const;

    /**
     * Check if this is an exact match query
     */
    bool is_exact_match() const { return exact_key.has_value(); }

    /**
     * Check if this is an unbounded scan
     */
    bool is_unbounded() const {
        return !exact_key.has_value() && !lower_bound.has_value() && !upper_bound.has_value();
    }
};

/**
 * A single raw key-value row
 */
struct RawRow {
    std::string key;
    std::string value;
};

/**
 * Scanner for raw (non-pivoted) LevelDB access
 *
 * Returns key-value pairs directly without any pivoting logic.
 * Supports exact matches and range queries via RawScanBounds.
 */
class RawScanner {
public:
    /**
     * Create a scanner for the given connection
     *
     * @param connection LevelDB connection
     */
    explicit RawScanner(std::shared_ptr<LevelDBConnection> connection);

    /**
     * Begin scanning with the given bounds
     *
     * @param bounds Scan bounds (exact match, range, or unbounded)
     */
    void begin_scan(const RawScanBounds& bounds);

    /**
     * Fetch the next row
     *
     * @return The next key-value pair, or std::nullopt if no more rows
     */
    std::optional<RawRow> next_row();

    /**
     * Re-scan from the beginning (same bounds)
     */
    void rescan();

    /**
     * End the scan
     */
    void end_scan();

    /**
     * Get scan statistics
     */
    struct Stats {
        size_t keys_scanned = 0;
    };

    const Stats& stats() const { return stats_; }

private:
    std::shared_ptr<LevelDBConnection> connection_;
    std::unique_ptr<LevelDBIterator> iterator_;
    RawScanBounds bounds_;
    Stats stats_;
    bool exact_match_returned_ = false;  // For single-row exact match queries
};

} // namespace level_pivot
