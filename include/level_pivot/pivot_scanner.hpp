#pragma once

#include "level_pivot/projection.hpp"
#include "level_pivot/connection_manager.hpp"
#include "level_pivot/type_converter.hpp"
#include <vector>
#include <optional>
#include <memory>
#include <unordered_map>

// Forward declarations for PostgreSQL types
extern "C" {
    typedef uintptr_t Datum;
}

namespace level_pivot {

/**
 * A single row of pivoted data
 */
struct PivotRow {
    std::vector<std::string> identity_values;  // Values for identity columns
    std::unordered_map<std::string, std::string> attr_values;  // attr -> value

    bool operator==(const PivotRow& other) const {
        return identity_values == other.identity_values &&
               attr_values == other.attr_values;
    }
};

/**
 * Scan state for iterating over pivoted rows
 */
class PivotScanner {
public:
    /**
     * Create a scanner for the given projection and connection
     *
     * @param projection Table projection
     * @param connection LevelDB connection
     */
    PivotScanner(const Projection& projection,
                 std::shared_ptr<LevelDBConnection> connection);

    /**
     * Begin scanning from the start of matching keys
     */
    void begin_scan();

    /**
     * Begin scanning with a partial prefix filter
     *
     * @param prefix_values Values for some identity columns (in order)
     */
    void begin_scan(const std::vector<std::string>& prefix_values);

    /**
     * Fetch the next pivoted row
     *
     * @return The next row, or std::nullopt if no more rows
     */
    std::optional<PivotRow> next_row();

    /**
     * Re-scan from the beginning (same filter)
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
        size_t rows_returned = 0;
        size_t keys_skipped = 0;  // Keys that didn't match pattern
    };

    const Stats& stats() const { return stats_; }

private:
    const Projection& projection_;
    std::shared_ptr<LevelDBConnection> connection_;
    std::unique_ptr<LevelDBIterator> iterator_;
    std::string prefix_;
    Stats stats_;

    // Current row accumulation state
    std::optional<std::vector<std::string>> current_identity_;
    std::unordered_map<std::string, std::string> current_attrs_;

    bool is_within_prefix(const std::string& key) const;
    bool is_within_prefix_view(std::string_view key) const;
    void accumulate_row();
    std::optional<PivotRow> emit_current_row();

    // Helper to convert string_view identity to owned strings
    static std::vector<std::string> materialize_identity(
        const std::vector<std::string_view>& views);

    // Helper to compare owned identity with parsed views
    static bool identity_matches(
        const std::vector<std::string>& identity,
        const std::vector<std::string_view>& views);
};

/**
 * Helpers for building Datum arrays from PivotRow
 */
class DatumBuilder {
public:
    /**
     * Build PostgreSQL Datum and null arrays from a PivotRow
     *
     * @param row The pivoted row data
     * @param projection The projection defining columns and types
     * @param values Output array of Datum values (must be preallocated)
     * @param nulls Output array of null flags (must be preallocated)
     */
    static void build_datums(const PivotRow& row,
                             const Projection& projection,
                             Datum* values,
                             bool* nulls);
};

} // namespace level_pivot
