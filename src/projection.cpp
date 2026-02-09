/**
 * projection.cpp - Maps PostgreSQL table columns to key pattern segments
 *
 * A Projection bridges the gap between a foreign table's column definitions
 * and the key pattern structure. For a table with columns (group, id, name, email)
 * and pattern "users##{group}##{id}##{attr}":
 *   - "group" and "id" are identity columns (they appear in the key pattern)
 *   - "name" and "email" are attr columns (their names become {attr} values)
 *
 * The projection builds O(1) lookup maps for efficient access during scanning
 * and writing, since these operations happen for every row processed.
 */

#include "level_pivot/projection.hpp"
#include <algorithm>
#include <stdexcept>

namespace level_pivot {

/**
 * PostgreSQL type OIDs from pg_type.h. These are stable across PostgreSQL
 * versions so we can hardcode them instead of doing syscache lookups.
 * This keeps the core library independent of PostgreSQL headers.
 */
namespace pg_oid {
    constexpr unsigned int BOOLOID = 16;
    constexpr unsigned int BYTEAOID = 17;
    constexpr unsigned int INT4OID = 23;
    constexpr unsigned int INT8OID = 20;
    constexpr unsigned int TEXTOID = 25;
    constexpr unsigned int NUMERICOID = 1700;
    constexpr unsigned int TIMESTAMPOID = 1114;
    constexpr unsigned int TIMESTAMPTZOID = 1184;
    constexpr unsigned int DATEOID = 1082;
    constexpr unsigned int JSONBOID = 3802;
    constexpr unsigned int VARCHAROID = 1043;
    constexpr unsigned int BPCHAROID = 1042;
}

/**
 * Maps PostgreSQL type OIDs to our PgType enum. Unknown types default to TEXT
 * since LevelDB stores everything as strings anyway - we can always round-trip
 * through TEXT representation.
 */
PgType pg_type_from_oid(unsigned int oid) {
    switch (oid) {
        case pg_oid::BOOLOID:
            return PgType::BOOLEAN;
        case pg_oid::BYTEAOID:
            return PgType::BYTEA;
        case pg_oid::INT4OID:
            return PgType::INTEGER;
        case pg_oid::INT8OID:
            return PgType::BIGINT;
        case pg_oid::TEXTOID:
        case pg_oid::VARCHAROID:
        case pg_oid::BPCHAROID:
            return PgType::TEXT;
        case pg_oid::NUMERICOID:
            return PgType::NUMERIC;
        case pg_oid::TIMESTAMPOID:
            return PgType::TIMESTAMP;
        case pg_oid::TIMESTAMPTZOID:
            return PgType::TIMESTAMPTZ;
        case pg_oid::DATEOID:
            return PgType::DATE;
        case pg_oid::JSONBOID:
            return PgType::JSONB;
        default:
            return PgType::TEXT;  // Default to TEXT for unknown types
    }
}

const char* pg_type_name(PgType type) {
    switch (type) {
        case PgType::TEXT: return "TEXT";
        case PgType::INTEGER: return "INTEGER";
        case PgType::BIGINT: return "BIGINT";
        case PgType::BOOLEAN: return "BOOLEAN";
        case PgType::NUMERIC: return "NUMERIC";
        case PgType::TIMESTAMP: return "TIMESTAMP";
        case PgType::TIMESTAMPTZ: return "TIMESTAMPTZ";
        case PgType::DATE: return "DATE";
        case PgType::JSONB: return "JSONB";
        case PgType::BYTEA: return "BYTEA";
    }
    return "UNKNOWN";
}

Projection::Projection(const KeyPattern& pattern, std::vector<ColumnDef> columns)
    : parser_(pattern), columns_(std::move(columns)) {
    build_indexes();
    validate();
}

/**
 * Builds lookup indexes for O(1) column access during row processing.
 * We need fast lookups because:
 *   - column_to_identity_index_: Maps column position to identity value index
 *     (used by DatumBuilder to fill identity columns from PivotRow)
 *   - identity_name_to_index_: Maps capture name to column index
 *   - attr_name_to_index_: Maps attr name to column index
 *   - attr_names_: Set for fast "is this an attr?" checks during scanning
 */
void Projection::build_indexes() {
    identity_columns_.clear();
    attr_columns_.clear();
    column_name_index_.clear();
    column_attnum_index_.clear();
    attr_names_.clear();
    column_to_identity_index_.clear();
    column_to_identity_index_.resize(columns_.size(), -1);
    identity_name_to_index_.clear();
    attr_name_to_index_.clear();

    // Map capture names to their index in the parsed key's capture_values array.
    // This ordering comes from the pattern and must match parse results.
    const auto& capture_names = parser_.pattern().capture_names();
    std::unordered_map<std::string, int> capture_to_index;
    for (size_t i = 0; i < capture_names.size(); ++i) {
        capture_to_index[capture_names[i]] = static_cast<int>(i);
    }

    for (size_t i = 0; i < columns_.size(); ++i) {
        const auto& col = columns_[i];

        // Every column needs name and attnum lookups
        column_name_index_[col.name] = i;
        column_attnum_index_[col.attnum] = i;

        if (col.is_identity) {
            // Identity columns map to capture values in parsed keys
            identity_name_to_index_[col.name] = static_cast<int>(identity_columns_.size());
            identity_columns_.push_back(&columns_[i]);

            // Pre-compute which identity_values index this column maps to.
            // This allows O(1) lookup in DatumBuilder instead of name searches.
            auto it = capture_to_index.find(col.name);
            if (it != capture_to_index.end()) {
                column_to_identity_index_[i] = it->second;
            }
        } else {
            // Attr columns have their name used as the {attr} value in keys
            attr_name_to_index_[col.name] = static_cast<int>(attr_columns_.size());
            attr_columns_.push_back(&columns_[i]);
            attr_names_.insert(col.name);
        }
    }
}

/**
 * Validates that the projection is consistent with the pattern:
 *   - Number of identity columns must match pattern's capture count
 *   - Identity column names must match capture names (order doesn't matter)
 *   - No duplicate column names or attnums
 *   - At least one attr column (otherwise nothing to pivot)
 */
void Projection::validate() const {
    const auto& pattern = parser_.pattern();

    // Identity column count must match capture count in pattern
    if (identity_columns_.size() != pattern.capture_count()) {
        throw std::invalid_argument(
            "Pattern has " + std::to_string(pattern.capture_count()) +
            " capture segments but projection has " +
            std::to_string(identity_columns_.size()) + " identity columns");
    }

    // Every identity column must correspond to a capture in the pattern
    const auto& capture_names = pattern.capture_names();
    for (size_t i = 0; i < identity_columns_.size(); ++i) {
        bool found = false;
        for (const auto& cap_name : capture_names) {
            if (identity_columns_[i]->name == cap_name) {
                found = true;
                break;
            }
        }
        if (!found) {
            throw std::invalid_argument(
                "Identity column '" + identity_columns_[i]->name +
                "' does not match any capture in pattern");
        }
    }

    // Column names must be unique
    std::unordered_set<std::string> seen_names;
    for (const auto& col : columns_) {
        if (seen_names.count(col.name)) {
            throw std::invalid_argument("Duplicate column name: " + col.name);
        }
        seen_names.insert(col.name);
    }

    // PostgreSQL attnums must be unique
    std::unordered_set<int> seen_attnums;
    for (const auto& col : columns_) {
        if (seen_attnums.count(col.attnum)) {
            throw std::invalid_argument(
                "Duplicate attnum: " + std::to_string(col.attnum));
        }
        seen_attnums.insert(col.attnum);
    }

    // Must have at least one attr column to pivot
    if (attr_columns_.empty()) {
        throw std::invalid_argument("Projection must have at least one attr column");
    }
}

const ColumnDef* Projection::column(const std::string& name) const {
    auto it = column_name_index_.find(name);
    if (it == column_name_index_.end()) {
        return nullptr;
    }
    return &columns_[it->second];
}

const ColumnDef* Projection::column_by_attnum(int attnum) const {
    auto it = column_attnum_index_.find(attnum);
    if (it == column_attnum_index_.end()) {
        return nullptr;
    }
    return &columns_[it->second];
}

int Projection::identity_column_index(const std::string& capture_name) const {
    auto it = identity_name_to_index_.find(capture_name);
    return it != identity_name_to_index_.end() ? it->second : -1;
}

int Projection::attr_column_index(const std::string& attr_name) const {
    auto it = attr_name_to_index_.find(attr_name);
    return it != attr_name_to_index_.end() ? it->second : -1;
}

} // namespace level_pivot
