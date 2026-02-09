#pragma once

#include "level_pivot/key_pattern.hpp"
#include "level_pivot/key_parser.hpp"
#include <string>
#include <string_view>
#include <vector>
#include <optional>
#include <unordered_map>
#include <unordered_set>

namespace level_pivot {

/**
 * PostgreSQL type identifiers
 */
enum class PgType {
    TEXT,       // Default
    INTEGER,
    BIGINT,
    BOOLEAN,
    NUMERIC,
    TIMESTAMP,
    TIMESTAMPTZ,
    DATE,
    JSONB,
    BYTEA
};

/**
 * Convert a PostgreSQL type OID to PgType
 */
PgType pg_type_from_oid(unsigned int oid);

/**
 * Get the name of a PgType
 */
const char* pg_type_name(PgType type);

/**
 * Column definition in a projection
 */
struct ColumnDef {
    std::string name;      // Column name (matches capture name or attr value)
    PgType type;           // PostgreSQL type
    int attnum;            // PostgreSQL attribute number (1-based)
    bool is_identity;      // True if this is a capture column (identity)
                           // False if this is an attr column (pivoted)

    bool operator==(const ColumnDef& other) const {
        return name == other.name && type == other.type &&
               attnum == other.attnum && is_identity == other.is_identity;
    }
};

/**
 * Table projection definition
 *
 * Defines how a foreign table maps to LevelDB data:
 *   - Pattern: How keys are structured
 *   - Identity columns: Map to {name} captures in pattern
 *   - Attr columns: Map to {attr} values (pivoted)
 */
class Projection {
public:
    /**
     * Create a projection from a key pattern and column definitions
     *
     * @param pattern The key pattern
     * @param columns Column definitions (identity + attr columns)
     * @throws std::invalid_argument if columns don't match pattern
     */
    Projection(const KeyPattern& pattern, std::vector<ColumnDef> columns);

    /**
     * Get the key parser
     */
    const KeyParser& parser() const { return parser_; }

    /**
     * Get all columns
     */
    const std::vector<ColumnDef>& columns() const { return columns_; }

    /**
     * Get identity columns (in pattern order)
     */
    const std::vector<const ColumnDef*>& identity_columns() const { return identity_columns_; }

    /**
     * Get attr columns (non-identity columns)
     */
    const std::vector<const ColumnDef*>& attr_columns() const { return attr_columns_; }

    /**
     * Get a column by name
     */
    const ColumnDef* column(const std::string& name) const;

    /**
     * Get a column by attribute number
     */
    const ColumnDef* column_by_attnum(int attnum) const;

    /**
     * Get the set of attr names this projection expects
     */
    const std::unordered_set<std::string>& attr_names() const { return attr_names_; }

    /**
     * Check if an attr name is in this projection
     */
    bool has_attr(std::string_view attr_name) const {
        // unordered_set::count doesn't support heterogeneous lookup in C++17,
        // so we need to construct a temporary string for the lookup
        return attr_names_.count(std::string(attr_name)) > 0;
    }

    /**
     * Get the column index for an identity column by its capture name
     * Returns -1 if not found
     */
    int identity_column_index(const std::string& capture_name) const;

    /**
     * Get the column index for an attr column by attr name
     * Returns -1 if not found
     */
    int attr_column_index(const std::string& attr_name) const;

    /**
     * Get the number of columns
     */
    size_t column_count() const { return columns_.size(); }

    /**
     * Get the identity value index for a column (O(1) lookup)
     *
     * For identity columns, returns the index into PivotRow::identity_values
     * For non-identity columns, returns -1
     *
     * @param column_index Index into columns()
     * @return Index into identity_values, or -1 if not identity
     */
    int column_to_identity_index(size_t column_index) const {
        if (column_index < column_to_identity_index_.size()) {
            return column_to_identity_index_[column_index];
        }
        return -1;
    }

private:
    KeyParser parser_;
    std::vector<ColumnDef> columns_;
    std::vector<const ColumnDef*> identity_columns_;
    std::vector<const ColumnDef*> attr_columns_;
    std::unordered_map<std::string, size_t> column_name_index_;
    std::unordered_map<int, size_t> column_attnum_index_;
    std::unordered_set<std::string> attr_names_;
    std::vector<int> column_to_identity_index_;  // -1 for non-identity columns

    // O(1) lookup maps for identity and attr column indices
    std::unordered_map<std::string, int> identity_name_to_index_;
    std::unordered_map<std::string, int> attr_name_to_index_;

    void build_indexes();
    void validate() const;
};

} // namespace level_pivot
