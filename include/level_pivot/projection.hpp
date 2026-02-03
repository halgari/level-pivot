#pragma once

#include "level_pivot/key_pattern.hpp"
#include "level_pivot/key_parser.hpp"
#include <string>
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
    bool has_attr(const std::string& attr_name) const {
        return attr_names_.count(attr_name) > 0;
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

private:
    KeyParser parser_;
    std::vector<ColumnDef> columns_;
    std::vector<const ColumnDef*> identity_columns_;
    std::vector<const ColumnDef*> attr_columns_;
    std::unordered_map<std::string, size_t> column_name_index_;
    std::unordered_map<int, size_t> column_attnum_index_;
    std::unordered_set<std::string> attr_names_;

    void build_indexes();
    void validate() const;
};

/**
 * Builder for creating Projection objects
 */
class ProjectionBuilder {
public:
    explicit ProjectionBuilder(const std::string& key_pattern);
    explicit ProjectionBuilder(const KeyPattern& pattern);

    /**
     * Add an identity column
     */
    ProjectionBuilder& add_identity(const std::string& name, PgType type, int attnum);

    /**
     * Add an attr column
     */
    ProjectionBuilder& add_attr(const std::string& attr_name, PgType type, int attnum);

    /**
     * Build the projection
     */
    Projection build() const;

private:
    KeyPattern pattern_;
    std::vector<ColumnDef> columns_;
};

} // namespace level_pivot
