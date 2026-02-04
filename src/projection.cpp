#include "level_pivot/projection.hpp"
#include <algorithm>
#include <stdexcept>

namespace level_pivot {

// PostgreSQL type OIDs (from pg_type.h)
// These are stable across PostgreSQL versions
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

void Projection::build_indexes() {
    identity_columns_.clear();
    attr_columns_.clear();
    column_name_index_.clear();
    column_attnum_index_.clear();
    attr_names_.clear();
    column_to_identity_index_.clear();
    column_to_identity_index_.resize(columns_.size(), -1);

    // Build mapping from capture name to identity index
    const auto& capture_names = parser_.pattern().capture_names();
    std::unordered_map<std::string, int> capture_to_index;
    for (size_t i = 0; i < capture_names.size(); ++i) {
        capture_to_index[capture_names[i]] = static_cast<int>(i);
    }

    for (size_t i = 0; i < columns_.size(); ++i) {
        const auto& col = columns_[i];

        column_name_index_[col.name] = i;
        column_attnum_index_[col.attnum] = i;

        if (col.is_identity) {
            identity_columns_.push_back(&columns_[i]);
            // Map column index to identity value index
            auto it = capture_to_index.find(col.name);
            if (it != capture_to_index.end()) {
                column_to_identity_index_[i] = it->second;
            }
        } else {
            attr_columns_.push_back(&columns_[i]);
            attr_names_.insert(col.name);
        }
    }
}

void Projection::validate() const {
    const auto& pattern = parser_.pattern();

    // Check that we have the right number of identity columns
    if (identity_columns_.size() != pattern.capture_count()) {
        throw std::invalid_argument(
            "Pattern has " + std::to_string(pattern.capture_count()) +
            " capture segments but projection has " +
            std::to_string(identity_columns_.size()) + " identity columns");
    }

    // Check that identity column names match capture names
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

    // Check for duplicate column names
    std::unordered_set<std::string> seen_names;
    for (const auto& col : columns_) {
        if (seen_names.count(col.name)) {
            throw std::invalid_argument("Duplicate column name: " + col.name);
        }
        seen_names.insert(col.name);
    }

    // Check for duplicate attnums
    std::unordered_set<int> seen_attnums;
    for (const auto& col : columns_) {
        if (seen_attnums.count(col.attnum)) {
            throw std::invalid_argument(
                "Duplicate attnum: " + std::to_string(col.attnum));
        }
        seen_attnums.insert(col.attnum);
    }

    // Must have at least one attr column
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
    for (size_t i = 0; i < identity_columns_.size(); ++i) {
        if (identity_columns_[i]->name == capture_name) {
            return static_cast<int>(i);
        }
    }
    return -1;
}

int Projection::attr_column_index(const std::string& attr_name) const {
    for (size_t i = 0; i < attr_columns_.size(); ++i) {
        if (attr_columns_[i]->name == attr_name) {
            return static_cast<int>(i);
        }
    }
    return -1;
}

} // namespace level_pivot
