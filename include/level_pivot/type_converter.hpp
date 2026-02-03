#pragma once

#include "level_pivot/projection.hpp"
#include <string>
#include <optional>
#include <cstdint>

// Forward declarations for PostgreSQL types
extern "C" {
    struct varlena;
    typedef uintptr_t Datum;
}

namespace level_pivot {

/**
 * Exception thrown on type conversion errors
 */
class TypeConversionError : public std::runtime_error {
public:
    TypeConversionError(const std::string& value, PgType target_type,
                        const std::string& reason);

    const std::string& value() const { return value_; }
    PgType target_type() const { return target_type_; }

private:
    std::string value_;
    PgType target_type_;
};

/**
 * Converts between LevelDB string values and PostgreSQL Datum types
 *
 * Handles conversion for:
 *   - TEXT: passthrough
 *   - INTEGER/BIGINT: parse as number
 *   - BOOLEAN: "true"/"false"/"1"/"0"
 *   - TIMESTAMP/TIMESTAMPTZ: ISO 8601 parsing
 *   - DATE: ISO 8601 date parsing
 *   - NUMERIC: decimal parsing
 *   - JSONB: JSON string parsing
 *   - BYTEA: hex or escape format
 */
class TypeConverter {
public:
    /**
     * Convert a string value to a PostgreSQL Datum
     *
     * @param value The string value from LevelDB
     * @param type Target PostgreSQL type
     * @param is_null Output parameter set to true if value represents NULL
     * @return The converted Datum (undefined if is_null is true)
     * @throws TypeConversionError if conversion fails
     */
    static Datum string_to_datum(const std::string& value, PgType type, bool& is_null);

    /**
     * Convert a PostgreSQL Datum to a string for storage in LevelDB
     *
     * @param datum The PostgreSQL Datum
     * @param type The Datum's type
     * @param is_null If true, datum is null and function returns empty string
     * @return String representation for LevelDB storage
     */
    static std::string datum_to_string(Datum datum, PgType type, bool is_null);

    /**
     * Parse an integer from a string
     *
     * @param value String representation
     * @return Parsed integer
     * @throws TypeConversionError on parse failure
     */
    static int32_t parse_int32(const std::string& value);

    /**
     * Parse a 64-bit integer from a string
     */
    static int64_t parse_int64(const std::string& value);

    /**
     * Parse a boolean from a string
     * Accepts: "true", "false", "t", "f", "1", "0", "yes", "no", "on", "off"
     */
    static bool parse_bool(const std::string& value);

    /**
     * Parse an ISO 8601 timestamp
     * Format: YYYY-MM-DD HH:MM:SS[.microseconds]
     * Returns microseconds since PostgreSQL epoch (2000-01-01)
     */
    static int64_t parse_timestamp(const std::string& value);

    /**
     * Parse an ISO 8601 date
     * Format: YYYY-MM-DD
     * Returns days since PostgreSQL epoch (2000-01-01)
     */
    static int32_t parse_date(const std::string& value);

    /**
     * Format a timestamp to ISO 8601
     */
    static std::string format_timestamp(int64_t pg_timestamp);

    /**
     * Format a date to ISO 8601
     */
    static std::string format_date(int32_t pg_date);

    /**
     * Check if a string represents a null value
     * Empty strings are not considered NULL by default
     */
    static bool is_null_string(const std::string& value);
};

} // namespace level_pivot
