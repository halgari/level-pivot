#pragma once

#include "level_pivot/projection.hpp"
#include <string>

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
 * Uses PostgreSQL's built-in type input/output functions for consistent
 * behavior with the database's native parsing and formatting.
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
     * Check if a string represents a null value
     * Empty strings are not considered NULL by default
     */
    static bool is_null_string(const std::string& value);
};

} // namespace level_pivot
