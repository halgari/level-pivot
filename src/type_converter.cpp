/**
 * type_converter.cpp - Bidirectional PostgreSQL Datum <-> string conversion
 *
 * LevelDB stores everything as strings, so we need type conversion:
 *   - string_to_datum: Parses strings from LevelDB into PostgreSQL values
 *   - datum_to_string: Serializes PostgreSQL values for LevelDB storage
 *
 * Supported types: TEXT, INTEGER, BIGINT, BOOLEAN, NUMERIC, TIMESTAMP,
 * TIMESTAMPTZ, DATE, JSONB, BYTEA. Unknown types fall back to TEXT.
 *
 * Memory: All allocations use palloc (PostgreSQL's memory allocator) so
 * they're automatically freed when the current memory context is reset.
 */

// PostgreSQL headers must come first for Windows compatibility
extern "C" {
#include "postgres.h"
#include "fmgr.h"
#include "utils/builtins.h"
#include "utils/numeric.h"
#include "utils/timestamp.h"
#include "utils/date.h"
#include "utils/jsonb.h"
}

#include "level_pivot/type_converter.hpp"

#include <charconv>

namespace level_pivot {

TypeConversionError::TypeConversionError(const std::string& value,
                                         PgType target_type,
                                         const std::string& reason)
    : std::runtime_error("Cannot convert '" + value + "' to " +
                         pg_type_name(target_type) + ": " + reason),
      value_(value),
      target_type_(target_type) {}

/**
 * Converts a string from LevelDB to a PostgreSQL Datum.
 *
 * Uses PostgreSQL's input functions (int4in, boolin, etc.) which handle
 * the same formats as SQL literals. This means LevelDB values can use
 * familiar formats like "true"/"false" for booleans, ISO dates, etc.
 *
 * BYTEA uses hex format with optional \x prefix for binary data.
 */
Datum TypeConverter::string_to_datum(const std::string& value, PgType type,
                                     bool& is_null) {
    is_null = false;

    // Check for explicit NULL marker
    if (is_null_string(value)) {
        is_null = true;
        return (Datum)0;
    }

    switch (type) {
        case PgType::TEXT: {
            // cstring_to_text_with_len handles non-null-terminated strings correctly
            text* result = cstring_to_text_with_len(value.c_str(), value.size());
            return PointerGetDatum(result);
        }

        case PgType::INTEGER: {
            Datum result = DirectFunctionCall1(int4in, CStringGetDatum(value.c_str()));
            return result;
        }

        case PgType::BIGINT: {
            Datum result = DirectFunctionCall1(int8in, CStringGetDatum(value.c_str()));
            return result;
        }

        case PgType::BOOLEAN: {
            Datum result = DirectFunctionCall1(boolin, CStringGetDatum(value.c_str()));
            return result;
        }

        case PgType::NUMERIC: {
            Datum result = DirectFunctionCall3(numeric_in,
                CStringGetDatum(value.c_str()),
                ObjectIdGetDatum(InvalidOid),
                Int32GetDatum(-1));
            return result;
        }

        case PgType::TIMESTAMP: {
            Datum result = DirectFunctionCall3(timestamp_in,
                CStringGetDatum(value.c_str()),
                ObjectIdGetDatum(InvalidOid),
                Int32GetDatum(-1));
            return result;
        }

        case PgType::TIMESTAMPTZ: {
            Datum result = DirectFunctionCall3(timestamptz_in,
                CStringGetDatum(value.c_str()),
                ObjectIdGetDatum(InvalidOid),
                Int32GetDatum(-1));
            return result;
        }

        case PgType::DATE: {
            Datum result = DirectFunctionCall1(date_in, CStringGetDatum(value.c_str()));
            return result;
        }

        case PgType::JSONB: {
            Datum result = DirectFunctionCall1(jsonb_in,
                CStringGetDatum(value.c_str()));
            return result;
        }

        case PgType::BYTEA: {
            // BYTEA uses hex encoding for safe string representation.
            // Accept with or without \x prefix for flexibility.
            std::string hex_value = value;
            if (hex_value.substr(0, 2) == "\\x") {
                hex_value = hex_value.substr(2);
            }

            // Each byte is 2 hex chars; allocate PostgreSQL varlena structure
            size_t len = hex_value.size() / 2;
            bytea* result = (bytea*)palloc(VARHDRSZ + len);
            SET_VARSIZE(result, VARHDRSZ + len);

            // Decode hex pairs to bytes using C++17 from_chars for speed
            unsigned char* data = (unsigned char*)VARDATA(result);
            for (size_t i = 0; i < len; ++i) {
                unsigned int byte = 0;
                auto [ptr, ec] = std::from_chars(hex_value.data() + i * 2,
                                                  hex_value.data() + i * 2 + 2,
                                                  byte, 16);
                if (ec != std::errc{}) {
                    throw TypeConversionError(value, PgType::BYTEA, "invalid hex format");
                }
                data[i] = static_cast<unsigned char>(byte);
            }
            return PointerGetDatum(result);
        }
    }

    // Should never reach here
    is_null = true;
    return (Datum)0;
}

/**
 * Converts a PostgreSQL Datum to a string for LevelDB storage.
 *
 * Uses PostgreSQL's output functions for consistent formatting.
 * The string representations are designed to round-trip correctly
 * through string_to_datum.
 *
 * Note: pfree() is called for temporary strings returned by output
 * functions to avoid memory leaks during bulk operations.
 */
std::string TypeConverter::datum_to_string(Datum datum, PgType type, bool is_null) {
    if (is_null) {
        return "";
    }

    switch (type) {
        case PgType::TEXT: {
            // Direct access to varlena data avoids unnecessary copying
            text* txt = DatumGetTextPP(datum);
            return std::string(VARDATA_ANY(txt), VARSIZE_ANY_EXHDR(txt));
        }

        case PgType::INTEGER: {
            char* str = DatumGetCString(DirectFunctionCall1(int4out, datum));
            std::string result(str);
            pfree(str);
            return result;
        }

        case PgType::BIGINT: {
            char* str = DatumGetCString(DirectFunctionCall1(int8out, datum));
            std::string result(str);
            pfree(str);
            return result;
        }

        case PgType::BOOLEAN: {
            char* str = DatumGetCString(DirectFunctionCall1(boolout, datum));
            std::string result(str);
            pfree(str);
            return result;
        }

        case PgType::NUMERIC: {
            char* str = DatumGetCString(DirectFunctionCall1(numeric_out, datum));
            std::string result(str);
            pfree(str);
            return result;
        }

        case PgType::TIMESTAMP: {
            char* str = DatumGetCString(DirectFunctionCall1(timestamp_out, datum));
            std::string result(str);
            pfree(str);
            return result;
        }

        case PgType::TIMESTAMPTZ: {
            char* str = DatumGetCString(DirectFunctionCall1(timestamptz_out, datum));
            std::string result(str);
            pfree(str);
            return result;
        }

        case PgType::DATE: {
            char* str = DatumGetCString(DirectFunctionCall1(date_out, datum));
            std::string result(str);
            pfree(str);
            return result;
        }

        case PgType::JSONB: {
            char* str = DatumGetCString(DirectFunctionCall1(jsonb_out, datum));
            std::string result(str);
            pfree(str);
            return result;
        }

        case PgType::BYTEA: {
            static constexpr char hex_chars[] = "0123456789abcdef";
            bytea* data = DatumGetByteaPP(datum);
            size_t len = VARSIZE_ANY_EXHDR(data);
            unsigned char* bytes = (unsigned char*)VARDATA_ANY(data);

            std::string result;
            result.reserve(2 + len * 2);
            result.append("\\x");
            for (size_t i = 0; i < len; ++i) {
                result += hex_chars[bytes[i] >> 4];
                result += hex_chars[bytes[i] & 0x0f];
            }
            return result;
        }
    }

    return "";
}

/**
 * Determines if a string should be treated as SQL NULL.
 *
 * Currently always returns false because in our model, NULL is
 * represented by the absence of a key in LevelDB, not by a special
 * value. If a key exists, it has a non-NULL value.
 */
bool TypeConverter::is_null_string(const std::string& value) {
    (void)value;
    return false;
}

} // namespace level_pivot
