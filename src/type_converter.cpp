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
            // Create PostgreSQL text datum
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
            // Assume hex format (\\x prefix optional)
            std::string hex_value = value;
            if (hex_value.substr(0, 2) == "\\x") {
                hex_value = hex_value.substr(2);
            }

            size_t len = hex_value.size() / 2;
            bytea* result = (bytea*)palloc(VARHDRSZ + len);
            SET_VARSIZE(result, VARHDRSZ + len);

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

std::string TypeConverter::datum_to_string(Datum datum, PgType type, bool is_null) {
    if (is_null) {
        return "";
    }

    switch (type) {
        case PgType::TEXT: {
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

bool TypeConverter::is_null_string(const std::string& value) {
    // We don't treat empty strings as NULL
    // Only explicit NULL markers would return true here
    // For now, we return false - LevelDB values are never NULL unless
    // the key doesn't exist
    (void)value;
    return false;
}

} // namespace level_pivot
