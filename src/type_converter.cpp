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

#include <cstring>
#include <cstdlib>
#include <cerrno>
#include <algorithm>
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
            int32_t val = parse_int32(value);
            return Int32GetDatum(val);
        }

        case PgType::BIGINT: {
            int64_t val = parse_int64(value);
            return Int64GetDatum(val);
        }

        case PgType::BOOLEAN: {
            bool val = parse_bool(value);
            return BoolGetDatum(val);
        }

        case PgType::NUMERIC: {
            // Use PostgreSQL's numeric_in function
            Datum result = DirectFunctionCall3(numeric_in,
                CStringGetDatum(value.c_str()),
                ObjectIdGetDatum(InvalidOid),
                Int32GetDatum(-1));
            return result;
        }

        case PgType::TIMESTAMP: {
            int64_t val = parse_timestamp(value);
            return TimestampGetDatum(val);
        }

        case PgType::TIMESTAMPTZ: {
            // Parse as timestamp, then convert to timestamptz
            // For simplicity, treat input as UTC
            int64_t val = parse_timestamp(value);
            return TimestampTzGetDatum(val);
        }

        case PgType::DATE: {
            int32_t val = parse_date(value);
            return DateADTGetDatum(val);
        }

        case PgType::JSONB: {
            // Use PostgreSQL's jsonb_in function
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
            return std::to_string(DatumGetInt32(datum));
        }

        case PgType::BIGINT: {
            return std::to_string(DatumGetInt64(datum));
        }

        case PgType::BOOLEAN: {
            return DatumGetBool(datum) ? "true" : "false";
        }

        case PgType::NUMERIC: {
            // Use PostgreSQL's numeric_out function
            char* str = DatumGetCString(DirectFunctionCall1(numeric_out, datum));
            std::string result(str);
            pfree(str);
            return result;
        }

        case PgType::TIMESTAMP: {
            return format_timestamp(DatumGetTimestamp(datum));
        }

        case PgType::TIMESTAMPTZ: {
            return format_timestamp(DatumGetTimestampTz(datum));
        }

        case PgType::DATE: {
            return format_date(DatumGetDateADT(datum));
        }

        case PgType::JSONB: {
            // Use PostgreSQL's jsonb_out function
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
            result = "\\x";
            for (size_t i = 0; i < len; ++i) {
                result += hex_chars[bytes[i] >> 4];
                result += hex_chars[bytes[i] & 0x0f];
            }
            return result;
        }
    }

    return "";
}

int32_t TypeConverter::parse_int32(const std::string& value) {
    // Trim whitespace
    const char* start = value.c_str();
    const char* end = start + value.size();

    while (start < end && std::isspace(*start)) ++start;
    while (end > start && std::isspace(*(end - 1))) --end;

    int32_t result;
    auto [ptr, ec] = std::from_chars(start, end, result);

    if (ec == std::errc::result_out_of_range) {
        throw TypeConversionError(value, PgType::INTEGER, "value out of range");
    }
    if (ec != std::errc{} || ptr != end) {
        throw TypeConversionError(value, PgType::INTEGER, "invalid integer format");
    }

    return result;
}

int64_t TypeConverter::parse_int64(const std::string& value) {
    const char* start = value.c_str();
    const char* end = start + value.size();

    while (start < end && std::isspace(*start)) ++start;
    while (end > start && std::isspace(*(end - 1))) --end;

    int64_t result;
    auto [ptr, ec] = std::from_chars(start, end, result);

    if (ec == std::errc::result_out_of_range) {
        throw TypeConversionError(value, PgType::BIGINT, "value out of range");
    }
    if (ec != std::errc{} || ptr != end) {
        throw TypeConversionError(value, PgType::BIGINT, "invalid integer format");
    }

    return result;
}

bool TypeConverter::parse_bool(const std::string& value) {
    std::string lower = value;
    std::transform(lower.begin(), lower.end(), lower.begin(), ::tolower);

    // Trim whitespace
    size_t start = lower.find_first_not_of(" \t\n\r");
    size_t end = lower.find_last_not_of(" \t\n\r");
    if (start == std::string::npos) {
        throw TypeConversionError(value, PgType::BOOLEAN, "empty value");
    }
    lower = lower.substr(start, end - start + 1);

    if (lower == "true" || lower == "t" || lower == "1" ||
        lower == "yes" || lower == "on") {
        return true;
    }
    if (lower == "false" || lower == "f" || lower == "0" ||
        lower == "no" || lower == "off") {
        return false;
    }

    throw TypeConversionError(value, PgType::BOOLEAN, "invalid boolean format");
}

int64_t TypeConverter::parse_timestamp(const std::string& value) {
    // Parse ISO 8601 format: YYYY-MM-DD HH:MM:SS[.microseconds]
    // or with T separator: YYYY-MM-DDTHH:MM:SS

    int year, month, day, hour = 0, min = 0, sec = 0;
    double frac_sec = 0.0;

    // Try different formats
    int n = std::sscanf(value.c_str(), "%d-%d-%d %d:%d:%d",
                        &year, &month, &day, &hour, &min, &sec);
    if (n < 3) {
        n = std::sscanf(value.c_str(), "%d-%d-%dT%d:%d:%d",
                        &year, &month, &day, &hour, &min, &sec);
    }

    if (n < 3) {
        throw TypeConversionError(value, PgType::TIMESTAMP, "invalid timestamp format");
    }

    // Check for fractional seconds
    size_t dot_pos = value.find('.');
    if (dot_pos != std::string::npos) {
        std::sscanf(value.c_str() + dot_pos, "%lf", &frac_sec);
    }

    // Validate ranges
    if (month < 1 || month > 12 || day < 1 || day > 31 ||
        hour < 0 || hour > 23 || min < 0 || min > 59 || sec < 0 || sec > 60) {
        throw TypeConversionError(value, PgType::TIMESTAMP, "datetime value out of range");
    }

    // Calculate PostgreSQL timestamp (microseconds since 2000-01-01)
    // Using Julian day calculation
    int a = (14 - month) / 12;
    int y = year + 4800 - a;
    int m = month + 12 * a - 3;
    int jd = day + (153 * m + 2) / 5 + 365 * y + y / 4 - y / 100 + y / 400 - 32045;

    // PostgreSQL epoch is 2000-01-01 = Julian day 2451545
    int64_t days_since_epoch = jd - 2451545;

    int64_t result = days_since_epoch * 86400LL * 1000000LL +
                     hour * 3600LL * 1000000LL +
                     min * 60LL * 1000000LL +
                     sec * 1000000LL +
                     static_cast<int64_t>(frac_sec * 1000000.0);

    return result;
}

int32_t TypeConverter::parse_date(const std::string& value) {
    int year, month, day;

    int n = std::sscanf(value.c_str(), "%d-%d-%d", &year, &month, &day);
    if (n != 3) {
        throw TypeConversionError(value, PgType::DATE, "invalid date format");
    }

    if (month < 1 || month > 12 || day < 1 || day > 31) {
        throw TypeConversionError(value, PgType::DATE, "date value out of range");
    }

    // Julian day calculation
    int a = (14 - month) / 12;
    int y = year + 4800 - a;
    int m = month + 12 * a - 3;
    int jd = day + (153 * m + 2) / 5 + 365 * y + y / 4 - y / 100 + y / 400 - 32045;

    // PostgreSQL date is days since 2000-01-01 = Julian day 2451545
    return jd - 2451545;
}

std::string TypeConverter::format_timestamp(int64_t pg_timestamp) {
    // Convert PostgreSQL timestamp to ISO 8601
    int64_t total_secs = pg_timestamp / 1000000LL;
    int64_t microseconds = pg_timestamp % 1000000LL;
    if (microseconds < 0) {
        microseconds += 1000000LL;
        total_secs -= 1;
    }

    // Days since 2000-01-01
    int64_t days = total_secs / 86400LL;
    int64_t time_secs = total_secs % 86400LL;
    if (time_secs < 0) {
        time_secs += 86400LL;
        days -= 1;
    }

    // Julian day
    int64_t jd = days + 2451545;

    // Convert Julian day to Gregorian
    int64_t l = jd + 68569;
    int64_t n = (4 * l) / 146097;
    l = l - (146097 * n + 3) / 4;
    int64_t i = (4000 * (l + 1)) / 1461001;
    l = l - (1461 * i) / 4 + 31;
    int64_t j = (80 * l) / 2447;
    int day = l - (2447 * j) / 80;
    l = j / 11;
    int month = j + 2 - (12 * l);
    int year = 100 * (n - 49) + i + l;

    int hour = time_secs / 3600;
    int min = (time_secs % 3600) / 60;
    int sec = time_secs % 60;

    char buf[64];
    if (microseconds > 0) {
        snprintf(buf, sizeof(buf), "%04d-%02d-%02d %02d:%02d:%02d.%06lld",
                 year, month, day, hour, min, sec,
                 static_cast<long long>(microseconds));
    } else {
        snprintf(buf, sizeof(buf), "%04d-%02d-%02d %02d:%02d:%02d",
                 year, month, day, hour, min, sec);
    }

    return std::string(buf);
}

std::string TypeConverter::format_date(int32_t pg_date) {
    // Julian day
    int64_t jd = static_cast<int64_t>(pg_date) + 2451545;

    // Convert Julian day to Gregorian
    int64_t l = jd + 68569;
    int64_t n = (4 * l) / 146097;
    l = l - (146097 * n + 3) / 4;
    int64_t i = (4000 * (l + 1)) / 1461001;
    l = l - (1461 * i) / 4 + 31;
    int64_t j = (80 * l) / 2447;
    int day = l - (2447 * j) / 80;
    l = j / 11;
    int month = j + 2 - (12 * l);
    int year = 100 * (n - 49) + i + l;

    char buf[16];
    snprintf(buf, sizeof(buf), "%04d-%02d-%02d", year, month, day);
    return std::string(buf);
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
