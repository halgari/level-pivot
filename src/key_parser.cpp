/**
 * key_parser.cpp - Runtime key parsing and building
 *
 * The parser takes LevelDB keys and extracts capture values and attr names
 * according to a KeyPattern. For example, given pattern "users##{group}##{id}##{attr}"
 * and key "users##admins##user001##email", it produces:
 *   - capture_values: ["admins", "user001"]
 *   - attr_name: "email"
 *
 * The parser also builds keys from values (for INSERT/UPDATE/DELETE operations).
 *
 * Performance optimizations:
 *   - SIMD-accelerated parsing for uniform delimiter patterns (AVX2/SSE2)
 *   - Zero-copy parse_view() returns string_views into the original key
 *   - Pre-computed key size estimates reduce allocations
 */

#include "level_pivot/key_parser.hpp"
#include <stdexcept>

namespace level_pivot {

KeyParser::KeyParser(const KeyPattern& pattern) : pattern_(pattern) {
    compute_estimated_key_size();
    try_init_simd_parser();
}

KeyParser::KeyParser(const std::string& pattern) : pattern_(pattern) {
    compute_estimated_key_size();
    try_init_simd_parser();
}

/**
 * Pre-compute an estimate of typical key size to reduce reallocations when
 * building keys. We assume ~16 bytes per capture/attr which is reasonable
 * for typical identifiers like "user001", "production", "email", etc.
 */
void KeyParser::compute_estimated_key_size() {
    constexpr size_t avg_capture_len = 16;

    estimated_key_size_ = 0;
    for (const auto& segment : pattern_.segments()) {
        if (std::holds_alternative<LiteralSegment>(segment)) {
            estimated_key_size_ += std::get<LiteralSegment>(segment).text.size();
        } else {
            estimated_key_size_ += avg_capture_len;
        }
    }
}

bool KeyParser::matches(const std::string& key) const {
    return parse(key).has_value();
}

namespace {

/**
 * Template-based parsing implementation shared between ParsedKey (owning strings)
 * and ParsedKeyView (non-owning string_views). This avoids code duplication while
 * allowing the hot path (parse_view) to avoid all allocations.
 */
template<typename ResultType>
struct ParsePolicy;

template<>
struct ParsePolicy<ParsedKey> {
    using StringType = std::string;
    static void add_capture(ParsedKey& result, std::string_view key, size_t pos, size_t len) {
        result.capture_values.emplace_back(key.substr(pos, len));
    }
    static void set_attr(ParsedKey& result, std::string_view key, size_t pos, size_t len) {
        result.attr_name = std::string(key.substr(pos, len));
    }
};

template<>
struct ParsePolicy<ParsedKeyView> {
    using StringType = std::string_view;
    static void add_capture(ParsedKeyView& result, std::string_view key, size_t pos, size_t len) {
        result.capture_values.push_back(key.substr(pos, len));
    }
    static void set_attr(ParsedKeyView& result, std::string_view key, size_t pos, size_t len) {
        result.attr_name = key.substr(pos, len);
    }
};

/**
 * Core parsing algorithm: walk through pattern segments, matching literals
 * exactly and extracting variable segments up to the next delimiter.
 *
 * The algorithm relies on KeyPattern's validation that ensures no consecutive
 * variable segments - this guarantees we always know where each variable ends
 * (either at the next literal or end of string).
 */
template<typename ResultType>
std::optional<ResultType> parse_impl(const KeyPattern& pattern, std::string_view key) {
    const auto& segments = pattern.segments();
    ResultType result;
    result.capture_values.reserve(pattern.capture_count());

    size_t key_pos = 0;

    for (size_t seg_idx = 0; seg_idx < segments.size(); ++seg_idx) {
        const auto& segment = segments[seg_idx];

        if (std::holds_alternative<LiteralSegment>(segment)) {
            // Literals must match exactly - this is how we find delimiters
            const auto& literal = std::get<LiteralSegment>(segment);

            if (key.compare(key_pos, literal.text.size(), literal.text) != 0) {
                return std::nullopt;
            }
            key_pos += literal.text.size();

        } else if (std::holds_alternative<CaptureSegment>(segment)) {
            // Find where this capture ends by looking for the next delimiter.
            // Because KeyPattern disallows consecutive variables, the next
            // segment is guaranteed to be a literal (or end of pattern).
            size_t end_pos;

            if (seg_idx + 1 < segments.size()) {
                const auto& next_literal = std::get<LiteralSegment>(segments[seg_idx + 1]);
                end_pos = key.find(next_literal.text, key_pos);
                if (end_pos == std::string_view::npos) {
                    return std::nullopt;
                }
            } else {
                end_pos = key.size();
            }

            // Empty captures are invalid - they'd create ambiguous keys
            if (end_pos == key_pos) {
                return std::nullopt;
            }

            ParsePolicy<ResultType>::add_capture(result, key, key_pos, end_pos - key_pos);
            key_pos = end_pos;

        } else if (std::holds_alternative<AttrSegment>(segment)) {
            // Attr extraction works the same as captures
            size_t end_pos;

            if (seg_idx + 1 < segments.size()) {
                const auto& next_literal = std::get<LiteralSegment>(segments[seg_idx + 1]);
                end_pos = key.find(next_literal.text, key_pos);
                if (end_pos == std::string_view::npos) {
                    return std::nullopt;
                }
            } else {
                end_pos = key.size();
            }

            if (end_pos == key_pos) {
                return std::nullopt;
            }

            ParsePolicy<ResultType>::set_attr(result, key, key_pos, end_pos - key_pos);
            key_pos = end_pos;
        }
    }

    // Ensure we consumed the entire key - leftover chars mean pattern mismatch
    if (key_pos != key.size()) {
        return std::nullopt;
    }

    return result;
}

} // anonymous namespace

std::optional<ParsedKey> KeyParser::parse(const std::string& key) const {
    return parse_impl<ParsedKey>(pattern_, key);
}

/**
 * Zero-copy parsing returns string_views into the original key.
 * Uses SIMD-accelerated parsing when the pattern has uniform delimiters,
 * falling back to the generic implementation otherwise.
 */
std::optional<ParsedKeyView> KeyParser::parse_view(std::string_view key) const {
    // SIMD path: uses vectorized delimiter detection for patterns like
    // "prefix##{a}##{b}##{attr}" where all delimiters are "##"
    if (simd_parser_) {
        std::string_view captures[16];  // Stack-allocated, max 16 captures
        std::string_view attr;
        if (simd_parser_->parse_fast(key, captures, attr)) {
            ParsedKeyView result;
            result.capture_values.reserve(pattern_.capture_count());
            for (size_t i = 0; i < pattern_.capture_count(); ++i) {
                result.capture_values.push_back(captures[i]);
            }
            result.attr_name = attr;
            return result;
        }
        return std::nullopt;
    }
    return parse_impl<ParsedKeyView>(pattern_, key);
}

/**
 * Builds a LevelDB key from capture values and attr name.
 * This is the inverse of parse() - used for INSERT, UPDATE, DELETE operations
 * that need to construct keys to write to LevelDB.
 */
std::string KeyParser::build(const std::vector<std::string>& capture_values,
                             const std::string& attr_name) const {
    if (capture_values.size() != pattern_.capture_count()) {
        throw std::invalid_argument(
            "Expected " + std::to_string(pattern_.capture_count()) +
            " capture values, got " + std::to_string(capture_values.size()));
    }

    if (attr_name.empty()) {
        throw std::invalid_argument("attr_name cannot be empty");
    }

    std::string result;
    result.reserve(estimated_key_size_);
    size_t capture_idx = 0;

    // Reassemble the key by walking segments and substituting values
    for (const auto& segment : pattern_.segments()) {
        if (std::holds_alternative<LiteralSegment>(segment)) {
            result += std::get<LiteralSegment>(segment).text;
        } else if (std::holds_alternative<CaptureSegment>(segment)) {
            if (capture_values[capture_idx].empty()) {
                throw std::invalid_argument(
                    "Capture value for '" +
                    std::get<CaptureSegment>(segment).name +
                    "' cannot be empty");
            }
            result += capture_values[capture_idx];
            ++capture_idx;
        } else if (std::holds_alternative<AttrSegment>(segment)) {
            result += attr_name;
        }
    }

    return result;
}

/**
 * Build variant that takes a name->value map instead of ordered vector.
 * More convenient when values come from PostgreSQL columns.
 */
std::string KeyParser::build(const std::unordered_map<std::string, std::string>& captures,
                             const std::string& attr_name) const {
    std::vector<std::string> capture_values;
    capture_values.reserve(pattern_.capture_count());

    // Convert map to ordered vector matching pattern's capture order
    for (const auto& name : pattern_.capture_names()) {
        auto it = captures.find(name);
        if (it == captures.end()) {
            throw std::invalid_argument("Missing capture value for '" + name + "'");
        }
        capture_values.push_back(it->second);
    }

    return build(capture_values, attr_name);
}

std::string KeyParser::build_prefix() const {
    return pattern_.literal_prefix();
}

/**
 * Builds a prefix for LevelDB seeks using partial capture values.
 * For pattern "users##{group}##{id}##{attr}" and captures ["admins"],
 * returns "users##admins##" - enabling efficient range scans over
 * all keys in the "admins" group.
 */
std::string KeyParser::build_prefix(const std::vector<std::string>& capture_values) const {
    std::string result;
    result.reserve(estimated_key_size_);
    size_t capture_idx = 0;

    for (const auto& segment : pattern_.segments()) {
        if (std::holds_alternative<LiteralSegment>(segment)) {
            result += std::get<LiteralSegment>(segment).text;
        } else if (std::holds_alternative<CaptureSegment>(segment)) {
            if (capture_idx >= capture_values.size()) {
                // Stop when we run out of provided capture values.
                // This creates a prefix for range scanning.
                break;
            }
            result += capture_values[capture_idx];
            ++capture_idx;
        } else if (std::holds_alternative<AttrSegment>(segment)) {
            // Never include attr in prefix - we want all attrs for an identity
            break;
        }
    }

    return result;
}

bool KeyParser::starts_with_prefix(const std::string& key) const {
    const auto& prefix = pattern_.literal_prefix();
    if (key.size() < prefix.size()) {
        return false;
    }
    return key.compare(0, prefix.size(), prefix) == 0;
}

/**
 * Checks if the pattern uses a single, uniform delimiter between all segments.
 * SIMD parsing can only be used for uniform delimiters because it searches
 * for a single delimiter pattern across the entire key.
 *
 * Examples:
 *   "prefix##{a}##{b}##{attr}" -> returns "##"
 *   "prefix##{a}__{b}##{attr}" -> returns nullopt (mixed ## and __)
 */
std::optional<std::string> KeyParser::try_get_uniform_delimiter() const {
    std::string delimiter;
    bool first_literal = true;

    for (size_t i = 0; i < pattern_.segments().size(); ++i) {
        const auto& seg = pattern_.segments()[i];

        if (std::holds_alternative<LiteralSegment>(seg)) {
            const auto& lit = std::get<LiteralSegment>(seg).text;

            // Skip the prefix - it's not a delimiter between captures
            if (first_literal && i == 0) {
                first_literal = false;
                continue;
            }

            if (delimiter.empty()) {
                delimiter = lit;
            } else if (delimiter != lit) {
                return std::nullopt;  // Non-uniform delimiters
            }
        } else {
            if (first_literal) {
                first_literal = false;
            }
        }
    }

    return delimiter.empty() ? std::nullopt : std::optional(delimiter);
}

/**
 * Initialize SIMD parser if the pattern supports it.
 * SIMD parsing provides ~3-5x speedup for key parsing by using vectorized
 * delimiter detection instead of byte-by-byte scanning.
 */
void KeyParser::try_init_simd_parser() {
    auto uniform_delim = try_get_uniform_delimiter();
    if (!uniform_delim) {
        return;  // Non-uniform delimiters, can't use SIMD
    }

    // Store owned copies - SIMD parser needs stable string_views
    simd_prefix_ = pattern_.literal_prefix();
    simd_delimiter_ = *uniform_delim;

    simd_parser_ = std::make_unique<SimdKeyParser>(
        simd_prefix_,
        simd_delimiter_,
        pattern_.capture_count()
    );
}

} // namespace level_pivot
