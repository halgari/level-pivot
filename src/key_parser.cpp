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

void KeyParser::compute_estimated_key_size() {
    // Estimate key size based on literals + average capture size
    // Assume ~16 bytes per capture/attr as reasonable estimate
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

// Unified parsing implementation that works for both ParsedKey and ParsedKeyView
// Uses a policy-based approach to handle type differences
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

template<typename ResultType>
std::optional<ResultType> parse_impl(const KeyPattern& pattern, std::string_view key) {
    const auto& segments = pattern.segments();
    ResultType result;
    result.capture_values.reserve(pattern.capture_count());

    size_t key_pos = 0;

    for (size_t seg_idx = 0; seg_idx < segments.size(); ++seg_idx) {
        const auto& segment = segments[seg_idx];

        if (std::holds_alternative<LiteralSegment>(segment)) {
            const auto& literal = std::get<LiteralSegment>(segment);

            if (key.compare(key_pos, literal.text.size(), literal.text) != 0) {
                return std::nullopt;
            }
            key_pos += literal.text.size();

        } else if (std::holds_alternative<CaptureSegment>(segment)) {
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

            ParsePolicy<ResultType>::add_capture(result, key, key_pos, end_pos - key_pos);
            key_pos = end_pos;

        } else if (std::holds_alternative<AttrSegment>(segment)) {
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

    if (key_pos != key.size()) {
        return std::nullopt;
    }

    return result;
}

} // anonymous namespace

std::optional<ParsedKey> KeyParser::parse(const std::string& key) const {
    return parse_impl<ParsedKey>(pattern_, key);
}

std::optional<ParsedKeyView> KeyParser::parse_view(std::string_view key) const {
    // Use SIMD parser if available (uniform delimiter pattern)
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

std::string KeyParser::build(const std::unordered_map<std::string, std::string>& captures,
                             const std::string& attr_name) const {
    std::vector<std::string> capture_values;
    capture_values.reserve(pattern_.capture_count());

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

std::string KeyParser::build_prefix(const std::vector<std::string>& capture_values) const {
    std::string result;
    result.reserve(estimated_key_size_);
    size_t capture_idx = 0;

    for (const auto& segment : pattern_.segments()) {
        if (std::holds_alternative<LiteralSegment>(segment)) {
            result += std::get<LiteralSegment>(segment).text;
        } else if (std::holds_alternative<CaptureSegment>(segment)) {
            if (capture_idx >= capture_values.size()) {
                // No more capture values provided, stop here
                break;
            }
            result += capture_values[capture_idx];
            ++capture_idx;
        } else if (std::holds_alternative<AttrSegment>(segment)) {
            // Stop before attr segment
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

std::optional<std::string> KeyParser::try_get_uniform_delimiter() const {
    std::string delimiter;
    bool first_literal = true;

    for (size_t i = 0; i < pattern_.segments().size(); ++i) {
        const auto& seg = pattern_.segments()[i];

        if (std::holds_alternative<LiteralSegment>(seg)) {
            const auto& lit = std::get<LiteralSegment>(seg).text;

            // Skip prefix literal (first literal before any capture)
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
            // We hit a capture or attr segment
            if (first_literal) {
                // Pattern starts with capture, no prefix
                first_literal = false;
            }
        }
    }

    return delimiter.empty() ? std::nullopt : std::optional(delimiter);
}

void KeyParser::try_init_simd_parser() {
    // Check if pattern has uniform delimiters
    auto uniform_delim = try_get_uniform_delimiter();
    if (!uniform_delim) {
        return;  // Non-uniform delimiters, can't use SIMD
    }

    // Store owned copies of strings for SIMD parser
    simd_prefix_ = pattern_.literal_prefix();
    simd_delimiter_ = *uniform_delim;

    // Create SIMD parser with stable string_views to our owned strings
    simd_parser_ = std::make_unique<SimdKeyParser>(
        simd_prefix_,
        simd_delimiter_,
        pattern_.capture_count()
    );
}

} // namespace level_pivot
