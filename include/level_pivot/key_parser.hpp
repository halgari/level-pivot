#pragma once

#include "level_pivot/key_pattern.hpp"
#include <string>
#include <string_view>
#include <vector>
#include <optional>
#include <unordered_map>

namespace level_pivot {

/**
 * Result of parsing a LevelDB key
 */
struct ParsedKey {
    std::vector<std::string> capture_values;  // Values for each capture segment
    std::string attr_name;                    // The attr value (column name)

    bool operator==(const ParsedKey& other) const {
        return capture_values == other.capture_values && attr_name == other.attr_name;
    }
};

/**
 * Zero-copy result of parsing a LevelDB key using string_view
 *
 * IMPORTANT: The string_views reference the original key string.
 * The key must remain valid for the lifetime of this struct.
 */
struct ParsedKeyView {
    std::vector<std::string_view> capture_values;  // Views into original key
    std::string_view attr_name;                    // View into original key

    bool operator==(const ParsedKeyView& other) const {
        return capture_values == other.capture_values && attr_name == other.attr_name;
    }

    // Convert to owning ParsedKey (materializes strings)
    ParsedKey to_owned() const {
        ParsedKey result;
        result.capture_values.reserve(capture_values.size());
        for (const auto& sv : capture_values) {
            result.capture_values.emplace_back(sv);
        }
        result.attr_name = std::string(attr_name);
        return result;
    }
};

/**
 * Parser/builder for LevelDB keys based on a pattern
 *
 * Uses a compiled KeyPattern to:
 *   - Match keys: Check if a LevelDB key matches the pattern
 *   - Extract values: Pull out captured segment values and attr name
 *   - Build keys: Construct a LevelDB key from identity values + attr name
 *   - Build prefix: Construct prefix for iteration
 */
class KeyParser {
public:
    /**
     * Create a parser from a pattern
     */
    explicit KeyParser(const KeyPattern& pattern);

    /**
     * Create a parser from a pattern string
     */
    explicit KeyParser(const std::string& pattern);

    /**
     * Get the underlying pattern
     */
    const KeyPattern& pattern() const { return pattern_; }

    /**
     * Check if a key matches the pattern
     */
    bool matches(const std::string& key) const;

    /**
     * Parse a key and extract capture values and attr name
     *
     * @param key The LevelDB key to parse
     * @return ParsedKey if the key matches, std::nullopt otherwise
     */
    std::optional<ParsedKey> parse(const std::string& key) const;

    /**
     * Parse a key using zero-copy string_view (faster, no allocations)
     *
     * IMPORTANT: The returned ParsedKeyView contains string_views that
     * reference the input key. The key must remain valid for the lifetime
     * of the returned ParsedKeyView.
     *
     * @param key The LevelDB key to parse
     * @return ParsedKeyView if the key matches, std::nullopt otherwise
     */
    std::optional<ParsedKeyView> parse_view(std::string_view key) const;

    /**
     * Build a key from capture values and attr name
     *
     * @param capture_values Values for each capture segment (in pattern order)
     * @param attr_name The attr value (column name)
     * @return The constructed LevelDB key
     * @throws std::invalid_argument if capture_values size doesn't match pattern
     */
    std::string build(const std::vector<std::string>& capture_values,
                      const std::string& attr_name) const;

    /**
     * Build a key using named captures
     *
     * @param captures Map of capture name -> value
     * @param attr_name The attr value (column name)
     * @return The constructed LevelDB key
     * @throws std::invalid_argument if a required capture is missing
     */
    std::string build(const std::unordered_map<std::string, std::string>& captures,
                      const std::string& attr_name) const;

    /**
     * Build a prefix for iteration
     *
     * Returns the literal prefix from the pattern, which can be used
     * for LevelDB Seek() to efficiently start iteration.
     */
    std::string build_prefix() const;

    /**
     * Build a prefix with some captures filled in
     *
     * Given partial capture values (in order), builds the longest possible
     * prefix including those values.
     *
     * @param capture_values Partial capture values (may be fewer than pattern expects)
     * @return The longest prefix that can be constructed
     */
    std::string build_prefix(const std::vector<std::string>& capture_values) const;

    /**
     * Check if a key starts with the pattern's literal prefix
     *
     * Useful for fast filtering during iteration.
     */
    bool starts_with_prefix(const std::string& key) const;

private:
    KeyPattern pattern_;
    size_t estimated_key_size_;  // Pre-computed estimate for build() reserve

    void compute_estimated_key_size();
};

} // namespace level_pivot
