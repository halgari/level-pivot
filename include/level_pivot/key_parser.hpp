#pragma once

#include "level_pivot/key_pattern.hpp"
#include <string>
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
};

} // namespace level_pivot
