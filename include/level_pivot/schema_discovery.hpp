#pragma once

#include "level_pivot/key_parser.hpp"
#include "level_pivot/connection_manager.hpp"
#include <vector>
#include <unordered_set>
#include <string>
#include <optional>

namespace level_pivot {

/**
 * Discovered attr information
 */
struct DiscoveredAttr {
    std::string name;
    size_t sample_count = 0;     // Number of times this attr was seen
    std::string sample_value;    // Sample value for type inference
};

/**
 * Result of schema discovery
 */
struct DiscoveryResult {
    std::vector<DiscoveredAttr> attrs;
    size_t keys_scanned = 0;
    size_t keys_matched = 0;
};

/**
 * Options for schema discovery
 */
struct DiscoveryOptions {
    size_t max_keys = 10000;     // Maximum keys to scan
    size_t sample_size = 100;    // Number of samples per attr
    std::string prefix_filter;   // Optional prefix to filter keys
};

/**
 * Discovers available attrs for a given key pattern
 *
 * Scans LevelDB to find all unique attr values matching the pattern,
 * supporting IMPORT FOREIGN SCHEMA functionality.
 */
class SchemaDiscovery {
public:
    /**
     * Create a discovery instance
     *
     * @param connection LevelDB connection
     */
    explicit SchemaDiscovery(std::shared_ptr<LevelDBConnection> connection);

    /**
     * Discover attrs matching a key pattern
     *
     * @param pattern Key pattern to match
     * @param options Discovery options
     * @return Discovery result with found attrs
     */
    DiscoveryResult discover(const KeyPattern& pattern,
                             const DiscoveryOptions& options = {});

    /**
     * List all unique prefixes in the database
     *
     * Useful for discovering available patterns.
     *
     * @param depth Number of "segments" to consider for prefix
     *              (where segments are delimited by common chars like ##, /, :)
     * @param max_prefixes Maximum number of prefixes to return
     * @return Vector of unique prefixes
     */
    std::vector<std::string> list_prefixes(size_t depth = 2,
                                           size_t max_prefixes = 100);

    /**
     * Infer a key pattern from sample keys
     *
     * Analyzes a set of keys to suggest a possible pattern.
     * This is a best-effort heuristic.
     *
     * @param sample_count Number of keys to sample
     * @return Suggested pattern string, or nullopt if cannot infer
     */
    std::optional<std::string> infer_pattern(size_t sample_count = 100);

private:
    std::shared_ptr<LevelDBConnection> connection_;

    // Common delimiters to look for when inferring patterns
    static constexpr const char* COMMON_DELIMITERS[] = {
        "##", "::", "//", "__", ":", "/", ".", "-", "_"
    };
};

/**
 * Generate SQL for creating a foreign table based on discovery
 */
std::string generate_foreign_table_sql(
    const std::string& table_name,
    const std::string& server_name,
    const std::string& key_pattern,
    const DiscoveryResult& discovery,
    bool use_text_type = true);

} // namespace level_pivot
