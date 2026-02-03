#include "level_pivot/schema_discovery.hpp"
#include <algorithm>
#include <cstring>
#include <sstream>
#include <unordered_map>
#include <regex>

namespace level_pivot {

SchemaDiscovery::SchemaDiscovery(std::shared_ptr<LevelDBConnection> connection)
    : connection_(std::move(connection)) {}

DiscoveryResult SchemaDiscovery::discover(const KeyPattern& pattern,
                                          const DiscoveryOptions& options) {
    DiscoveryResult result;
    KeyParser parser(pattern);

    std::unordered_map<std::string, DiscoveredAttr> attr_map;

    auto iter = connection_->iterator();

    if (options.prefix_filter.empty()) {
        std::string prefix = pattern.literal_prefix();
        if (prefix.empty()) {
            iter.seek_to_first();
        } else {
            iter.seek(prefix);
        }
    } else {
        iter.seek(options.prefix_filter);
    }

    while (iter.valid() && result.keys_scanned < options.max_keys) {
        std::string key = iter.key();

        // Check prefix filter
        if (!options.prefix_filter.empty() &&
            key.compare(0, options.prefix_filter.size(), options.prefix_filter) != 0) {
            break;
        }

        // Check pattern prefix
        if (!parser.starts_with_prefix(key)) {
            break;
        }

        ++result.keys_scanned;

        // Try to parse
        auto parsed = parser.parse(key);
        if (parsed) {
            ++result.keys_matched;

            auto& attr = attr_map[parsed->attr_name];
            attr.name = parsed->attr_name;
            ++attr.sample_count;

            // Store a sample value if we haven't reached the limit
            if (attr.sample_count <= options.sample_size && attr.sample_value.empty()) {
                attr.sample_value = iter.value();
            }
        }

        iter.next();
    }

    // Convert map to vector and sort by frequency
    result.attrs.reserve(attr_map.size());
    for (auto& [name, attr] : attr_map) {
        result.attrs.push_back(std::move(attr));
    }

    std::sort(result.attrs.begin(), result.attrs.end(),
              [](const DiscoveredAttr& a, const DiscoveredAttr& b) {
                  return a.sample_count > b.sample_count;
              });

    return result;
}

std::vector<std::string> SchemaDiscovery::list_prefixes(size_t depth,
                                                         size_t max_prefixes) {
    std::unordered_set<std::string> prefixes;
    std::vector<std::string> result;

    // Common delimiter patterns
    std::regex delim_regex("(##|::|//|__|:|/|\\.|-)");

    auto iter = connection_->iterator();
    iter.seek_to_first();

    std::string last_prefix;

    while (iter.valid() && result.size() < max_prefixes) {
        std::string key = iter.key();

        // Find prefix by counting delimiters
        std::string prefix;
        size_t delim_count = 0;

        std::smatch match;
        std::string remaining = key;

        while (delim_count < depth) {
            if (std::regex_search(remaining, match, delim_regex)) {
                prefix += remaining.substr(0, match.position() + match.length());
                remaining = match.suffix();
                ++delim_count;
            } else {
                // No more delimiters, use whole key segment
                if (delim_count == 0) {
                    prefix = key;
                }
                break;
            }
        }

        if (!prefix.empty() && prefix != last_prefix) {
            if (prefixes.insert(prefix).second) {
                result.push_back(prefix);
            }
            last_prefix = prefix;

            // Skip to next different prefix
            iter.seek(prefix + "\xFF");
            continue;
        }

        iter.next();
    }

    std::sort(result.begin(), result.end());
    return result;
}

std::optional<std::string> SchemaDiscovery::infer_pattern(size_t sample_count) {
    std::vector<std::string> samples;
    samples.reserve(sample_count);

    auto iter = connection_->iterator();
    iter.seek_to_first();

    while (iter.valid() && samples.size() < sample_count) {
        samples.push_back(iter.key());
        iter.next();
    }

    if (samples.empty()) {
        return std::nullopt;
    }

    // Find the most common delimiter
    std::unordered_map<std::string, size_t> delim_counts;
    for (const char* delim : COMMON_DELIMITERS) {
        for (const auto& key : samples) {
            size_t count = 0;
            size_t pos = 0;
            while ((pos = key.find(delim, pos)) != std::string::npos) {
                ++count;
                pos += strlen(delim);
            }
            delim_counts[delim] += count;
        }
    }

    // Find most common delimiter
    std::string best_delim;
    size_t best_count = 0;
    for (const auto& [delim, count] : delim_counts) {
        if (count > best_count) {
            best_count = count;
            best_delim = delim;
        }
    }

    if (best_delim.empty()) {
        return std::nullopt;
    }

    // Split first key by delimiter and create pattern
    const auto& first_key = samples[0];
    std::vector<std::string> parts;
    size_t start = 0;
    size_t pos;

    while ((pos = first_key.find(best_delim, start)) != std::string::npos) {
        parts.push_back(first_key.substr(start, pos - start));
        start = pos + best_delim.size();
    }
    if (start < first_key.size()) {
        parts.push_back(first_key.substr(start));
    }

    if (parts.size() < 2) {
        return std::nullopt;
    }

    // Analyze which parts are constant vs variable
    std::vector<bool> is_constant(parts.size(), true);
    std::vector<std::unordered_set<std::string>> part_values(parts.size());

    for (const auto& key : samples) {
        std::vector<std::string> key_parts;
        start = 0;

        while ((pos = key.find(best_delim, start)) != std::string::npos) {
            key_parts.push_back(key.substr(start, pos - start));
            start = pos + best_delim.size();
        }
        if (start < key.size()) {
            key_parts.push_back(key.substr(start));
        }

        if (key_parts.size() == parts.size()) {
            for (size_t i = 0; i < parts.size(); ++i) {
                part_values[i].insert(key_parts[i]);
                if (key_parts[i] != parts[i]) {
                    is_constant[i] = false;
                }
            }
        }
    }

    // Build pattern
    std::ostringstream pattern;
    int var_idx = 1;

    for (size_t i = 0; i < parts.size(); ++i) {
        if (i > 0) {
            pattern << best_delim;
        }

        if (is_constant[i]) {
            pattern << parts[i];
        } else if (i == parts.size() - 1) {
            // Last part is typically attr
            pattern << "{attr}";
        } else {
            pattern << "{col" << var_idx++ << "}";
        }
    }

    return pattern.str();
}

std::string generate_foreign_table_sql(
    const std::string& table_name,
    const std::string& server_name,
    const std::string& key_pattern,
    const DiscoveryResult& discovery,
    bool use_text_type) {

    (void)use_text_type;  // Reserved for future type inference

    std::ostringstream sql;

    sql << "CREATE FOREIGN TABLE " << table_name << " (\n";

    // Parse pattern to get identity columns
    KeyPattern pattern(key_pattern);
    const auto& capture_names = pattern.capture_names();

    bool first = true;

    // Add identity columns
    for (const auto& name : capture_names) {
        if (!first) sql << ",\n";
        first = false;
        sql << "    " << name << " TEXT";
    }

    // Add attr columns
    for (const auto& attr : discovery.attrs) {
        if (!first) sql << ",\n";
        first = false;
        sql << "    " << attr.name << " TEXT";
    }

    sql << "\n)\n";
    sql << "SERVER " << server_name << "\n";
    sql << "OPTIONS (\n";
    sql << "    key_pattern '" << key_pattern << "'\n";
    sql << ");";

    return sql.str();
}

} // namespace level_pivot
