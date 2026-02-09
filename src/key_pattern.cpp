/**
 * key_pattern.cpp - Parses key pattern strings into structured segments
 *
 * Key patterns define how LevelDB keys map to table columns. A pattern like
 * "users##{group}##{id}##{attr}" is parsed into segments:
 *   - LiteralSegment("users##")
 *   - CaptureSegment("group")
 *   - LiteralSegment("##")
 *   - CaptureSegment("id")
 *   - LiteralSegment("##")
 *   - AttrSegment()
 *
 * The parser enforces rules that ensure unambiguous key parsing at runtime:
 *   - Exactly one {attr} segment required (for pivoting)
 *   - No consecutive variable segments (would be ambiguous without delimiter)
 *   - Placeholder names must be alphanumeric
 */

#include "level_pivot/key_pattern.hpp"
#include <algorithm>

namespace level_pivot {

KeyPattern::KeyPattern(const std::string& pattern) : pattern_(pattern) {
    parse(pattern);
    compute_literal_prefix();
    validate();
}

/**
 * Parses the pattern string character by character, building a list of segments.
 * We accumulate literal characters until hitting '{', then extract the placeholder
 * name up to '}'. This simple state machine handles nested cases correctly because
 * placeholder names cannot contain braces.
 */
void KeyPattern::parse(const std::string& pattern) {
    if (pattern.empty()) {
        throw KeyPatternError("Key pattern cannot be empty");
    }

    size_t pos = 0;
    std::string current_literal;

    while (pos < pattern.size()) {
        if (pattern[pos] == '{') {
            // Flush accumulated literal before processing placeholder.
            // This ensures literals are never merged across placeholders.
            if (!current_literal.empty()) {
                segments_.push_back(LiteralSegment{current_literal});
                current_literal.clear();
            }

            // Extract placeholder name between { and }
            size_t end = pattern.find('}', pos);
            if (end == std::string::npos) {
                throw KeyPatternError("Unclosed '{' in pattern at position " + std::to_string(pos));
            }

            std::string name = pattern.substr(pos + 1, end - pos - 1);

            if (name.empty()) {
                throw KeyPatternError("Empty placeholder '{}' in pattern");
            }

            // Restrict placeholder names to safe identifier characters.
            // This prevents SQL injection and ensures names work as column names.
            for (char c : name) {
                if (!std::isalnum(c) && c != '_') {
                    throw KeyPatternError("Invalid character '" + std::string(1, c) +
                                         "' in placeholder name '" + name + "'");
                }
            }

            // The special "attr" placeholder marks where pivoted column names appear.
            // All other placeholders become identity columns (part of the row key).
            if (name == "attr") {
                if (has_attr_) {
                    throw KeyPatternError("Multiple {attr} segments in pattern");
                }
                segments_.push_back(AttrSegment{});
                has_attr_ = true;
                attr_index_ = static_cast<int>(segments_.size()) - 1;
            } else {
                // Duplicate capture names would create ambiguity in the column mapping
                if (std::find(capture_names_.begin(), capture_names_.end(), name) != capture_names_.end()) {
                    throw KeyPatternError("Duplicate capture name '" + name + "' in pattern");
                }
                segments_.push_back(CaptureSegment{name});
                capture_names_.push_back(name);
            }

            pos = end + 1;
        } else {
            current_literal += pattern[pos];
            ++pos;
        }
    }

    // Don't lose trailing literals (e.g., pattern ending in "##suffix")
    if (!current_literal.empty()) {
        segments_.push_back(LiteralSegment{current_literal});
    }
}

/**
 * Computes the longest literal prefix before any variable segment.
 * This prefix enables efficient LevelDB seeks - we can jump directly to
 * keys starting with this prefix instead of scanning from the beginning.
 * For "users##{group}##{id}##{attr}", the prefix is "users##".
 */
void KeyPattern::compute_literal_prefix() {
    literal_prefix_.clear();

    for (const auto& segment : segments_) {
        if (std::holds_alternative<LiteralSegment>(segment)) {
            literal_prefix_ += std::get<LiteralSegment>(segment).text;
        } else {
            // Stop at first variable - can't include it in prefix
            break;
        }
    }
}

/**
 * Validates pattern structure to ensure it can be parsed unambiguously at runtime.
 * The key constraint is that variable segments (captures and attr) must be
 * separated by literal delimiters. Without this, "ab" couldn't be parsed
 * unambiguously as capture1="a",capture2="b" vs capture1="ab",capture2="".
 */
void KeyPattern::validate() const {
    if (segments_.empty()) {
        throw KeyPatternError("Pattern must have at least one segment");
    }

    // Every pivot table needs {attr} - it's what makes the pivoting work
    if (!has_attr_) {
        throw KeyPatternError("Pattern must contain {attr} segment");
    }

    // {attr} followed immediately by another variable would be ambiguous.
    // Where does attr end and the next capture begin?
    if (attr_index_ >= 0 && static_cast<size_t>(attr_index_) + 1 < segments_.size()) {
        const auto& next = segments_[attr_index_ + 1];
        if (!std::holds_alternative<LiteralSegment>(next)) {
            throw KeyPatternError("{attr} must be followed by a literal delimiter or end of pattern");
        }
    }

    // Same logic for all consecutive variable pairs - need delimiters between them
    for (size_t i = 0; i + 1 < segments_.size(); ++i) {
        bool current_is_variable = !std::holds_alternative<LiteralSegment>(segments_[i]);
        bool next_is_variable = !std::holds_alternative<LiteralSegment>(segments_[i + 1]);

        if (current_is_variable && next_is_variable) {
            throw KeyPatternError("Consecutive variable segments without delimiter between them");
        }
    }
}

bool KeyPattern::has_capture(const std::string& name) const {
    return std::find(capture_names_.begin(), capture_names_.end(), name) != capture_names_.end();
}

/**
 * Returns the position of a capture in the capture_names_ list.
 * This index maps directly to the order captures appear in parsed keys,
 * which is essential for building PivotRow::identity_values correctly.
 */
int KeyPattern::capture_index(const std::string& name) const {
    auto it = std::find(capture_names_.begin(), capture_names_.end(), name);
    if (it == capture_names_.end()) {
        return -1;
    }
    return static_cast<int>(std::distance(capture_names_.begin(), it));
}

} // namespace level_pivot
