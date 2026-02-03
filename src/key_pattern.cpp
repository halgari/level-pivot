#include "level_pivot/key_pattern.hpp"
#include <algorithm>

namespace level_pivot {

KeyPattern::KeyPattern(const std::string& pattern) : pattern_(pattern) {
    parse(pattern);
    compute_literal_prefix();
    validate();
}

void KeyPattern::parse(const std::string& pattern) {
    if (pattern.empty()) {
        throw KeyPatternError("Key pattern cannot be empty");
    }

    size_t pos = 0;
    std::string current_literal;

    while (pos < pattern.size()) {
        if (pattern[pos] == '{') {
            // Save any accumulated literal
            if (!current_literal.empty()) {
                segments_.push_back(LiteralSegment{current_literal});
                current_literal.clear();
            }

            // Find closing brace
            size_t end = pattern.find('}', pos);
            if (end == std::string::npos) {
                throw KeyPatternError("Unclosed '{' in pattern at position " + std::to_string(pos));
            }

            std::string name = pattern.substr(pos + 1, end - pos - 1);

            if (name.empty()) {
                throw KeyPatternError("Empty placeholder '{}' in pattern");
            }

            // Check for invalid characters in name
            for (char c : name) {
                if (!std::isalnum(c) && c != '_') {
                    throw KeyPatternError("Invalid character '" + std::string(1, c) +
                                         "' in placeholder name '" + name + "'");
                }
            }

            if (name == "attr") {
                if (has_attr_) {
                    throw KeyPatternError("Multiple {attr} segments in pattern");
                }
                segments_.push_back(AttrSegment{});
                has_attr_ = true;
                attr_index_ = static_cast<int>(segments_.size()) - 1;
            } else {
                // Check for duplicate capture names
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

    // Save any remaining literal
    if (!current_literal.empty()) {
        segments_.push_back(LiteralSegment{current_literal});
    }
}

void KeyPattern::compute_literal_prefix() {
    literal_prefix_.clear();

    for (const auto& segment : segments_) {
        if (std::holds_alternative<LiteralSegment>(segment)) {
            literal_prefix_ += std::get<LiteralSegment>(segment).text;
        } else {
            // Stop at first non-literal segment
            break;
        }
    }
}

void KeyPattern::validate() const {
    if (segments_.empty()) {
        throw KeyPatternError("Pattern must have at least one segment");
    }

    if (!has_attr_) {
        throw KeyPatternError("Pattern must contain {attr} segment");
    }

    // Ensure attr is not immediately followed by a capture (would be ambiguous)
    // This is OK: ...{attr}## or ...{attr} (end of pattern)
    // This is not OK: ...{attr}{foo}... (no delimiter between)
    if (attr_index_ >= 0 && static_cast<size_t>(attr_index_) + 1 < segments_.size()) {
        const auto& next = segments_[attr_index_ + 1];
        if (!std::holds_alternative<LiteralSegment>(next)) {
            throw KeyPatternError("{attr} must be followed by a literal delimiter or end of pattern");
        }
    }

    // Check that captures are separated by literals (for unambiguous parsing)
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

int KeyPattern::capture_index(const std::string& name) const {
    auto it = std::find(capture_names_.begin(), capture_names_.end(), name);
    if (it == capture_names_.end()) {
        return -1;
    }
    return static_cast<int>(std::distance(capture_names_.begin(), it));
}

} // namespace level_pivot
