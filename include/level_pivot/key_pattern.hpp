#pragma once

#include <string>
#include <vector>
#include <variant>
#include <optional>
#include <stdexcept>

namespace level_pivot {

/**
 * Exception thrown when a key pattern is invalid
 */
class KeyPatternError : public std::runtime_error {
public:
    explicit KeyPatternError(const std::string& msg) : std::runtime_error(msg) {}
};

/**
 * Segment types in a key pattern
 */
struct LiteralSegment {
    std::string text;

    bool operator==(const LiteralSegment& other) const {
        return text == other.text;
    }
};

struct CaptureSegment {
    std::string name;  // The column name (e.g., "arg", "sub_arg")

    bool operator==(const CaptureSegment& other) const {
        return name == other.name;
    }
};

struct AttrSegment {
    bool operator==(const AttrSegment&) const { return true; }
};

using PatternSegment = std::variant<LiteralSegment, CaptureSegment, AttrSegment>;

/**
 * Parsed key pattern
 *
 * Parses patterns like:
 *   - this###{arg}__{sub_arg}##pat##{attr}
 *   - users##{group}##{id}##{attr}
 *   - {tenant}:{env}/{service}/{attr}
 *
 * Into a sequence of segments:
 *   - LiteralSegment: Fixed text to match exactly
 *   - CaptureSegment: {name} placeholder that captures variable text
 *   - AttrSegment: Special {attr} that marks the pivot point
 */
class KeyPattern {
public:
    /**
     * Parse a key pattern string
     *
     * @param pattern The pattern string (e.g., "users##{group}##{id}##{attr}")
     * @throws KeyPatternError if the pattern is invalid
     */
    explicit KeyPattern(const std::string& pattern);

    /**
     * Get the original pattern string
     */
    const std::string& pattern() const { return pattern_; }

    /**
     * Get all parsed segments
     */
    const std::vector<PatternSegment>& segments() const { return segments_; }

    /**
     * Get names of all capture segments (identity columns)
     * Does NOT include "attr"
     */
    const std::vector<std::string>& capture_names() const { return capture_names_; }

    /**
     * Get the literal prefix (all text before first variable segment)
     * Useful for LevelDB iteration
     */
    const std::string& literal_prefix() const { return literal_prefix_; }

    /**
     * Check if the pattern has an {attr} segment
     */
    bool has_attr() const { return has_attr_; }

    /**
     * Get the index of the attr segment in segments()
     * Returns -1 if no attr segment
     */
    int attr_index() const { return attr_index_; }

    /**
     * Get the number of capture segments (not including attr)
     */
    size_t capture_count() const { return capture_names_.size(); }

    /**
     * Check if a capture name exists
     */
    bool has_capture(const std::string& name) const;

    /**
     * Get index of a capture segment by name
     * Returns -1 if not found
     */
    int capture_index(const std::string& name) const;

private:
    std::string pattern_;
    std::vector<PatternSegment> segments_;
    std::vector<std::string> capture_names_;
    std::string literal_prefix_;
    bool has_attr_ = false;
    int attr_index_ = -1;

    void parse(const std::string& pattern);
    void compute_literal_prefix();
    void validate() const;
};

} // namespace level_pivot
