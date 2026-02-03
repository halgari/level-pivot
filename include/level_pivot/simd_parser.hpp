#pragma once

#include <string_view>
#include <vector>
#include <optional>
#include <cstdint>
#include <cstring>

#if defined(__x86_64__) || defined(_M_X64)
#include <immintrin.h>
#define LEVEL_PIVOT_HAS_SSE2 1
#if defined(__AVX2__)
#define LEVEL_PIVOT_HAS_AVX2 1
#endif
#endif

namespace level_pivot {

/**
 * SIMD-optimized key parser for patterns with a single repeated delimiter
 *
 * This is a specialized fast-path for common patterns like:
 *   prefix##capture1##capture2##...##attr
 *
 * Uses SSE2/AVX2 to find all delimiter positions in a single pass,
 * then validates the structure.
 */
class SimdKeyParser {
public:
    /**
     * Result of SIMD parsing - zero-copy views into original key
     */
    struct Result {
        std::string_view prefix;                    // Literal prefix (may be empty)
        std::vector<std::string_view> captures;     // Capture values
        std::string_view attr;                      // Attr value
    };

    /**
     * Create a parser for a pattern with uniform delimiter
     *
     * @param prefix Literal prefix before first capture (e.g., "users")
     * @param delimiter Delimiter between segments (e.g., "##")
     * @param num_captures Number of capture segments (not including attr)
     */
    SimdKeyParser(std::string_view prefix, std::string_view delimiter, size_t num_captures)
        : prefix_(prefix), delimiter_(delimiter), num_captures_(num_captures) {
        // Pre-compute total delimiters expected: captures + attr = num_captures + 1
        // But delimiters separate them, so we need num_captures + 1 delimiters
        // (prefix##cap1##cap2##attr has 3 delimiters for 2 captures)
        num_delimiters_ = num_captures_ + 1;
    }

    /**
     * Parse a key using SIMD-accelerated delimiter search
     */
    std::optional<Result> parse(std::string_view key) const {
        // Quick prefix check
        if (key.size() < prefix_.size() + delimiter_.size() * num_delimiters_) {
            return std::nullopt;
        }

        if (!prefix_.empty()) {
            if (key.substr(0, prefix_.size()) != prefix_) {
                return std::nullopt;
            }
        }

        // Find all delimiter positions using SIMD
        size_t search_start = prefix_.size();

        // Use stack allocation for delimiter positions (max 16 captures)
        size_t delim_stack[17];
        size_t delim_count = 0;

#if LEVEL_PIVOT_HAS_AVX2
        find_delimiters_avx2_fast(key, search_start, delim_stack, delim_count, num_delimiters_ + 1);
#elif LEVEL_PIVOT_HAS_SSE2
        find_delimiters_sse2_fast(key, search_start, delim_stack, delim_count, num_delimiters_ + 1);
#else
        find_delimiters_scalar_fast(key, search_start, delim_stack, delim_count, num_delimiters_ + 1);
#endif

        // Validate we found exactly the right number of delimiters
        if (delim_count != num_delimiters_) {
            return std::nullopt;
        }

        // Extract captures and attr
        Result result;
        result.prefix = key.substr(0, prefix_.size());
        result.captures.reserve(num_captures_);

        size_t pos = prefix_.size();

        // First delimiter should be right after prefix
        if (delim_stack[0] != pos) {
            return std::nullopt;
        }
        pos += delimiter_.size();

        // Extract each capture
        for (size_t i = 0; i < num_captures_; ++i) {
            size_t end = delim_stack[i + 1];
            if (end <= pos) {
                return std::nullopt;  // Empty capture
            }
            result.captures.push_back(key.substr(pos, end - pos));
            pos = end + delimiter_.size();
        }

        // Remaining is attr
        if (pos >= key.size()) {
            return std::nullopt;  // Empty attr
        }
        result.attr = key.substr(pos);

        return result;
    }

    /**
     * Ultra-fast parse that returns views without Result struct allocation
     * Returns false if no match, fills captures array directly
     */
    bool parse_fast(std::string_view key,
                    std::string_view* captures,  // Pre-allocated array
                    std::string_view& attr) const {
        // Quick prefix check
        if (key.size() < prefix_.size() + delimiter_.size() * num_delimiters_) {
            return false;
        }

        if (!prefix_.empty()) {
            if (key.substr(0, prefix_.size()) != prefix_) {
                return false;
            }
        }

        size_t search_start = prefix_.size();
        size_t delim_stack[17];
        size_t delim_count = 0;

#if LEVEL_PIVOT_HAS_AVX2
        find_delimiters_avx2_fast(key, search_start, delim_stack, delim_count, num_delimiters_ + 1);
#elif LEVEL_PIVOT_HAS_SSE2
        find_delimiters_sse2_fast(key, search_start, delim_stack, delim_count, num_delimiters_ + 1);
#else
        find_delimiters_scalar_fast(key, search_start, delim_stack, delim_count, num_delimiters_ + 1);
#endif

        if (delim_count != num_delimiters_) {
            return false;
        }

        size_t pos = prefix_.size();
        if (delim_stack[0] != pos) {
            return false;
        }
        pos += delimiter_.size();

        for (size_t i = 0; i < num_captures_; ++i) {
            size_t end = delim_stack[i + 1];
            if (end <= pos) {
                return false;
            }
            captures[i] = key.substr(pos, end - pos);
            pos = end + delimiter_.size();
        }

        if (pos >= key.size()) {
            return false;
        }
        attr = key.substr(pos);

        return true;
    }

    /**
     * Parse without prefix validation (for benchmarking delimiter search)
     */
    std::optional<Result> parse_after_prefix(std::string_view key, size_t start_pos) const {
        std::vector<size_t> delim_positions;
        delim_positions.reserve(num_delimiters_ + 1);

#if LEVEL_PIVOT_HAS_AVX2
        find_delimiters_avx2(key, start_pos, delim_positions);
#elif LEVEL_PIVOT_HAS_SSE2
        find_delimiters_sse2(key, start_pos, delim_positions);
#else
        find_delimiters_scalar(key, start_pos, delim_positions);
#endif

        if (delim_positions.size() < num_delimiters_) {
            return std::nullopt;
        }

        Result result;
        result.captures.reserve(num_captures_);

        size_t pos = start_pos;

        for (size_t i = 0; i < num_captures_; ++i) {
            size_t end = delim_positions[i];
            if (end <= pos) {
                return std::nullopt;
            }
            result.captures.push_back(key.substr(pos, end - pos));
            pos = end + delimiter_.size();
        }

        size_t attr_end = (delim_positions.size() > num_captures_)
            ? delim_positions[num_captures_]
            : key.size();

        if (attr_end <= pos) {
            return std::nullopt;
        }
        result.attr = key.substr(pos, attr_end - pos);

        return result;
    }

private:
    std::string_view prefix_;
    std::string_view delimiter_;
    size_t num_captures_;
    size_t num_delimiters_;

    // Fast versions using stack arrays
#if LEVEL_PIVOT_HAS_AVX2
    void find_delimiters_avx2_fast(std::string_view key, size_t start,
                                    size_t* positions, size_t& count, size_t max_count) const {
        if (delimiter_.size() != 2) {
            find_delimiters_scalar_fast(key, start, positions, count, max_count);
            return;
        }

        const char d0 = delimiter_[0];
        const char d1 = delimiter_[1];
        const char* data = key.data();
        const size_t len = key.size();

        __m256i vd0 = _mm256_set1_epi8(d0);

        size_t i = start;
        count = 0;

        while (i + 32 <= len && count < max_count) {
            __m256i chunk = _mm256_loadu_si256(reinterpret_cast<const __m256i*>(data + i));
            __m256i eq0 = _mm256_cmpeq_epi8(chunk, vd0);
            uint32_t mask0 = _mm256_movemask_epi8(eq0);

            while (mask0 && count < max_count) {
                uint32_t bit_pos = __builtin_ctz(mask0);
                size_t pos = i + bit_pos;

                if (pos + 1 < len && data[pos + 1] == d1) {
                    positions[count++] = pos;
                }
                mask0 &= mask0 - 1;
            }
            i += 32;
        }

        while (i + 1 < len && count < max_count) {
            if (data[i] == d0 && data[i + 1] == d1) {
                positions[count++] = i;
            }
            ++i;
        }
    }
#endif

#if LEVEL_PIVOT_HAS_SSE2
    void find_delimiters_sse2_fast(std::string_view key, size_t start,
                                    size_t* positions, size_t& count, size_t max_count) const {
        if (delimiter_.size() != 2) {
            find_delimiters_scalar_fast(key, start, positions, count, max_count);
            return;
        }

        const char d0 = delimiter_[0];
        const char d1 = delimiter_[1];
        const char* data = key.data();
        const size_t len = key.size();

        __m128i vd0 = _mm_set1_epi8(d0);

        size_t i = start;
        count = 0;

        while (i + 16 <= len && count < max_count) {
            __m128i chunk = _mm_loadu_si128(reinterpret_cast<const __m128i*>(data + i));
            __m128i eq0 = _mm_cmpeq_epi8(chunk, vd0);
            uint32_t mask0 = _mm_movemask_epi8(eq0);

            while (mask0 && count < max_count) {
                uint32_t bit_pos = __builtin_ctz(mask0);
                size_t pos = i + bit_pos;

                if (pos + 1 < len && data[pos + 1] == d1) {
                    positions[count++] = pos;
                }
                mask0 &= mask0 - 1;
            }
            i += 16;
        }

        while (i + 1 < len && count < max_count) {
            if (data[i] == d0 && data[i + 1] == d1) {
                positions[count++] = i;
            }
            ++i;
        }
    }
#endif

    void find_delimiters_scalar_fast(std::string_view key, size_t start,
                                      size_t* positions, size_t& count, size_t max_count) const {
        size_t pos = start;
        count = 0;
        while ((pos = key.find(delimiter_, pos)) != std::string_view::npos && count < max_count) {
            positions[count++] = pos;
            pos += delimiter_.size();
        }
    }

    // Original vector-based versions (kept for compatibility)
#if LEVEL_PIVOT_HAS_AVX2
    void find_delimiters_avx2(std::string_view key, size_t start,
                               std::vector<size_t>& positions) const {
        if (delimiter_.size() != 2) {
            // Fall back to scalar for non-2-byte delimiters
            find_delimiters_scalar(key, start, positions);
            return;
        }

        const char d0 = delimiter_[0];
        const char d1 = delimiter_[1];
        const char* data = key.data();
        const size_t len = key.size();

        // Broadcast delimiter chars to AVX2 registers
        __m256i vd0 = _mm256_set1_epi8(d0);
        __m256i vd1 = _mm256_set1_epi8(d1);

        size_t i = start;

        // Process 32 bytes at a time
        while (i + 32 <= len) {
            __m256i chunk = _mm256_loadu_si256(reinterpret_cast<const __m256i*>(data + i));

            // Find positions matching first delimiter char
            __m256i eq0 = _mm256_cmpeq_epi8(chunk, vd0);
            uint32_t mask0 = _mm256_movemask_epi8(eq0);

            // For each match of first char, check if next char matches
            while (mask0) {
                uint32_t bit_pos = __builtin_ctz(mask0);
                size_t pos = i + bit_pos;

                if (pos + 1 < len && data[pos + 1] == d1) {
                    positions.push_back(pos);
                }

                mask0 &= mask0 - 1;  // Clear lowest set bit
            }

            i += 32;
        }

        // Handle remaining bytes with scalar
        while (i + 1 < len) {
            if (data[i] == d0 && data[i + 1] == d1) {
                positions.push_back(i);
            }
            ++i;
        }
    }
#endif

#if LEVEL_PIVOT_HAS_SSE2
    void find_delimiters_sse2(std::string_view key, size_t start,
                               std::vector<size_t>& positions) const {
        if (delimiter_.size() != 2) {
            find_delimiters_scalar(key, start, positions);
            return;
        }

        const char d0 = delimiter_[0];
        const char d1 = delimiter_[1];
        const char* data = key.data();
        const size_t len = key.size();

        // Broadcast delimiter chars to SSE2 registers
        __m128i vd0 = _mm_set1_epi8(d0);
        __m128i vd1 = _mm_set1_epi8(d1);

        size_t i = start;

        // Process 16 bytes at a time
        while (i + 16 <= len) {
            __m128i chunk = _mm_loadu_si128(reinterpret_cast<const __m128i*>(data + i));

            __m128i eq0 = _mm_cmpeq_epi8(chunk, vd0);
            uint32_t mask0 = _mm_movemask_epi8(eq0);

            while (mask0) {
                uint32_t bit_pos = __builtin_ctz(mask0);
                size_t pos = i + bit_pos;

                if (pos + 1 < len && data[pos + 1] == d1) {
                    positions.push_back(pos);
                }

                mask0 &= mask0 - 1;
            }

            i += 16;
        }

        // Handle remaining bytes
        while (i + 1 < len) {
            if (data[i] == d0 && data[i + 1] == d1) {
                positions.push_back(i);
            }
            ++i;
        }
    }
#endif

    void find_delimiters_scalar(std::string_view key, size_t start,
                                 std::vector<size_t>& positions) const {
        size_t pos = start;
        while ((pos = key.find(delimiter_, pos)) != std::string_view::npos) {
            positions.push_back(pos);
            pos += delimiter_.size();
        }
    }
};

/**
 * Even faster: compile pattern to direct offsets for fixed-structure keys
 *
 * For patterns where capture lengths are known or bounded, we can
 * avoid searching entirely and just validate at known positions.
 */
class CompiledKeyParser {
public:
    struct Segment {
        enum Type { LITERAL, CAPTURE, ATTR };
        Type type;
        size_t offset;      // Offset from key start (for literals)
        size_t length;      // Length (for literals)
        std::string_view text;  // For literals
    };

    // Build from pattern analysis
    static CompiledKeyParser compile(std::string_view pattern);

private:
    std::vector<Segment> segments_;
};

} // namespace level_pivot
