#pragma once

#include <string_view>
#include <vector>
#include <optional>
#include <cstdint>
#include <cstring>

#if defined(__x86_64__) || defined(_M_X64)
#define LEVEL_PIVOT_X86_64 1
#include <immintrin.h>
#if defined(_MSC_VER)
#include <intrin.h>
#endif
#endif

namespace level_pivot {

// =============================================================================
// CPU Feature Detection (runs once, cached)
// =============================================================================

namespace detail {

struct CpuFeatures {
    bool has_sse2 = false;
    bool has_avx2 = false;

    static const CpuFeatures& get() {
        static const CpuFeatures instance = detect();
        return instance;
    }

private:
    static CpuFeatures detect() {
        CpuFeatures f;
#if defined(LEVEL_PIVOT_X86_64)
#if defined(_MSC_VER)
        int cpuInfo[4];
        __cpuid(cpuInfo, 0);
        int nIds = cpuInfo[0];

        if (nIds >= 1) {
            __cpuid(cpuInfo, 1);
            f.has_sse2 = (cpuInfo[3] & (1 << 26)) != 0;
        }
        if (nIds >= 7) {
            __cpuidex(cpuInfo, 7, 0);
            f.has_avx2 = (cpuInfo[1] & (1 << 5)) != 0;
        }
#else
        // GCC/Clang
        __builtin_cpu_init();
        f.has_sse2 = __builtin_cpu_supports("sse2");
        f.has_avx2 = __builtin_cpu_supports("avx2");
#endif
#endif
        return f;
    }
};

} // namespace detail

// =============================================================================
// SIMD Implementation Functions (with target attributes for runtime dispatch)
// =============================================================================

namespace detail {

// Scalar implementation (always available)
inline void find_delimiters_scalar(
    const char* data, size_t len, size_t start,
    char d0, char d1, size_t delim_len,
    size_t* positions, size_t& count, size_t max_count)
{
    if (delim_len == 2) {
        size_t i = start;
        count = 0;
        while (i + 1 < len && count < max_count) {
            if (data[i] == d0 && data[i + 1] == d1) {
                positions[count++] = i;
                i += 2;
            } else {
                ++i;
            }
        }
    } else {
        // General case - use string_view::find
        std::string_view key(data, len);
        std::string_view delim(&d0, delim_len);
        size_t pos = start;
        count = 0;
        while ((pos = key.find(delim, pos)) != std::string_view::npos && count < max_count) {
            positions[count++] = pos;
            pos += delim_len;
        }
    }
}

#if defined(LEVEL_PIVOT_X86_64)

// SSE2 implementation
#if defined(_MSC_VER)
inline void find_delimiters_sse2(
#else
__attribute__((target("sse2")))
inline void find_delimiters_sse2(
#endif
    const char* data, size_t len, size_t start,
    char d0, char d1, size_t delim_len,
    size_t* positions, size_t& count, size_t max_count)
{
    if (delim_len != 2) {
        find_delimiters_scalar(data, len, start, d0, d1, delim_len, positions, count, max_count);
        return;
    }

    __m128i vd0 = _mm_set1_epi8(d0);
    size_t i = start;
    count = 0;

    while (i + 16 <= len && count < max_count) {
        __m128i chunk = _mm_loadu_si128(reinterpret_cast<const __m128i*>(data + i));
        __m128i eq0 = _mm_cmpeq_epi8(chunk, vd0);
        uint32_t mask0 = static_cast<uint32_t>(_mm_movemask_epi8(eq0));

        while (mask0 && count < max_count) {
#if defined(_MSC_VER)
            unsigned long bit_pos;
            _BitScanForward(&bit_pos, mask0);
#else
            uint32_t bit_pos = static_cast<uint32_t>(__builtin_ctz(mask0));
#endif
            size_t pos = i + bit_pos;

            if (pos + 1 < len && data[pos + 1] == d1) {
                positions[count++] = pos;
            }
            mask0 &= mask0 - 1;
        }
        i += 16;
    }

    // Handle tail
    while (i + 1 < len && count < max_count) {
        if (data[i] == d0 && data[i + 1] == d1) {
            positions[count++] = i;
        }
        ++i;
    }
}

// AVX2 implementation
#if defined(_MSC_VER)
inline void find_delimiters_avx2(
#else
__attribute__((target("avx2")))
inline void find_delimiters_avx2(
#endif
    const char* data, size_t len, size_t start,
    char d0, char d1, size_t delim_len,
    size_t* positions, size_t& count, size_t max_count)
{
    if (delim_len != 2) {
        find_delimiters_scalar(data, len, start, d0, d1, delim_len, positions, count, max_count);
        return;
    }

    __m256i vd0 = _mm256_set1_epi8(d0);
    size_t i = start;
    count = 0;

    while (i + 32 <= len && count < max_count) {
        __m256i chunk = _mm256_loadu_si256(reinterpret_cast<const __m256i*>(data + i));
        __m256i eq0 = _mm256_cmpeq_epi8(chunk, vd0);
        uint32_t mask0 = static_cast<uint32_t>(_mm256_movemask_epi8(eq0));

        while (mask0 && count < max_count) {
#if defined(_MSC_VER)
            unsigned long bit_pos;
            _BitScanForward(&bit_pos, mask0);
#else
            uint32_t bit_pos = static_cast<uint32_t>(__builtin_ctz(mask0));
#endif
            size_t pos = i + bit_pos;

            if (pos + 1 < len && data[pos + 1] == d1) {
                positions[count++] = pos;
            }
            mask0 &= mask0 - 1;
        }
        i += 32;
    }

    // Handle tail with SSE2 or scalar
    while (i + 1 < len && count < max_count) {
        if (data[i] == d0 && data[i + 1] == d1) {
            positions[count++] = i;
        }
        ++i;
    }
}

#endif // LEVEL_PIVOT_X86_64

// Function pointer type for delimiter finding
using FindDelimitersFn = void(*)(
    const char* data, size_t len, size_t start,
    char d0, char d1, size_t delim_len,
    size_t* positions, size_t& count, size_t max_count);

// Runtime dispatcher - selects best implementation once
inline FindDelimitersFn select_find_delimiters() {
#if defined(LEVEL_PIVOT_X86_64)
    const auto& cpu = CpuFeatures::get();
    if (cpu.has_avx2) return find_delimiters_avx2;
    if (cpu.has_sse2) return find_delimiters_sse2;
#endif
    return find_delimiters_scalar;
}

// Global function pointer - initialized once on first use
inline FindDelimitersFn get_find_delimiters() {
    static const FindDelimitersFn fn = select_find_delimiters();
    return fn;
}

} // namespace detail

/**
 * SIMD-optimized key parser for patterns with a single repeated delimiter
 *
 * This is a specialized fast-path for common patterns like:
 *   prefix##capture1##capture2##...##attr
 *
 * Uses runtime CPU detection to select SSE2/AVX2/scalar implementation.
 * Detection happens once at startup; subsequent calls have zero overhead.
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
        : prefix_(prefix)
        , delimiter_(delimiter)
        , num_captures_(num_captures)
        , num_delimiters_(num_captures + 1)
        , find_delimiters_(detail::get_find_delimiters())
    {
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

        // Find all delimiter positions using runtime-selected SIMD
        size_t search_start = prefix_.size();
        size_t delim_stack[17];
        size_t delim_count = 0;

        find_delimiters_(
            key.data(), key.size(), search_start,
            delimiter_[0], delimiter_.size() > 1 ? delimiter_[1] : '\0', delimiter_.size(),
            delim_stack, delim_count, num_delimiters_ + 1);

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

        find_delimiters_(
            key.data(), key.size(), search_start,
            delimiter_[0], delimiter_.size() > 1 ? delimiter_[1] : '\0', delimiter_.size(),
            delim_stack, delim_count, num_delimiters_ + 1);

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
     * Get the name of the SIMD implementation being used
     */
    static const char* implementation_name() {
#if defined(LEVEL_PIVOT_X86_64)
        const auto& cpu = detail::CpuFeatures::get();
        if (cpu.has_avx2) return "AVX2";
        if (cpu.has_sse2) return "SSE2";
#endif
        return "scalar";
    }

private:
    std::string_view prefix_;
    std::string_view delimiter_;
    size_t num_captures_;
    size_t num_delimiters_;
    detail::FindDelimitersFn find_delimiters_;
};

} // namespace level_pivot
