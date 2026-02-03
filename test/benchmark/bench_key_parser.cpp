#include <benchmark/benchmark.h>
#include "level_pivot/key_parser.hpp"
#include "level_pivot/simd_parser.hpp"
#include <string>
#include <vector>
#include <unordered_map>

using namespace level_pivot;

// ============================================================================
// Key Parsing Benchmarks
// ============================================================================

static void BM_KeyParser_Parse_Simple(benchmark::State& state) {
    KeyParser parser("users##{id}##{attr}");
    std::string key = "users##user001##email";

    for (auto _ : state) {
        auto result = parser.parse(key);
        benchmark::DoNotOptimize(result);
    }
    state.SetItemsProcessed(state.iterations());
}
BENCHMARK(BM_KeyParser_Parse_Simple);

static void BM_KeyParser_Parse_MultiSegment(benchmark::State& state) {
    KeyParser parser("users##{group}##{id}##{attr}");
    std::string key = "users##admins##user001##email";

    for (auto _ : state) {
        auto result = parser.parse(key);
        benchmark::DoNotOptimize(result);
    }
    state.SetItemsProcessed(state.iterations());
}
BENCHMARK(BM_KeyParser_Parse_MultiSegment);

static void BM_KeyParser_Parse_FiveCaptures(benchmark::State& state) {
    KeyParser parser("{tenant}##{env}##{service}##{region}##{id}##{attr}");
    std::string key = "acme##production##users##us-east-1##user12345##profile";

    for (auto _ : state) {
        auto result = parser.parse(key);
        benchmark::DoNotOptimize(result);
    }
    state.SetItemsProcessed(state.iterations());
}
BENCHMARK(BM_KeyParser_Parse_FiveCaptures);

static void BM_KeyParser_Parse_LongKey(benchmark::State& state) {
    KeyParser parser("users##{id}##{attr}");
    // Simulate a longer key with longer values
    std::string key = "users##user_with_very_long_identifier_12345678901234567890##email_address_field";

    for (auto _ : state) {
        auto result = parser.parse(key);
        benchmark::DoNotOptimize(result);
    }
    state.SetItemsProcessed(state.iterations());
}
BENCHMARK(BM_KeyParser_Parse_LongKey);

static void BM_KeyParser_Parse_MixedDelimiters(benchmark::State& state) {
    KeyParser parser("{tenant}:{env}/{service}/{attr}");
    std::string key = "acme:production/users/name";

    for (auto _ : state) {
        auto result = parser.parse(key);
        benchmark::DoNotOptimize(result);
    }
    state.SetItemsProcessed(state.iterations());
}
BENCHMARK(BM_KeyParser_Parse_MixedDelimiters);

static void BM_KeyParser_Parse_NoMatch(benchmark::State& state) {
    KeyParser parser("users##{id}##{attr}");
    std::string key = "products##item001##price";  // Wrong prefix

    for (auto _ : state) {
        auto result = parser.parse(key);
        benchmark::DoNotOptimize(result);
    }
    state.SetItemsProcessed(state.iterations());
}
BENCHMARK(BM_KeyParser_Parse_NoMatch);

// ============================================================================
// Zero-Copy Parsing Benchmarks (string_view)
// ============================================================================

static void BM_KeyParser_ParseView_Simple(benchmark::State& state) {
    KeyParser parser("users##{id}##{attr}");
    std::string key = "users##user001##email";

    for (auto _ : state) {
        auto result = parser.parse_view(key);
        benchmark::DoNotOptimize(result);
    }
    state.SetItemsProcessed(state.iterations());
}
BENCHMARK(BM_KeyParser_ParseView_Simple);

static void BM_KeyParser_ParseView_MultiSegment(benchmark::State& state) {
    KeyParser parser("users##{group}##{id}##{attr}");
    std::string key = "users##admins##user001##email";

    for (auto _ : state) {
        auto result = parser.parse_view(key);
        benchmark::DoNotOptimize(result);
    }
    state.SetItemsProcessed(state.iterations());
}
BENCHMARK(BM_KeyParser_ParseView_MultiSegment);

static void BM_KeyParser_ParseView_FiveCaptures(benchmark::State& state) {
    KeyParser parser("{tenant}##{env}##{service}##{region}##{id}##{attr}");
    std::string key = "acme##production##users##us-east-1##user12345##profile";

    for (auto _ : state) {
        auto result = parser.parse_view(key);
        benchmark::DoNotOptimize(result);
    }
    state.SetItemsProcessed(state.iterations());
}
BENCHMARK(BM_KeyParser_ParseView_FiveCaptures);

static void BM_KeyParser_ParseView_LongKey(benchmark::State& state) {
    KeyParser parser("users##{id}##{attr}");
    std::string key = "users##user_with_very_long_identifier_12345678901234567890##email_address_field";

    for (auto _ : state) {
        auto result = parser.parse_view(key);
        benchmark::DoNotOptimize(result);
    }
    state.SetItemsProcessed(state.iterations());
}
BENCHMARK(BM_KeyParser_ParseView_LongKey);

static void BM_KeyParser_ParseView_MixedDelimiters(benchmark::State& state) {
    KeyParser parser("{tenant}:{env}/{service}/{attr}");
    std::string key = "acme:production/users/name";

    for (auto _ : state) {
        auto result = parser.parse_view(key);
        benchmark::DoNotOptimize(result);
    }
    state.SetItemsProcessed(state.iterations());
}
BENCHMARK(BM_KeyParser_ParseView_MixedDelimiters);

static void BM_KeyParser_ParseView_NoMatch(benchmark::State& state) {
    KeyParser parser("users##{id}##{attr}");
    std::string key = "products##item001##price";  // Wrong prefix

    for (auto _ : state) {
        auto result = parser.parse_view(key);
        benchmark::DoNotOptimize(result);
    }
    state.SetItemsProcessed(state.iterations());
}
BENCHMARK(BM_KeyParser_ParseView_NoMatch);

// ============================================================================
// Key Building Benchmarks
// ============================================================================

static void BM_KeyParser_Build_Simple(benchmark::State& state) {
    KeyParser parser("users##{id}##{attr}");
    std::vector<std::string> captures = {"user001"};
    std::string attr = "email";

    for (auto _ : state) {
        auto result = parser.build(captures, attr);
        benchmark::DoNotOptimize(result);
    }
    state.SetItemsProcessed(state.iterations());
}
BENCHMARK(BM_KeyParser_Build_Simple);

static void BM_KeyParser_Build_MultiSegment(benchmark::State& state) {
    KeyParser parser("users##{group}##{id}##{attr}");
    std::vector<std::string> captures = {"admins", "user001"};
    std::string attr = "email";

    for (auto _ : state) {
        auto result = parser.build(captures, attr);
        benchmark::DoNotOptimize(result);
    }
    state.SetItemsProcessed(state.iterations());
}
BENCHMARK(BM_KeyParser_Build_MultiSegment);

static void BM_KeyParser_Build_FiveCaptures(benchmark::State& state) {
    KeyParser parser("{tenant}##{env}##{service}##{region}##{id}##{attr}");
    std::vector<std::string> captures = {"acme", "production", "users", "us-east-1", "user12345"};
    std::string attr = "profile";

    for (auto _ : state) {
        auto result = parser.build(captures, attr);
        benchmark::DoNotOptimize(result);
    }
    state.SetItemsProcessed(state.iterations());
}
BENCHMARK(BM_KeyParser_Build_FiveCaptures);

static void BM_KeyParser_Build_NamedCaptures(benchmark::State& state) {
    KeyParser parser("users##{group}##{id}##{attr}");
    std::unordered_map<std::string, std::string> captures = {
        {"group", "admins"},
        {"id", "user001"}
    };
    std::string attr = "email";

    for (auto _ : state) {
        auto result = parser.build(captures, attr);
        benchmark::DoNotOptimize(result);
    }
    state.SetItemsProcessed(state.iterations());
}
BENCHMARK(BM_KeyParser_Build_NamedCaptures);

// ============================================================================
// Prefix Building Benchmarks
// ============================================================================

static void BM_KeyParser_BuildPrefix_Empty(benchmark::State& state) {
    KeyParser parser("users##{group}##{id}##{attr}");

    for (auto _ : state) {
        auto result = parser.build_prefix();
        benchmark::DoNotOptimize(result);
    }
    state.SetItemsProcessed(state.iterations());
}
BENCHMARK(BM_KeyParser_BuildPrefix_Empty);

static void BM_KeyParser_BuildPrefix_WithCaptures(benchmark::State& state) {
    KeyParser parser("users##{group}##{id}##{attr}");
    std::vector<std::string> captures = {"admins", "user001"};

    for (auto _ : state) {
        auto result = parser.build_prefix(captures);
        benchmark::DoNotOptimize(result);
    }
    state.SetItemsProcessed(state.iterations());
}
BENCHMARK(BM_KeyParser_BuildPrefix_WithCaptures);

static void BM_KeyParser_BuildPrefix_Partial(benchmark::State& state) {
    KeyParser parser("users##{group}##{id}##{attr}");
    std::vector<std::string> captures = {"admins"};  // Only first capture

    for (auto _ : state) {
        auto result = parser.build_prefix(captures);
        benchmark::DoNotOptimize(result);
    }
    state.SetItemsProcessed(state.iterations());
}
BENCHMARK(BM_KeyParser_BuildPrefix_Partial);

// ============================================================================
// Matching/Filtering Benchmarks
// ============================================================================

static void BM_KeyParser_Matches_Hit(benchmark::State& state) {
    KeyParser parser("users##{id}##{attr}");
    std::string key = "users##user001##email";

    for (auto _ : state) {
        bool result = parser.matches(key);
        benchmark::DoNotOptimize(result);
    }
    state.SetItemsProcessed(state.iterations());
}
BENCHMARK(BM_KeyParser_Matches_Hit);

static void BM_KeyParser_Matches_Miss(benchmark::State& state) {
    KeyParser parser("users##{id}##{attr}");
    std::string key = "products##item001##price";

    for (auto _ : state) {
        bool result = parser.matches(key);
        benchmark::DoNotOptimize(result);
    }
    state.SetItemsProcessed(state.iterations());
}
BENCHMARK(BM_KeyParser_Matches_Miss);

static void BM_KeyParser_StartsWithPrefix(benchmark::State& state) {
    KeyParser parser("users##{id}##{attr}");
    std::string key = "users##user001##email";

    for (auto _ : state) {
        bool result = parser.starts_with_prefix(key);
        benchmark::DoNotOptimize(result);
    }
    state.SetItemsProcessed(state.iterations());
}
BENCHMARK(BM_KeyParser_StartsWithPrefix);

// ============================================================================
// Baseline Comparisons
// ============================================================================

static void BM_Baseline_StringSubstr(benchmark::State& state) {
    std::string key = "users##admins##user001##email";
    size_t start = 7;  // After "users##"
    size_t len = 6;    // "admins"

    for (auto _ : state) {
        auto result = key.substr(start, len);
        benchmark::DoNotOptimize(result);
    }
    state.SetItemsProcessed(state.iterations());
}
BENCHMARK(BM_Baseline_StringSubstr);

static void BM_Baseline_StringFind(benchmark::State& state) {
    std::string key = "users##admins##user001##email";
    std::string delim = "##";

    for (auto _ : state) {
        auto pos = key.find(delim, 7);
        benchmark::DoNotOptimize(pos);
    }
    state.SetItemsProcessed(state.iterations());
}
BENCHMARK(BM_Baseline_StringFind);

static void BM_Baseline_StringCompare(benchmark::State& state) {
    std::string key = "users##admins##user001##email";
    std::string prefix = "users##";

    for (auto _ : state) {
        bool result = key.compare(0, prefix.size(), prefix) == 0;
        benchmark::DoNotOptimize(result);
    }
    state.SetItemsProcessed(state.iterations());
}
BENCHMARK(BM_Baseline_StringCompare);

static void BM_Baseline_HashMapLookup(benchmark::State& state) {
    std::unordered_map<std::string, std::string> map = {
        {"group", "admins"},
        {"id", "user001"},
        {"email", "test@example.com"}
    };
    std::string key = "id";

    for (auto _ : state) {
        auto it = map.find(key);
        benchmark::DoNotOptimize(it);
    }
    state.SetItemsProcessed(state.iterations());
}
BENCHMARK(BM_Baseline_HashMapLookup);

static void BM_Baseline_HashMapInsert(benchmark::State& state) {
    std::string key = "email";
    std::string value = "test@example.com";

    for (auto _ : state) {
        std::unordered_map<std::string, std::string> map;
        map[key] = value;
        benchmark::DoNotOptimize(map);
    }
    state.SetItemsProcessed(state.iterations());
}
BENCHMARK(BM_Baseline_HashMapInsert);

static void BM_Baseline_VectorPushBack(benchmark::State& state) {
    std::string value = "user001";

    for (auto _ : state) {
        std::vector<std::string> vec;
        vec.reserve(3);
        vec.push_back(value);
        vec.push_back(value);
        vec.push_back(value);
        benchmark::DoNotOptimize(vec);
    }
    state.SetItemsProcessed(state.iterations());
}
BENCHMARK(BM_Baseline_VectorPushBack);

// ============================================================================
// SIMD-Accelerated Parsing Benchmarks
// ============================================================================

static void BM_SimdParser_Simple(benchmark::State& state) {
    // Pattern: users##{id}##{attr}
    SimdKeyParser parser("users", "##", 1);
    std::string key = "users##user001##email";

    for (auto _ : state) {
        auto result = parser.parse(key);
        benchmark::DoNotOptimize(result);
    }
    state.SetItemsProcessed(state.iterations());
}
BENCHMARK(BM_SimdParser_Simple);

static void BM_SimdParser_MultiSegment(benchmark::State& state) {
    // Pattern: users##{group}##{id}##{attr}
    SimdKeyParser parser("users", "##", 2);
    std::string key = "users##admins##user001##email";

    for (auto _ : state) {
        auto result = parser.parse(key);
        benchmark::DoNotOptimize(result);
    }
    state.SetItemsProcessed(state.iterations());
}
BENCHMARK(BM_SimdParser_MultiSegment);

static void BM_SimdParser_FiveCaptures(benchmark::State& state) {
    // Pattern: {tenant}##{env}##{service}##{region}##{id}##{attr}
    // Note: No prefix in this pattern, so we use empty prefix
    SimdKeyParser parser("", "##", 5);
    std::string key = "acme##production##users##us-east-1##user12345##profile";

    for (auto _ : state) {
        auto result = parser.parse(key);
        benchmark::DoNotOptimize(result);
    }
    state.SetItemsProcessed(state.iterations());
}
BENCHMARK(BM_SimdParser_FiveCaptures);

static void BM_SimdParser_LongKey(benchmark::State& state) {
    SimdKeyParser parser("users", "##", 1);
    std::string key = "users##user_with_very_long_identifier_12345678901234567890##email_address_field";

    for (auto _ : state) {
        auto result = parser.parse(key);
        benchmark::DoNotOptimize(result);
    }
    state.SetItemsProcessed(state.iterations());
}
BENCHMARK(BM_SimdParser_LongKey);

static void BM_SimdParser_NoMatch(benchmark::State& state) {
    SimdKeyParser parser("users", "##", 1);
    std::string key = "products##item001##price";  // Wrong prefix

    for (auto _ : state) {
        auto result = parser.parse(key);
        benchmark::DoNotOptimize(result);
    }
    state.SetItemsProcessed(state.iterations());
}
BENCHMARK(BM_SimdParser_NoMatch);

// Comparison with very long keys where SIMD shines
static void BM_KeyParser_ParseView_VeryLongKey(benchmark::State& state) {
    KeyParser parser("data##{id}##{attr}");
    std::string key = "data##" + std::string(200, 'x') + "##" + std::string(100, 'y');

    for (auto _ : state) {
        auto result = parser.parse_view(key);
        benchmark::DoNotOptimize(result);
    }
    state.SetItemsProcessed(state.iterations());
    state.SetBytesProcessed(state.iterations() * key.size());
}
BENCHMARK(BM_KeyParser_ParseView_VeryLongKey);

static void BM_SimdParser_VeryLongKey(benchmark::State& state) {
    SimdKeyParser parser("data", "##", 1);
    std::string key = "data##" + std::string(200, 'x') + "##" + std::string(100, 'y');

    for (auto _ : state) {
        auto result = parser.parse(key);
        benchmark::DoNotOptimize(result);
    }
    state.SetItemsProcessed(state.iterations());
    state.SetBytesProcessed(state.iterations() * key.size());
}
BENCHMARK(BM_SimdParser_VeryLongKey);

// Ultra-fast parsing with pre-allocated arrays
static void BM_SimdParser_Fast_Simple(benchmark::State& state) {
    SimdKeyParser parser("users", "##", 1);
    std::string key = "users##user001##email";
    std::string_view captures[1];
    std::string_view attr;

    for (auto _ : state) {
        bool ok = parser.parse_fast(key, captures, attr);
        benchmark::DoNotOptimize(ok);
        benchmark::DoNotOptimize(captures[0]);
        benchmark::DoNotOptimize(attr);
    }
    state.SetItemsProcessed(state.iterations());
}
BENCHMARK(BM_SimdParser_Fast_Simple);

static void BM_SimdParser_Fast_MultiSegment(benchmark::State& state) {
    SimdKeyParser parser("users", "##", 2);
    std::string key = "users##admins##user001##email";
    std::string_view captures[2];
    std::string_view attr;

    for (auto _ : state) {
        bool ok = parser.parse_fast(key, captures, attr);
        benchmark::DoNotOptimize(ok);
        benchmark::DoNotOptimize(captures);
        benchmark::DoNotOptimize(attr);
    }
    state.SetItemsProcessed(state.iterations());
}
BENCHMARK(BM_SimdParser_Fast_MultiSegment);

static void BM_SimdParser_Fast_FiveCaptures(benchmark::State& state) {
    SimdKeyParser parser("", "##", 5);
    std::string key = "acme##production##users##us-east-1##user12345##profile";
    std::string_view captures[5];
    std::string_view attr;

    for (auto _ : state) {
        bool ok = parser.parse_fast(key, captures, attr);
        benchmark::DoNotOptimize(ok);
        benchmark::DoNotOptimize(captures);
        benchmark::DoNotOptimize(attr);
    }
    state.SetItemsProcessed(state.iterations());
}
BENCHMARK(BM_SimdParser_Fast_FiveCaptures);

static void BM_SimdParser_Fast_NoMatch(benchmark::State& state) {
    SimdKeyParser parser("users", "##", 1);
    std::string key = "products##item001##price";
    std::string_view captures[1];
    std::string_view attr;

    for (auto _ : state) {
        bool ok = parser.parse_fast(key, captures, attr);
        benchmark::DoNotOptimize(ok);
    }
    state.SetItemsProcessed(state.iterations());
}
BENCHMARK(BM_SimdParser_Fast_NoMatch);

BENCHMARK_MAIN();
