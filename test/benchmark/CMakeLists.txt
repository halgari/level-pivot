# Benchmarks for level_pivot

add_executable(level_pivot_benchmarks
    bench_key_parser.cpp
    ${CMAKE_SOURCE_DIR}/src/key_pattern.cpp
    ${CMAKE_SOURCE_DIR}/src/key_parser.cpp
)

target_include_directories(level_pivot_benchmarks PRIVATE
    ${CMAKE_SOURCE_DIR}/include
)

# Link benchmark library - handle both vcpkg config mode and module mode
if(TARGET benchmark::benchmark)
    target_link_libraries(level_pivot_benchmarks PRIVATE
        benchmark::benchmark
        benchmark::benchmark_main
    )
elseif(TARGET benchmark)
    target_link_libraries(level_pivot_benchmarks PRIVATE
        benchmark
    )
else()
    message(FATAL_ERROR "Google Benchmark target not found")
endif()

# Compiler optimizations for accurate benchmarking
target_compile_options(level_pivot_benchmarks PRIVATE
    $<$<CXX_COMPILER_ID:GNU,Clang,AppleClang>:-O3>
    $<$<CXX_COMPILER_ID:MSVC>:/O2>
)
