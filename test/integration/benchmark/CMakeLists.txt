# Benchmark tests for level_pivot FDW
#
# These benchmarks measure performance of various FDW operations
# against LevelDB with different dataset sizes.

find_program(BASH_EXECUTABLE bash)

if(BASH_EXECUTABLE)
    # Full benchmark suite (long running)
    add_test(
        NAME integration_benchmarks
        COMMAND ${BASH_EXECUTABLE} ${CMAKE_CURRENT_SOURCE_DIR}/run_benchmarks.sh ${CMAKE_BINARY_DIR}
        WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}
    )

    set_tests_properties(integration_benchmarks PROPERTIES
        ENVIRONMENT "BUILD_DIR=${CMAKE_BINARY_DIR}"
        TIMEOUT 1800
        LABELS "benchmark;integration"
    )

    # Custom target for full benchmark suite
    add_custom_target(benchmark-integration
        COMMAND ${BASH_EXECUTABLE} ${CMAKE_CURRENT_SOURCE_DIR}/run_benchmarks.sh ${CMAKE_BINARY_DIR}
        DEPENDS level_pivot
        WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}
        COMMENT "Running integration benchmarks (full suite)"
    )

    # Custom target for quick benchmark (1K rows, 1 iteration)
    add_custom_target(benchmark-integration-quick
        COMMAND ${CMAKE_COMMAND} -E env SIZES=1000 ITERATIONS=1
            ${BASH_EXECUTABLE} ${CMAKE_CURRENT_SOURCE_DIR}/run_benchmarks.sh ${CMAKE_BINARY_DIR}
        DEPENDS level_pivot
        WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}
        COMMENT "Running integration benchmarks (quick)"
    )
else()
    message(STATUS "Bash not found, skipping benchmark tests")
endif()
