# Integration tests using embedded PostgreSQL
#
# These tests download pre-built PostgreSQL binaries from
# zonkyio/embedded-postgres-binaries and run the FDW against
# a real PostgreSQL instance.
#
# Supported platforms: Linux (x86_64, ARM64), macOS (x86_64, ARM64)

find_program(BASH_EXECUTABLE bash)

if(BASH_EXECUTABLE)
    # Main integration test
    add_test(
        NAME integration_tests
        COMMAND ${BASH_EXECUTABLE} ${CMAKE_CURRENT_SOURCE_DIR}/run_tests.sh
        WORKING_DIRECTORY ${CMAKE_BINARY_DIR}
    )

    set_tests_properties(integration_tests PROPERTIES
        ENVIRONMENT "BUILD_DIR=${CMAKE_BINARY_DIR}"
        TIMEOUT 300
        LABELS "integration"
    )

    # Add a separate target for running integration tests
    add_custom_target(integration
        COMMAND ${CMAKE_CTEST_COMMAND} -R integration_tests --output-on-failure
        DEPENDS level_pivot
        WORKING_DIRECTORY ${CMAKE_BINARY_DIR}
        COMMENT "Running integration tests"
    )
else()
    message(STATUS "Bash not found, skipping integration tests")
endif()
