# Unit tests for level_pivot

# Test executable - only tests that don't need PostgreSQL headers
add_executable(level_pivot_tests
    test_key_pattern.cpp
    test_key_parser.cpp
    test_raw_scanner.cpp
    test_notify.cpp
    test_main.cpp
    ${CMAKE_SOURCE_DIR}/src/key_pattern.cpp
    ${CMAKE_SOURCE_DIR}/src/key_parser.cpp
)

target_include_directories(level_pivot_tests PRIVATE
    ${CMAKE_SOURCE_DIR}/include
)

# Link GTest - handle both vcpkg config mode and module mode
if(TARGET GTest::gtest)
    target_link_libraries(level_pivot_tests PRIVATE
        GTest::gtest
        GTest::gtest_main
        level_pivot_core
    )
elseif(TARGET GTest::GTest)
    target_link_libraries(level_pivot_tests PRIVATE
        GTest::GTest
        GTest::Main
        level_pivot_core
    )
else()
    # Fallback for older CMake/GTest
    target_link_libraries(level_pivot_tests PRIVATE
        gtest
        gtest_main
        level_pivot_core
    )
endif()

# Register tests with CTest
include(GoogleTest)
gtest_discover_tests(level_pivot_tests)
