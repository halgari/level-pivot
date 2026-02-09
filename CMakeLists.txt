cmake_minimum_required(VERSION 3.16)
project(level_pivot VERSION 1.0 LANGUAGES CXX C)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_EXPORT_COMPILE_COMMANDS ON)

# Include custom CMake modules
list(APPEND CMAKE_MODULE_PATH "${CMAKE_CURRENT_SOURCE_DIR}/cmake")

# Cross-platform settings
include(CrossPlatform)

# Find PostgreSQL (always use our custom finder for server headers)
find_package(PostgreSQL REQUIRED)

# Find LevelDB - try vcpkg config first, then our custom finder
find_package(leveldb CONFIG QUIET)
if(leveldb_FOUND)
    message(STATUS "Found LevelDB via vcpkg config")
    # vcpkg provides leveldb::leveldb target
    set(LEVELDB_TARGET leveldb::leveldb)
else()
    find_package(LevelDB REQUIRED)
    set(LEVELDB_TARGET LevelDB::LevelDB)
endif()

# Core library (shared between FDW and tests)
add_library(level_pivot_core STATIC
    src/key_pattern.cpp
    src/key_parser.cpp
    src/projection.cpp
    src/type_converter.cpp
    src/pivot_scanner.cpp
    src/raw_scanner.cpp
    src/connection_manager.cpp
    src/writer.cpp
    src/raw_writer.cpp
    src/schema_discovery.cpp
)

target_include_directories(level_pivot_core PUBLIC
    ${CMAKE_CURRENT_SOURCE_DIR}/include
    ${PostgreSQL_INCLUDE_DIRS}
    ${PostgreSQL_SERVER_INCLUDE_DIRS}
)

# Add PostgreSQL's Windows port directories for Unix header compatibility
if(WIN32)
    target_include_directories(level_pivot_core PUBLIC
        ${PostgreSQL_SERVER_INCLUDE_DIRS}/port/win32
        ${PostgreSQL_SERVER_INCLUDE_DIRS}/port/win32_msvc
    )
endif()

target_link_libraries(level_pivot_core PUBLIC
    ${LEVELDB_TARGET}
)

# Optimize core library for performance
target_compile_options(level_pivot_core PRIVATE
    $<$<CXX_COMPILER_ID:GNU,Clang,AppleClang>:-O3>
    $<$<CXX_COMPILER_ID:MSVC>:/O2>
)

# FDW shared library
add_library(level_pivot MODULE
    src/level_pivot_fdw.cpp
    src/fdw_handler.cpp
    src/fdw_validator.cpp
    src/error.cpp
)

target_link_libraries(level_pivot PRIVATE
    level_pivot_core
)

# Platform-specific settings for the FDW module
if(WIN32)
    target_link_libraries(level_pivot PRIVATE ${PostgreSQL_LIBRARY})
    set_target_properties(level_pivot PROPERTIES
        PREFIX ""
        SUFFIX ".dll"
    )
elseif(APPLE)
    set_target_properties(level_pivot PROPERTIES
        PREFIX ""
        SUFFIX ".so"
        LINK_FLAGS "-bundle -bundle_loader ${PostgreSQL_LIBRARY_DIR}/../bin/postgres"
    )
else()
    set_target_properties(level_pivot PROPERTIES
        PREFIX ""
        SUFFIX ".so"
    )
endif()

# Enable Link-Time Optimization for release builds
include(CheckIPOSupported)
check_ipo_supported(RESULT ipo_supported OUTPUT ipo_error)
if(ipo_supported)
    set_property(TARGET level_pivot_core PROPERTY INTERPROCEDURAL_OPTIMIZATION TRUE)
    set_property(TARGET level_pivot PROPERTY INTERPROCEDURAL_OPTIMIZATION TRUE)
endif()

# Installation
find_program(PG_CONFIG_EXECUTABLE pg_config
    HINTS
        $ENV{PGDIR}/bin
        $ENV{PostgreSQL_ROOT}/bin
)

if(PG_CONFIG_EXECUTABLE)
    execute_process(
        COMMAND ${PG_CONFIG_EXECUTABLE} --pkglibdir
        OUTPUT_VARIABLE PostgreSQL_PKGLIB_DIR
        OUTPUT_STRIP_TRAILING_WHITESPACE
    )

    execute_process(
        COMMAND ${PG_CONFIG_EXECUTABLE} --sharedir
        OUTPUT_VARIABLE PostgreSQL_SHARE_DIR
        OUTPUT_STRIP_TRAILING_WHITESPACE
    )

    install(TARGETS level_pivot
        LIBRARY DESTINATION ${PostgreSQL_PKGLIB_DIR}
    )

    install(FILES
        sql/level_pivot--1.0.sql
        sql/level_pivot.control
        DESTINATION ${PostgreSQL_SHARE_DIR}/extension
    )
else()
    message(WARNING "pg_config not found, install targets may not work correctly")
endif()

# Testing
option(BUILD_TESTING "Build tests" ON)

if(BUILD_TESTING)
    enable_testing()

    # Try to find GTest
    find_package(GTest CONFIG QUIET)
    if(NOT GTest_FOUND)
        find_package(GTest QUIET)
    endif()

    if(GTest_FOUND)
        add_subdirectory(test/unit)
    else()
        message(STATUS "GTest not found, skipping unit tests")
    endif()

    # Integration tests (uses embedded PostgreSQL)
    add_subdirectory(test/integration)
endif()

# Benchmarks
option(BUILD_BENCHMARKS "Build benchmarks" OFF)

if(BUILD_BENCHMARKS)
    find_package(benchmark CONFIG QUIET)
    if(NOT benchmark_FOUND)
        find_package(benchmark QUIET)
    endif()

    if(benchmark_FOUND)
        add_subdirectory(test/benchmark)
    else()
        message(STATUS "Google Benchmark not found, skipping benchmarks")
    endif()
endif()

# Print configuration summary
message(STATUS "")
message(STATUS "level_pivot configuration:")
message(STATUS "  PostgreSQL include: ${PostgreSQL_INCLUDE_DIRS}")
message(STATUS "  PostgreSQL server include: ${PostgreSQL_SERVER_INCLUDE_DIRS}")
message(STATUS "  LevelDB target: ${LEVELDB_TARGET}")
if(PG_CONFIG_EXECUTABLE)
    message(STATUS "  Install lib dir: ${PostgreSQL_PKGLIB_DIR}")
    message(STATUS "  Install share dir: ${PostgreSQL_SHARE_DIR}")
endif()
message(STATUS "")
