# FindPostgreSQL.cmake
# Finds PostgreSQL installation using pg_config
#
# Sets:
#   PostgreSQL_FOUND
#   PostgreSQL_INCLUDE_DIRS
#   PostgreSQL_SERVER_INCLUDE_DIRS
#   PostgreSQL_LIBRARY_DIRS
#   PostgreSQL_LIBRARIES
#   PostgreSQL_VERSION

find_program(PG_CONFIG pg_config
    HINTS
        $ENV{PGDIR}/bin
        $ENV{PostgreSQL_ROOT}/bin
        /usr/local/pgsql/bin
        /usr/pgsql-*/bin
)

if(NOT PG_CONFIG)
    message(FATAL_ERROR "pg_config not found. Set PostgreSQL_ROOT or ensure pg_config is in PATH.")
endif()

# Get include directories
execute_process(
    COMMAND ${PG_CONFIG} --includedir
    OUTPUT_VARIABLE PostgreSQL_INCLUDE_DIR
    OUTPUT_STRIP_TRAILING_WHITESPACE
)

execute_process(
    COMMAND ${PG_CONFIG} --includedir-server
    OUTPUT_VARIABLE PostgreSQL_SERVER_INCLUDE_DIR
    OUTPUT_STRIP_TRAILING_WHITESPACE
)

# Get library directory
execute_process(
    COMMAND ${PG_CONFIG} --libdir
    OUTPUT_VARIABLE PostgreSQL_LIBRARY_DIR
    OUTPUT_STRIP_TRAILING_WHITESPACE
)

# Get version
execute_process(
    COMMAND ${PG_CONFIG} --version
    OUTPUT_VARIABLE PostgreSQL_VERSION_STRING
    OUTPUT_STRIP_TRAILING_WHITESPACE
)

string(REGEX REPLACE "PostgreSQL ([0-9]+\\.[0-9]+).*" "\\1" PostgreSQL_VERSION "${PostgreSQL_VERSION_STRING}")

# Find library (needed on Windows)
if(WIN32)
    find_library(PostgreSQL_LIBRARY
        NAMES postgres libpq
        HINTS ${PostgreSQL_LIBRARY_DIR}
    )
endif()

# Set output variables
set(PostgreSQL_INCLUDE_DIRS ${PostgreSQL_INCLUDE_DIR})
set(PostgreSQL_SERVER_INCLUDE_DIRS ${PostgreSQL_SERVER_INCLUDE_DIR})
set(PostgreSQL_LIBRARY_DIRS ${PostgreSQL_LIBRARY_DIR})

include(FindPackageHandleStandardArgs)
find_package_handle_standard_args(PostgreSQL
    REQUIRED_VARS PostgreSQL_INCLUDE_DIRS PostgreSQL_SERVER_INCLUDE_DIRS
    VERSION_VAR PostgreSQL_VERSION
)

mark_as_advanced(
    PG_CONFIG
    PostgreSQL_INCLUDE_DIR
    PostgreSQL_SERVER_INCLUDE_DIR
    PostgreSQL_LIBRARY_DIR
)
