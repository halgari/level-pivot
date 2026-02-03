# FindLevelDB.cmake
# Finds LevelDB installation
#
# Sets:
#   LevelDB_FOUND
#   LEVELDB_INCLUDE_DIR
#   LEVELDB_LIBRARY
#   LevelDB::LevelDB (imported target)

find_path(LEVELDB_INCLUDE_DIR
    NAMES leveldb/db.h
    HINTS
        $ENV{LEVELDB_ROOT}/include
        /usr/local/include
        /usr/include
)

find_library(LEVELDB_LIBRARY
    NAMES leveldb libleveldb
    HINTS
        $ENV{LEVELDB_ROOT}/lib
        /usr/local/lib
        /usr/lib
        /usr/lib/x86_64-linux-gnu
)

include(FindPackageHandleStandardArgs)
find_package_handle_standard_args(LevelDB
    REQUIRED_VARS LEVELDB_LIBRARY LEVELDB_INCLUDE_DIR
)

if(LevelDB_FOUND AND NOT TARGET LevelDB::LevelDB)
    add_library(LevelDB::LevelDB UNKNOWN IMPORTED)
    set_target_properties(LevelDB::LevelDB PROPERTIES
        IMPORTED_LOCATION "${LEVELDB_LIBRARY}"
        INTERFACE_INCLUDE_DIRECTORIES "${LEVELDB_INCLUDE_DIR}"
    )
endif()

mark_as_advanced(LEVELDB_INCLUDE_DIR LEVELDB_LIBRARY)
