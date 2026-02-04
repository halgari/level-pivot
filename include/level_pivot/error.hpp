#pragma once

#include <string>
#include <stdexcept>
#include <functional>

namespace level_pivot {

/**
 * Base exception class for level_pivot errors
 */
class LevelPivotError : public std::runtime_error {
public:
    explicit LevelPivotError(const std::string& msg) : std::runtime_error(msg) {}
};

/**
 * Error in LevelDB operations
 */
class LevelDBError : public LevelPivotError {
public:
    explicit LevelDBError(const std::string& msg) : LevelPivotError(msg) {}
};

/**
 * Error in FDW configuration/options
 */
class ConfigError : public LevelPivotError {
public:
    explicit ConfigError(const std::string& msg) : LevelPivotError(msg) {}
};

/**
 * Common exception handlers for PostgreSQL-C++ bridge
 *
 * This internal macro contains all the catch blocks shared between
 * PG_TRY_CPP and PG_TRY_CPP_RETURN. The return_stmt parameter is either
 * empty (for void functions) or "return default_val;" for functions
 * that return values.
 */
#define LEVEL_PIVOT_EXCEPTION_HANDLERS(return_stmt)                            \
    catch (const ::level_pivot::TypeConversionError& e) {                      \
        ereport(ERROR,                                                          \
            (errcode(ERRCODE_INVALID_TEXT_REPRESENTATION),                     \
             errmsg("level_pivot: %s", e.what())));                             \
        return_stmt                                                             \
    } catch (const ::level_pivot::KeyPatternError& e) {                        \
        ereport(ERROR,                                                          \
            (errcode(ERRCODE_INVALID_PARAMETER_VALUE),                         \
             errmsg("level_pivot: invalid key pattern: %s", e.what())));       \
        return_stmt                                                             \
    } catch (const ::level_pivot::LevelDBError& e) {                           \
        ereport(ERROR,                                                          \
            (errcode(ERRCODE_FDW_ERROR),                                        \
             errmsg("level_pivot: LevelDB error: %s", e.what())));              \
        return_stmt                                                             \
    } catch (const ::level_pivot::ConfigError& e) {                            \
        ereport(ERROR,                                                          \
            (errcode(ERRCODE_FDW_INVALID_OPTION_NAME),                         \
             errmsg("level_pivot: configuration error: %s", e.what())));       \
        return_stmt                                                             \
    } catch (const ::level_pivot::LevelPivotError& e) {                        \
        ereport(ERROR,                                                          \
            (errcode(ERRCODE_FDW_ERROR),                                        \
             errmsg("level_pivot: %s", e.what())));                             \
        return_stmt                                                             \
    } catch (const std::invalid_argument& e) {                                 \
        ereport(ERROR,                                                          \
            (errcode(ERRCODE_INVALID_PARAMETER_VALUE),                         \
             errmsg("level_pivot: invalid argument: %s", e.what())));          \
        return_stmt                                                             \
    } catch (const std::out_of_range& e) {                                     \
        ereport(ERROR,                                                          \
            (errcode(ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE),                      \
             errmsg("level_pivot: value out of range: %s", e.what())));        \
        return_stmt                                                             \
    } catch (const std::exception& e) {                                        \
        ereport(ERROR,                                                          \
            (errcode(ERRCODE_INTERNAL_ERROR),                                  \
             errmsg("level_pivot: internal error: %s", e.what())));            \
        return_stmt                                                             \
    } catch (...) {                                                            \
        ereport(ERROR,                                                          \
            (errcode(ERRCODE_INTERNAL_ERROR),                                  \
             errmsg("level_pivot: unknown internal error")));                  \
        return_stmt                                                             \
    }

/**
 * Convert C++ exceptions to PostgreSQL errors
 *
 * This macro catches C++ exceptions and converts them to PostgreSQL ereport()
 * calls. It should wrap any C++ code called from PostgreSQL C callbacks.
 *
 * Usage:
 *   PG_TRY_CPP({
 *       // C++ code that may throw
 *       do_something();
 *   });
 */
#define PG_TRY_CPP(block)                                                      \
    do {                                                                        \
        try {                                                                   \
            block                                                               \
        } LEVEL_PIVOT_EXCEPTION_HANDLERS(/* empty */)                          \
    } while (0)

/**
 * Helper for returning from a function after catching an exception
 *
 * Usage:
 *   Datum my_func(PG_FUNCTION_ARGS) {
 *       PG_TRY_CPP_RETURN({
 *           return do_something();
 *       }, (Datum)0);
 *   }
 */
#define PG_TRY_CPP_RETURN(block, default_val)                                  \
    do {                                                                        \
        try {                                                                   \
            block                                                               \
        } LEVEL_PIVOT_EXCEPTION_HANDLERS(return default_val;)                  \
    } while (0)

/**
 * RAII guard for PostgreSQL memory context
 *
 * Switches to the specified memory context and restores the previous
 * context when the guard goes out of scope.
 */
class MemoryContextGuard {
public:
    explicit MemoryContextGuard(void* new_context);
    ~MemoryContextGuard();

    // Non-copyable
    MemoryContextGuard(const MemoryContextGuard&) = delete;
    MemoryContextGuard& operator=(const MemoryContextGuard&) = delete;

private:
    void* old_context_;
};

} // namespace level_pivot
