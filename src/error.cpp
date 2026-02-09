/**
 * error.cpp - PostgreSQL error handling and memory context utilities
 *
 * PostgreSQL has its own error handling system (ereport/elog) and memory
 * management (MemoryContext). This module provides RAII wrappers that
 * integrate C++ with PostgreSQL's patterns.
 *
 * MemoryContextGuard: Temporarily switches memory context and restores
 * on scope exit. This is important because PostgreSQL allocations go into
 * the "current" context, which determines their lifetime.
 *
 * Example: When building tuples, we switch to a short-lived context that
 * gets reset after each row, avoiding memory accumulation during large scans.
 */

extern "C" {
#include "postgres.h"
#include "utils/memutils.h"
}

#include "level_pivot/error.hpp"

namespace level_pivot {

/**
 * Saves current context and switches to new_context.
 * Uses void* to avoid PostgreSQL headers in the hpp file.
 */
MemoryContextGuard::MemoryContextGuard(void* new_context)
    : old_context_(MemoryContextSwitchTo(static_cast<MemoryContext>(new_context)))
{
}

/**
 * Restores the previous memory context.
 * This runs even if an exception is thrown (RAII pattern).
 */
MemoryContextGuard::~MemoryContextGuard()
{
    MemoryContextSwitchTo(static_cast<MemoryContext>(old_context_));
}

} // namespace level_pivot
