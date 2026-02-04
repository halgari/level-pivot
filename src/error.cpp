/**
 * Implementation of error handling utilities
 */

extern "C" {
#include "postgres.h"
#include "utils/memutils.h"
}

#include "level_pivot/error.hpp"

namespace level_pivot {

MemoryContextGuard::MemoryContextGuard(void* new_context)
    : old_context_(MemoryContextSwitchTo(static_cast<MemoryContext>(new_context)))
{
}

MemoryContextGuard::~MemoryContextGuard()
{
    MemoryContextSwitchTo(static_cast<MemoryContext>(old_context_));
}

} // namespace level_pivot
