#pragma once

extern "C" {
#include "postgres.h"
#include "utils/memutils.h"
}

#include <new>
#include <type_traits>
#include <utility>

namespace level_pivot {

namespace detail {

/**
 * Destructor callback registered with PostgreSQL memory context.
 * Called when the memory context is reset or deleted.
 */
template<typename T>
void pg_destruct_callback(void* arg)
{
    T* obj = static_cast<T*>(arg);
    obj->~T();
}

} // namespace detail

/**
 * Construct a C++ object in PostgreSQL-managed memory with automatic cleanup.
 *
 * This function:
 * 1. Allocates memory via palloc in the specified memory context
 * 2. Uses placement new to construct the object
 * 3. Registers a MemoryContextCallback to call the destructor when the
 *    context is reset or deleted
 *
 * This ensures C++ destructors run even if PostgreSQL errors (longjmp)
 * bypass normal C++ stack unwinding.
 *
 * @tparam T The type of object to construct
 * @tparam Args Constructor argument types
 * @param ctx The PostgreSQL memory context to allocate in
 * @param args Constructor arguments
 * @return Pointer to the constructed object
 */
template<typename T, typename... Args>
T* pg_construct(MemoryContext ctx, Args&&... args)
{
    MemoryContext old_ctx = MemoryContextSwitchTo(ctx);

    // Allocate memory for the object and the callback structure
    void* mem = palloc(sizeof(T));
    MemoryContextCallback* cb = static_cast<MemoryContextCallback*>(
        palloc(sizeof(MemoryContextCallback)));

    MemoryContextSwitchTo(old_ctx);

    // Construct the object using placement new
    T* obj = new (mem) T(std::forward<Args>(args)...);

    // Register callback to call destructor on context cleanup
    cb->func = detail::pg_destruct_callback<T>;
    cb->arg = obj;
    MemoryContextRegisterResetCallback(ctx, cb);

    return obj;
}

/**
 * Stack-based temporary array with palloc fallback for larger sizes.
 *
 * Uses stack storage for small arrays (up to StackSize elements),
 * falling back to palloc for larger arrays. RAII cleanup ensures
 * palloc'd memory is freed even if exceptions occur.
 *
 * @tparam T Element type
 * @tparam StackSize Maximum number of elements to store on stack (default 64)
 */
template<typename T, size_t StackSize = 64>
class TempArray {
public:
    explicit TempArray(size_t size)
        : size_(size)
        , heap_data_(nullptr)
    {
        if (size_ <= StackSize) {
            data_ = stack_data_;
        } else {
            heap_data_ = static_cast<T*>(palloc(size_ * sizeof(T)));
            data_ = heap_data_;
        }
    }

    ~TempArray()
    {
        if (heap_data_) {
            pfree(heap_data_);
        }
    }

    // Non-copyable
    TempArray(const TempArray&) = delete;
    TempArray& operator=(const TempArray&) = delete;

    // Accessors
    T* data() { return data_; }
    const T* data() const { return data_; }
    size_t size() const { return size_; }

    T& operator[](size_t i) { return data_[i]; }
    const T& operator[](size_t i) const { return data_[i]; }

private:
    size_t size_;
    T* data_;
    T* heap_data_;
    T stack_data_[StackSize];
};

} // namespace level_pivot
