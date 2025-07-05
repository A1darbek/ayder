// slab_alloc.h
#ifndef SLAB_ALLOC_H
#define SLAB_ALLOC_H

#include <stddef.h>

/// Initialize the slab allocator.  Must be called exactly once
/// before any calls to slab_alloc/slab_free.
void slab_init(void);

/// Allocate at least `size` bytes.  Returns a pointer that can
/// later be freed with slab_free().
void *slab_alloc(size_t size);

/// Free a pointer returned by slab_alloc().
void slab_free(void *ptr);

/// Destroy the slab allocator, freeing all slab pages and any
/// malloc-fallback blocks that remain.
void slab_destroy(void);



#endif // SLAB_ALLOC_H
