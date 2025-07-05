// slab_alloc.c - FIXED VERSION - Proper pointer validation
#define _POSIX_C_SOURCE 200112L
#include "slab_alloc.h"
#include <stdlib.h>
#include <stdint.h>
#include <unistd.h>
#include <string.h>
#include <assert.h>

/// --- Configuration ---
/// List of size-classes (bytes); must be ascending.
static const size_t size_classes[] = {
        64, 128, 256, 512, 1024, 2048, 4096
};
#define NUM_CLASSES \
    (sizeof(size_classes)/sizeof(size_classes[0]))

/// We'll carve each slab page from a 64 KiB chunk
static size_t PAGE_SIZE;

/// Magic number to identify valid slab pages
#define SLAB_MAGIC 0xDEADBEEF

/// Header prepended to each block in a slab
typedef struct slab_header {
    struct slab_header *next_free;
} slab_header;

/// Metadata at start of each slab page - FIXED with proper validation
typedef struct slab_page {
    uint32_t              magic;       // Magic number for validation
    struct slab_page     *next;
    int                   class_idx;
    size_t                block_size;  // Cache the block size
    void                 *page_start;  // Start of actual page memory
    void                 *page_end;    // End of actual page memory
} slab_page;

/// One slab class descriptor
typedef struct {
    size_t         block_size;    // size_classes[i]
    slab_header   *free_list;     // singly-linked free blocks
    slab_page     *pages;         // list of allocated pages
} slab_class;

static slab_class classes[NUM_CLASSES];
static int slab_initialized = 0;

void slab_init(void) {
    if (slab_initialized) return;

    // Determine page size (multiple of OS page size).
    long ps = sysconf(_SC_PAGESIZE);
    if (ps <= 0) ps = 4096;
    PAGE_SIZE = (size_t)ps * 16;        // 64 KiB on 4 KiB pages

    // Initialize each size class
    for (int i = 0; i < NUM_CLASSES; i++) {
        classes[i].block_size = size_classes[i];
        classes[i].free_list  = NULL;
        classes[i].pages      = NULL;
    }

    slab_initialized = 1;
}

static int find_class(size_t size) {
    for (int i = 0; i < NUM_CLASSES; i++) {
        if (size <= classes[i].block_size)
            return i;
    }
    return -1;  // larger than max class
}

// FIXED: Proper page validation - iterate through known pages instead of address masking
static slab_page* find_page_for_ptr(void *ptr) {
    if (!ptr || !slab_initialized) return NULL;

    // Search through all allocated pages instead of using address arithmetic
    for (int ci = 0; ci < NUM_CLASSES; ci++) {
        slab_page *pg = classes[ci].pages;
        while (pg) {
            // Check if pointer is within this page's bounds
            if (ptr >= pg->page_start && ptr < pg->page_end) {
                // Double-check magic number for safety
                if (pg->magic == SLAB_MAGIC) {
                    return pg;
                }
            }
            pg = pg->next;
        }
    }

    return NULL;  // Not found in any slab page
}

void *slab_alloc(size_t size) {
    if (!slab_initialized) slab_init();

    int ci = find_class(size);
    if (ci < 0) {
        // fallback for large allocations
        return malloc(size);
    }
    slab_class *cl = &classes[ci];

    // Refill from a new page if empty
    if (!cl->free_list) {
        // allocate one big slab page, page-aligned
        void *mem;
        if (posix_memalign(&mem, PAGE_SIZE, PAGE_SIZE) != 0)
            return malloc(size);  // fallback on failure

        // Clear the entire page
        memset(mem, 0, PAGE_SIZE);

        slab_page *pg = (slab_page*)mem;
        pg->magic     = SLAB_MAGIC;
        pg->next      = cl->pages;
        pg->class_idx = ci;
        pg->block_size = cl->block_size;
        pg->page_start = mem;
        pg->page_end   = (char*)mem + PAGE_SIZE;
        cl->pages     = pg;

        // carve it into blocks - FIXED calculation
        size_t header_sz = sizeof(slab_header);
        size_t bs        = cl->block_size + header_sz;
        size_t usable_space = PAGE_SIZE - sizeof(slab_page);

        // Ensure we don't overflow the page
        size_t max_blocks = usable_space / bs;
        if (max_blocks == 0) {
            // Block too large for page, fallback
            free(mem);
            return malloc(size);
        }

        char *blk = (char*)mem + sizeof(slab_page);
        for (size_t j = 0; j < max_blocks; j++) {
            // Make sure we don't exceed page bounds
            if (blk + bs > (char*)pg->page_end) break;

            slab_header *hdr = (slab_header*)blk;
            hdr->next_free   = cl->free_list;
            cl->free_list    = hdr;
            blk += bs;
        }
    }

    // Pop one block
    if (!cl->free_list) {
        // Something went wrong, fallback
        return malloc(size);
    }

    slab_header *h = cl->free_list;
    cl->free_list = h->next_free;

    // Return pointer after header
    return (void*)((char*)h + sizeof(slab_header));
}

void slab_free(void *ptr) {
    if (!ptr) return;  // Handle NULL gracefully

    if (!slab_initialized) {
        free(ptr); // fallback for uninitialized
        return;
    }

    // Get pointer to header
    slab_header *h = (slab_header*)((char*)ptr - sizeof(slab_header));

    // Find the page this pointer belongs to - FIXED validation
    slab_page *pg = find_page_for_ptr(h);
    if (!pg) {
        // Not a slab allocation, use regular free
        free(ptr);
        return;
    }

    // Validate class index before using it
    int ci = pg->class_idx;
    if (ci < 0 || ci >= NUM_CLASSES) {
        // Corrupted page, fallback to regular free
        free(ptr);
        return;
    }

    // Return to free list
    slab_class *cl = &classes[ci];
    h->next_free = cl->free_list;
    cl->free_list = h;
}

void slab_destroy(void) {
    if (!slab_initialized) return;

    // Free all pages
    for (int i = 0; i < NUM_CLASSES; i++) {
        slab_page *pg = classes[i].pages;
        while (pg) {
            slab_page *next = pg->next;
            free(pg);
            pg = next;
        }
        classes[i].pages = NULL;
        classes[i].free_list = NULL;
    }

    slab_initialized = 0;
}