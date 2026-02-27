// slab_alloc.c - P99 OPTIMIZED VERSION
#if defined(__linux__) || defined(__gnu_linux__)
#ifndef _GNU_SOURCE
#define _GNU_SOURCE
#endif
#elif defined(__unix__) || defined(__APPLE__) && defined(__MACH__)
    #ifndef _POSIX_C_SOURCE
        #define _POSIX_C_SOURCE 200809L
    #endif
#endif
#include "slab_alloc.h"
#include <stdlib.h>
#include <stdint.h>
#include <unistd.h>
#include <string.h>
#include <assert.h>
#include <sys/mman.h>

/// --- P99 Optimization Configuration ---
/// More granular size classes to reduce internal fragmentation
static const size_t size_classes[] = {
        32, 64, 96, 128, 192, 256, 384, 512, 768, 1024, 1536, 2048, 3072, 4096, 6144, 8192
};
#define NUM_CLASSES (sizeof(size_classes)/sizeof(size_classes[0]))

/// Use 2MB huge pages when available for better TLB performance
static size_t PAGE_SIZE = 2 * 1024 * 1024;
static size_t FALLBACK_PAGE_SIZE = 64 * 1024;

/// Magic number to identify valid slab pages
#define SLAB_MAGIC 0xDEADBEEF

/// Cache line size for alignment (avoid false sharing)
#define CACHE_LINE_SIZE 64

/// Pre-allocate pages to avoid allocation during hot path
#define PREALLOC_PAGES_PER_CLASS 2

/// Header prepended to each block in a slab - optimized for cache efficiency
typedef struct slab_header {
    struct slab_header *next_free;
} __attribute__((aligned(16))) slab_header;

/// Metadata at start of each slab page - cache-aligned
typedef struct slab_page {
    uint32_t              magic;
    struct slab_page     *next;
    int                   class_idx;
    size_t                block_size;
    void                 *page_start;
    void                 *page_end;
    uint32_t              free_count;      // Track free blocks for efficiency
    uint32_t              total_blocks;    // Total blocks in this page
    char                  padding[CACHE_LINE_SIZE - 48]; // Pad to cache line
} __attribute__((aligned(CACHE_LINE_SIZE))) slab_page;

/// One slab class descriptor - optimized layout
typedef struct {
    size_t         block_size;
    slab_header   *free_list;        // Hot path - keep at front
    slab_page     *pages;
    slab_page     *pages_with_free;  // Quick access to pages with free blocks
    uint32_t       total_pages;
    uint32_t       pages_with_free_count;
    char           padding[CACHE_LINE_SIZE - 40]; // Avoid false sharing
} __attribute__((aligned(CACHE_LINE_SIZE))) slab_class;

static slab_class classes[NUM_CLASSES];
static int slab_initialized = 0;
static int use_huge_pages = 0;

/// Fast class lookup table for common sizes (0-8192 bytes)
#define LOOKUP_TABLE_SIZE 8193
static int8_t class_lookup[LOOKUP_TABLE_SIZE];

/// Optimized page allocation with huge pages support
static slab_page* slab_alloc_page(slab_class *cl, int class_idx) {
    void *mem;

    if (use_huge_pages) {
        mem = mmap(NULL, PAGE_SIZE, PROT_READ | PROT_WRITE,
                   MAP_PRIVATE | MAP_ANONYMOUS | MAP_HUGETLB, -1, 0);
        if (mem == MAP_FAILED) {
            // Fallback to regular allocation
            if (posix_memalign(&mem, FALLBACK_PAGE_SIZE, PAGE_SIZE) != 0) {
                return NULL;
            }
        }
    } else {
        if (posix_memalign(&mem, PAGE_SIZE, PAGE_SIZE) != 0) {
            return NULL;
        }
    }

    // Clear only the metadata portion, not the entire page
    slab_page *pg = (slab_page*)mem;
    memset(pg, 0, sizeof(slab_page));

    pg->magic = SLAB_MAGIC;
    pg->next = cl->pages;
    pg->class_idx = class_idx;
    pg->block_size = cl->block_size;
    pg->page_start = mem;
    pg->page_end = (char*)mem + PAGE_SIZE;

    cl->pages = pg;
    cl->total_pages++;

    // Carve into blocks - optimized calculation
    size_t header_sz = sizeof(slab_header);
    size_t block_total_sz = ((cl->block_size + header_sz + 15) & ~15); // 16-byte align
    size_t usable_space = PAGE_SIZE - sizeof(slab_page);
    size_t max_blocks = usable_space / block_total_sz;

    if (max_blocks == 0) {
        // Block too large, cleanup and fail
        if (use_huge_pages) {
            munmap(mem, PAGE_SIZE);
        } else {
            free(mem);
        }
        cl->total_pages--;
        cl->pages = pg->next;
        return NULL;
    }

    pg->total_blocks = max_blocks;
    pg->free_count = max_blocks;

    // Create free list for this page
    char *blk = (char*)mem + sizeof(slab_page);
    slab_header *prev_header = NULL;

    for (size_t j = 0; j < max_blocks; j++) {
        if (blk + block_total_sz > (char*)pg->page_end) break;

        slab_header *hdr = (slab_header*)blk;
        if (prev_header) {
            prev_header->next_free = hdr;
        } else {
            cl->free_list = hdr; // First block becomes head of global free list
        }
        prev_header = hdr;
        blk += block_total_sz;
    }

    if (prev_header) {
        prev_header->next_free = NULL;
    }

    cl->pages_with_free = NULL;
    cl->pages_with_free_count = 0;

    return pg;
}

void slab_init(void) {
    if (slab_initialized) return;

    // Try to detect huge page support
    void *test_huge = mmap(NULL, PAGE_SIZE, PROT_READ | PROT_WRITE,
                           MAP_PRIVATE | MAP_ANONYMOUS | MAP_HUGETLB, -1, 0);
    if (test_huge != MAP_FAILED) {
        munmap(test_huge, PAGE_SIZE);
        use_huge_pages = 1;
    } else {
        PAGE_SIZE = FALLBACK_PAGE_SIZE;
        use_huge_pages = 0;
    }

    // Initialize fast lookup table
    memset(class_lookup, -1, sizeof(class_lookup));
    for (size_t  i = 0; i < NUM_CLASSES; i++) {
        if (i == 0) {
            // Fill entries 0 to size_classes[0]
            for (size_t j = 0; j <= size_classes[0] && j < LOOKUP_TABLE_SIZE; j++) {
                class_lookup[j] = i;
            }
        } else {
            // Fill entries from size_classes[i-1]+1 to size_classes[i]
            for (size_t j = size_classes[i-1] + 1;
                 j <= size_classes[i] && j < LOOKUP_TABLE_SIZE; j++) {
                class_lookup[j] = i;
            }
        }
    }

    // Initialize each size class
    for (size_t  i = 0; i < NUM_CLASSES; i++) {
        classes[i].block_size = size_classes[i];
        classes[i].free_list = NULL;
        classes[i].pages = NULL;
        classes[i].pages_with_free = NULL;
        classes[i].total_pages = 0;
        classes[i].pages_with_free_count = 0;
    }

    // Pre-allocate pages for common sizes to avoid allocation in hot path
    for (size_t  i = 0; i < NUM_CLASSES && i < 8; i++) {
        for (int j = 0; j < PREALLOC_PAGES_PER_CLASS; j++) {
            slab_alloc_page(&classes[i], i);
        }
    }

    slab_initialized = 1;
}

/// Fast class lookup - O(1) for common sizes
static inline int find_class_fast(size_t size) {
    if (size < LOOKUP_TABLE_SIZE) {
        return class_lookup[size];
    }

    // Fallback to binary search for larger sizes
    int left = 0, right = NUM_CLASSES - 1;
    while (left <= right) {
        int mid = (left + right) / 2;
        if (size <= size_classes[mid]) {
            if (mid == 0 || size > size_classes[mid - 1]) {
                return mid;
            }
            right = mid - 1;
        } else {
            left = mid + 1;
        }
    }
    return -1;
}


/// Find page for pointer - optimized with early exit
static slab_page* find_page_for_ptr(void *ptr) {
    if (!ptr || !slab_initialized) return NULL;

    // Use address alignment hint for faster search
    uintptr_t addr = (uintptr_t)ptr;
    uintptr_t page_mask = PAGE_SIZE - 1;
    void *page_start = (void*)(addr & ~page_mask);

    // Search only relevant classes first (common sizes)
    for (size_t ci = 0; ci < 8 && ci < NUM_CLASSES; ci++) {
        slab_page *pg = classes[ci].pages;
        while (pg) {
            if (pg->page_start == page_start && pg->magic == SLAB_MAGIC) {
                if (ptr >= pg->page_start && ptr < pg->page_end) {
                    return pg;
                }
            }
            pg = pg->next;
        }
    }

    // Search remaining classes if not found
    for (size_t ci = 8; ci < NUM_CLASSES; ci++) {
        slab_page *pg = classes[ci].pages;
        while (pg) {
            if (ptr >= pg->page_start && ptr < pg->page_end && pg->magic == SLAB_MAGIC) {
                return pg;
            }
            pg = pg->next;
        }
    }

    return NULL;
}

void *slab_alloc(size_t size) {
    if (!slab_initialized) slab_init();

    int ci = find_class_fast(size);
    if (ci < 0) {
        // Fallback for very large allocations
        return malloc(size);
    }

    slab_class *cl = &classes[ci];

    // Hot path: try to allocate from existing free list
    if (cl->free_list) {
        slab_header *h = cl->free_list;
        cl->free_list = h->next_free;

        // Update page free count (find which page this came from)
        // For p99, we could maintain per-page free lists, but this adds complexity

        return (void*)((char*)h + sizeof(slab_header));
    }

    // Cold path: need new page
    slab_page *pg = slab_alloc_page(cl, ci);
    if (!pg) {
        return malloc(size); // Ultimate fallback
    }

    // Should now have blocks in free_list
    if (!cl->free_list) {
        return malloc(size); // Something went wrong
    }

    slab_header *h = cl->free_list;
    cl->free_list = h->next_free;

    return (void*)((char*)h + sizeof(slab_header));
}

void slab_free(void *ptr) {
    if (!ptr) return;

    if (!slab_initialized) {
        free(ptr);
        return;
    }

    slab_header *h = (slab_header*)((char*)ptr - sizeof(slab_header));
    slab_page *pg = find_page_for_ptr(h);

    if (!pg) {
        free(ptr);
        return;
    }

    int ci = pg->class_idx;
    if (ci < 0 || (size_t)ci >= NUM_CLASSES) {
        free(ptr);
        return;
    }

    // Return to free list - LIFO for better cache locality
    slab_class *cl = &classes[ci];
    h->next_free = cl->free_list;
    cl->free_list = h;

    pg->free_count++;

    // If page becomes completely free and we have multiple pages, consider deallocation
    if (pg->free_count == pg->total_blocks && cl->total_pages > 2) {
        // TODO: Implement page deallocation for memory efficiency
        // This is a trade-off between memory usage and allocation performance
    }
}


void slab_destroy(void) {
    if (!slab_initialized) return;

    for (size_t  i = 0; i < NUM_CLASSES; i++) {
        slab_page *pg = classes[i].pages;
        while (pg) {
            slab_page *next = pg->next;
            if (use_huge_pages) {
                munmap(pg, PAGE_SIZE);
            } else {
                free(pg);
            }
            pg = next;
        }
        classes[i].pages = NULL;
        classes[i].pages_with_free = NULL;
        classes[i].free_list = NULL;
        classes[i].total_pages = 0;
        classes[i].pages_with_free_count = 0;
    }

    slab_initialized = 0;
}
