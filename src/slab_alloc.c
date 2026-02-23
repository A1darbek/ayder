// slab_alloc.c - P99 OPTIMIZED VERSION (fixed)

#if defined(__linux__) || defined(__gnu_linux__)
#  ifndef _GNU_SOURCE
#    define _GNU_SOURCE
#  endif
#elif (defined(__unix__) || defined(__APPLE__)) && defined(__MACH__)
#  ifndef _POSIX_C_SOURCE
#    define _POSIX_C_SOURCE 200809L
#  endif
#endif

#include "slab_alloc.h"

#include <assert.h>
#include <stdint.h>
#include <stdlib.h>
#include <string.h>
#include <sys/mman.h>
#include <unistd.h>

/// --- P99 Optimization Configuration ---
/// More granular size classes to reduce internal fragmentation
static const size_t size_classes[] = {32,  64,   96,   128,  192,  256,  384,  512,
                                      768, 1024, 1536, 2048, 3072, 4096, 6144, 8192};
#define NUM_CLASSES (sizeof(size_classes) / sizeof(size_classes[0]))

/// Page size targets (avoid PAGE_SIZE macro collision)
static size_t g_slab_page_bytes     = 2 * 1024 * 1024; // 2MB target
static size_t g_fallback_page_bytes = 64 * 1024;       // 64KB fallback (if huge pages not supported at all)

/// Magic number to identify valid slab pages
#define SLAB_MAGIC 0xDEADBEEF

/// Cache line size for alignment (avoid false sharing)
#define CACHE_LINE_SIZE 64

/// Pre-allocate pages to avoid allocation during hot path
#define PREALLOC_PAGES_PER_CLASS 2

/// Header prepended to each block in a slab
typedef struct slab_header {
  struct slab_header *next_free;
} __attribute__((aligned(16))) slab_header;

/// Metadata at start of each slab page
typedef struct slab_page {
  uint32_t magic;
  struct slab_page *next;
  int class_idx;
  size_t block_size;
  void *page_start;
  void *page_end;
  uint32_t free_count;
  uint32_t total_blocks;
  uint8_t is_mmap; // 1 if allocated via mmap (hugetlb), 0 if via malloc/posix_memalign
} __attribute__((aligned(CACHE_LINE_SIZE))) slab_page;

/// One slab class descriptor
typedef struct slab_class {
  size_t block_size;
  slab_header *free_list; // hot path
  slab_page *pages;
  uint32_t total_pages;
} __attribute__((aligned(CACHE_LINE_SIZE))) slab_class;

static slab_class classes[NUM_CLASSES];
static int slab_initialized = 0;
static int use_huge_pages   = 0;

/// Fast class lookup table for common sizes (0-8192 bytes)
#define LOOKUP_TABLE_SIZE 8193
static int8_t class_lookup[LOOKUP_TABLE_SIZE];

/// Optimized page allocation with huge pages support
static slab_page *slab_alloc_page(slab_class *cl, int class_idx) {
  void *mem     = NULL;
  int used_mmap = 0;

#if defined(MAP_HUGETLB)
  if (use_huge_pages) {
    void *m = mmap(NULL, g_slab_page_bytes, PROT_READ | PROT_WRITE, MAP_PRIVATE | MAP_ANONYMOUS | MAP_HUGETLB, -1, 0);
    if (m != MAP_FAILED) {
      mem       = m;
      used_mmap = 1;
    } else {
      // Fall back to aligned heap allocation (still aligned to g_slab_page_bytes so masking works)
      if (posix_memalign(&mem, g_slab_page_bytes, g_slab_page_bytes) != 0) {
        return NULL;
      }
      used_mmap = 0;
    }
  } else
#endif
  {
    if (posix_memalign(&mem, g_slab_page_bytes, g_slab_page_bytes) != 0) {
      return NULL;
    }
    used_mmap = 0;
  }

  // Init page header (only metadata)
  slab_page *pg = (slab_page *)mem;
  memset(pg, 0, sizeof(*pg));

  pg->magic      = SLAB_MAGIC;
  pg->class_idx  = class_idx;
  pg->block_size = cl->block_size;
  pg->page_start = mem;
  pg->page_end   = (char *)mem + g_slab_page_bytes;
  pg->is_mmap    = (uint8_t)used_mmap;

  // Link into class pages list
  pg->next  = cl->pages;
  cl->pages = pg;
  cl->total_pages++;

  // Carve into blocks
  const size_t header_sz      = sizeof(slab_header);
  const size_t block_total_sz = (size_t)((cl->block_size + header_sz + 15) & ~((size_t)15)); // 16B align
  const size_t usable_space   = g_slab_page_bytes - sizeof(slab_page);
  const size_t max_blocks     = usable_space / block_total_sz;

  if (max_blocks == 0) {
    // cleanup and fail
    cl->pages = pg->next;
    cl->total_pages--;

    if (used_mmap) {
      munmap(mem, g_slab_page_bytes);
    } else {
      free(mem);
    }
    return NULL;
  }

  pg->total_blocks = (uint32_t)max_blocks;
  pg->free_count   = (uint32_t)max_blocks;

  // Build a per-page free list, then prepend it to the class free_list
  slab_header *old_head = cl->free_list;
  slab_header *first    = NULL;
  slab_header *prev     = NULL;

  char *blk = (char *)mem + sizeof(slab_page);
  for (size_t j = 0; j < max_blocks; j++) {
    if (blk + block_total_sz > (char *)pg->page_end)
      break;

    slab_header *hdr = (slab_header *)blk;
    hdr->next_free   = NULL;

    if (!first)
      first = hdr;
    if (prev)
      prev->next_free = hdr;
    prev = hdr;

    blk += block_total_sz;
  }

  if (prev) {
    prev->next_free = old_head;
    cl->free_list   = first ? first : old_head;
  } else {
    // should not happen if max_blocks > 0, but be safe
    cl->free_list = old_head;
  }

  return pg;
}

void slab_init(void) {
  if (slab_initialized)
    return;

#if defined(MAP_HUGETLB)
  // Try to detect huge page support
  void *test_huge =
      mmap(NULL, g_slab_page_bytes, PROT_READ | PROT_WRITE, MAP_PRIVATE | MAP_ANONYMOUS | MAP_HUGETLB, -1, 0);
  if (test_huge != MAP_FAILED) {
    munmap(test_huge, g_slab_page_bytes);
    use_huge_pages = 1;
  } else {
    // Fall back to smaller page size for all slabs (keeps masking & alignment consistent)
    g_slab_page_bytes = g_fallback_page_bytes;
    use_huge_pages    = 0;
  }
#else
  // Non-linux/unsupported: just use fallback page size
  g_slab_page_bytes = g_fallback_page_bytes;
  use_huge_pages    = 0;
#endif

  // Initialize fast lookup table
  memset(class_lookup, -1, sizeof(class_lookup));
  for (size_t i = 0; i < NUM_CLASSES; i++) {
    if (i == 0) {
      for (size_t j = 0; j <= size_classes[0] && j < LOOKUP_TABLE_SIZE; j++) {
        class_lookup[j] = (int8_t)i;
      }
    } else {
      for (size_t j = size_classes[i - 1] + 1; j <= size_classes[i] && j < LOOKUP_TABLE_SIZE; j++) {
        class_lookup[j] = (int8_t)i;
      }
    }
  }

  // Initialize each size class
  for (size_t i = 0; i < NUM_CLASSES; i++) {
    classes[i].block_size  = size_classes[i];
    classes[i].free_list   = NULL;
    classes[i].pages       = NULL;
    classes[i].total_pages = 0;
  }

  // Pre-allocate pages for common sizes to avoid allocation in hot path
  for (size_t i = 0; i < NUM_CLASSES && i < 8; i++) {
    for (int j = 0; j < PREALLOC_PAGES_PER_CLASS; j++) {
      (void)slab_alloc_page(&classes[i], i);
    }
  }

  slab_initialized = 1;
}

/// Fast class lookup - O(1) for common sizes
static inline int find_class_fast(size_t size) {
  if (size < LOOKUP_TABLE_SIZE) {
    return class_lookup[size];
  }

  // Fallback to binary search
  int left = 0, right = (int)NUM_CLASSES - 1;
  while (left <= right) {
    int mid = (left + right) / 2;
    if (size <= size_classes[mid]) {
      if (mid == 0 || size > size_classes[mid - 1])
        return mid;
      right = mid - 1;
    } else {
      left = mid + 1;
    }
  }
  return -1;
}

/// Find page for pointer
static slab_page *find_page_for_ptr(void *ptr) {
  if (!ptr || !slab_initialized)
    return NULL;

  // Masking assumes g_slab_page_bytes is power-of-two (2MB or 64KB here)
  uintptr_t addr      = (uintptr_t)ptr;
  uintptr_t page_mask = (uintptr_t)g_slab_page_bytes - 1;
  void *page_start    = (void *)(addr & ~page_mask);

  // Search classes (common first)
  for (size_t ci = 0; ci < (int)NUM_CLASSES; ci++) {
    slab_page *pg = classes[ci].pages;
    while (pg) {
      if (pg->page_start == page_start && pg->magic == SLAB_MAGIC) {
        if (ptr >= pg->page_start && ptr < pg->page_end)
          return pg;
      }
      pg = pg->next;
    }
  }
  return NULL;
}

void *slab_alloc(size_t size) {
  if (!slab_initialized)
    slab_init();

  int ci = find_class_fast(size);
  if (ci < 0)
    return malloc(size); // very large allocations fallback

  slab_class *cl = &classes[ci];

  // Hot path
  if (cl->free_list) {
    slab_header *h = cl->free_list;
    cl->free_list  = h->next_free;
    return (void *)((char *)h + sizeof(slab_header));
  }

  // Cold path: allocate new page
  slab_page *pg = slab_alloc_page(cl, ci);
  if (!pg || !cl->free_list)
    return malloc(size);

  slab_header *h = cl->free_list;
  cl->free_list  = h->next_free;
  return (void *)((char *)h + sizeof(slab_header));
}

void slab_free(void *ptr) {
  if (!ptr)
    return;

  if (!slab_initialized) {
    free(ptr);
    return;
  }

  slab_header *h = (slab_header *)((char *)ptr - sizeof(slab_header));
  slab_page *pg  = find_page_for_ptr((void *)h);

  if (!pg) {
    free(ptr);
    return;
  }

  int ci = pg->class_idx;
  if (ci < 0 || ci >= (int)NUM_CLASSES) {
    free(ptr);
    return;
  }

  slab_class *cl = &classes[ci];
  h->next_free   = cl->free_list;
  cl->free_list  = h;

  if (pg->free_count < pg->total_blocks)
    pg->free_count++;
}

void slab_destroy(void) {
  if (!slab_initialized)
    return;

  for (size_t i = 0; i < (int)NUM_CLASSES; i++) {
    slab_page *pg = classes[i].pages;
    while (pg) {
      slab_page *next = pg->next;

      if (pg->is_mmap) {
        munmap(pg, g_slab_page_bytes);
      } else {
        free(pg);
      }

      pg = next;
    }

    classes[i].pages       = NULL;
    classes[i].free_list   = NULL;
    classes[i].total_pages = 0;
  }

  slab_initialized = 0;
}
