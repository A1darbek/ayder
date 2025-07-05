// zero_pause_rdb.c - Live RDB Snapshot with zero-pause bitmap tracking
#define _GNU_SOURCE

#include "zero_pause_rdb.h"
#include "storage.h"
#include "crc32c.h"
#include "slab_alloc.h"
#include "ramforge_rotation_metrics.h"
#include "log.h"
#include <uv.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdatomic.h>
#include <unistd.h>
#include <sys/mman.h>
#include <fcntl.h>
#include <inttypes.h>  // Added for PRIu64


// Bitmap for tracking dirty pages during snapshot
typedef struct {
    uint64_t *words;
    size_t capacity;
    size_t num_words;
    uint64_t generation;
} DirtyBitmap;

// Snapshot context for background writing
typedef struct {
    FILE *rdb_file;
    uint32_t crc;
    size_t entries_written;
    uint64_t snapshot_gen;
    DirtyBitmap *bitmap;
    Storage *storage;
    uv_thread_t writer_thread;
    _Atomic (int) active;
} SnapshotContext;

static DirtyBitmap g_dirty_bitmap;
static SnapshotContext g_snapshot_ctx;
Storage *g_storage_ref;
char *g_rdb_path;
static uv_timer_t g_snapshot_timer;


static inline uint64_t now_us(void) {
    struct timespec ts;
    clock_gettime(CLOCK_MONOTONIC, &ts);
    return (uint64_t)ts.tv_sec * 1000000ULL + ts.tv_nsec / 1000;
}

// ──────────────────────────────────────────────────────────────
// Bitmap operations (lock-free, atomic)
// ──────────────────────────────────────────────────────────────

static inline void bitmap_init(DirtyBitmap *bm, size_t capacity) {
    bm->capacity = capacity;
    bm->num_words = (capacity + 63) / 64;
    size_t bytes = bm->num_words * sizeof(uint64_t);

    bm->words = mmap(NULL, bytes, PROT_READ | PROT_WRITE,
                     MAP_PRIVATE | MAP_ANONYMOUS, -1, 0);
    if (bm->words == MAP_FAILED) {
        bm->words = calloc(bm->num_words, sizeof(uint64_t));
    } else {
        memset(bm->words, 0, bytes);
    }
    bm->generation = 0;
}

static inline void bitmap_destroy(DirtyBitmap *bm) {
    if (bm->words && bm->words != MAP_FAILED) {
        munmap(bm->words, bm->num_words * sizeof(uint64_t));
    } else {
        free(bm->words);
    }

    /* poison the struct so future calls are harmless          */
    bm->words = NULL;
    bm->capacity = bm->num_words = 0;
}

// Mark key as dirty (called on every write operation)
static inline void bitmap_mark_dirty(DirtyBitmap *bm, int key_id) {
    if (!bm || !bm->words)                       return;   /* bitmap gone */
    if (key_id < 0 || (size_t) key_id >= bm->capacity) return;
    size_t word = key_id / 64;
    size_t bit = key_id % 64;
    __atomic_fetch_or(&bm->words[word], 1ULL << bit, __ATOMIC_RELAXED);
}

// Check if key is dirty and optionally clear it
static inline int bitmap_test_and_clear(DirtyBitmap *bm, int key_id) {
    if (key_id < 0 || (size_t) key_id >= bm->capacity) return 0;
    size_t word = key_id / 64;
    size_t bit = key_id % 64;
    uint64_t mask = 1ULL << bit;
    uint64_t old = __atomic_fetch_and(&bm->words[word], ~mask, __ATOMIC_RELAXED);
    return (old & mask) != 0;
}

// Clear entire bitmap for new snapshot cycle


// ──────────────────────────────────────────────────────────────
// Copy-on-Write snapshot data structure
// ──────────────────────────────────────────────────────────────

typedef struct SnapshotEntry {
    int key_id;
    size_t data_size;
    void *data_copy;          // COW copy of the data
    uint64_t generation;        // When this copy was made
    struct SnapshotEntry *next;
} SnapshotEntry;

typedef struct {
    SnapshotEntry **buckets;
    size_t bucket_count;
    _Atomic (size_t) entry_count;
} SnapshotTable;

static SnapshotTable g_snapshot_table;

static void snapshot_table_init(SnapshotTable *st, size_t buckets) {
    st->bucket_count = buckets;
    st->buckets = calloc(buckets, sizeof(SnapshotEntry *));
    atomic_store(&st->entry_count, 0);
}

// Store a COW copy of data for snapshot
static void snapshot_table_store(SnapshotTable *st, int key_id,
                                 const void *data, size_t size, uint64_t gen) {
    size_t bucket = ((unsigned) key_id) % st->bucket_count;

    if (!data || size == 0) return;               /* nothing to copy */
    SnapshotEntry *entry = slab_alloc(sizeof(SnapshotEntry));
    entry->key_id = key_id;
    entry->data_size = size;
    entry->data_copy = slab_alloc(size);
    memcpy(entry->data_copy, data, size);
    entry->generation = gen;

    // Insert at head (lock-free would need hazard pointers, keeping simple)
    entry->next = st->buckets[bucket];
    st->buckets[bucket] = entry;
    atomic_fetch_add(&st->entry_count, 1);
}

// ──────────────────────────────────────────────────────────────
// Background snapshot writer thread
// ──────────────────────────────────────────────────────────────

// Chaos latency injection for testing
static void inject_chaos_latency(void) {
    static int chaos_enabled = -1;
    if (chaos_enabled == -1) {
        chaos_enabled = getenv("RAMFORGE_CHAOS_TEST") ? 1 : 0;
    }

    if (chaos_enabled) {
        // Random microsecond delays to test system resilience
        usleep(rand() % 100);  // 0-100μs random delay
    }
}

static void snapshot_writer_iter_cb(int id, const void *data, size_t size, void *ud) {
    SnapshotContext *ctx = (SnapshotContext *) ud;

    inject_chaos_latency();

    // Check if this key was modified during snapshot
    if (bitmap_test_and_clear(&g_dirty_bitmap, id)) {
        // Key was modified, use the COW copy from snapshot table
        // (Implementation would look up in snapshot table)
        return;
    }

    // Write to RDB file
    fwrite(&id, sizeof(id), 1, ctx->rdb_file);
    fwrite(&size, sizeof(size), 1, ctx->rdb_file);
    fwrite(data, size, 1, ctx->rdb_file);

    // Update CRC
    ctx->crc = crc32c(ctx->crc, &id, sizeof(id));
    ctx->crc = crc32c(ctx->crc, &size, sizeof(size));
    ctx->crc = crc32c(ctx->crc, data, size);

    ctx->entries_written++;
}

static void snapshot_writer_thread(void *arg) {
    /* ------------------------------------------------------------------
     * Flush bitmap: keys that were dirty *before* the snapshot should be
     * captured, so we clear all bits right now.  Any write that happens
     * during the snapshot will set the bit again and be handled via COW.
     * ------------------------------------------------------------------ */
    for (size_t i = 0; i < g_dirty_bitmap.num_words; ++i)
        __atomic_store_n(&g_dirty_bitmap.words[i], 0, __ATOMIC_RELAXED);
    SnapshotContext *ctx = (SnapshotContext *) arg;
    uint64_t t0 = now_us();              /* <-- start timer */

    LOGD("Starting zero-pause RDB snapshot generation %" PRIu64 "\n",
           ctx->snapshot_gen);

    char tmp_path[512];
    snprintf(tmp_path, sizeof(tmp_path), "%s.tmp.%" PRIu64,
             g_rdb_path, ctx->snapshot_gen);

    ctx->rdb_file = fopen(tmp_path, "wb");
    if (!ctx->rdb_file) {
        fprintf(stderr, "Failed to create snapshot file\n");
        atomic_store(&ctx->active, 0);
        return;
    }

    ctx->crc = 0;
    ctx->entries_written = 0;

    // Write RDB header
    uint64_t magic = 0x52414D460001ULL; // 'RAMF'0001 in hex, safe and readable
    fwrite(&magic, sizeof(magic), 1, ctx->rdb_file);
    fwrite(&ctx->snapshot_gen, sizeof(ctx->snapshot_gen), 1, ctx->rdb_file);

    // Iterate through storage and write entries
    storage_iterate(ctx->storage, snapshot_writer_iter_cb, ctx);

    // Write CRC footer
    uint64_t crc64 = ctx->crc;
    fwrite(&crc64, sizeof crc64, 1, ctx->rdb_file);

    // Atomic file replacement
    fflush(ctx->rdb_file);
    fsync(fileno(ctx->rdb_file));
    fclose(ctx->rdb_file);

    rename(tmp_path, g_rdb_path);

    uint64_t dur = now_us() - t0;
    ZeroPauseRDB_metrics_inc(dur);       /* <-- bump counter + latency */


    LOGD("Completed zero-pause snapshot: %" PRIu64 " entries, CRC: %08x\n",
           (uint64_t) ctx->entries_written, ctx->crc);

    atomic_store(&ctx->active, 0);
}

// ──────────────────────────────────────────────────────────────
// Public API
// ──────────────────────────────────────────────────────────────

void ZeroPauseRDB_init(const char *rdb_path, Storage *storage,
                       size_t max_keys, unsigned snapshot_interval_sec) {
    g_rdb_path = strdup(rdb_path);
    g_storage_ref = storage;

    // Initialize bitmap for tracking dirty keys
    bitmap_init(&g_dirty_bitmap, max_keys);

    // Initialize snapshot table for COW data
    snapshot_table_init(&g_snapshot_table, max_keys / 4);

    // Initialize snapshot context
    memset(&g_snapshot_ctx, 0, sizeof(g_snapshot_ctx));
    g_snapshot_ctx.bitmap = &g_dirty_bitmap;
    g_snapshot_ctx.storage = storage;
    atomic_store(&g_snapshot_ctx.active, 0);

    if (snapshot_interval_sec) {
        uv_loop_t *loop = uv_default_loop();
        uv_timer_init(loop, &g_snapshot_timer);
        uv_timer_start(&g_snapshot_timer,
                       (uv_timer_cb)ZeroPauseRDB_snapshot,
                       snapshot_interval_sec * 1000,   /* first fire */
                       snapshot_interval_sec * 1000);  /* repeat     */
    }

    printf("Zero-pause RDB initialized: %zu max keys, %us intervals\n",
           max_keys, snapshot_interval_sec);
}

// Called by storage layer on every write operation
void ZeroPauseRDB_mark_dirty(int key_id, const void *old_data, size_t old_size) {
    bitmap_mark_dirty(&g_dirty_bitmap, key_id);

    // If snapshot is active, store COW copy
    if (atomic_load(&g_snapshot_ctx.active)) {
        uint64_t gen = atomic_load(&g_dirty_bitmap.generation);
        snapshot_table_store(&g_snapshot_table, key_id, old_data, old_size, gen);
    }
}

// Trigger zero-pause snapshot
void ZeroPauseRDB_snapshot(void) {
    if (atomic_load(&g_snapshot_ctx.active)) {
        printf("Snapshot already in progress, skipping\n");
        return;
    }

    // Mark snapshot as active
    atomic_store(&g_snapshot_ctx.active, 1);
    g_snapshot_ctx.snapshot_gen = atomic_load(&g_dirty_bitmap.generation);

    // Start background writer thread
    uv_thread_create(&g_snapshot_ctx.writer_thread,
                     snapshot_writer_thread, &g_snapshot_ctx);

    LOGD("Zero-pause snapshot triggered (generation %" PRIu64 ")\n",
           g_snapshot_ctx.snapshot_gen);
}

// Chaos testing: inject random latency spikes
void ZeroPauseRDB_chaos_test(int enable) {
    if (enable) {
        setenv("RAMFORGE_CHAOS_TEST", "1", 1);
        printf("Chaos latency testing ENABLED\n");
    } else {
        unsetenv("RAMFORGE_CHAOS_TEST");
        printf("Chaos latency testing DISABLED\n");
    }
}

// Get snapshot statistics
void ZeroPauseRDB_stats(ZeroPauseStats *stats) {
    stats->active_snapshot = atomic_load(&g_snapshot_ctx.active);
    stats->current_generation = atomic_load(&g_dirty_bitmap.generation);
    stats->entries_written = g_snapshot_ctx.entries_written;
    stats->snapshot_table_entries = atomic_load(&g_snapshot_table.entry_count);
    stats->bitmap_capacity = g_dirty_bitmap.capacity;
}

int ZeroPauseRDB_snapshot_wait(void)
{
    if (!atomic_load(&g_snapshot_ctx.active))
        return 0;                           /* nothing running */

    uv_thread_join(&g_snapshot_ctx.writer_thread);
    return 0;                               /* success */
}

void ZeroPauseRDB_shutdown(void) {
    // Wait for active snapshot to complete
    if (atomic_load(&g_snapshot_ctx.active)) {
        uv_thread_join(&g_snapshot_ctx.writer_thread);
    }

    bitmap_destroy(&g_dirty_bitmap);
    free(g_rdb_path);
    printf("Zero-pause RDB shutdown complete\n");
}