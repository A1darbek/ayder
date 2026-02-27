/* aof_batch.c ultra-fast io_uring AOF with batching + CRC32C + non-blocking rewrite */
#define _GNU_SOURCE
#include <pthread.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <fcntl.h>
#include <stdio.h>
#include <errno.h>
#include <stdatomic.h>
#include <stdbool.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <liburing.h>
#include <sys/uio.h>

#include "aof_batch.h"
#include "storage.h"
#include "crc32c.h"
#include <inttypes.h>

#include "metrics_shared.h"      // for g_metrics
#include "rf_broker.h"
#include "ramforge_ha_integration.h"
#include <stdatomic.h>

#ifndef cpu_relax
#if defined(__aarch64__)
    // For ARM64: use the 'yield' instruction
    #define cpu_relax() asm volatile("yield" ::: "memory")
#elif defined(__x86_64__)
    // For x86: use the 'pause' instruction
    #define cpu_relax() asm volatile("pause" ::: "memory")
#else
    // Fallback: standard compiler barrier
    #define cpu_relax() asm volatile("" ::: "memory")
#endif
#endif


#define SEALED_BATCH_SIZE 256
#define SEALED_BUFFER_SIZE (8 * 1024 * 1024)  /* 8MB sealed buffer */
#define MAX_PENDING_SEALS 32

#define SEALED_RING_SIZE 8192          /* Larger ring buffer */
#define SEALED_BATCH_GROUP_SIZE 512     /* Batch more operations */
#define SEALED_MMAP_SIZE (64 * 1024 * 1024)  /* 64MB memory mapped */
#define MAX_SEALED_BATCHES 4096         /* More concurrent batches */


typedef struct {
    uint64_t batch_id;
    uint32_t size;
    uint32_t offset_in_mmap;   /* Offset in memory mapped region */
    uint64_t timestamp;
    _Atomic int state;         /* 0=free, 1=writing, 2=ready, 3=synced */
} sealed_batch_slot_t;

typedef struct {
    /* Memory mapped circular buffer */
    void *mmap_buffer;
    size_t mmap_size;
    _Atomic uint64_t write_head;
    _Atomic uint64_t sync_tail;

    /* Lock-free batch slots */
    sealed_batch_slot_t slots[MAX_SEALED_BATCHES];
    _Atomic uint64_t slot_head;
    _Atomic uint64_t slot_tail;

    /* Single writer fd with O_DIRECT */
    int fd;
    _Atomic uint64_t next_batch_id;

    /* Async sync state */
    pthread_t sync_thread;
    _Atomic bool shutdown;

    /* Metrics */
    _Atomic uint64_t batches_written;
    _Atomic uint64_t total_sync_time_us;
    _Atomic uint64_t last_synced_batch_id;

    _Atomic bool gc_active;         /* pause appends/sync during sealed GC */
    pthread_mutex_t gc_lock;

} sealed_aof_t;

static sealed_aof_t g_opt_sealed = {0};


static uint64_t local_generation = 0;

/*  Enhanced configuration for io_uring */
#define DEFAULT_RING_CAP (1 << 16)            /* 64K entries */
#define BUFFER_POOL_SIZE 2048                 /* Pre-allocated buffers */
#define MAX_BUFFER_SIZE 8192                  /* Max size per buffer */
#define URING_QUEUE_DEPTH 512                 /* io_uring queue depth */
#define BATCH_SIZE 256                        /* Batch size for io_uring */
#define WRITE_AHEAD_BUFFER_SIZE (4 * 1024 * 1024)  /* 4MB write-ahead buffer */


#ifndef atomic_uint64_t
typedef _Atomic(uint64_t) atomic_uint64_t;
#endif

#ifndef ATOMIC_VAR_INIT
#define ATOMIC_VAR_INIT(value) (value)
#endif

#if __has_include(<valgrind/valgrind.h>)
#  include <valgrind/valgrind.h>
#include <sys/statvfs.h>
#include <libgen.h>

#else
#  define RUNNING_ON_VALGRIND 0
#endif
static inline int running_under_valgrind(void) { return RUNNING_ON_VALGRIND; }


/*  io_uring optimized structures  */
typedef struct {
    struct io_uring ring;
    struct io_uring_cqe *cqes[URING_QUEUE_DEPTH];
    atomic_int pending_writes;
    atomic_bool initialized;
    pthread_mutex_t submit_lock;
} uring_ctx_t;

typedef struct {
    void *buffer;
    size_t size;
    int fd_used;           /* The fd we submitted with */
    uint64_t generation;   /* Generation when submitted */
    size_t offset;
    atomic_bool submitted;
    atomic_bool completed;
    uint64_t sequence;
} write_batch_t;

/* Write-ahead logging buffer */
typedef struct {
    char *buffer;
    size_t size;
    size_t capacity;
    size_t write_pos;
    atomic_bool flushing;
    pthread_mutex_t lock;
} wal_buffer_t;

/*  Improved Buffer pool for zero-malloc AOF_append */
typedef struct buffer_node {
    void *data;
    size_t capacity;
    struct buffer_node *next;
    atomic_bool in_use;
} buffer_node_t;

typedef struct {
    buffer_node_t *free_list;
    buffer_node_t *all_nodes;
    size_t free_count;
    size_t total_allocated;
    pthread_mutex_t lock;
    atomic_bool shutdown_flag;
} buffer_pool_t;

static buffer_pool_t g_buffer_pool = {0};
static uring_ctx_t g_uring_ctx = {0};
static wal_buffer_t g_wal_buffer = {0};

static pthread_t g_live_tail_thread;
static _Atomic bool g_live_tail_stop = false;
static _Atomic bool g_live_tail_started = false;



static inline uint64_t now_us(void) {
    struct timespec ts;
    clock_gettime(CLOCK_MONOTONIC, &ts);
    return (uint64_t)ts.tv_sec * 1000000ULL + ts.tv_nsec / 1000;
}
static inline uint64_t rf_fetch_add_u64_relaxed(_Atomic uint64_t *obj, uint64_t inc) {
    uint64_t cur = atomic_load_explicit(obj, memory_order_relaxed);
    for (;;) {
        uint64_t next = cur + inc;  // 'cur' is a plain uint64_t
        if (atomic_compare_exchange_weak_explicit(
                obj, &cur, next,
                memory_order_relaxed,   // success
                memory_order_relaxed))  // failure
            return cur;  // previous value
        /* on failure, 'cur' was updated to the current object value; retry */
    }
}




/*types / globals */
typedef struct {
    int id;
    uint32_t sz;
    void *data;
    int from_pool;
    uint64_t sequence;
    atomic_bool written;
} aof_cmd_t;

typedef struct {
    char *source_path;
    char *output_path;
    Storage *storage;
} rewrite_task_t;

typedef struct {
    int fd;
    uint64_t generation;
    pthread_mutex_t lock;
} worker_fd_cache_t;

typedef struct {
    _Atomic int master_fd;
    _Atomic uint64_t master_generation;
    pthread_rwlock_t rotation_rwlock;  /* Readers: workers, Writer: rotator */
    char *file_path;
} global_fd_state_t;

static global_fd_state_t g_fd_state = {
        .master_fd = ATOMIC_VAR_INIT(-1),
        .master_generation = ATOMIC_VAR_INIT(0),
        .rotation_rwlock = PTHREAD_RWLOCK_INITIALIZER,
        .file_path = NULL
};

static __thread worker_fd_cache_t tls_fd_cache = {-1, 0, PTHREAD_MUTEX_INITIALIZER};
static pthread_once_t tls_init_once = PTHREAD_ONCE_INIT;

static aof_cmd_t     *ring;
static size_t         cap, mask, head, tail;
static pthread_mutex_t lock;
static pthread_cond_t  cond;

static int            fd = -1;
static char          *g_path = NULL;
static unsigned       flush_ms = 10;
static int            mode_always = 0;
static pthread_t      writer;
static pthread_t      uring_worker;
static atomic_bool    running = ATOMIC_VAR_INIT(false);
static atomic_uint64_t write_sequence = ATOMIC_VAR_INIT(0);

/*  Non-blocking rewrite state*/
static atomic_bool    rewrite_active = ATOMIC_VAR_INIT(false);
static pthread_t      rewrite_thread;
static aof_cmd_t     *rewrite_buffer;
static size_t         rewrite_buf_cap, rewrite_buf_mask;
static size_t         rewrite_buf_head, rewrite_buf_tail;
static pthread_mutex_t rewrite_lock;

static pthread_t segment_rewrite_thread;
static atomic_bool segment_rewrite_active = ATOMIC_VAR_INIT(false);

static atomic_bool rotation_in_progress = ATOMIC_VAR_INIT(false);
static pthread_mutex_t rotation_lock = PTHREAD_MUTEX_INITIALIZER;

static _Atomic int g_test_hold = 0;      // 1 = block before fsync
static _Atomic int g_test_release = 0;   // 1 = release hold
static _Atomic int g_test_delay_us = 0;  // sleep before fsync (microseconds)


#define HN 10
static const double fsync_bucket_le[HN] = {0.0005,0.001,0.002,0.005,0.010,0.020,0.050,0.100,0.250,0.500}; // seconds
static const double seal_bucket_le[HN]  = {0.0005,0.001,0.002,0.005,0.010,0.020,0.050,0.100,0.250,0.500};

static _Atomic uint64_t g_fsync_hist[HN];
static _Atomic uint64_t g_seal_hist[HN];
static _Atomic uint64_t g_fsync_count = 0;
static _Atomic uint64_t g_seal_count  = 0;
static _Atomic double   g_fsync_sum_s = 0.0;
static _Atomic double   g_seal_sum_s  = 0.0;

static inline void hist_obs(_Atomic uint64_t *arr, const double *le, int n, double v, _Atomic uint64_t *cnt, _Atomic double *sum){
    for (int i=0;i<n;i++){ if (v <= le[i]) { atomic_fetch_add(&arr[i],1); goto out; } }
    // +Inf bucket is computed in exporter via cumulative sum, so no explicit bucket here.
    out:
    atomic_fetch_add(cnt, 1);
    double old = atomic_load(sum), neu;
    do { neu = old + v; } while (!atomic_compare_exchange_weak(sum, &old, neu));
}

// Snapshots for exporter (copy atomically-ish; cheap and fine for Prometheus scraping)
void AOF_fsync_hist_snapshot(uint64_t *b, size_t n, double *sum_s, uint64_t *count){
    size_t m = (n < HN) ? n : HN;
    for (size_t i=0;i<m;i++) b[i] = atomic_load(&g_fsync_hist[i]);
    *sum_s  = atomic_load(&g_fsync_sum_s);
    *count  = atomic_load(&g_fsync_count);
}
void AOF_seal_latency_hist_snapshot(uint64_t *b, size_t n, double *sum_s, uint64_t *count){
    size_t m = (n < HN) ? n : HN;
    for (size_t i=0;i<m;i++) b[i] = atomic_load(&g_seal_hist[i]);
    *sum_s  = atomic_load(&g_seal_sum_s);
    *count  = atomic_load(&g_seal_count);
}
const double* AOF_fsync_bucket_bounds(int *n){ *n = HN; return fsync_bucket_le; }
const double* AOF_seal_bucket_bounds(int *n){ *n = HN; return seal_bucket_le; }

static inline size_t write_sealed_batch_to_buffer(char *buf, uint64_t batch_id,
                                                  uint64_t timestamp, uint32_t msg_count,
                                                  const void *data, uint32_t size) {
    char *ptr = buf;

    /* Sealed batch header */
    struct {
        uint32_t magic;           /* 0x5EA1ED01 */
        uint64_t batch_id;
        uint64_t timestamp;
        uint32_t msg_count;
        uint32_t total_size;
        uint32_t header_crc;
    } __attribute__((packed)) header = {
            .magic = 0x5EA1ED01,
            .batch_id = batch_id,
            .timestamp = timestamp,
            .msg_count = msg_count,
            .total_size = size,
            .header_crc = 0
    };

    /* Calculate header CRC */
    header.header_crc = crc32c(0, &header, sizeof(header) - 4);

    /* Write header */
    memcpy(ptr, &header, sizeof(header));
    ptr += sizeof(header);

    /* Write data */
    memcpy(ptr, data, size);
    ptr += size;

    /* Calculate and write final CRC */
    uint32_t final_crc = crc32c(header.header_crc, data, size);
    memcpy(ptr, &final_crc, 4);
    ptr += 4;

    return ptr - buf;
}

static uint32_t sealed_reserve_contiguous(size_t aligned_size) {
    for (;;) {
        uint64_t cur = atomic_load(&g_opt_sealed.write_head);
        uint32_t off = (uint32_t) (cur % g_opt_sealed.mmap_size);
        uint64_t advance = aligned_size;
        uint32_t out_off = off;

        if ((uint64_t) off + (uint64_t) aligned_size > g_opt_sealed.mmap_size) {
            /* not enough space to keep record contiguous pad to end, wrap */
            uint64_t pad = (uint64_t) g_opt_sealed.mmap_size - off;
            advance += pad;
            out_off = 0; /* actual record starts at 0 after padding */
            /* Try to claim [pad + aligned_size] in one go */
        }

        if (atomic_compare_exchange_weak(&g_opt_sealed.write_head, &cur, cur + advance)) {
            if (out_off == 0 && off != 0) {
                /* We inserted padding: zero the tail to keep things tidy/aligned */
                memset((char *) g_opt_sealed.mmap_buffer + off, 0, g_opt_sealed.mmap_size - off);
            }
            return out_off;
        }
    }
}


void AOF_sealed_test_set_hold(int on) { atomic_store(&g_test_hold, on ? 1 : 0); if (on) atomic_store(&g_test_release, 0); }
void AOF_sealed_test_release(void)   { atomic_store(&g_test_release, 1); }
void AOF_sealed_test_set_delay_us(int us) { atomic_store(&g_test_delay_us, us > 0 ? us : 0); }
/* Sealed batch group sync thread */
static void *sealed_sync_thread(void *arg) {
    (void)arg;

    struct iovec iovecs[SEALED_BATCH_GROUP_SIZE];
    sealed_batch_slot_t *ready_slots[SEALED_BATCH_GROUP_SIZE];

    while (!atomic_load(&g_opt_sealed.shutdown)) {
        if (atomic_load(&g_opt_sealed.gc_active)) {
            struct timespec ts = {0, 200000};
            nanosleep(&ts, NULL);
            continue;
        }

        uint64_t h = atomic_load_explicit(&g_opt_sealed.slot_head, memory_order_acquire);
        uint64_t t = atomic_load_explicit(&g_opt_sealed.slot_tail, memory_order_acquire);

        size_t ready_count = 0;

        while (t < h && ready_count < SEALED_BATCH_GROUP_SIZE) {
            sealed_batch_slot_t *slot = &g_opt_sealed.slots[t % MAX_SEALED_BATCHES];

            if (atomic_load_explicit(&slot->state, memory_order_acquire) != 2)
                break;

            ready_slots[ready_count] = slot;
            iovecs[ready_count].iov_base = (char*)g_opt_sealed.mmap_buffer + slot->offset_in_mmap;
            iovecs[ready_count].iov_len  = slot->size;
            ready_count++;
            t++;
        }

        if (ready_count == 0) {
            struct timespec ts = {0, 100000};
            nanosleep(&ts, NULL);
            continue;
        }

        uint64_t sync_start = now_us();

        ssize_t written = writev(g_opt_sealed.fd, iovecs, (int)ready_count);
        if (written <= 0) { perror("sealed writev failed"); continue; }

        if (fsync(g_opt_sealed.fd) != 0) { perror("sealed fsync failed"); continue; }

        uint64_t max_id = ready_slots[ready_count - 1]->batch_id;
        atomic_store_explicit(&g_opt_sealed.last_synced_batch_id, max_id, memory_order_release);

        for (size_t i = 0; i < ready_count; i++)
            atomic_store_explicit(&ready_slots[i]->state, 0, memory_order_release);

        atomic_store_explicit(&g_opt_sealed.slot_tail, t, memory_order_release);

        double fs_s = ((double)(now_us() - sync_start)) / 1e6;
        hist_obs(g_fsync_hist, fsync_bucket_le, HN, fs_s, &g_fsync_count, &g_fsync_sum_s);
    }

    return NULL;
}



uint64_t AOF_sealed_last_synced_id(void) {
    return atomic_load(&g_opt_sealed.last_synced_batch_id);
}

/* Initialize sealed AOF system */
int AOF_sealed_init(const char *path) {
    char sealed_file[512];
    snprintf(sealed_file, sizeof(sealed_file), "%s.sealed", path);

    /* Open with O_DIRECT for bypassing page cache */
    g_opt_sealed.fd = open(sealed_file,
                           O_CREAT | O_APPEND | O_WRONLY | O_DIRECT | O_CLOEXEC,
                           0600);
    if (g_opt_sealed.fd < 0) {
        /* Fallback without O_DIRECT if not supported */
        g_opt_sealed.fd = open(sealed_file,
                               O_CREAT | O_APPEND | O_WRONLY | O_CLOEXEC,
                               0600);
    }

    if (g_opt_sealed.fd < 0) {
        perror("Failed to open optimized sealed file");
        return -1;
    }

    /* Memory map aligned buffer for O_DIRECT */
    g_opt_sealed.mmap_buffer = mmap(NULL, SEALED_MMAP_SIZE,
                                    PROT_READ | PROT_WRITE,
                                    MAP_PRIVATE | MAP_ANONYMOUS, -1, 0);
    if (g_opt_sealed.mmap_buffer == MAP_FAILED) {
        close(g_opt_sealed.fd);
        return -1;
    }

    g_opt_sealed.mmap_size = SEALED_MMAP_SIZE;
    atomic_store(&g_opt_sealed.write_head, 0);
    atomic_store(&g_opt_sealed.sync_tail, 0);
    atomic_store(&g_opt_sealed.slot_head, 0);
    atomic_store(&g_opt_sealed.slot_tail, 0);
    atomic_store(&g_opt_sealed.next_batch_id, 1);
    atomic_store(&g_opt_sealed.shutdown, false);
    atomic_store(&g_opt_sealed.gc_active, false);
    if (pthread_mutex_init(&g_opt_sealed.gc_lock, NULL) != 0) {
        munmap(g_opt_sealed.mmap_buffer, SEALED_MMAP_SIZE);
        close(g_opt_sealed.fd);
        return -1;
    }
    atomic_store(&g_opt_sealed.last_synced_batch_id, 0);


    const char *h = getenv("RF_SEALED_TEST_HOLD");
    const char *d = getenv("RF_SEALED_TEST_DELAY_US");
    if (h) AOF_sealed_test_set_hold(atoi(h));
    if (d) AOF_sealed_test_set_delay_us(atoi(d));


    /* Initialize all slots as free */
    for (int i = 0; i < MAX_SEALED_BATCHES; i++) {
        atomic_store(&g_opt_sealed.slots[i].state, 0);
    }

    /* Start optimized sync thread */
    if (pthread_create(&g_opt_sealed.sync_thread, NULL,
                       sealed_sync_thread, NULL) != 0) {
        munmap(g_opt_sealed.mmap_buffer, SEALED_MMAP_SIZE);
        close(g_opt_sealed.fd);
        return -1;
    }

    return 0;
}

static inline void sealed_wait_space(void) {
    while (1) {
        uint64_t h = atomic_load_explicit(&g_opt_sealed.slot_head, memory_order_acquire);
        uint64_t t = atomic_load_explicit(&g_opt_sealed.slot_tail, memory_order_acquire);
        if (h - t < MAX_SEALED_BATCHES) return;
        struct timespec ts = {0, 100000}; // 100s
        nanosleep(&ts, NULL);
    }
}

/* Sealed append function for AOF */
uint64_t AOF_append_sealed(int id, const void *data, size_t sz) {
    if (atomic_load(&g_opt_sealed.shutdown)) return 0;

    while (atomic_load(&g_opt_sealed.gc_active)) {
        struct timespec ts = {0, 100000};
        nanosleep(&ts, NULL);
    }

    sealed_wait_space();

    uint64_t batch_id = rf_fetch_add_u64_relaxed(&g_opt_sealed.next_batch_id, 1);
    uint64_t timestamp = now_us();

    size_t total_size   = 32 + sz + 4;
    size_t aligned_size = (total_size + 511) & ~((size_t)511);

    uint32_t mmap_offset = sealed_reserve_contiguous(aligned_size);

    uint64_t seq = atomic_fetch_add_explicit(&g_opt_sealed.slot_head, 1, memory_order_acq_rel);
    sealed_batch_slot_t *slot = &g_opt_sealed.slots[seq % MAX_SEALED_BATCHES];

    // claim slot
    int expect = 0;
    while (!atomic_compare_exchange_weak_explicit(&slot->state, &expect, 1,
                                                  memory_order_acq_rel, memory_order_relaxed)) {
        expect = 0;
        cpu_relax();
    }

    char *write_ptr = (char*)g_opt_sealed.mmap_buffer + mmap_offset;

    uint32_t magic = 0x5EA1ED02, sz32 = (uint32_t)sz, id32 = (uint32_t)id, reserved = 0;
    memcpy(write_ptr + 0,  &magic,     4);
    memcpy(write_ptr + 4,  &batch_id,  8);
    memcpy(write_ptr + 12, &timestamp, 8);
    memcpy(write_ptr + 20, &sz32,      4);
    memcpy(write_ptr + 24, &id32,      4);
    memcpy(write_ptr + 28, &reserved,  4);

    memcpy(write_ptr + 32, data, sz);

    uint32_t crc = crc32c(0, write_ptr, (unsigned)(32 + sz));
    memcpy(write_ptr + 32 + sz, &crc, 4);

    if (aligned_size > total_size) {
        memset(write_ptr + total_size, 0, aligned_size - total_size);
    }

    slot->batch_id = batch_id;
    slot->size = (uint32_t)aligned_size;
    slot->offset_in_mmap = mmap_offset;
    slot->timestamp = timestamp;

    atomic_thread_fence(memory_order_release);
    atomic_store_explicit(&slot->state, 2, memory_order_release); // ready

    return batch_id;
}


int AOF_sealed_wait(uint64_t batch_id) {
    while (!atomic_load(&g_opt_sealed.shutdown)) {
        uint64_t last = atomic_load_explicit(&g_opt_sealed.last_synced_batch_id, memory_order_acquire);
        if (last >= batch_id) return 0;
        struct timespec ts = {0, 100000}; // 100s
        nanosleep(&ts, NULL);
    }
    return -1;
}

static inline uint32_t rd32(const void *p){ uint32_t v; memcpy(&v,p,4); return v; }
static inline uint64_t rd64(const void *p){ uint64_t v; memcpy(&v,p,8); return v; }
/* small helpers used by sealed GC and replay */
static inline uint32_t rd32_gc(const void *p){ uint32_t v; memcpy(&v,p,4); return v; }
static inline uint64_t rd64_gc(const void *p){ uint64_t v; memcpy(&v,p,8); return v; }

/* Public: compact the sealed file by dropping old chunks (by age, then by size) */
int AOF_sealed_gc(uint64_t max_age_ms, uint64_t max_keep_bytes, uint64_t *bytes_after)
{
    pthread_mutex_lock(&g_opt_sealed.gc_lock);
    atomic_store(&g_opt_sealed.gc_active, true);

    /* Wait until no "ready" slots remain to make file state consistent */
    for (;;) {
        bool any_ready = false;
        for (int i = 0; i < MAX_SEALED_BATCHES; i++)
            if (atomic_load(&g_opt_sealed.slots[i].state) == 2) { any_ready = true; break; }
        if (!any_ready) break;
        struct timespec ts = {0, 200000}; nanosleep(&ts, NULL);
    }

    /* Close fd so we can rewrite atomically */
    if (g_opt_sealed.fd >= 0) { fsync(g_opt_sealed.fd); close(g_opt_sealed.fd); g_opt_sealed.fd = -1; }

    char path[1024];
    snprintf(path, sizeof(path), "%s.sealed", g_fd_state.file_path);

    int in = open(path, O_RDONLY | O_CLOEXEC);
    if (in < 0) goto reopen;

    struct stat st;
    if (fstat(in, &st) != 0 || st.st_size == 0) { close(in); goto reopen; }

    size_t fsz = (size_t)st.st_size;
    uint8_t *buf = malloc(fsz);
    if (!buf) { close(in); goto reopen; }
    ssize_t r = read(in, buf, fsz);
    close(in);
    if (r != (ssize_t)fsz) { free(buf); goto reopen; }

    uint64_t now   = now_us();
    uint64_t cutus = (max_age_ms ? now - max_age_ms * 1000ULL : 0);

    const uint8_t *p = buf, *end = buf + fsz, *keep_from = buf;

    /* pass 1: drop up to age cutoff */
    while (p + 32 <= end) {
        uint32_t magic = rd32_gc(p + 0);
        if (magic != 0x5EA1ED02) {
            uintptr_t cur = (uintptr_t)(p - buf);
            uintptr_t next = (cur + 511u) & ~((uintptr_t)511u);
            if (next > (uintptr_t)fsz) break;
            p = buf + next; continue;
        }
        uint64_t ts = rd64_gc(p + 12);
        uint32_t sz = rd32_gc(p + 20);
        size_t total = 32 + (size_t)sz + 4;
        size_t aligned = (total + 511) & ~((size_t)511);
        if (cutus && ts < cutus) { keep_from = p + aligned; p += aligned; } else { break; }
    }

    /* pass 2: enforce max tail-bytes */
    if (max_keep_bytes) {
        const uint8_t *q = keep_from;
        while (q + 32 <= end && (size_t)(end - q) > max_keep_bytes) {
            if (rd32_gc(q + 0) != 0x5EA1ED02) {
                uintptr_t cur = (uintptr_t)(q - buf);
                uintptr_t next = (cur + 511u) & ~((uintptr_t)511u);
                if (next > (uintptr_t)fsz) break;
                q = buf + next; continue;
            }
            uint32_t sz = rd32_gc(q + 20);
            size_t total = 32 + (size_t)sz + 4;
            size_t aligned = (total + 511) & ~((size_t)511);
            q += aligned;
        }
        keep_from = q;
    }

    if (keep_from >= end) {
        int out = open(path, O_TRUNC | O_WRONLY | O_CLOEXEC, 0600);
        if (out >= 0) { fsync(out); close(out); }
        if (bytes_after) *bytes_after = 0;
        free(buf); goto reopen;
    }
    char tmp[1024];
    snprintf(tmp, sizeof(tmp), "%s.tmp", path);
    int out = open(tmp, O_CREAT | O_TRUNC | O_WRONLY | O_CLOEXEC, 0600);
    if (out >= 0) {
        size_t keep_sz = (size_t)(end - keep_from);
        write(out, keep_from, keep_sz);
        fsync(out); close(out);
        rename(tmp, path);
        if (bytes_after) *bytes_after = (uint64_t)keep_sz;
    }
    free(buf);

    reopen:
    g_opt_sealed.fd = open(path, O_CREAT | O_APPEND | O_WRONLY | O_CLOEXEC, 0600);
    atomic_store(&g_opt_sealed.gc_active, false);
    pthread_mutex_unlock(&g_opt_sealed.gc_lock);
    return 0;
}

size_t AOF_sealed_queue_depth(void) {
    size_t ready = 0;
    for (int i = 0; i < MAX_SEALED_BATCHES; i++) {
        if (atomic_load(&g_opt_sealed.slots[i].state) == 2) ready++;
    }
    return ready;
}

size_t AOF_sealed_replay(const char *aof_path) {
    char path[1024];
    snprintf(path, sizeof(path), "%s.sealed", aof_path ? aof_path : "./append.aof");

    int local_fd = open(path, O_RDONLY | O_CLOEXEC);
    if (local_fd < 0) {
        if (errno != ENOENT)
            fprintf(stderr, "AOF_sealed_replay: open(%s): %s\n", path, strerror(errno));
        return 0;
    }

    struct stat st;
    if (fstat(local_fd, &st) != 0 || st.st_size == 0) {
        close(local_fd);
        return 0;
    }

    const size_t fsz = (size_t)st.st_size;

    uint8_t *buf = malloc(fsz);
    if (!buf) {
        close(local_fd);
        return 0;
    }

    ssize_t r = read(local_fd, buf, fsz);
    close(local_fd);
    if (r != (ssize_t)fsz) {
        free(buf);
        return 0;
    }

    size_t applied = 0, bad = 0, skipped = 0;

    const uint8_t *p = buf, *end = buf + fsz;
    while (p + 32 <= end) {
        const uint32_t magic = rd32(p + 0);
        if (magic != 0x5EA1ED02) {
            const uintptr_t cur  = (uintptr_t)(p - buf);
            const uintptr_t next = (cur + 511u) & ~((uintptr_t)511u);
            if (next <= (uintptr_t)fsz) p = buf + next; else break;
            skipped++;
            continue;
        }

        const uint32_t sz     = rd32(p + 20);
        const uint32_t rec_id = rd32(p + 24);

        const uint8_t *payload = p + 32;
        if (payload + sz + 4 > end) { bad++; break; }

        const uint32_t crc_file = rd32(payload + sz);
        const uint32_t crc_calc = crc32c(0, p, 32 + sz);

        if (crc_calc != crc_file) {
            fprintf(stderr, "AOF_sealed_replay: CRC mismatch (id=%u, sz=%u)\n", rec_id, sz);
            bad++;
        } else {
            if (!RAMForge_HA_replay_record((uint32_t)rec_id, payload, sz))
                rf_broker_replay_aof((int)rec_id, payload, sz);
            applied++;
        }

        size_t total   = 32 + (size_t)sz + 4;
        size_t aligned = (total + 511) & ~((size_t)511);
        p += aligned;
    }

    if (applied || bad || skipped) {
        printf(" PID %d sealed replay: applied=%zu, skipped=%zu, bad=%zu (file=%s, %zu bytes)\n",
               getpid(), applied, skipped, bad, path, fsz);
    }

    free(buf);
    return applied;
}

static void init_tls_fd_cache(void) {
    if (pthread_mutex_init(&tls_fd_cache.lock, NULL) != 0) {
        fprintf(stderr, "Failed to initialize TLS fd cache mutex\n");
        exit(1);
    }
    tls_fd_cache.fd = -1;
    tls_fd_cache.generation = 0;
}

/*  Safe fd acquisition with generation check */
static int get_valid_fd_for_write(void) {
    pthread_once(&tls_init_once, init_tls_fd_cache);

    /* Fast path: check if our cached fd is still valid */
    uint64_t current_gen = atomic_load(&g_fd_state.master_generation);

    pthread_mutex_lock(&tls_fd_cache.lock);

    if (tls_fd_cache.fd >= 0 && tls_fd_cache.generation == current_gen) {
        int cached_fd = tls_fd_cache.fd;
        pthread_mutex_unlock(&tls_fd_cache.lock);
        return cached_fd;
    }

    /* Slow path: need to refresh our fd */
    if (tls_fd_cache.fd >= 0) {
        close(tls_fd_cache.fd);
        tls_fd_cache.fd = -1;
    }

    /* Acquire read lock to prevent rotation during fd acquisition */
    pthread_rwlock_rdlock(&g_fd_state.rotation_rwlock);

    /* Re-check generation after acquiring lock */
    current_gen = atomic_load(&g_fd_state.master_generation);

    /* Open our own fd to the current file */
    int new_fd = open(g_fd_state.file_path, O_CREAT | O_APPEND | O_WRONLY | O_CLOEXEC, 0600);

    pthread_rwlock_unlock(&g_fd_state.rotation_rwlock);

    if (new_fd >= 0) {
        tls_fd_cache.fd = new_fd;
        tls_fd_cache.generation = current_gen;

        atomic_fetch_add(&g_metrics.aof_switches, 1);
    } else {
        perror("get_valid_fd_for_write: open failed");
    }

    pthread_mutex_unlock(&tls_fd_cache.lock);
    return new_fd;
}

/*  io_uring initialization  */
static int init_io_uring(void) {
    if (running_under_valgrind()) {
        printf(" Valgrind detected disabling io_uring for this run\n");
        atomic_store(&g_uring_ctx.initialized, false);
        return -1; // NOTE: Aidarbek eger baska error shyksa onda return -1 ge ornyna keltir
    }
    struct io_uring_params params = {0};
    params.flags = IORING_SETUP_SQPOLL | IORING_SETUP_SQ_AFF;
    params.sq_thread_idle = 2000;  /* 2 second idle timeout */


    int ret = io_uring_queue_init_params(URING_QUEUE_DEPTH, &g_uring_ctx.ring, &params);
    if (ret < 0) {
        fprintf(stderr, "io_uring_queue_init_params failed: %s\n", strerror(-ret));
        atomic_store(&g_uring_ctx.initialized, false);
        return -1;
    }

    if (pthread_mutex_init(&g_uring_ctx.submit_lock, NULL) != 0) {
        io_uring_queue_exit(&g_uring_ctx.ring);
        return -1;
    }

    atomic_store(&g_uring_ctx.pending_writes, 0);
    atomic_store(&g_uring_ctx.initialized, true);

    return 0;
}

static void destroy_io_uring(void) {
    if (atomic_load(&g_uring_ctx.initialized)) {
        /* Wait for pending operations */
        while (atomic_load(&g_uring_ctx.pending_writes) > 0) {
            struct io_uring_cqe *cqe;
            if (io_uring_wait_cqe_timeout(&g_uring_ctx.ring, &cqe,
                                          &(struct __kernel_timespec){0, 1000000}) == 0) { // 1ms
                atomic_fetch_sub(&g_uring_ctx.pending_writes, 1);
                io_uring_cqe_seen(&g_uring_ctx.ring, cqe);
            }
        }


        pthread_mutex_destroy(&g_uring_ctx.submit_lock);
        io_uring_queue_exit(&g_uring_ctx.ring);
        atomic_store(&g_uring_ctx.initialized, false);
        printf(" io_uring destroyed\n");
    }
}

/*  Write-ahead buffer initialization */
static int init_wal_buffer(void) {
    g_wal_buffer.buffer = mmap(NULL, WRITE_AHEAD_BUFFER_SIZE,
                               PROT_READ | PROT_WRITE,
                               MAP_PRIVATE | MAP_ANONYMOUS, -1, 0);
    if (g_wal_buffer.buffer == MAP_FAILED) {
        perror("mmap WAL buffer");
        return -1;
    }

    g_wal_buffer.capacity = WRITE_AHEAD_BUFFER_SIZE;
    g_wal_buffer.size = 0;
    g_wal_buffer.write_pos = 0;
    atomic_store(&g_wal_buffer.flushing, false);

    if (pthread_mutex_init(&g_wal_buffer.lock, NULL) != 0) {
        munmap(g_wal_buffer.buffer, WRITE_AHEAD_BUFFER_SIZE);
        return -1;
    }

    return 0;
}

static void destroy_wal_buffer(void) {
    if (g_wal_buffer.buffer != MAP_FAILED && g_wal_buffer.buffer != NULL) {
        pthread_mutex_destroy(&g_wal_buffer.lock);
        munmap(g_wal_buffer.buffer, WRITE_AHEAD_BUFFER_SIZE);
        g_wal_buffer.buffer = NULL;
        printf(" WAL buffer destroyed\n");
    }
}

/*  Enhanced buffer pool with lock-free operations */
static int init_buffer_pool(void) {
    memset(&g_buffer_pool, 0, sizeof(g_buffer_pool));

    if (pthread_mutex_init(&g_buffer_pool.lock, NULL) != 0) {
        return -1;
    }

    atomic_store(&g_buffer_pool.shutdown_flag, false);

    /* Pre-allocate all nodes */
    buffer_node_t *nodes = calloc(BUFFER_POOL_SIZE, sizeof(buffer_node_t));
    if (!nodes) {
        pthread_mutex_destroy(&g_buffer_pool.lock);
        return -1;
    }

    /* Link all nodes and allocate their buffers */
    buffer_node_t *prev = NULL;
    size_t successful = 0;

    for (size_t i = 0; i < BUFFER_POOL_SIZE; i++) {
        nodes[i].data = aligned_alloc(4096, MAX_BUFFER_SIZE);  /* Page-aligned for io_uring */
        if (!nodes[i].data) {
            break;
        }

        nodes[i].capacity = MAX_BUFFER_SIZE;
        nodes[i].next = prev;
        atomic_store(&nodes[i].in_use, false);
        prev = &nodes[i];
        successful++;
    }

    g_buffer_pool.free_list = prev;
    g_buffer_pool.all_nodes = nodes;
    g_buffer_pool.free_count = successful;
    g_buffer_pool.total_allocated = successful;

    return 0;
}

static void *get_buffer_from_pool(size_t needed_size) {
    if (needed_size > MAX_BUFFER_SIZE) {
        return aligned_alloc(4096, (needed_size + 4095) & ~4095);
    }

    if (atomic_load(&g_buffer_pool.shutdown_flag)) {
        return aligned_alloc(4096, (needed_size + 4095) & ~4095);
    }

    pthread_mutex_lock(&g_buffer_pool.lock);

    if (g_buffer_pool.free_list && g_buffer_pool.free_count > 0) {
        buffer_node_t *node = g_buffer_pool.free_list;
        g_buffer_pool.free_list = node->next;
        g_buffer_pool.free_count--;
        atomic_store(&node->in_use, true);

        pthread_mutex_unlock(&g_buffer_pool.lock);
        return node->data;
    }

    pthread_mutex_unlock(&g_buffer_pool.lock);
    return aligned_alloc(4096, (needed_size + 4095) & ~4095);
}

static void return_buffer_to_pool(void *data, size_t size) {
    if (size > MAX_BUFFER_SIZE || !data) {
        free(data);
        return;
    }

    if (atomic_load(&g_buffer_pool.shutdown_flag)) {
        free(data);
        return;
    }

    pthread_mutex_lock(&g_buffer_pool.lock);

    /* Find the node that owns this data buffer */
    buffer_node_t *found = NULL;
    for (size_t i = 0; i < g_buffer_pool.total_allocated; i++) {
        if (g_buffer_pool.all_nodes[i].data == data) {
            found = &g_buffer_pool.all_nodes[i];
            break;
        }
    }

    if (found && g_buffer_pool.free_count < g_buffer_pool.total_allocated) {
        atomic_store(&found->in_use, false);
        found->next = g_buffer_pool.free_list;
        g_buffer_pool.free_list = found;
        g_buffer_pool.free_count++;
        pthread_mutex_unlock(&g_buffer_pool.lock);
        return;
    }

    pthread_mutex_unlock(&g_buffer_pool.lock);
    free(data);
}

static void destroy_buffer_pool(void) {
    printf("  Destroying buffer pool...\n");

    atomic_store(&g_buffer_pool.shutdown_flag, true);

    pthread_mutex_lock(&g_buffer_pool.lock);

    if (g_buffer_pool.all_nodes) {
        for (size_t i = 0; i < g_buffer_pool.total_allocated; i++) {
            free(g_buffer_pool.all_nodes[i].data);
            g_buffer_pool.all_nodes[i].data = NULL;
        }
        free(g_buffer_pool.all_nodes);
        g_buffer_pool.all_nodes = NULL;
    }

    g_buffer_pool.free_list = NULL;
    g_buffer_pool.free_count = 0;
    g_buffer_pool.total_allocated = 0;

    pthread_mutex_unlock(&g_buffer_pool.lock);
    pthread_mutex_destroy(&g_buffer_pool.lock);

    printf(" Buffer pool destroyed\n");
}

static inline bool is_fd_valid(int check_fd) {
    if (check_fd < 0) return false;

    /* Quick validation - try fcntl */
    int flags = fcntl(check_fd, F_GETFL);
    return flags != -1;
}

static void AOF_reopen_if_needed(void) {
    uint64_t gen = atomic_load(&g_metrics.aof_generation);

    /* Check if rotation is in progress - wait if needed */
    if (atomic_load(&rotation_in_progress)) {
        pthread_mutex_lock(&rotation_lock);
        pthread_mutex_unlock(&rotation_lock); /* Just to wait */
    }

    if (gen != local_generation || !is_fd_valid(fd)) {
        pthread_mutex_lock(&rotation_lock);

        /* Double-check after acquiring lock */
        gen = atomic_load(&g_metrics.aof_generation);
        if (gen != local_generation || !is_fd_valid(fd)) {
            int newfd = open(g_path, O_CREAT | O_APPEND | O_WRONLY | O_CLOEXEC, 0600);
            if (newfd >= 0) {
                int old = fd;
                fd = newfd;
                if (old >= 0 && is_fd_valid(old)) {
                    close(old);
                }
                local_generation = gen;
                atomic_fetch_add(&g_metrics.aof_switches, 1);
            } else {
                perror("AOF_reopen_if_needed: open");
            }
        }

        pthread_mutex_unlock(&rotation_lock);
    }
}

static int submit_uring_write(const void *buffer, size_t size) {
    if (!atomic_load(&g_uring_ctx.initialized)) {
        return -1;
    }

    /* Get a validated fd */
    int write_fd = get_valid_fd_for_write();
    if (write_fd < 0) {
        fprintf(stderr, " Failed to get valid fd for write\n");
        return -1;
    }

    uint64_t current_gen = atomic_load(&g_fd_state.master_generation);

    write_batch_t *batch = malloc(sizeof(*batch));
    if (!batch) return -1;

    batch->buffer = malloc(size);
    if (!batch->buffer) {
        free(batch);
        return -1;
    }

    memcpy(batch->buffer, buffer, size);
    batch->size = size;
    batch->fd_used = write_fd;
    batch->generation = current_gen;
    atomic_fetch_add(&write_sequence, 1);
    batch->sequence = atomic_load(&write_sequence) - 1;
    atomic_store(&batch->submitted, false);
    atomic_store(&batch->completed, false);

    pthread_mutex_lock(&g_uring_ctx.submit_lock);

    /* Double-check fd is still valid before submission */
    if (fcntl(write_fd, F_GETFL) == -1) {
        pthread_mutex_unlock(&g_uring_ctx.submit_lock);
        free(batch->buffer);
        free(batch);
        fprintf(stderr, " fd became invalid during submission preparation\n");
        return -1;
    }

    struct io_uring_sqe *sqe = io_uring_get_sqe(&g_uring_ctx.ring);
    if (!sqe) {
        pthread_mutex_unlock(&g_uring_ctx.submit_lock);
        free(batch->buffer);
        free(batch);
        return -1;
    }

    io_uring_prep_write(sqe, write_fd, batch->buffer, size, -1);
    io_uring_sqe_set_data(sqe, batch);
    atomic_store(&batch->submitted, true);

    int ret = io_uring_submit(&g_uring_ctx.ring);
    if (ret > 0) {
        atomic_fetch_add(&g_uring_ctx.pending_writes, 1);
        pthread_mutex_unlock(&g_uring_ctx.submit_lock);
        return 0;
    } else {
        atomic_store(&batch->submitted, false);
        free(batch->buffer);
        free(batch);
        pthread_mutex_unlock(&g_uring_ctx.submit_lock);
        return -1;
    }
}

/*  Fast record writing with CRC  */
static inline size_t write_record_to_buffer(char *buf, int id, const void *data, uint32_t size) {
    char *ptr = buf;

    /* Write header */
    memcpy(ptr, &id, 4); ptr += 4;
    memcpy(ptr, &size, 4); ptr += 4;

    /* Write data */
    memcpy(ptr, data, size); ptr += size;

    /* Calculate and write CRC */
    uint32_t crc = crc32c(0, &id, 4);
    crc = crc32c(crc, &size, 4);
    crc = crc32c(crc, data, size);
    memcpy(ptr, &crc, 4); ptr += 4;

    return ptr - buf;
}

/*  io_uring worker thread */
static void *uring_worker_thread(void *arg) {
    (void)arg;
    if (!atomic_load(&g_uring_ctx.initialized)) {
        return NULL;
    }

    while (atomic_load(&running)) {
        struct io_uring_cqe *cqe;
        int ret = io_uring_wait_cqe_timeout(&g_uring_ctx.ring, &cqe,
                                            &(struct __kernel_timespec){0, 100000000}); /* 100ms */

        if (ret == -ETIME) {
            continue;
        }

        if (ret < 0) {
            if (ret != -EINTR) {
                fprintf(stderr, "io_uring_wait_cqe error: %s\n", strerror(-ret));
            }
            continue;
        }

        /* Process completion */
        write_batch_t *batch = (write_batch_t *)cqe->user_data;
        if (batch) {
            if (cqe->res < 0) {
                if (cqe->res == -EBADF) {
                    /* This is expected during rotation - log at debug level only */
                    uint64_t current_gen = atomic_load(&g_fd_state.master_generation);
                    if (batch->generation < current_gen) {
                        printf(" Expected EBADF during rotation (gen %" PRIu64 " -> %" PRIu64 ")\n",
                               batch->generation, current_gen);
                    } else {
                        fprintf(stderr, " Unexpected EBADF (same generation %" PRIu64 ")\n",
                                current_gen);
                    }
                } else {
                    fprintf(stderr, "io_uring write failed: %s\n", strerror(-cqe->res));
                }
                atomic_fetch_add(&g_metrics.write_failures, 1);
            } else {
                atomic_store(&batch->completed, true);
                atomic_fetch_add(&g_metrics.successful_writes, 1);
            }

            free(batch->buffer);
            free(batch);
        }

        atomic_fetch_sub(&g_uring_ctx.pending_writes, 1);
        io_uring_cqe_seen(&g_uring_ctx.ring, cqe);
    }

    printf(" Enhanced io_uring worker thread exiting\n");
    return NULL;
}

static void cleanup_ring_entry(aof_cmd_t *c) {
    if (c->data) {
        if (c->from_pool) {
            return_buffer_to_pool(c->data, c->sz);
        } else {
            free(c->data);
        }
        c->data = NULL;
    }
    c->from_pool = 0;
    atomic_store(&c->written, false);
}

/*  Enhanced background writer with io_uring batching */
static void *writer_thread(void *arg) {
    (void)arg;

    size_t per_rec = 12 + MAX_BUFFER_SIZE;
    size_t batch_size = (size_t) BATCH_SIZE * per_rec;
    size_t alloc_sz = (batch_size + 4095) & ~((size_t) 4095);
    char *batch_buffer = aligned_alloc(4096, alloc_sz);
    if (!batch_buffer) {
        fprintf(stderr, "Failed to allocate batch buffer\n");
        return NULL;
    }

    pthread_cleanup_push(free, batch_buffer);

            while (atomic_load(&running)) {
                /* Check for rotation at the start of each iteration */
                AOF_reopen_if_needed();

                pthread_mutex_lock(&lock);

                while (head == tail && atomic_load(&running)) {
                    pthread_cond_wait(&cond, &lock);
                }

                if (!atomic_load(&running)) {
                    pthread_mutex_unlock(&lock);
                    break;
                }

                /* Batch processing */
                size_t batch_count = 0;
                size_t total_batch_size = 0;

                while (head != tail && batch_count < BATCH_SIZE && is_fd_valid(fd)) {
                    aof_cmd_t *c = &ring[tail];

                    /* Write to batch buffer */
                    size_t record_size = write_record_to_buffer(
                            batch_buffer + total_batch_size, c->id, c->data, c->sz);

                    total_batch_size += record_size;
                    batch_count++;

                    /* Buffer for rewrite if active */
                    if (atomic_load(&rewrite_active)) {
                        pthread_mutex_lock(&rewrite_lock);
                        size_t nxt = (rewrite_buf_head + 1) & rewrite_buf_mask;
                        if (nxt != rewrite_buf_tail) {
                            void *copy = malloc(c->sz);
                            if (copy) {
                                memcpy(copy, c->data, c->sz);
                                rewrite_buffer[rewrite_buf_head] = (aof_cmd_t){
                                        c->id, c->sz, copy, 0, c->sequence, ATOMIC_VAR_INIT(false)
                                };
                                rewrite_buf_head = nxt;
                            }
                        }
                        pthread_mutex_unlock(&rewrite_lock);
                    }

                    cleanup_ring_entry(c);
                    tail = (tail + 1) & mask;
                }

                pthread_mutex_unlock(&lock);

                /* Submit batch */
                if (batch_count > 0) {
                    if (atomic_load(&g_uring_ctx.initialized)) {
                        if (submit_uring_write(batch_buffer, total_batch_size) != 0) {
                            /* Fallback to synchronous write on uring failure */
                            AOF_reopen_if_needed();
                            if (is_fd_valid(fd)) {
                                ssize_t w = write(fd, batch_buffer, total_batch_size);
                                if (w != (ssize_t) total_batch_size) {
                                    if (errno == EBADF) {
                                        fprintf(stderr, "  Write failed due to bad fd - attempting recovery\n");
                                        AOF_reopen_if_needed();
                                    } else {
                                        perror("AOF sync write fallback");
                                    }
                                }
                            }
                        }
                    } else {
                        /* Synchronous fallback */
                        AOF_reopen_if_needed();
                        if (is_fd_valid(fd)) {
                            ssize_t w = write(fd, batch_buffer, total_batch_size);
                            if (w != (ssize_t) total_batch_size) {
                                if (errno == EBADF) {
                                    fprintf(stderr, " Sync write failed due to bad fd\n");
                                    AOF_reopen_if_needed();
                                } else {
                                    perror("AOF write");
                                }
                            }
                        }
                    }
                }

                pthread_cond_signal(&cond);

                /* Sleep briefly to allow batching */
                struct timespec ts;
                ts.tv_sec = 0;
                ts.tv_nsec = flush_ms * 1000000ULL;
                nanosleep(&ts, NULL);
            }

    pthread_cleanup_pop(1);
    printf(" Enhanced writer thread exiting\n");
    return NULL;
}

/* Segment rewrite (unchanged but optimized) */
static void dump_record_cb(int id, const void *data, size_t sz, void *ud) {
    int out_fd  = (int)(intptr_t)ud;

    /* Use pre-allocated buffer for record writing */
    char record_buf[12 + MAX_BUFFER_SIZE];
    size_t record_size = write_record_to_buffer(record_buf, id, data, (uint32_t)sz);

    ssize_t written = write(out_fd , record_buf, record_size);
    if (written != (ssize_t)record_size) {
        fprintf(stderr, "Write error in dump_record_cb\n");
    }
}

static void *segment_rewrite_thread_func(void *arg) {
    rewrite_task_t *task = (rewrite_task_t*)arg;

    printf("Starting segment rewrite: %s\n", task->source_path);

    char tmp_path[1024];
    snprintf(tmp_path, sizeof(tmp_path), "%s.compact", task->output_path);

    int fd_out = open(tmp_path, O_CREAT | O_TRUNC | O_WRONLY | O_CLOEXEC, 0600);
    if (fd_out < 0) {
        perror("segment_rewrite: open output");
        goto cleanup;
    }

    Storage temp_storage;
    storage_init(&temp_storage);

    int fd_in = open(task->source_path, O_RDONLY | O_CLOEXEC);
    if (fd_in < 0) {
        perror("segment_rewrite: open input");
        close(fd_out);
        unlink(tmp_path);
        storage_destroy(&temp_storage);
        goto cleanup;
    }

    /* Parse AOF file */
    int id;
    uint32_t size, crc_file;
    while (read(fd_in, &id, 4) == 4) {
        if (read(fd_in, &size, 4) != 4) {
            fprintf(stderr, "segment_rewrite: corrupt size field\n");
            break;
        }

        void *buf = malloc(size);
        if (!buf) {
            fprintf(stderr, "segment_rewrite: malloc failed\n");
            break;
        }

        if (read(fd_in, buf, size) != (ssize_t)size) {
            fprintf(stderr, "segment_rewrite: corrupt data field\n");
            free(buf);
            break;
        }

        if (read(fd_in, &crc_file, 4) != 4) {
            fprintf(stderr, "segment_rewrite: corrupt crc field\n");
            free(buf);
            break;
        }

        uint32_t crc = crc32c(0, &id, 4);
        crc = crc32c(crc, &size, 4);
        crc = crc32c(crc, buf, size);

        if (crc != crc_file) {
            fprintf(stderr, "segment_rewrite: CRC mismatch\n");
            free(buf);
            break;
        }

        storage_save(&temp_storage, id, buf, size);
        free(buf);
    }

    close(fd_in);

    storage_iterate(&temp_storage, dump_record_cb, (void*)(intptr_t)fd_out);

    fsync(fd_out);
    close(fd_out);

    if (rename(tmp_path, task->output_path) != 0) {
        perror("segment_rewrite: rename failed");
        unlink(tmp_path);
    } else {
        printf("Segment rewrite complete: %s\n", task->output_path);
    }

    storage_destroy(&temp_storage);

    cleanup:
    free(task->source_path);
    free(task->output_path);
    free(task);
    atomic_store(&segment_rewrite_active, false);
    return NULL;
}

/* Rest of the segment rewrite functions (unchanged)*/
int AOF_begin_rewrite(const char *source_path) {
    if (atomic_load(&segment_rewrite_active)) {
        printf(" Segment rewrite already in progress\n");
        return -1;
    }

    rewrite_task_t *task = malloc(sizeof(rewrite_task_t));
    if (!task) {
        return -1;
    }

    task->source_path = strdup(source_path);
    task->output_path = strdup(source_path);
    task->storage = NULL;

    if (!task->source_path || !task->output_path) {
        free(task->source_path);
        free(task->output_path);
        free(task);
        return -1;
    }

    atomic_store(&segment_rewrite_active, true);

    if (pthread_create(&segment_rewrite_thread, NULL, segment_rewrite_thread_func, task)) {
        perror("pthread_create segment rewrite");
        atomic_store(&segment_rewrite_active, false);
        free(task->source_path);
        free(task->output_path);
        free(task);
        return -1;
    }

    pthread_detach(segment_rewrite_thread);
    return 0;
}

int AOF_segment_rewrite_in_progress(void) {
    return atomic_load(&segment_rewrite_active);
}


/*  Non-blocking rewrite thread (enhanced) */
static void *rewrite_thread_func(void *arg) {
    Storage *st = (Storage*)arg;

    printf(" Starting non-blocking AOF rewrite...\n");

    char tmp[512];
    snprintf(tmp, sizeof tmp, "%s.rewrite", g_path);
    int fd_tmp = open(tmp, O_CREAT | O_TRUNC | O_WRONLY | O_CLOEXEC, 0600);
    if (fd_tmp < 0) {
        perror("rewrite: open tmp");
        atomic_store(&rewrite_active, false);
        return NULL;
    }

    storage_iterate(st, dump_record_cb, (void*)(intptr_t)fd_tmp);

    pthread_mutex_lock(&rewrite_lock);
    printf(" Applying %zu buffered operations...\n",
           (rewrite_buf_head - rewrite_buf_tail) & rewrite_buf_mask);

    while (rewrite_buf_tail != rewrite_buf_head) {
        aof_cmd_t *c = &rewrite_buffer[rewrite_buf_tail];

        char record_buf[12 + MAX_BUFFER_SIZE];
        size_t record_size = write_record_to_buffer(record_buf, c->id, c->data, c->sz);
        write(fd_tmp, record_buf, record_size);

        free(c->data);
        c->data = NULL;
        rewrite_buf_tail = (rewrite_buf_tail + 1) & rewrite_buf_mask;
    }
    pthread_mutex_unlock(&rewrite_lock);

    fsync(fd_tmp);
    close(fd_tmp);

    pthread_mutex_lock(&lock);

    while (head != tail) {
        aof_cmd_t *c = &ring[tail];

        char record_buf[12 + MAX_BUFFER_SIZE];
        size_t record_size = write_record_to_buffer(record_buf, c->id, c->data, c->sz);
        write(fd, record_buf, record_size);

        cleanup_ring_entry(c);
        tail = (tail + 1) & mask;
    }
    fsync(fd);

    int old_fd = fd;
    fd = -1;
    close(old_fd);

    if (rename(tmp, g_path) != 0) {
        perror("rewrite: rename failed");
        fd = open(g_path, O_APPEND | O_WRONLY | O_CLOEXEC, 0600);
        if (fd < 0) {
            perror("rewrite: failed to restore fd");
            pthread_mutex_unlock(&lock);
            exit(1);
        }
        atomic_store(&rewrite_active, false);
        pthread_mutex_unlock(&lock);
        return NULL;
    }

    fd = open(g_path, O_APPEND | O_WRONLY | O_CLOEXEC, 0600);
    if (fd < 0) {
        perror("rewrite: reopen failed");
        pthread_mutex_unlock(&lock);
        exit(1);
    }

    pthread_mutex_unlock(&lock);

    atomic_store(&rewrite_active, false);
    printf("Non-blocking AOF rewrite complete!\n");
    return NULL;
}

/* Public API (enhanced) */
void AOF_init(const char *path, size_t ring_capacity, unsigned interval_ms) {


    if (AOF_sealed_init(path) != 0) {
        fprintf(stderr, "Warning: Sealed AOF initialization failed\n");
    }
    /* Initialize global fd state */
    g_fd_state.file_path = strdup(path);

    if (pthread_rwlock_init(&g_fd_state.rotation_rwlock, NULL) != 0) {
        fprintf(stderr, "Failed to initialize rotation rwlock\n");
        exit(1);
    }

    /* Open initial master fd */
    int initial_fd = open(path, O_CREAT | O_APPEND | O_WRONLY | O_CLOEXEC, 0600);
    if (initial_fd < 0) {
        perror("AOF_init_safe: open");
        exit(1);
    }

    atomic_store(&g_fd_state.master_fd, initial_fd);
    atomic_store(&g_fd_state.master_generation, 1);

    /* Initialize the rest of AOF system */
    mode_always = (interval_ms == 0);
    flush_ms = mode_always ? 1000 : interval_ms;
    g_path = strdup(path);

    /* Initialize components in order */
    if (init_buffer_pool() != 0) {
        fprintf(stderr, "Failed to initialize buffer pool\n");
        exit(1);
    }

    if (init_io_uring() != 0) {
        fprintf(stderr, "Failed to initialize io_uring\n");
        exit(1);
    }

    if (init_wal_buffer() != 0) {
        fprintf(stderr, "Failed to initialize WAL buffer\n");
        exit(1);
    }

    /* Initialize ring buffer for non-always mode */
    if (!mode_always) {
        cap = 1;
        while (cap < ring_capacity) cap <<= 1;
        mask = cap - 1;
        ring = calloc(cap, sizeof *ring);

        for (size_t i = 0; i < cap; i++) {
            atomic_init(&ring[i].written, false);
        }

        rewrite_buf_cap = cap;
        rewrite_buf_mask = cap - 1;
        rewrite_buffer = calloc(cap, sizeof(aof_cmd_t));

        if (pthread_mutex_init(&lock, NULL) != 0 ||
            pthread_cond_init(&cond, NULL) != 0 ||
            pthread_mutex_init(&rewrite_lock, NULL) != 0) {
            fprintf(stderr, "Failed to initialize mutexes\n");
            exit(1);
        }
    }

    atomic_store(&running, true);

    /* Start enhanced worker threads */
    if (atomic_load(&g_uring_ctx.initialized)) {
        if (pthread_create(&uring_worker, NULL, uring_worker_thread, NULL) != 0) {
            fprintf(stderr, "Failed to create safe io_uring worker thread\n");
            exit(1);
        }
    }

    if (!mode_always) {
        if (pthread_create(&writer, NULL, writer_thread, NULL) != 0) {
            fprintf(stderr, "Failed to create writer thread\n");
            exit(1);
        }
    }


    atexit(AOF_shutdown);
}

/*  AOF_append with pool allocation */
void AOF_append(int id, const void *data, size_t sz) {
    if (!atomic_load(&running)) return;
    AOF_reopen_if_needed();

    void *buf = get_buffer_from_pool(sz);
    if (!buf) {
        fprintf(stderr, "AOF_append: buffer allocation failed\n");
        return;
    }

    memcpy(buf, data, sz);
    int from_pool = (sz <= MAX_BUFFER_SIZE);

    if (mode_always) {
        /* Direct write with io_uring */
        char record_buf[12 + MAX_BUFFER_SIZE];
        size_t record_size = write_record_to_buffer(record_buf, id, buf, (uint32_t)sz);

        if (atomic_load(&g_uring_ctx.initialized)) {
            write_batch_t *batch = malloc(sizeof(write_batch_t));
            if (batch) {
                batch->buffer = malloc(record_size);
                if (batch->buffer) {
                    memcpy(batch->buffer, record_buf, record_size);
                    batch->size = record_size;
                    atomic_fetch_add(&write_sequence, 1);
                    batch->sequence = atomic_load(&write_sequence) - 1;
                    atomic_store(&batch->submitted, false);
                    atomic_store(&batch->completed, false);
                    pthread_mutex_lock(&g_uring_ctx.submit_lock);
                    struct io_uring_sqe *sqe = io_uring_get_sqe(&g_uring_ctx.ring);
                    if (sqe) {
                        io_uring_prep_write(sqe, fd, batch->buffer, record_size, -1);
                        io_uring_sqe_set_data(sqe, batch);
                        io_uring_submit(&g_uring_ctx.ring);
                        atomic_fetch_add(&g_uring_ctx.pending_writes, 1);
                    }
                    pthread_mutex_unlock(&g_uring_ctx.submit_lock);
                }
            }
        } else {
            ssize_t w = write(fd, record_buf, record_size);
            if (w != (ssize_t) record_size) perror("AOF direct write");
        }

        if (from_pool) {
            return_buffer_to_pool(buf, sz);
        } else {
            free(buf);
        }
        return;
    }

    /* Buffered mode */
    pthread_mutex_lock(&lock);

    size_t nxt = (head + 1) & mask;
    while (nxt == tail) {
        pthread_cond_wait(&cond, &lock);
        nxt = (head + 1) & mask;
    }

    aof_cmd_t *c = &ring[head];
    c->id = id;
    c->sz = (uint32_t)sz;
    c->data = buf;
    c->from_pool = from_pool;
    atomic_fetch_add(&write_sequence, 1);
    c->sequence = atomic_load(&write_sequence) - 1;
    atomic_store(&c->written, false);

    head = nxt;
    pthread_cond_signal(&cond);
    pthread_mutex_unlock(&lock);
}

/* AOF_load function */
void AOF_load(Storage *st) {
    if (!g_path || !st) return;

    int fd_load = open(g_path, O_RDONLY | O_CLOEXEC);
    if (fd_load < 0) {
        if (errno == ENOENT) {
            printf(" AOF file not found, starting fresh\n");
            return;
        }
        perror("AOF_load: open");
        return;
    }


    size_t loaded = 0;
    int id;
    uint32_t size, crc_file;

    while (read(fd_load, &id, 4) == 4) {
        if (read(fd_load, &size, 4) != 4) {
            fprintf(stderr, "AOF_load: corrupt size field\n");
            break;
        }

        if (size > 16 * 1024 * 1024) { /* 16MB sanity check */
            fprintf(stderr, "AOF_load: size too large: %u\n", size);
            break;
        }

        void *buf = malloc(size);
        if (!buf) {
            fprintf(stderr, "AOF_load: malloc failed\n");
            break;
        }

        if (read(fd_load, buf, size) != (ssize_t)size) {
            fprintf(stderr, "AOF_load: corrupt data field\n");
            free(buf);
            break;
        }

        if (read(fd_load, &crc_file, 4) != 4) {
            fprintf(stderr, "AOF_load: corrupt crc field\n");
            free(buf);
            break;
        }

        /* Verify CRC */
        uint32_t crc = crc32c(0, &id, 4);
        crc = crc32c(crc, &size, 4);
        crc = crc32c(crc, buf, size);

        if (crc != crc_file) {
            fprintf(stderr, "AOF_load: CRC mismatch at record %zu\n", loaded);
            RAMForge_record_crc_validation(0);
            free(buf);
            break;
        }

        /* Apply to storage */
        storage_save(st, id, buf, size);
        free(buf);
        loaded++;
    }

    close(fd_load);
}
/* AOF_rewrite function  */
void AOF_rewrite(Storage *st) {
    if (atomic_load(&rewrite_active)) {
        printf("  AOF rewrite already in progress\n");
        return;
    }

    atomic_store(&rewrite_active, true);

    if (pthread_create(&rewrite_thread, NULL, rewrite_thread_func, st) != 0) {
        perror("AOF_rewrite: pthread_create");
        atomic_store(&rewrite_active, false);
        return;
    }

    pthread_detach(rewrite_thread);
    printf(" AOF rewrite started in background\n");
}

/* AOF_shutdown function  */
void AOF_shutdown(void) {
    printf(" AOF shutdown initiated...\n");

    atomic_store(&g_opt_sealed.shutdown, true);

    if (g_opt_sealed.sync_thread) {
        pthread_join(g_opt_sealed.sync_thread, NULL);
    }

    if (g_opt_sealed.fd >= 0) {
        fsync(g_opt_sealed.fd);
        close(g_opt_sealed.fd);
    }

    if (g_opt_sealed.mmap_buffer != MAP_FAILED) {
        munmap(g_opt_sealed.mmap_buffer, g_opt_sealed.mmap_size);
    }

    atomic_store(&running, false);

    /* Signal all threads to wake up */
    pthread_mutex_lock(&lock);
    pthread_cond_broadcast(&cond);
    pthread_mutex_unlock(&lock);

    /* Wait for threads to finish */
    if (writer) {
        pthread_join(writer, NULL);
        printf(" Writer thread joined\n");
    }

    if (atomic_load(&g_uring_ctx.initialized)) {
        pthread_join(uring_worker, NULL);
        printf(" io_uring worker thread joined\n");
    }

    /* Wait for any active rewrite to complete */
    if (atomic_load(&rewrite_active)) {
        printf(" Waiting for rewrite to complete...\n");
        while (atomic_load(&rewrite_active)) {
            usleep(100000); /* 100ms */
        }
    }

    /* Cleanup resources */
    if (atomic_load(&g_uring_ctx.initialized)) {
        destroy_io_uring();
    }
    destroy_wal_buffer();
    destroy_buffer_pool();

    if (fd >= 0) {
        close(fd);
        fd = -1;
    }

    free(ring);
    free(rewrite_buffer);
    free(g_path);
    free(g_fd_state.file_path);

    pthread_mutex_destroy(&lock);
    pthread_mutex_destroy(&g_opt_sealed.gc_lock);
    pthread_cond_destroy(&cond);
    pthread_mutex_destroy(&rewrite_lock);

    printf(" AOF shutdown complete\n");
}

/*  Status functions  */
int AOF_rewrite_in_progress(void) {
    return atomic_load(&rewrite_active);
}

size_t AOF_pending_writes(void) {
    if (!atomic_load(&running)) return 0;

    pthread_mutex_lock(&lock);
    size_t pending = (head - tail) & mask;
    pthread_mutex_unlock(&lock);

    return pending + (atomic_load(&g_uring_ctx.initialized)
                      ? atomic_load(&g_uring_ctx.pending_writes) : 0);
}

void AOF_prepare_for_rotation(void) {
    printf("Preparing AOF for rotation...\n");

    /* Wait for all pending writes to complete */
    while (AOF_pending_writes() > 0) {
        usleep(1000); /* 1ms */
    }

    /* Ensure everything is flushed to disk */
    AOF_sync();

    printf(" AOF prepared for rotation\n");
}

/* Atomically rotate the AOF file */
int AOF_rotate_file(const char *new_path) {
    printf(" Starting safe AOF rotation...\n");

    /* Acquire write lock to block all new fd acquisitions */
    pthread_rwlock_wrlock(&g_fd_state.rotation_rwlock);

    /* Wait for all pending io_uring operations to drain */
    printf(" Draining %d pending io_uring operations...\n",
           atomic_load(&g_uring_ctx.pending_writes));

    int timeout_ms = 5000; /* 5 second timeout */
    while (atomic_load(&g_uring_ctx.pending_writes) > 0 && timeout_ms > 0) {
        struct io_uring_cqe *cqe;
        int ret = io_uring_wait_cqe_timeout(&g_uring_ctx.ring, &cqe,
                                            &(struct __kernel_timespec){0, 10000000}); /* 10ms */
        if (ret == 0) {
            write_batch_t *batch = (write_batch_t *)cqe->user_data;
            if (batch) {
                free(batch->buffer);
                free(batch);
            }
            atomic_fetch_sub(&g_uring_ctx.pending_writes, 1);
            io_uring_cqe_seen(&g_uring_ctx.ring, cqe);
        }
        timeout_ms -= 10;
    }

    if (atomic_load(&g_uring_ctx.pending_writes) > 0) {
        printf("  Timeout waiting for pending writes, forcing rotation\n");
    }

    /* Close master fd if open */
    int old_master_fd = atomic_exchange(&g_fd_state.master_fd, -1);
    if (old_master_fd >= 0) {
        fsync(old_master_fd);
        close(old_master_fd);
        printf(" Closed master AOF fd\n");
    }

    /* Perform the rotation */
    if (rename(g_fd_state.file_path, new_path) != 0) {
        perror("AOF rotation: rename failed");

        /* Try to recover by reopening */
        int recovery_fd = open(g_fd_state.file_path, O_CREAT | O_APPEND | O_WRONLY | O_CLOEXEC, 0600);
        atomic_store(&g_fd_state.master_fd, recovery_fd);

        pthread_rwlock_unlock(&g_fd_state.rotation_rwlock);
        return -1;
    }

    /* Create new empty file */
    int new_master_fd = open(g_fd_state.file_path, O_CREAT | O_TRUNC | O_APPEND | O_WRONLY | O_CLOEXEC, 0600);
    if (new_master_fd < 0) {
        perror("AOF rotation: failed to create new file");
        pthread_rwlock_unlock(&g_fd_state.rotation_rwlock);
        return -1;
    }

    atomic_store(&g_fd_state.master_fd, new_master_fd);

    /* Increment generation to invalidate all cached fds */
    uint64_t new_gen = atomic_fetch_add(&g_fd_state.master_generation, 1) + 1;

    printf(" New AOF file opened: %s (fd=%d, generation=%" PRIu64 ")\n",
           g_fd_state.file_path, new_master_fd, new_gen);

    /* Release write lock - workers can now acquire new fds */
    pthread_rwlock_unlock(&g_fd_state.rotation_rwlock);

    printf(" Safe AOF rotation complete\n");
    return 0;
}

static int aof_tail_reopen(const char *path,
                           int *fd_tail,
                           struct stat *st_out,
                           ino_t *cur_ino,
                           off_t *off,
                           size_t *have)
{
    if (*fd_tail >= 0) close(*fd_tail);
    *fd_tail = open(path, O_RDONLY | O_CLOEXEC);
    if (*fd_tail < 0) return -1;

    if (fstat(*fd_tail, st_out) != 0) {
        close(*fd_tail);
        *fd_tail = -1;
        return -1;
    }

    *cur_ino = st_out->st_ino;

    // Start after what Persistence_zp_init() already loaded
    *off  = st_out->st_size;
    *have = 0;
    return 0;
}


static void *aof_live_tail_loop(void *arg) {
    (void)arg;

    const char *path = g_fd_state.file_path;
    int fd_tail = -1;
    struct stat st = {0};
    ino_t cur_ino = 0;
    off_t off = 0;

    size_t buf_cap = 1 << 20;   // 1MB
    uint8_t *buf = malloc(buf_cap);
    size_t have = 0;

    // Initial open (may spin until file appears)
    if (aof_tail_reopen(path, &fd_tail, &st, &cur_ino, &off, &have) != 0) {
        while (!atomic_load(&g_live_tail_stop) &&
               aof_tail_reopen(path, &fd_tail, &st, &cur_ino, &off, &have) != 0) {
            usleep(2000);
        }
    }

    while (!atomic_load(&g_live_tail_stop)) {
        // Detect truncation or rotation
        if (fstat(fd_tail, &st) != 0) {
            (void)aof_tail_reopen(path, &fd_tail, &st, &cur_ino, &off, &have);
            continue;
        }
        if (st.st_ino != cur_ino || st.st_size < off) {
            (void)aof_tail_reopen(path, &fd_tail, &st, &cur_ino, &off, &have);
            continue;
        }

        // Read any new bytes
        if (st.st_size > off) {
            size_t need = (size_t)(st.st_size - off);
            if (have + need > cap) {
                size_t ncap = cap;
                while (ncap < have + need) ncap <<= 1;
                uint8_t *nb = (uint8_t*)realloc(buf, ncap);
                if (!nb) {  // OOM: drop unread tail safely
                    have = 0;
                    off  = st.st_size;
                } else {
                    buf = nb; cap = ncap;
                }
            }
            ssize_t n = pread(fd_tail, buf + have, need, off);
            if (n > 0) { have += (size_t)n; off += n; }
        }

        // Parse whole records: [id:4][size:4][payload:size][crc:4]
        size_t cons = 0;
        while (have - cons >= 12) {
            uint32_t id = rd32(buf + cons);
            uint32_t sz = rd32(buf + cons + 4);
            size_t rec = (size_t)12 + sz;
            if (have - cons < rec) break;   // wait for more bytes

            uint32_t crc_file = rd32(buf + cons + 8 + sz);
            uint32_t crc = crc32c(0, &id, 4);
            crc = crc32c(crc, &sz, 4);
            crc = crc32c(crc, buf + cons + 8, sz);

            if (crc == crc_file) {
                if (!RAMForge_HA_replay_record((uint32_t)id, buf + cons + 8, sz))
                    rf_broker_replay_aof((int)id, buf + cons + 8, sz);
            } else {
                // Incomplete/torn record  rewind unread tail to re-read later
                off -= (have - cons);
                have = cons;
                break;
            }
            cons += rec;
        }
        if (cons) {
            memmove(buf, buf + cons, have - cons);
            have -= cons;
        }

        struct timespec ts = {0, 2*1000*1000}; // 2ms
        nanosleep(&ts, NULL);
    }

    if (fd_tail >= 0) close(fd_tail);
    free(buf);
    return NULL;
}


void AOF_live_follow_start(void) {
    bool expected = false;
    if (!atomic_compare_exchange_strong(&g_live_tail_started, &expected, true)) {
        return; // already started
    }
    atomic_store(&g_live_tail_stop, false);

    int rc = pthread_create(&g_live_tail_thread, NULL, aof_live_tail_loop, NULL);
    if (rc != 0) {
        perror("pthread_create live follower");
        atomic_store(&g_live_tail_started, false);
    }
}

void AOF_live_follow_stop(void) {
    if (!atomic_load(&g_live_tail_started)) return;
    atomic_store(&g_live_tail_stop, true);
    pthread_join(g_live_tail_thread, NULL);  // join to flush/cleanup
    atomic_store(&g_live_tail_started, false);
}

/* Sync function for critical operations */
void AOF_sync(void) {
    if (fd >= 0) {
        fsync(fd);
    }
}


