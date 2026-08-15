/* aof_batch.c ??? ultra-fast io_uring AOF with batching + CRC32C + non-blocking rewrite */
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
#include <sys/file.h>
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
    void *buffer;              /* immutable, aligned until fsync completes */
    uint64_t timestamp;
    _Atomic int state;         /* 0=free, 1=writing, 2=ready, 3=synced */
} sealed_batch_slot_t;

typedef struct {
    /* Lock-free batch slots */
    sealed_batch_slot_t slots[MAX_SEALED_BATCHES];
    _Atomic uint64_t slot_head;
    _Atomic uint64_t slot_tail;

    /* Single writer fd with O_DIRECT */
    int fd;

    /* Async sync state */
    pthread_t sync_thread;
    _Atomic bool shutdown;
    _Atomic bool failed;

    /* Metrics */
    _Atomic uint64_t batches_written;
    _Atomic uint64_t total_sync_time_us;
    _Atomic uint64_t last_synced_batch_id;

    _Atomic bool gc_active;         /* pause appends/sync during sealed GC */
    _Atomic uint64_t active_appenders;
    pthread_mutex_t gc_lock;

} sealed_aof_t;

static sealed_aof_t g_opt_sealed = {0};
static _Atomic bool g_aof_shutdown_once = false;


static uint64_t local_generation = 0;

/* ????????? Enhanced configuration for io_uring ????????? */
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


/* ????????? io_uring optimized structures ????????? */
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

/* ????????? Write-ahead logging buffer ????????? */
typedef struct {
    char *buffer;
    size_t size;
    size_t capacity;
    size_t write_pos;
    atomic_bool flushing;
    pthread_mutex_t lock;
} wal_buffer_t;

/* ????????? Improved Buffer pool for zero-malloc AOF_append ????????? */
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
/* ????????? types / globals ????????????????????????????????????????????????????????????????????????????????? */
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
static bool            legacy_locks_initialized;

static int            fd = -1;
static char          *g_path = NULL;
static unsigned       flush_ms = 10;
static int            mode_always = 0;
static pthread_t      writer;
static pthread_t      uring_worker;
static atomic_bool    running = ATOMIC_VAR_INIT(false);
static atomic_uint64_t write_sequence = ATOMIC_VAR_INIT(0);

/* ????????? Non-blocking rewrite state ????????????????????????????????????????????? */
static atomic_bool    rewrite_active = ATOMIC_VAR_INIT(false);
static pthread_t      rewrite_thread;
static aof_cmd_t     *rewrite_buffer;
static size_t         rewrite_buf_cap, rewrite_buf_mask;
static size_t         rewrite_buf_head, rewrite_buf_tail;
static pthread_mutex_t rewrite_lock;
static atomic_bool    rewrite_delta_failed = ATOMIC_VAR_INIT(false);

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

void AOF_sealed_test_set_hold(int on) { atomic_store(&g_test_hold, on ? 1 : 0); if (on) atomic_store(&g_test_release, 0); }
void AOF_sealed_test_release(void)   { atomic_store(&g_test_release, 1); }
void AOF_sealed_test_set_delay_us(int us) { atomic_store(&g_test_delay_us, us > 0 ? us : 0); }

/* writev(2) is allowed to make partial progress. Mutate this private iovec
 * copy until every byte has been appended, or fail the sealed writer before
 * publishing a durability watermark. */
static int sealed_writev_full(int sealed_fd, struct iovec *iovecs, size_t iovcnt,
                              size_t expected_bytes) {
    size_t iov_index = 0;
    size_t written_total = 0;

    while (iov_index < iovcnt) {
        ssize_t n = writev(sealed_fd, &iovecs[iov_index], (int)(iovcnt - iov_index));
        if (n < 0) {
            if (errno == EINTR) continue;
            return -1;
        }
        if (n == 0) {
            errno = EIO;
            return -1;
        }

        written_total += (size_t)n;
        size_t consumed = (size_t)n;
        while (iov_index < iovcnt && consumed >= iovecs[iov_index].iov_len) {
            consumed -= iovecs[iov_index].iov_len;
            iov_index++;
        }
        if (consumed != 0 && iov_index < iovcnt) {
            iovecs[iov_index].iov_base = (char *)iovecs[iov_index].iov_base + consumed;
            iovecs[iov_index].iov_len -= consumed;
        }
    }

    if (written_total != expected_bytes) {
        errno = EIO;
        return -1;
    }
    return 0;
}

static int sealed_fsync_full(int sealed_fd) {
    int rc;
    do {
        rc = fsync(sealed_fd);
    } while (rc != 0 && errno == EINTR);
    return rc;
}

static int aof_write_full(int out_fd, const void *buffer, size_t size) {
    const uint8_t *p = (const uint8_t *)buffer;
    size_t written = 0;
    while (written < size) {
        ssize_t n = write(out_fd, p + written, size - written);
        if (n < 0) {
            if (errno == EINTR) continue;
            return -1;
        }
        if (n == 0) {
            errno = EIO;
            return -1;
        }
        written += (size_t)n;
    }
    return 0;
}

/* Returns 0 after a complete read, 1 only for a clean EOF before any byte,
 * and -1 for errors or truncated fields. */
static int aof_read_full_or_eof(int in_fd, void *buffer, size_t size) {
    uint8_t *p = (uint8_t *)buffer;
    size_t read_total = 0;
    while (read_total < size) {
        ssize_t n = read(in_fd, p + read_total, size - read_total);
        if (n < 0) {
            if (errno == EINTR) continue;
            return -1;
        }
        if (n == 0) {
            if (read_total == 0) return 1;
            errno = EIO;
            return -1;
        }
        read_total += (size_t)n;
    }
    return 0;
}

/* Persist directory-entry changes such as first creation and atomic rename. */
static int sealed_fsync_parent_dir(const char *path) {
    if (!path || !*path) {
        errno = EINVAL;
        return -1;
    }

    char parent[1024];
    size_t n = strlen(path);
    if (n >= sizeof(parent)) {
        errno = ENAMETOOLONG;
        return -1;
    }
    memcpy(parent, path, n + 1);

    char *slash = strrchr(parent, '/');
    if (!slash) {
        memcpy(parent, ".", 2);
    } else if (slash == parent) {
        slash[1] = '\0';
    } else {
        *slash = '\0';
    }

    int dir_fd = open(parent, O_RDONLY | O_DIRECTORY | O_CLOEXEC);
    if (dir_fd < 0) return -1;
    int rc;
    do {
        rc = fsync(dir_fd);
    } while (rc != 0 && errno == EINTR);
    int saved = errno;
    if (close(dir_fd) != 0 && rc == 0) {
        rc = -1;
        saved = errno;
    }
    errno = saved;
    return rc;
}

static int sealed_shared_failed(void) {
    return g_metrics_ptr != NULL &&
           atomic_load_explicit(&g_metrics.persistence_failures_total,
                                memory_order_acquire) != 0;
}

static int sealed_lock_with_timeout(int sealed_fd) {
    uint64_t timeout_ms = 5000;
    const char *configured = getenv("RF_SEALED_LOCK_TIMEOUT_MS");
    if (configured && *configured) {
        char *end = NULL;
        unsigned long long parsed = strtoull(configured, &end, 10);
        if (end != configured && *end == '\0' && parsed > 0) timeout_ms = parsed;
    }

    uint64_t start = now_us();
    for (;;) {
        if (flock(sealed_fd, LOCK_EX | LOCK_NB) == 0) return 0;
        if (errno != EINTR && errno != EWOULDBLOCK && errno != EAGAIN) return -1;
        if (sealed_shared_failed()) {
            errno = ECANCELED;
            return -1;
        }
        if (now_us() - start >= timeout_ms * 1000ULL) {
            errno = ETIMEDOUT;
            return -1;
        }
        struct timespec ts = {0, 1000000};
        nanosleep(&ts, NULL);
    }
}

static void sealed_mark_failed(void) {
    bool already_failed = atomic_exchange_explicit(&g_opt_sealed.failed, true,
                                                    memory_order_acq_rel);
    if (!already_failed) {
        atomic_fetch_add(&g_metrics.write_failures, 1);
        atomic_fetch_add(&g_metrics.persistence_failures_total, 1);
    }
}

static void shared_hist_obs(_Atomic uint64_t *buckets, const double *bounds,
                            double seconds, _Atomic uint64_t *count,
                            _Atomic uint64_t *sum_us) {
    for (int i = 0; i < AYDER_METRIC_HIST_BUCKETS; i++) {
        if (seconds <= bounds[i]) {
            atomic_fetch_add(&buckets[i], 1);
            break;
        }
    }
    atomic_fetch_add(count, 1);
    atomic_fetch_add(sum_us, (uint64_t)(seconds * 1000000.0));
}

/* Sealed batch group sync thread */
static void *sealed_sync_thread(void *arg) {
    (void)arg;

    struct iovec iovecs[SEALED_BATCH_GROUP_SIZE];
    sealed_batch_slot_t *ready_slots[SEALED_BATCH_GROUP_SIZE];

    while (!atomic_load(&g_opt_sealed.shutdown)) {
        if (sealed_shared_failed()) return NULL;

        uint64_t h = atomic_load_explicit(&g_opt_sealed.slot_head, memory_order_acquire);
        uint64_t t = atomic_load_explicit(&g_opt_sealed.slot_tail, memory_order_acquire);

        size_t ready_count = 0;

        while (t < h && ready_count < SEALED_BATCH_GROUP_SIZE) {
            sealed_batch_slot_t *slot = &g_opt_sealed.slots[t % MAX_SEALED_BATCHES];

            if (atomic_load_explicit(&slot->state, memory_order_acquire) != 2)
                break;

            ready_slots[ready_count] = slot;
            iovecs[ready_count].iov_base = slot->buffer;
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

        size_t expected_bytes = 0;
        for (size_t i = 0; i < ready_count; i++) {
            if (SIZE_MAX - expected_bytes < iovecs[i].iov_len) {
                errno = EOVERFLOW;
                sealed_mark_failed();
                fprintf(stderr, "sealed writev size overflow; persistence disabled\n");
                return NULL;
            }
            expected_bytes += iovecs[i].iov_len;
        }

        if (sealed_lock_with_timeout(g_opt_sealed.fd) != 0) {
            perror("sealed file lock failed; persistence disabled");
            sealed_mark_failed();
            return NULL;
        }

        if (sealed_shared_failed()) {
            (void)flock(g_opt_sealed.fd, LOCK_UN);
            return NULL;
        }

        if (sealed_writev_full(g_opt_sealed.fd, iovecs, ready_count, expected_bytes) != 0) {
            perror("sealed writev failed; persistence disabled");
            sealed_mark_failed();
            (void)flock(g_opt_sealed.fd, LOCK_UN);
            return NULL;
        }

        if (sealed_fsync_full(g_opt_sealed.fd) != 0) {
            perror("sealed fsync failed; persistence disabled");
            sealed_mark_failed();
            (void)flock(g_opt_sealed.fd, LOCK_UN);
            return NULL;
        }

        if (flock(g_opt_sealed.fd, LOCK_UN) != 0) {
            perror("sealed file unlock failed; persistence disabled");
            sealed_mark_failed();
            close(g_opt_sealed.fd);
            g_opt_sealed.fd = -1;
            return NULL;
        }

        uint64_t max_id = ready_slots[ready_count - 1]->batch_id;
        uint64_t ready_timestamps[SEALED_BATCH_GROUP_SIZE];
        for (size_t i = 0; i < ready_count; i++)
            ready_timestamps[i] = ready_slots[i]->timestamp;
        atomic_store_explicit(&g_opt_sealed.last_synced_batch_id, max_id, memory_order_release);

        for (size_t i = 0; i < ready_count; i++) {
            free(ready_slots[i]->buffer);
            ready_slots[i]->buffer = NULL;
            atomic_store_explicit(&ready_slots[i]->state, 0, memory_order_release);
        }

        atomic_store_explicit(&g_opt_sealed.slot_tail, t, memory_order_release);

        uint64_t sync_complete_us = now_us();
        double fs_s = ((double)(sync_complete_us - sync_start)) / 1e6;
        hist_obs(g_fsync_hist, fsync_bucket_le, HN, fs_s, &g_fsync_count, &g_fsync_sum_s);
        shared_hist_obs(g_metrics.fsync_hist, fsync_bucket_le, fs_s,
                        &g_metrics.fsync_count, &g_metrics.fsync_sum_us);
        atomic_fetch_add(&g_metrics.successful_writes, ready_count);
        for (size_t i = 0; i < ready_count; i++) {
            double sealed_s = ((double)(sync_complete_us - ready_timestamps[i])) / 1e6;
            hist_obs(g_seal_hist, seal_bucket_le, HN, sealed_s,
                     &g_seal_count, &g_seal_sum_s);
            shared_hist_obs(g_metrics.sealed_latency_hist, seal_bucket_le, sealed_s,
                            &g_metrics.sealed_latency_count, &g_metrics.sealed_latency_sum_us);
        }
    }

    return NULL;
}



uint64_t AOF_sealed_last_synced_id(void) {
    return atomic_load(&g_opt_sealed.last_synced_batch_id);
}

int AOF_sealed_is_healthy(void) {
    return !atomic_load_explicit(&g_opt_sealed.failed, memory_order_acquire) &&
           !sealed_shared_failed();
}

/* Initialize sealed AOF system */
int AOF_sealed_init(const char *path) {
    char sealed_file[512];
    int path_len = snprintf(sealed_file, sizeof(sealed_file), "%s.sealed", path);
    if (path_len < 0 || (size_t)path_len >= sizeof(sealed_file)) {
        errno = ENAMETOOLONG;
        return -1;
    }
    int existed = access(sealed_file, F_OK) == 0;

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

    if (!existed &&
        (sealed_fsync_full(g_opt_sealed.fd) != 0 ||
         sealed_fsync_parent_dir(sealed_file) != 0)) {
        perror("Failed to persist sealed file creation");
        close(g_opt_sealed.fd);
        g_opt_sealed.fd = -1;
        return -1;
    }

    atomic_store(&g_opt_sealed.slot_head, 0);
    atomic_store(&g_opt_sealed.slot_tail, 0);
    atomic_store(&g_opt_sealed.shutdown, false);
    atomic_store(&g_opt_sealed.failed, false);
    atomic_store(&g_opt_sealed.gc_active, false);
    atomic_store(&g_opt_sealed.active_appenders, 0);
    if (pthread_mutex_init(&g_opt_sealed.gc_lock, NULL) != 0) {
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
        close(g_opt_sealed.fd);
        return -1;
    }

    return 0;
}

static uint64_t sealed_claim_slot_sequence(void) {
    for (uint32_t attempt = 0; attempt < 50000; attempt++) {
        if (atomic_load_explicit(&g_opt_sealed.shutdown, memory_order_acquire) ||
            atomic_load_explicit(&g_opt_sealed.failed, memory_order_acquire) ||
            sealed_shared_failed()) {
            return UINT64_MAX;
        }
        uint64_t h = atomic_load_explicit(&g_opt_sealed.slot_head, memory_order_acquire);
        uint64_t t = atomic_load_explicit(&g_opt_sealed.slot_tail, memory_order_acquire);
        if (h - t < MAX_SEALED_BATCHES &&
            atomic_compare_exchange_weak_explicit(&g_opt_sealed.slot_head, &h, h + 1,
                                                  memory_order_acq_rel, memory_order_acquire)) {
            return h;
        }
        struct timespec ts = {0, 100000}; // 100??s
        nanosleep(&ts, NULL);
    }
    errno = ETIMEDOUT;
    return UINT64_MAX;
}

/* Sealed append function for AOF */
uint64_t AOF_append_sealed(int id, const void *data, size_t sz) {
    if (atomic_load_explicit(&g_opt_sealed.shutdown, memory_order_acquire) ||
        atomic_load_explicit(&g_opt_sealed.failed, memory_order_acquire) ||
        sealed_shared_failed()) return 0;
    if ((!data && sz != 0) || sz > UINT32_MAX ||
        sz > SEALED_MMAP_SIZE - 36U - 511U) {
        errno = EOVERFLOW;
        return 0;
    }

    int append_registered = 0;
    for (uint32_t attempt = 0; attempt < 50000; attempt++) {
        if (atomic_load_explicit(&g_opt_sealed.gc_active, memory_order_acquire)) {
            if (!AOF_sealed_is_healthy() || atomic_load(&g_opt_sealed.shutdown))
                return 0;
            struct timespec ts = {0, 100000};
            nanosleep(&ts, NULL);
            continue;
        }
        atomic_fetch_add_explicit(&g_opt_sealed.active_appenders, 1, memory_order_acq_rel);
        if (!atomic_load_explicit(&g_opt_sealed.gc_active, memory_order_acquire)) {
            append_registered = 1;
            break;
        }
        atomic_fetch_sub_explicit(&g_opt_sealed.active_appenders, 1, memory_order_acq_rel);
    }
    if (!append_registered) {
        errno = ETIMEDOUT;
        return 0;
    }

    size_t total_size   = 32 + sz + 4;
    size_t aligned_size = (total_size + 511) & ~((size_t)511);
    void *record_buffer = NULL;
    if (posix_memalign(&record_buffer, 4096, aligned_size) != 0) {
        atomic_fetch_sub(&g_opt_sealed.active_appenders, 1);
        return 0;
    }

    uint64_t timestamp = now_us();

    uint64_t seq = sealed_claim_slot_sequence();
    if (seq == UINT64_MAX) {
        free(record_buffer);
        atomic_fetch_sub(&g_opt_sealed.active_appenders, 1);
        return 0;
    }
    /* The public batch ID is derived from the publication sequence. This makes
     * the monotonic synced watermark safe even if producers are preempted: a
     * later ID can never occupy an earlier slot. */
    uint64_t batch_id = seq + 1;
    sealed_batch_slot_t *slot = &g_opt_sealed.slots[seq % MAX_SEALED_BATCHES];

    // claim slot
    int expect = 0;
    while (!atomic_compare_exchange_weak_explicit(&slot->state, &expect, 1,
                                                  memory_order_acq_rel, memory_order_relaxed)) {
        expect = 0;
        cpu_relax();
    }

    char *write_ptr = record_buffer;

    uint32_t magic = 0x5EA1ED02, sz32 = (uint32_t)sz, id32 = (uint32_t)id, reserved = 0;
    memcpy(write_ptr + 0,  &magic,     4);
    memcpy(write_ptr + 4,  &batch_id,  8);
    memcpy(write_ptr + 12, &timestamp, 8);
    memcpy(write_ptr + 20, &sz32,      4);
    memcpy(write_ptr + 24, &id32,      4);
    memcpy(write_ptr + 28, &reserved,  4);

    if (sz) memcpy(write_ptr + 32, data, sz);

    uint32_t crc = crc32c(0, write_ptr, (unsigned)(32 + sz));
    memcpy(write_ptr + 32 + sz, &crc, 4);

    if (aligned_size > total_size) {
        memset(write_ptr + total_size, 0, aligned_size - total_size);
    }

    slot->batch_id = batch_id;
    slot->size = (uint32_t)aligned_size;
    slot->buffer = record_buffer;
    slot->timestamp = timestamp;

    atomic_thread_fence(memory_order_release);
    atomic_store_explicit(&slot->state, 2, memory_order_release); // ready
    atomic_fetch_sub_explicit(&g_opt_sealed.active_appenders, 1, memory_order_release);

    return batch_id;
}


int AOF_sealed_wait(uint64_t batch_id) {
    while (!atomic_load(&g_opt_sealed.shutdown)) {
        if (atomic_load_explicit(&g_opt_sealed.failed, memory_order_acquire) ||
            sealed_shared_failed()) return -1;
        uint64_t last = atomic_load_explicit(&g_opt_sealed.last_synced_batch_id, memory_order_acquire);
        if (last >= batch_id) return 0;
        struct timespec ts = {0, 100000}; // 100??s
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
    (void)max_age_ms;
    (void)max_keep_bytes;
    if (bytes_after) *bytes_after = 0;
    errno = ENOTSUP;
    fprintf(stderr,
            "sealed GC is disabled pending safe cross-process drain, lock, rename, and fd-reopen coordination\n");
    return -1;
#if 0
    int gc_rc = -1;
    const char *experimental = getenv("RF_ENABLE_EXPERIMENTAL_SEALED_GC");
    if (!experimental || strcmp(experimental, "1") != 0) {
        errno = ENOTSUP;
        fprintf(stderr,
                "sealed GC is disabled by default pending cross-process inode coordination; "
                "set RF_ENABLE_EXPERIMENTAL_SEALED_GC=1 only for isolated evaluation\n");
        return -1;
    }

    const char *worker_count = getenv("RF_EFFECTIVE_WORKER_COUNT");
    if (!worker_count || strtol(worker_count, NULL, 10) != 1) {
        errno = ENOTSUP;
        fprintf(stderr,
                "sealed GC is disabled unless exactly one worker is configured; "
                "multi-worker inode coordination is not implemented\n");
        return -1;
    }

    pthread_mutex_lock(&g_opt_sealed.gc_lock);
    atomic_store(&g_opt_sealed.gc_active, true);

    while (atomic_load_explicit(&g_opt_sealed.active_appenders,
                                memory_order_acquire) != 0) {
        struct timespec ts = {0, 200000};
        nanosleep(&ts, NULL);
    }

    /* Wait until no "ready" slots remain to make file state consistent */
    for (;;) {
        bool any_ready = false;
        for (int i = 0; i < MAX_SEALED_BATCHES; i++)
            if (atomic_load(&g_opt_sealed.slots[i].state) == 2) { any_ready = true; break; }
        if (!any_ready) break;
        struct timespec ts = {0, 200000}; nanosleep(&ts, NULL);
    }

    /* Close fd so we can rewrite atomically */
    if (g_opt_sealed.fd >= 0) {
        if (sealed_fsync_full(g_opt_sealed.fd) != 0 ||
            close(g_opt_sealed.fd) != 0) {
            sealed_mark_failed();
            g_opt_sealed.fd = -1;
            goto reopen;
        }
        g_opt_sealed.fd = -1;
    }

    char path[1024];
    snprintf(path, sizeof(path), "%s.sealed", g_fd_state.file_path);

    int in = open(path, O_RDONLY | O_CLOEXEC);
    if (in < 0) goto reopen;

    struct stat st;
    if (fstat(in, &st) != 0) { close(in); goto reopen; }
    if (st.st_size == 0) { close(in); gc_rc = 0; goto reopen; }

    size_t fsz = (size_t)st.st_size;
    uint8_t *buf = calloc(1, fsz);
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
            uintptr_t next = ((cur / 512u) + 1u) * 512u;
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
                uintptr_t next = ((cur / 512u) + 1u) * 512u;
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

    char tmp[1024];
    int tmp_len = snprintf(tmp, sizeof(tmp), "%s.tmp.XXXXXX", path);
    if (tmp_len < 0 || (size_t)tmp_len >= sizeof(tmp)) {
        errno = ENAMETOOLONG;
        goto gc_done;
    }
    int out = mkstemp(tmp);
    if (out < 0) goto gc_done;
    if (fcntl(out, F_SETFD, FD_CLOEXEC) != 0 || fchmod(out, 0600) != 0) goto gc_tmp_fail;

    size_t keep_sz = keep_from >= end ? 0 : (size_t)(end - keep_from);
    if ((keep_sz && sealed_write_full(out, keep_from, keep_sz) != 0) ||
        sealed_fsync_full(out) != 0) goto gc_tmp_fail;
    if (close(out) != 0) {
        out = -1;
        goto gc_tmp_fail;
    }
    out = -1;
    if (rename(tmp, path) != 0 || sealed_fsync_parent_dir(path) != 0) goto gc_tmp_fail;
    if (bytes_after) *bytes_after = (uint64_t)keep_sz;
    gc_rc = 0;
    goto gc_done;

gc_tmp_fail:
    {
        int saved = errno;
        if (out >= 0) close(out);
        unlink(tmp);
        errno = saved;
    }

gc_done:
    free(buf);

    reopen:
    g_opt_sealed.fd = open(path, O_CREAT | O_APPEND | O_WRONLY | O_CLOEXEC, 0600);
    if (g_opt_sealed.fd < 0) {
        sealed_mark_failed();
        gc_rc = -1;
    }
    atomic_store(&g_opt_sealed.gc_active, false);
    pthread_mutex_unlock(&g_opt_sealed.gc_lock);
    return gc_rc;
#endif
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

    int local_fd = open(path, O_RDWR | O_CLOEXEC);
    if (local_fd < 0) {
        if (errno != ENOENT)
            fprintf(stderr, "AOF_sealed_replay: open(%s): %s\n", path, strerror(errno));
        return errno == ENOENT ? 0 : SIZE_MAX;
    }
    if (sealed_lock_with_timeout(local_fd) != 0) {
        fprintf(stderr, "AOF_sealed_replay: lock(%s): %s\n", path, strerror(errno));
        close(local_fd);
        return SIZE_MAX;
    }

    struct stat st;
    if (fstat(local_fd, &st) != 0) {
        (void)flock(local_fd, LOCK_UN);
        close(local_fd);
        return SIZE_MAX;
    }
    if (st.st_size == 0) {
        (void)flock(local_fd, LOCK_UN);
        close(local_fd);
        return 0;
    }

    const size_t fsz = (size_t)st.st_size;

    uint8_t *buf = calloc(1, fsz);
    if (!buf) {
        (void)flock(local_fd, LOCK_UN);
        close(local_fd);
        return SIZE_MAX;
    }

    size_t read_total = 0;
    while (read_total < fsz) {
        ssize_t r = pread(local_fd, buf + read_total, fsz - read_total,
                          (off_t)read_total);
        if (r < 0 && errno == EINTR) continue;
        if (r <= 0) break;
        read_total += (size_t)r;
    }
    if (read_total != fsz) {
        free(buf);
        (void)flock(local_fd, LOCK_UN);
        close(local_fd);
        return SIZE_MAX;
    }

    size_t applied = 0;
    size_t last_good = 0;
    int corrupt = 0;
    int partial_tail = 0;

    const uint8_t *p = buf, *end = buf + fsz;
    while (p < end) {
        if ((size_t)(end - p) < 32) {
            partial_tail = 1;
            break;
        }
        const uint32_t magic = rd32(p + 0);
        if (magic != 0x5EA1ED02) {
            fprintf(stderr, "AOF_sealed_replay: invalid magic at offset %zu\n",
                    (size_t)(p - buf));
            corrupt = 1;
            break;
        }

        const uint32_t sz     = rd32(p + 20);
        const uint32_t rec_id = rd32(p + 24);
        const size_t total = 32 + (size_t)sz + 4;
        const size_t aligned = (total + 511) & ~((size_t)511);
        if (total < (size_t)sz || aligned < total ||
            aligned > (size_t)(end - p)) {
            partial_tail = 1;
            break;
        }

        const uint8_t *payload = p + 32;

        const uint32_t crc_file = rd32(payload + sz);
        const uint32_t crc_calc = crc32c(0, p, 32 + sz);

        if (crc_calc != crc_file) {
            fprintf(stderr, "AOF_sealed_replay: CRC mismatch (id=%u, sz=%u)\n", rec_id, sz);
            corrupt = 1;
            break;
        } else {
            int handled = RAMForge_HA_replay_record((uint32_t)rec_id,
                                                    payload, sz);
            if (handled < 0 ||
                (handled == 0 &&
                 rf_broker_apply_aof_checked((int)rec_id, payload, sz) != 0)) {
                fprintf(stderr,
                        "AOF_sealed_replay: semantic apply failed (id=%u, sz=%u)\n",
                        rec_id, sz);
                corrupt = 1;
                break;
            }
            applied++;
        }

        p += aligned;
        last_good = (size_t)(p - buf);
    }

    if (partial_tail && !corrupt) {
        fprintf(stderr,
                "AOF_sealed_replay: removing uncommitted partial tail at offset %zu (file=%zu)\n",
                last_good, fsz);
        if (ftruncate(local_fd, (off_t)last_good) != 0 || fsync(local_fd) != 0) {
            corrupt = 1;
        }
    }

    if (applied || corrupt || partial_tail) {
        printf("PID %d sealed replay: applied=%zu, repaired_tail=%d, corrupt=%d (file=%s, %zu bytes)\n",
               getpid(), applied, partial_tail && !corrupt, corrupt, path, fsz);
    }

    free(buf);
    (void)flock(local_fd, LOCK_UN);
    close(local_fd);
    return corrupt ? SIZE_MAX : applied;
}

static void init_tls_fd_cache(void) {
    if (pthread_mutex_init(&tls_fd_cache.lock, NULL) != 0) {
        fprintf(stderr, "Failed to initialize TLS fd cache mutex\n");
        exit(1);
    }
    tls_fd_cache.fd = -1;
    tls_fd_cache.generation = 0;
}

/* ????????? Safe fd acquisition with generation check ????????? */
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

/* ????????? io_uring initialization ????????????????????????????????????????????????????????? */
static int init_io_uring(void) {
    if (running_under_valgrind()) {
        printf("??????  Valgrind detected ??? disabling io_uring for this run\n");
        atomic_store(&g_uring_ctx.initialized, false);
        return -1; // NOTE: Aidarbek eger baska error shyksa onda return -1 ge ornyna keltir
    }
    struct io_uring_params params = {0};
    params.flags = IORING_SETUP_SQPOLL | IORING_SETUP_SQ_AFF;
    params.sq_thread_idle = 2000;  /* 2 second idle timeout */

    int ret = io_uring_queue_init_params(URING_QUEUE_DEPTH, &g_uring_ctx.ring, &params);
    if (ret == -EPERM || ret == -EINVAL || ret == -EOPNOTSUPP) {
        /* SQPOLL commonly requires privileges that containers and WSL2 do not
         * grant. Retry with the portable kernel-worker path before falling
         * back to ordinary writes. */
        fprintf(stderr,
                "io_uring SQPOLL unavailable (%s); retrying without SQPOLL\n",
                strerror(-ret));
        memset(&params, 0, sizeof(params));
        ret = io_uring_queue_init_params(URING_QUEUE_DEPTH, &g_uring_ctx.ring, &params);
    }
    if (ret < 0) {
        fprintf(stderr,
                "io_uring unavailable (%s); using synchronous AOF writes\n",
                strerror(-ret));
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
        printf("??? io_uring destroyed\n");
    }
}

/* ????????? Write-ahead buffer initialization ????????????????????????????????? */
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
        printf("??? WAL buffer destroyed\n");
    }
}

/* ????????? Enhanced buffer pool with lock-free operations ????????? */
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
    printf("???????  Destroying buffer pool...\n");

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

    printf("??? Buffer pool destroyed\n");
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
        fprintf(stderr, "??? Failed to get valid fd for write\n");
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
        fprintf(stderr, "??? fd became invalid during submission preparation\n");
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

/* ????????? Fast record writing with CRC ????????????????????????????????????????????? */
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

/* ????????? io_uring worker thread ????????????????????????????????????????????????????????? */
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
                        printf("???? Expected EBADF during rotation (gen %" PRIu64 " -> %" PRIu64 ")\n",
                               batch->generation, current_gen);
                    } else {
                        fprintf(stderr, "??????  Unexpected EBADF (same generation %" PRIu64 ")\n",
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

    printf("???? Enhanced io_uring worker thread exiting\n");
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

/* ????????? Enhanced background writer with io_uring batching ????????? */
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
                            void *copy = malloc(c->sz ? c->sz : 1);
                            if (copy) {
                                memcpy(copy, c->data, c->sz);
                                rewrite_buffer[rewrite_buf_head] = (aof_cmd_t){
                                        c->id, c->sz, copy, 0, c->sequence, ATOMIC_VAR_INIT(false)
                                };
                                rewrite_buf_head = nxt;
                            } else atomic_store(&rewrite_delta_failed, true);
                        } else atomic_store(&rewrite_delta_failed, true);
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
                            int fallback_failed = 0;
                            if (is_fd_valid(fd)) {
                                if (aof_write_full(fd, batch_buffer,
                                                   total_batch_size) != 0) {
                                    perror("AOF sync write fallback");
                                    fallback_failed = 1;
                                }
                            } else {
                                errno = EBADF;
                                fallback_failed = 1;
                            }
                            if (fallback_failed) {
                                AOF_reopen_if_needed();
                                sealed_mark_failed();
                                atomic_store(&running, false);
                            }
                        }
                    } else {
                        /* Synchronous fallback */
                        AOF_reopen_if_needed();
                        if (is_fd_valid(fd)) {
                            if (aof_write_full(fd, batch_buffer,
                                               total_batch_size) != 0) {
                                perror("AOF write");
                                sealed_mark_failed();
                                atomic_store(&running, false);
                            }
                        } else {
                            errno = EBADF;
                            sealed_mark_failed();
                            atomic_store(&running, false);
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
    printf("???? Enhanced writer thread exiting\n");
    return NULL;
}

typedef struct {
    int fd;
    int failed;
} rewrite_output_t;

static void dump_record_cb(int id, const void *data, size_t sz, void *ud) {
    rewrite_output_t *output = (rewrite_output_t *)ud;
    if (!output || output->failed) return;
    if (sz > UINT32_MAX || sz > SIZE_MAX - 12) {
        errno = EOVERFLOW;
        output->failed = 1;
        return;
    }
    size_t capacity = 12 + sz;
    char stack_record[12 + MAX_BUFFER_SIZE];
    char *record = capacity <= sizeof(stack_record)
                 ? stack_record : malloc(capacity);
    if (!record) {
        output->failed = 1;
        return;
    }
    size_t record_size = write_record_to_buffer(record, id, data, (uint32_t)sz);
    if (aof_write_full(output->fd, record, record_size) != 0)
        output->failed = 1;
    if (record != stack_record) free(record);
}

static void *segment_rewrite_thread_func(void *arg) {
    rewrite_task_t *task = (rewrite_task_t*)arg;

    printf("???? Starting segment rewrite: %s\n", task->source_path);

    char tmp_path[1024];
    int path_len = snprintf(tmp_path, sizeof(tmp_path), "%s.compact",
                            task->output_path);
    if (path_len < 0 || (size_t)path_len >= sizeof(tmp_path)) {
        errno = ENAMETOOLONG;
        perror("segment_rewrite: temporary path");
        goto cleanup;
    }

    int fd_out = open(tmp_path, O_CREAT | O_TRUNC | O_WRONLY | O_CLOEXEC, 0600);
    if (fd_out < 0) {
        perror("segment_rewrite: open output");
        goto cleanup;
    }

    Storage temp_storage;
    storage_init(&temp_storage);
    if (!temp_storage.flags || !temp_storage.keys || !temp_storage.values ||
        !temp_storage.val_sizes) {
        fprintf(stderr, "segment_rewrite: storage initialization failed\n");
        free(temp_storage.flags);
        free(temp_storage.keys);
        free(temp_storage.values);
        free(temp_storage.val_sizes);
        close(fd_out);
        unlink(tmp_path);
        goto cleanup;
    }

    int fd_in = open(task->source_path, O_RDONLY | O_CLOEXEC);
    if (fd_in < 0) {
        perror("segment_rewrite: open input");
        close(fd_out);
        unlink(tmp_path);
        storage_destroy(&temp_storage);
        goto cleanup;
    }

    int parse_ok = 1;
    int id;
    uint32_t size, crc_file;
    for (;;) {
        int read_rc = aof_read_full_or_eof(fd_in, &id, sizeof(id));
        if (read_rc == 1) break;
        if (read_rc != 0 ||
            aof_read_full_or_eof(fd_in, &size, sizeof(size)) != 0) {
            fprintf(stderr, "segment_rewrite: corrupt record header\n");
            parse_ok = 0;
            break;
        }
        if (size > 16u * 1024u * 1024u) {
            fprintf(stderr, "segment_rewrite: record too large: %u\n", size);
            parse_ok = 0;
            break;
        }
        void *buf = malloc(size ? size : 1);
        if (!buf) {
            fprintf(stderr, "segment_rewrite: malloc failed\n");
            parse_ok = 0;
            break;
        }
        if ((size && aof_read_full_or_eof(fd_in, buf, size) != 0) ||
            aof_read_full_or_eof(fd_in, &crc_file, sizeof(crc_file)) != 0) {
            fprintf(stderr, "segment_rewrite: corrupt data field\n");
            free(buf);
            parse_ok = 0;
            break;
        }
        uint32_t crc = crc32c(0, &id, 4);
        crc = crc32c(crc, &size, 4);
        crc = crc32c(crc, buf, size);

        if (crc != crc_file) {
            fprintf(stderr, "segment_rewrite: CRC mismatch\n");
            free(buf);
            parse_ok = 0;
            break;
        }

        if (storage_save(&temp_storage, id, buf, size) != 0) {
            fprintf(stderr, "segment_rewrite: storage update failed\n");
            free(buf);
            parse_ok = 0;
            break;
        }
        free(buf);
    }

    if (close(fd_in) != 0) parse_ok = 0;

    rewrite_output_t output = {.fd = fd_out, .failed = 0};
    if (parse_ok) storage_iterate(&temp_storage, dump_record_cb, &output);
    int output_ok = parse_ok && !output.failed;
    if (output_ok && sealed_fsync_full(fd_out) != 0) output_ok = 0;
    if (close(fd_out) != 0) output_ok = 0;
    if (!output_ok) {
        perror("segment_rewrite: durable output failed");
        unlink(tmp_path);
    } else if (rename(tmp_path, task->output_path) != 0 ||
               sealed_fsync_parent_dir(task->output_path) != 0) {
        perror("segment_rewrite: rename failed");
        unlink(tmp_path);
    } else {
        printf("??? Segment rewrite complete: %s\n", task->output_path);
    }

    storage_destroy(&temp_storage);

    cleanup:
    free(task->source_path);
    free(task->output_path);
    free(task);
    atomic_store(&segment_rewrite_active, false);
    return NULL;
}

/* ????????? Rest of the segment rewrite functions (unchanged) ????????? */
int AOF_begin_rewrite(const char *source_path) {
    if (atomic_load(&segment_rewrite_active)) {
        printf("??????  Segment rewrite already in progress\n");
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

static void rewrite_discard_delta_locked(void) {
    while (rewrite_buf_tail != rewrite_buf_head) {
        aof_cmd_t *command = &rewrite_buffer[rewrite_buf_tail];
        free(command->data);
        command->data = NULL;
        rewrite_buf_tail = (rewrite_buf_tail + 1) & rewrite_buf_mask;
    }
}

static void rewrite_abort_buffered_delta(void) {
    atomic_store(&rewrite_active, false);
    pthread_mutex_lock(&lock);
    pthread_mutex_lock(&rewrite_lock);
    rewrite_discard_delta_locked();
    pthread_mutex_unlock(&rewrite_lock);
    pthread_mutex_unlock(&lock);
}


/* ????????? Non-blocking rewrite thread (enhanced) ????????????????????????????????? */
static void *rewrite_thread_func(void *arg) {
    Storage *st = (Storage*)arg;

    printf("???? Starting non-blocking AOF rewrite...\n");

    char tmp[512];
    int path_len = snprintf(tmp, sizeof tmp, "%s.rewrite", g_path);
    if (path_len < 0 || (size_t)path_len >= sizeof(tmp)) {
        errno = ENAMETOOLONG;
        perror("rewrite: temporary path");
        rewrite_abort_buffered_delta();
        return NULL;
    }
    int fd_tmp = open(tmp, O_CREAT | O_TRUNC | O_WRONLY | O_APPEND | O_CLOEXEC,
                      0600);
    if (fd_tmp < 0) {
        perror("rewrite: open tmp");
        rewrite_abort_buffered_delta();
        return NULL;
    }

    rewrite_output_t output = {.fd = fd_tmp, .failed = 0};
    storage_iterate(st, dump_record_cb, &output);
    if (output.failed) {
        perror("rewrite: snapshot write");
        close(fd_tmp);
        unlink(tmp);
        rewrite_abort_buffered_delta();
        return NULL;
    }

    /* Writer lock order is lock -> rewrite_lock. Holding both closes the gap
     * between the buffered delta and commands still waiting in the ring. */
    pthread_mutex_lock(&lock);
    pthread_mutex_lock(&rewrite_lock);
    if (atomic_load(&rewrite_delta_failed)) output.failed = 1;
    printf("???? Applying %zu buffered operations...\n",
           (rewrite_buf_head - rewrite_buf_tail) & rewrite_buf_mask);

    size_t delta_cursor = rewrite_buf_tail;
    while (delta_cursor != rewrite_buf_head && !output.failed) {
        aof_cmd_t *c = &rewrite_buffer[delta_cursor];
        dump_record_cb(c->id, c->data, c->sz, &output);
        delta_cursor = (delta_cursor + 1) & rewrite_buf_mask;
    }
    size_t ring_cursor = tail;
    while (ring_cursor != head && !output.failed) {
        aof_cmd_t *c = &ring[ring_cursor];
        dump_record_cb(c->id, c->data, c->sz, &output);
        ring_cursor = (ring_cursor + 1) & mask;
    }
    if (!output.failed && sealed_fsync_full(fd_tmp) != 0) output.failed = 1;
    if (!output.failed && rename(tmp, g_path) != 0) output.failed = 1;
    int directory_failed = 0;
    if (!output.failed && sealed_fsync_parent_dir(g_path) != 0)
        directory_failed = 1;

    if (output.failed) {
        perror("rewrite: durable replacement failed");
        close(fd_tmp);
        unlink(tmp);
        rewrite_discard_delta_locked();
        pthread_mutex_unlock(&rewrite_lock);
        pthread_mutex_unlock(&lock);
        atomic_store(&rewrite_active, false);
        return NULL;
    }

    rewrite_discard_delta_locked();
    while (tail != head) {
        cleanup_ring_entry(&ring[tail]);
        tail = (tail + 1) & mask;
    }
    int old_fd = fd;
    fd = fd_tmp;
    if (old_fd >= 0 && close(old_fd) != 0)
        perror("rewrite: close old AOF");
    pthread_mutex_unlock(&rewrite_lock);
    pthread_mutex_unlock(&lock);

    if (directory_failed) {
        perror("rewrite: parent directory fsync");
        sealed_mark_failed();
    }
    atomic_store(&rewrite_active, false);
    printf("??? Non-blocking AOF rewrite complete!\n");
    return NULL;
}

/* ????????? Public API (enhanced) ????????????????????????????????????????????????????????????????????? */
void AOF_init(const char *path, size_t ring_capacity, unsigned interval_ms) {

    int main_aof_existed = access(path, F_OK) == 0;
    if (AOF_sealed_init(path) != 0) {
        fprintf(stderr, "Sealed AOF initialization failed; refusing to start without durable writes\n");
        exit(1);
    }
    /* Initialize global fd state */
    g_fd_state.file_path = strdup(path);
    if (!g_fd_state.file_path) {
        fprintf(stderr, "Failed to allocate AOF path\n");
        exit(1);
    }

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
    if (!main_aof_existed &&
        (sealed_fsync_full(initial_fd) != 0 ||
         sealed_fsync_parent_dir(path) != 0)) {
        perror("AOF_init: failed to persist AOF file creation");
        close(initial_fd);
        exit(1);
    }

    atomic_store(&g_fd_state.master_fd, initial_fd);
    atomic_store(&g_fd_state.master_generation, 1);

    /* Initialize the rest of AOF system */
    mode_always = (interval_ms == 0);
    flush_ms = mode_always ? 1000 : interval_ms;
    g_path = strdup(path);
    if (!g_path) {
        fprintf(stderr, "Failed to allocate active AOF path\n");
        exit(1);
    }

    /* Initialize components in order */
    if (init_buffer_pool() != 0) {
        fprintf(stderr, "Failed to initialize buffer pool\n");
        exit(1);
    }

    (void)init_io_uring();

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
        legacy_locks_initialized = true;
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

/* ????????? AOF_append with pool allocation ????????? */
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
        if (sz > UINT32_MAX || sz > SIZE_MAX - 12) {
            errno = EOVERFLOW;
            sealed_mark_failed();
        } else {
            size_t record_capacity = 12 + sz;
            char stack_record[12 + MAX_BUFFER_SIZE];
            char *record = record_capacity <= sizeof(stack_record)
                         ? stack_record : malloc(record_capacity);
            if (!record) {
                sealed_mark_failed();
            } else {
                size_t record_size = write_record_to_buffer(
                    record, id, buf, (uint32_t)sz);
                int write_rc = atomic_load(&g_uring_ctx.initialized)
                             ? submit_uring_write(record, record_size) : -1;
                if (write_rc != 0) {
                    AOF_reopen_if_needed();
                    write_rc = is_fd_valid(fd)
                             ? aof_write_full(fd, record, record_size) : -1;
                }
                if (write_rc != 0) {
                    perror("AOF direct write");
                    sealed_mark_failed();
                }
                if (record != stack_record) free(record);
            }
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

//int AOF_fs_stats(uint64_t *bytes_total, uint64_t *bytes_free, uint64_t *inodes_total, uint64_t *inodes_free){
//    if (!g_fd_state.file_path) return -1;
//    char pathcpy[1024]; strncpy(pathcpy, g_fd_state.file_path, sizeof(pathcpy)-1); pathcpy[sizeof(pathcpy)-1]=0;
//    char *dir = dirname(pathcpy); if (!dir) dir = ".";
//    struct statvfs s;
//    if (statvfs(dir, &s) != 0) return -1;
//    if (bytes_total)  *bytes_total  = (uint64_t)s.f_frsize * (uint64_t)s.f_blocks;
//    if (bytes_free)   *bytes_free   = (uint64_t)s.f_frsize * (uint64_t)s.f_bfree;
//    if (inodes_total) *inodes_total = (uint64_t)s.f_files;
//    if (inodes_free)  *inodes_free  = (uint64_t)s.f_ffree;
//    return 0;
//}

/* ????????? AOF_load function ????????? */
void AOF_load(Storage *st) {
    if (!g_path || !st) return;

    int fd_load = open(g_path, O_RDONLY | O_CLOEXEC);
    if (fd_load < 0) {
        if (errno == ENOENT) {
            printf("???? AOF file not found, starting fresh\n");
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
        if (storage_save(st, id, buf, size) != 0) {
            fprintf(stderr, "AOF_load: storage update failed\n");
            free(buf);
            break;
        }
        free(buf);
        loaded++;
    }

    close(fd_load);
}
/* ????????? AOF_rewrite function ????????? */
void AOF_rewrite(Storage *st) {
    if (mode_always || !rewrite_buffer || !ring || !st) {
        fprintf(stderr, "AOF_rewrite: buffered AOF mode and initialized storage are required\n");
        return;
    }
    bool expected = false;
    if (!atomic_compare_exchange_strong(&rewrite_active, &expected, true)) {
        printf("??????  AOF rewrite already in progress\n");
        return;
    }
    atomic_store(&rewrite_delta_failed, false);

    if (pthread_create(&rewrite_thread, NULL, rewrite_thread_func, st) != 0) {
        perror("AOF_rewrite: pthread_create");
        rewrite_abort_buffered_delta();
        return;
    }

    pthread_detach(rewrite_thread);
    printf("???? AOF rewrite started in background\n");
}

/* ????????? AOF_shutdown function ????????? */
void AOF_shutdown(void) {
    if (atomic_exchange_explicit(&g_aof_shutdown_once, true,
                                 memory_order_acq_rel)) return;
    printf("???? AOF shutdown initiated...\n");

    atomic_store(&g_opt_sealed.shutdown, true);

    if (g_opt_sealed.sync_thread) {
        pthread_join(g_opt_sealed.sync_thread, NULL);
    }

    if (g_opt_sealed.fd >= 0) {
        if (sealed_fsync_full(g_opt_sealed.fd) != 0)
            sealed_mark_failed();
        if (close(g_opt_sealed.fd) != 0)
            sealed_mark_failed();
        g_opt_sealed.fd = -1;
    }

    for (int i = 0; i < MAX_SEALED_BATCHES; i++) {
        free(g_opt_sealed.slots[i].buffer);
        g_opt_sealed.slots[i].buffer = NULL;
    }

    atomic_store(&running, false);

    /* Signal all threads to wake up */
    if (legacy_locks_initialized) {
        pthread_mutex_lock(&lock);
        pthread_cond_broadcast(&cond);
        pthread_mutex_unlock(&lock);
    }

    /* Wait for threads to finish */
    if (writer) {
        pthread_join(writer, NULL);
        printf("??? Writer thread joined\n");
    }

    if (atomic_load(&g_uring_ctx.initialized)) {
        pthread_join(uring_worker, NULL);
        printf("??? io_uring worker thread joined\n");
    }

    /* Wait for any active rewrite to complete */
    if (atomic_load(&rewrite_active)) {
        printf("??? Waiting for rewrite to complete...\n");
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

    if (legacy_locks_initialized) {
        pthread_mutex_destroy(&lock);
        pthread_cond_destroy(&cond);
        pthread_mutex_destroy(&rewrite_lock);
        legacy_locks_initialized = false;
    }
    pthread_mutex_destroy(&g_opt_sealed.gc_lock);

    printf("??? AOF shutdown complete\n");
}

/* ????????? Status functions ????????? */
int AOF_rewrite_in_progress(void) {
    return atomic_load(&rewrite_active);
}

size_t AOF_pending_writes(void) {
    if (!atomic_load(&running)) return 0;

    size_t pending = 0;
    if (legacy_locks_initialized) {
        pthread_mutex_lock(&lock);
        pending = (head - tail) & mask;
        pthread_mutex_unlock(&lock);
    }

    return pending + (atomic_load(&g_uring_ctx.initialized)
                      ? atomic_load(&g_uring_ctx.pending_writes) : 0);
}

void AOF_prepare_for_rotation(void) {
    printf("???? Preparing AOF for rotation...\n");

    /* Wait for all pending writes to complete */
    while (AOF_pending_writes() > 0) {
        usleep(1000); /* 1ms */
    }

    /* Ensure everything is flushed to disk */
    AOF_sync();

    printf("??? AOF prepared for rotation\n");
}

/* Atomically rotate the AOF file */
int AOF_rotate_file(const char *new_path) {
    printf("???? Starting safe AOF rotation...\n");
    if (!new_path || !*new_path || !g_fd_state.file_path) {
        errno = EINVAL;
        return -1;
    }

    /* Acquire write lock to block all new fd acquisitions */
    pthread_rwlock_wrlock(&g_fd_state.rotation_rwlock);

    /* Wait for all pending io_uring operations to drain */
    printf("??? Draining %d pending io_uring operations...\n",
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
        fprintf(stderr, "AOF rotation: pending writes did not drain\n");
        pthread_rwlock_unlock(&g_fd_state.rotation_rwlock);
        errno = ETIMEDOUT;
        return -1;
    }

    /* Sync and close the old descriptor only after all async writes finish. */
    int old_master_fd = atomic_exchange(&g_fd_state.master_fd, -1);
    if (old_master_fd >= 0) {
        if (sealed_fsync_full(old_master_fd) != 0) {
            atomic_store(&g_fd_state.master_fd, old_master_fd);
            pthread_rwlock_unlock(&g_fd_state.rotation_rwlock);
            return -1;
        }
        if (close(old_master_fd) != 0) {
            pthread_rwlock_unlock(&g_fd_state.rotation_rwlock);
            return -1;
        }
        printf("???? Closed master AOF fd\n");
    }

    /* Perform the rotation */
    if (rename(g_fd_state.file_path, new_path) != 0) {
        perror("AOF rotation: rename failed");

        /* Try to recover by reopening */
        int recovery_fd = open(g_fd_state.file_path, O_CREAT | O_APPEND | O_WRONLY | O_CLOEXEC, 0600);
        if (recovery_fd < 0) sealed_mark_failed();
        else atomic_store(&g_fd_state.master_fd, recovery_fd);

        pthread_rwlock_unlock(&g_fd_state.rotation_rwlock);
        return -1;
    }
    if (sealed_fsync_parent_dir(new_path) != 0) {
        perror("AOF rotation: rotated directory fsync failed");
        sealed_mark_failed();
        pthread_rwlock_unlock(&g_fd_state.rotation_rwlock);
        return -1;
    }

    /* Create new empty file */
    int new_master_fd = open(g_fd_state.file_path, O_CREAT | O_TRUNC | O_APPEND | O_WRONLY | O_CLOEXEC, 0600);
    if (new_master_fd < 0) {
        perror("AOF rotation: failed to create new file");
        if (rename(new_path, g_fd_state.file_path) != 0) {
            sealed_mark_failed();
        } else {
            int restored = open(g_fd_state.file_path,
                                O_APPEND | O_WRONLY | O_CLOEXEC);
            if (restored < 0) sealed_mark_failed();
            else atomic_store(&g_fd_state.master_fd, restored);
            if (sealed_fsync_parent_dir(g_fd_state.file_path) != 0)
                sealed_mark_failed();
        }
        pthread_rwlock_unlock(&g_fd_state.rotation_rwlock);
        return -1;
    }
    if (sealed_fsync_full(new_master_fd) != 0 ||
        sealed_fsync_parent_dir(g_fd_state.file_path) != 0) {
        close(new_master_fd);
        sealed_mark_failed();
        pthread_rwlock_unlock(&g_fd_state.rotation_rwlock);
        return -1;
    }

    atomic_store(&g_fd_state.master_fd, new_master_fd);

    /* Increment generation to invalidate all cached fds */
    uint64_t new_gen = atomic_fetch_add(&g_fd_state.master_generation, 1) + 1;

    printf("???? New AOF file opened: %s (fd=%d, generation=%" PRIu64 ")\n",
           g_fd_state.file_path, new_master_fd, new_gen);

    /* Release write lock - workers can now acquire new fds */
    pthread_rwlock_unlock(&g_fd_state.rotation_rwlock);

    printf("??? Safe AOF rotation complete\n");
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
    if (!buf) {
        sealed_mark_failed();
        return NULL;
    }

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
            if (need > SIZE_MAX - have) {
                sealed_mark_failed();
                break;
            }
            if (have + need > buf_cap) {
                size_t ncap = buf_cap;
                while (ncap < have + need) {
                    if (ncap > SIZE_MAX / 2) {
                        ncap = 0;
                        break;
                    }
                    ncap <<= 1;
                }
                if (ncap == 0) {
                    sealed_mark_failed();
                    break;
                }
                uint8_t *nb = (uint8_t*)realloc(buf, ncap);
                if (!nb) {
                    sealed_mark_failed();
                    break;
                }
                buf = nb;
                buf_cap = ncap;
            }
            ssize_t n = pread(fd_tail, buf + have, need, off);
            if (n > 0) { have += (size_t)n; off += n; }
        }

        // Parse whole records: [id:4][size:4][payload:size][crc:4]
        size_t cons = 0;
        while (have - cons >= 12) {
            uint32_t id = rd32(buf + cons);
            uint32_t sz = rd32(buf + cons + 4);
            size_t rec = (size_t)12 + (size_t)sz;
            if (rec < (size_t)sz) {
                sealed_mark_failed();
                break;
            }
            if (have - cons < rec) break;   // wait for more bytes

            uint32_t crc_file = rd32(buf + cons + 8 + sz);
            uint32_t crc = crc32c(0, &id, 4);
            crc = crc32c(crc, &sz, 4);
            crc = crc32c(crc, buf + cons + 8, sz);

            if (crc == crc_file) {
                int handled = RAMForge_HA_replay_record(
                    (uint32_t)id, buf + cons + 8, sz);
                if (handled < 0 ||
                    (handled == 0 &&
                     rf_broker_apply_aof_checked((int)id, buf + cons + 8,
                                                 sz) != 0)) {
                    sealed_mark_failed();
                    break;
                }
            } else {
                /* The complete record length is visible, so a CRC mismatch is
                 * corruption, not a partial record to spin on forever. */
                sealed_mark_failed();
                break;
            }
            cons += rec;
        }
        if (!AOF_sealed_is_healthy()) break;
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

/* ????????? Sync function for critical operations ????????? */
void AOF_sync(void) {
    if (fd >= 0) {
        if (sealed_fsync_full(fd) != 0) sealed_mark_failed();
    }
}
