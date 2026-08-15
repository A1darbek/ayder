// rf_broker.c - Zero-Copy, Lock-Free, SIMD-Optimized Message Broker

#define _GNU_SOURCE
#include "rf_broker.h"
#include "slab_alloc.h"
#include "shared_storage.h"
#include "aof_batch.h"
#include "metrics_shared.h"
#include "ramforge_ha_integration.h"
#include <pthread.h>
#include <string.h>
#include <stdio.h>
#include <stdlib.h>
#include <errno.h>
#include <stdatomic.h>
#include <time.h>
#include <sys/mman.h>
#include <unistd.h>
#include <sched.h>
#include <stddef.h>   // max_align_t
#include <stdalign.h> // _Alignof
#include <stdbool.h>
#include <openssl/evp.h>

// SIMD support detection
#ifdef __AVX2__
#include <immintrin.h>
#define SIMD_ENABLED 1
#elif defined(__SSE2__)
#include <emmintrin.h>
#define SIMD_ENABLED 1
#else
#define SIMD_ENABLED 0
#endif

#ifndef MIN
#define MIN(a,b) ((a)<(b)?(a):(b))
#endif

#define likely(x) __builtin_expect(!!(x), 1)
#define unlikely(x) __builtin_expect(!!(x), 0)

#define AOF_REC_BROKER_MSG_BATCH 0x4242
#define IDEMP_BUCKETS 1024
#define RF_SLOT_FLAG_VALID 0x1u
#define RF_SLOT_FLAG_LARGE 0x2u
#define RF_SLOT_FLAG_TOMBSTONE 0x4u
#define RF_SHARED_TOPIC_PENDING 1u
#define RF_SHARED_TOPIC_READY 2u

typedef struct {
    uint32_t id;
    uint16_t parts;
    char name[RF_MAX_TOPIC_LEN];
} aof_topic_rec_t;



static inline size_t align_up_sz(size_t x, size_t a) {
    return (x + (a - 1)) & ~(a - 1);
}

/* Use max_align_t so ANY struct cast is safe (aof_msg_rec_t has uint64_t, etc.) */
#define RF_BUS_HEAP_ALIGN ((size_t)_Alignof(max_align_t))

extern SharedStorage *g_shared_storage;

// ═══════════════════════════════════════════════════════════════════════════════
// Lock-Free Ring Buffer with Zero-Copy Message Storage
// ═══════════════════════════════════════════════════════════════════════════════


typedef struct {
    aof_msg_rec_t *pool;
    size_t pool_size;
    size_t pool_used;
    _Atomic size_t pool_head;
} aof_batch_pool_t;

static __thread aof_batch_pool_t tls_aof_pool = {0};


static rf_bus_t *g_bus;             /* shared, MAP_SHARED, inherited by fork */
static pthread_t g_promoter;
static _Thread_local uint64_t tls_read_seq = 0;
static int g_worker_id = -1;
static _Atomic uint64_t g_last_ha_read_barrier_req = 0;
static _Atomic uint64_t g_ha_fwd_req_seq = 1;
static _Thread_local rf_write_receipt_t g_last_write_receipt;
static _Thread_local int g_replay_apply_error;

static int replay_idemp_v2(uint32_t rec_id, const void *data, size_t size);
static int replay_plain_batch_atomic(const void *data, size_t size);
static rf_topic_t *rf_topic_require_by_id(uint32_t id);

/* Fault injection is deliberately unavailable unless both the global test
 * switch and a specific fault are set. Production configuration cannot enable
 * these paths accidentally through a partially populated environment. */
static int rf_test_faults_enabled(void) {
    const char *value = getenv("RF_TEST_FAULTS");
    return value && strcmp(value, "1") == 0;
}

static int rf_test_bus_apply_should_fail(uint32_t record_type) {
    if (!rf_test_faults_enabled()) return 0;
    const char *value = getenv("RF_TEST_BUS_APPLY_FAIL_REC_ID");
    if (!value || !*value) return 0;
    errno = 0;
    char *end = NULL;
    unsigned long parsed = strtoul(value, &end, 0);
    return errno == 0 && end != value && *end == '\0' &&
           parsed <= UINT32_MAX && (uint32_t)parsed == record_type;
}

#define SHMEM_HA_WRITE_RSP_BASE 0x5A170000u

static int ha_fwd_rsp_key(uint64_t req_id) {
    uint32_t folded = (uint32_t)req_id ^ (uint32_t)(req_id >> 32);
    uint32_t key = (SHMEM_HA_WRITE_RSP_BASE ^ folded) | 1u;
    return -(int)(key & 0x7fffffffu);
}

static inline void init_worker_id_from_env(void) {
    if (g_worker_id >= 0) return;
    const char *e = getenv("RF_WORKER_ID");
    if (e && *e) g_worker_id = atoi(e); else g_worker_id = 0;
}
static inline uint64_t now_us(void) {
    struct timespec ts;
    clock_gettime(CLOCK_MONOTONIC, &ts);
    return (uint64_t)ts.tv_sec * 1000000ULL + ts.tv_nsec / 1000;
}

int rf_wait_ha_read_barrier(uint64_t req_id, uint32_t timeout_ms) {
    if (req_id == 0) return -1;
    uint64_t start = now_us();
    uint64_t timeout_us = ((uint64_t)(timeout_ms ? timeout_ms : 3000u)) * 1000ULL;
    for (;;) {
        if (atomic_load_explicit(&g_last_ha_read_barrier_req, memory_order_acquire) >= req_id) return 0;
        if ((now_us() - start) >= timeout_us) return -1;
        usleep(200);
    }
}

#define RF_TS_FLAG_WALL   0x0001u
#define RF_EPOCH_US_MIN   1000000000000000ULL  /* ~2001-09-09 in microseconds */

static inline uint64_t wall_us_raw(void) {
        struct timespec ts;
        clock_gettime(CLOCK_REALTIME, &ts);
        return (uint64_t)ts.tv_sec * 1000000ULL + (uint64_t)ts.tv_nsec / 1000ULL;
    }

static inline uint64_t ttl_now_wall_us(void) {
       static _Thread_local uint64_t last = 0;
        uint64_t t = wall_us_raw();
        if (t < last) t = last;
        else last = t;
        return t;
    }
static inline uint64_t normalize_persisted_ts_us(uint64_t ts_us, uint16_t reserved, uint64_t now_wall) {
    if (reserved & RF_TS_FLAG_WALL) {
        if (!ts_us) return now_wall;
        if (ts_us > now_wall) return now_wall;
        return ts_us;
    }
    if (ts_us >= RF_EPOCH_US_MIN) {
        if (ts_us > now_wall) return now_wall;
        return ts_us;
    }
    return now_wall;
}


static inline void rf_ttl_apply_to_partition(rf_partition_t *p, uint64_t ttl_ms) {
    uint64_t us = ttl_ms ? (ttl_ms * 1000ULL) : 0;
    atomic_store_explicit(&p->ttl_ms, us, memory_order_release);
    atomic_store_explicit(&p->ttl_window_us, us, memory_order_release);
}

static inline uint64_t slot_payload_bytes(const rf_msg_slot_t *s) {
    return (uint64_t)atomic_load_explicit((_Atomic uint32_t*)&s->key_len, memory_order_acquire) +
           (uint64_t)atomic_load_explicit((_Atomic uint32_t*)&s->val_len, memory_order_acquire);
}

static inline void rf_ttl_invalidate_slot(rf_msg_slot_t *slot) {
    uint16_t f = atomic_load_explicit(&slot->flags, memory_order_acquire);
    if ((f & 3) == 3 && slot->blob && slot->blob != slot->inline_data) {
        void *blob = slot->blob;
        slot->blob = NULL;
        slab_free(blob);
    }
    /* make invisible last */
    atomic_store_explicit(&slot->flags, 0, memory_order_release);
}

static inline void rf_invalidate_slot(rf_partition_t *p, rf_msg_slot_t *slot) {
    uint64_t b = slot_payload_bytes(slot);
    rf_ttl_invalidate_slot(slot);
    /* make the slot unmistakably "not for this logical offset" */
    atomic_store_explicit(&slot->offset, UINT64_MAX, memory_order_release);
    if (b) atomic_fetch_sub_explicit(&p->live_bytes, b, memory_order_relaxed);
}

static inline int rf_slot_ready_for_offset(const rf_msg_slot_t *slot, uint64_t off) {
    uint16_t f = atomic_load_explicit((_Atomic uint16_t *) &slot->flags, memory_order_acquire);
    if (!(f & 1)) return 0;
    uint64_t so = atomic_load_explicit((_Atomic uint64_t *) &slot->offset, memory_order_acquire);
    return so == off;
}

/* avoid leaking blobs / double-counting live_bytes when overwriting slots */
static inline void rf_slot_evict_if_needed(rf_partition_t *p, rf_msg_slot_t *slot, uint64_t new_off) {
    uint16_t f = atomic_load_explicit(&slot->flags, memory_order_acquire);
    if (!(f & 1)) return;
    uint64_t so = atomic_load_explicit(&slot->offset, memory_order_acquire);
    if (so == new_off) return;
    rf_invalidate_slot(p, slot);
}

static inline void rf_size_sweep_if_over(rf_partition_t *p, uint64_t now) {
    uint64_t maxb = atomic_load_explicit(&p->retain_max_bytes, memory_order_acquire);
    if (!maxb) return;
    if (atomic_load_explicit(&p->live_bytes, memory_order_acquire) <= maxb) return;

    uint64_t last = atomic_load_explicit(&p->last_size_gc_us, memory_order_acquire);
    if (now - last < 2000) return; /* ~2ms throttle like TTL */
    if (!atomic_compare_exchange_strong(&p->last_size_gc_us, &last, now)) return;

    const uint64_t wh = atomic_load_explicit(&p->write_head, memory_order_acquire);
    const uint64_t oldest = (wh > p->capacity) ? (wh - p->capacity) : 0;
    uint64_t span = wh - oldest;
    if (!span) return;

    uint64_t cur = atomic_load_explicit(&p->size_gc_cursor, memory_order_relaxed);
    if (cur >= span) cur = 0;

    /* GC up to N slots this tick, stop early if under cap */
    const uint64_t STEPS = 128;
    uint64_t o = oldest + cur;
    for (uint64_t i = 0; i < STEPS && o < wh; i++, o++) {
        if (atomic_load_explicit(&p->live_bytes, memory_order_acquire) <= maxb) break;
        rf_msg_slot_t *s = &p->ring[o & p->capacity_mask];

        uint16_t f = atomic_load_explicit(&s->flags, memory_order_acquire);
        if (!(f & 1)) continue;
        uint64_t so = atomic_load_explicit(&s->offset, memory_order_acquire);
        if (so != o) continue; /* already overwritten / not the intended logical slot */
        rf_invalidate_slot(p, s);
    }

    /* remember where we stopped relative to current oldest */
    span = wh - oldest;
    uint64_t next_rel = (o > oldest) ? (o - oldest) : 0;
    if (span) next_rel %= span;
    atomic_store_explicit(&p->size_gc_cursor, next_rel, memory_order_relaxed);
}

static inline void rf_size_sweep_maybe(rf_partition_t *p) {
        uint64_t maxb = atomic_load_explicit(&p->retain_max_bytes, memory_order_acquire);
        if (!maxb) return;
        if (atomic_load_explicit(&p->live_bytes, memory_order_acquire) <= maxb) return;
        rf_size_sweep_if_over(p, now_us());
    }


static uint32_t rf_bus_timeout_ms(uint32_t requested) {
    if (requested) return requested;
    const char *e = getenv("RF_BUS_PUBLISH_TIMEOUT_MS");
    if (e && *e) {
        unsigned long v = strtoul(e, NULL, 10);
        if (v > 0 && v <= UINT32_MAX) return (uint32_t)v;
    }
    return 5000;
}

static int rf_lock_shared(pthread_mutex_t *lock) {
    int rc = pthread_mutex_lock(lock);
#ifdef EOWNERDEAD
    if (rc == EOWNERDEAD) {
        if (pthread_mutex_consistent(lock) != 0) return -1;
        return 0;
    }
#endif
    return rc == 0 ? 0 : -1;
}

static uint64_t rf_bus_min_cursor(const _Atomic uint64_t *cursors) {
    uint32_t workers = g_bus->worker_count;
    uint64_t min = UINT64_MAX;
    for (uint32_t i = 0; i < workers; i++) {
        uint64_t v = atomic_load_explicit(&cursors[i], memory_order_acquire);
        if (v < min) min = v;
    }
    return min == UINT64_MAX ? 0 : min;
}

static rf_msg_slot_t* rf_ring_alloc(size_t cap) {
    size_t bytes;
    if (__builtin_mul_overflow(cap, sizeof(rf_msg_slot_t), &bytes)) return NULL;


    void *p = mmap(NULL, bytes,
                   PROT_READ | PROT_WRITE,
                   MAP_PRIVATE | MAP_ANONYMOUS
                   #ifdef MAP_NORESERVE
                   | MAP_NORESERVE
#endif
            , -1, 0);
    if (p == MAP_FAILED) return NULL;

    // mmap is page-aligned (>=64), so aligned(64) is satisfied.
    return (rf_msg_slot_t*)p;
}



static int rf_partition_ensure_ring(rf_partition_t *p) {
    rf_msg_slot_t *r = atomic_load_explicit(&p->ring, memory_order_acquire);
    if (r) return 0;

    rf_msg_slot_t *newr = rf_ring_alloc(p->capacity);
    if (!newr) return -1;

    rf_msg_slot_t *expected = NULL;
    if (!atomic_compare_exchange_strong_explicit(
            &p->ring, &expected, newr,
            memory_order_release, memory_order_relaxed)) {
        // someone else installed it
        size_t bytes = p->capacity * sizeof(rf_msg_slot_t);
        munmap(newr, bytes);
    }
    return 0;
}


int rf_bus_init_pre_fork(size_t heap_bytes, size_t desc_count){
    if (g_bus) return 0;

    uint32_t dc = 1; while (dc < desc_count) dc <<= 1;
    desc_count = dc;

    const size_t desc_align = _Alignof(bus_desc_t);
    const size_t heap_align = RF_BUS_HEAP_ALIGN;

    size_t off_desc = align_up_sz(sizeof(rf_bus_t), desc_align);
    size_t desc_bytes = desc_count * sizeof(bus_desc_t);
    size_t off_heap = align_up_sz(off_desc + desc_bytes, heap_align);
    size_t total = off_heap + heap_bytes;

    void *mem = mmap(NULL, total, PROT_READ|PROT_WRITE,
                     MAP_SHARED|MAP_ANONYMOUS, -1, 0);
    if (mem == MAP_FAILED) return -1;

    g_bus = (rf_bus_t*)mem;

    g_bus->desc = (bus_desc_t *)((uint8_t*)mem + off_desc);
    g_bus->desc_mask = (uint32_t)desc_count - 1;
    memset(g_bus->desc, 0, desc_bytes);

    g_bus->heap = (uint8_t*)mem + off_heap;
    g_bus->heap_size = (uint32_t)heap_bytes;

    const char *worker_env = getenv("RF_EFFECTIVE_WORKER_COUNT");
    unsigned long workers = worker_env && *worker_env ? strtoul(worker_env, NULL, 10) : 1;
    if (workers == 0) workers = 1;
    if (workers > RF_MAX_WORKERS) {
        munmap(mem, total);
        errno = E2BIG;
        return -1;
    }
    g_bus->worker_count = (uint32_t)workers;

    pthread_mutexattr_t mattr;
    if (pthread_mutexattr_init(&mattr) != 0 ||
        pthread_mutexattr_setpshared(&mattr, PTHREAD_PROCESS_SHARED) != 0) {
        munmap(mem, total);
        return -1;
    }
#if defined(__linux__)
    if (pthread_mutexattr_setrobust(&mattr, PTHREAD_MUTEX_ROBUST) != 0) {
        pthread_mutexattr_destroy(&mattr);
        munmap(mem, total);
        return -1;
    }
#endif
    if (pthread_mutex_init(&g_bus->publish_lock, &mattr) != 0 ||
        pthread_mutex_init(&g_bus->topic_lock, &mattr) != 0) {
        pthread_mutexattr_destroy(&mattr);
        munmap(mem, total);
        return -1;
    }
    for (uint32_t i = 0; i < RF_IDEMP_LOCKS; i++) {
        if (pthread_mutex_init(&g_bus->idemp_locks[i], &mattr) != 0) {
            pthread_mutexattr_destroy(&mattr);
            munmap(mem, total);
            return -1;
        }
    }
    pthread_mutexattr_destroy(&mattr);

    atomic_store(&g_bus->data_head, 0);
    atomic_store(&g_bus->desc_head, 0);
    atomic_store(&g_bus->drops, 0);
    atomic_store(&g_bus->topic_count, 0);
    for (uint32_t i = 0; i < RF_MAX_WORKERS; i++) {
        atomic_store(&g_bus->consumer_seq[i], 0);
        atomic_store(&g_bus->consumer_data_tail[i], 0);
        atomic_store(&g_bus->consumer_failed_seq[i], 0);
    }

    return 0;
}


static int rf_bus_publish_seq(int rec_id, const void *payload, uint32_t size,
                              uint32_t timeout_ms, uint64_t *seq_out) {
    if (!g_bus || !payload || !size) return -1;
    if (size > g_bus->heap_size) {
        errno = EMSGSIZE;
        return -1;
    }

    timeout_ms = rf_bus_timeout_ms(timeout_ms);
    uint64_t start = now_us();
    const size_t aligned_size = align_up_sz(size, RF_BUS_HEAP_ALIGN);

    for (;;) {
        if (rf_lock_shared(&g_bus->publish_lock) != 0) return -1;

        uint64_t d = atomic_load_explicit(&g_bus->desc_head, memory_order_acquire);
        uint64_t min_seq = rf_bus_min_cursor(g_bus->consumer_seq);
        uint64_t cur = atomic_load_explicit(&g_bus->data_head, memory_order_acquire);
        uint64_t min_tail = rf_bus_min_cursor(g_bus->consumer_data_tail);

        uint32_t raw = (uint32_t)(cur % g_bus->heap_size);
        uint32_t off = (uint32_t)align_up_sz(raw, RF_BUS_HEAP_ALIGN);
        uint64_t advance;
        if ((uint64_t)off + size <= g_bus->heap_size) {
            advance = (uint64_t)(off - raw) + (uint64_t)aligned_size;
        } else {
            advance = (uint64_t)(g_bus->heap_size - raw) + (uint64_t)aligned_size;
            off = 0;
        }
        uint64_t data_end = cur + advance;

        int desc_full = (d - min_seq) >= (uint64_t)(g_bus->desc_mask + 1);
        int heap_full = (data_end - min_tail) > (uint64_t)g_bus->heap_size;
        if (!desc_full && !heap_full) {
            uint32_t idx = (uint32_t)d & g_bus->desc_mask;
            bus_desc_t *ds = &g_bus->desc[idx];
            atomic_store_explicit(&ds->valid, 0, memory_order_relaxed);
            memcpy(g_bus->heap + off, payload, size);
            ds->rec_id = (uint32_t)rec_id;
            ds->size = size;
            ds->off = off;
            ds->seq = d;
            ds->data_end = data_end;
            atomic_store_explicit(&g_bus->data_head, data_end, memory_order_release);
            atomic_store_explicit(&ds->valid, 1, memory_order_release);
            atomic_store_explicit(&g_bus->desc_head, d + 1, memory_order_release);
            pthread_mutex_unlock(&g_bus->publish_lock);
            if (seq_out) *seq_out = d;
            return 0;
        }

        pthread_mutex_unlock(&g_bus->publish_lock);
        if ((now_us() - start) >= (uint64_t)timeout_ms * 1000ULL) {
            atomic_fetch_add_explicit(&g_bus->drops, 1, memory_order_relaxed);
            errno = ETIMEDOUT;
            return -1;
        }
        struct timespec ts = {0, 200000};
        nanosleep(&ts, NULL);
    }
}

int rf_bus_publish(int rec_id, const void *payload, uint32_t size){
    return rf_bus_publish_seq(rec_id, payload, size, 0, NULL);
}

int rf_bus_publish_wait_except(int rec_id, const void *payload, uint32_t size,
                               uint32_t timeout_ms, int except_worker,
                               uint64_t *seq_out) {
    uint64_t seq = 0;
    timeout_ms = rf_bus_timeout_ms(timeout_ms);
    uint64_t start = now_us();
    if (rf_bus_publish_seq(rec_id, payload, size, timeout_ms, &seq) != 0) return -1;

    for (;;) {
        int complete = 1;
        for (uint32_t i = 0; i < g_bus->worker_count; i++) {
            if ((int)i == except_worker) continue;
            uint64_t failed = atomic_load_explicit(
                &g_bus->consumer_failed_seq[i], memory_order_acquire);
            if (failed != 0 && failed <= seq + 1) {
                errno = EIO;
                return -1;
            }
            if (atomic_load_explicit(&g_bus->consumer_seq[i], memory_order_acquire) <= seq) {
                complete = 0;
                break;
            }
        }
        if (complete) {
            if (seq_out) *seq_out = seq;
            return 0;
        }
        if ((now_us() - start) >= (uint64_t)timeout_ms * 1000ULL) {
            errno = ETIMEDOUT;
            return -1;
        }
        struct timespec ts = {0, 200000};
        nanosleep(&ts, NULL);
    }
}

int rf_bus_publish_wait(int rec_id, const void *payload, uint32_t size,
                        uint32_t timeout_ms, uint64_t *seq_out) {
    return rf_bus_publish_wait_except(rec_id, payload, size, timeout_ms, -1,
                                      seq_out);
}

int rf_mutation_lock(uint64_t key) {
    if (!g_bus) return -1;
    uint32_t stripe = (uint32_t)(key ^ (key >> 32)) & (RF_IDEMP_LOCKS - 1);
    return rf_lock_shared(&g_bus->idemp_locks[stripe]);
}

void rf_mutation_unlock(uint64_t key) {
    if (!g_bus) return;
    uint32_t stripe = (uint32_t)(key ^ (key >> 32)) & (RF_IDEMP_LOCKS - 1);
    (void)pthread_mutex_unlock(&g_bus->idemp_locks[stripe]);
}

int rf_persist_apply_record(uint32_t record_type, const void *data, uint32_t size,
                            uint32_t timeout_ms, rf_write_receipt_t *receipt) {
    rf_write_receipt_t result;
    memset(&result, 0, sizeof(result));
    g_last_write_receipt = result;
    if (receipt) *receipt = result;
    if ((!data && size) || !g_bus) return -1;
    init_worker_id_from_env();

    if (RAMForge_HA_is_enabled()) {
        ha_write_receipt_t ha_receipt;
        int rc = RAMForge_HA_replicate_write_checked(record_type, data, size,
                                                      timeout_ms, &ha_receipt);
        result.batch_id = ha_receipt.aof_batch_id;
        result.raft_index = ha_receipt.raft_index;
        result.local_fsync = ha_receipt.local_fsync;
        result.quorum_committed = ha_receipt.quorum_committed;
        result.failover_safe = ha_receipt.quorum_committed;
        result.applied_all_workers = ha_receipt.applied_all_workers;
        if (receipt) *receipt = result;
        g_last_write_receipt = result;
        return rc;
    }

    const char *rocket = getenv("RF_ROCKET");
    int rocket_on = rocket && *rocket && *rocket != '0';
    if (!rocket_on) {
        result.batch_id = AOF_append_sealed((int)record_type, data, size);
        if (result.batch_id == 0 || AOF_sealed_wait(result.batch_id) != 0) {
            if (receipt) *receipt = result;
            g_last_write_receipt = result;
            return -2;
        }
        result.local_fsync = 1;
    }

    int handled = RAMForge_HA_replay_record(record_type, data, size);
    if (handled < 0 ||
        (handled == 0 &&
         rf_broker_apply_aof_checked(record_type, data, size) != 0)) {
        if (receipt) *receipt = result;
        g_last_write_receipt = result;
        return -4;
    }
    if (rf_bus_publish_wait_except((int)record_type, data, size,
                                   timeout_ms, g_worker_id, NULL) != 0) {
        if (receipt) *receipt = result;
        g_last_write_receipt = result;
        return -3;
    }
    result.applied_all_workers = 1;
    if (receipt) *receipt = result;
    g_last_write_receipt = result;
    return 0;
}

void rf_last_write_receipt(rf_write_receipt_t *receipt) {
    if (receipt) *receipt = g_last_write_receipt;
}

int rf_kv_put_transaction(int32_t skey, int has_expected_cas,
                          uint64_t expected_cas, uint64_t expires_us,
                          const void *value, uint32_t value_len,
                          uint32_t timeout_ms, uint64_t *new_cas,
                          uint64_t *current_cas, rf_write_receipt_t *receipt) {
    if ((!value && value_len) ||
        sizeof(kvrec_t) + (size_t)value_len > MAX_DATA_SIZE) return -1;
    uint64_t lock_key = (uint64_t)(uint32_t)skey;
    if (rf_mutation_lock(lock_key) != 0) return -1;

    uint8_t current[MAX_DATA_SIZE];
    uint64_t observed_cas = 0;
    if (shared_storage_get_fast(g_shared_storage, skey, current, sizeof(current)))
        observed_cas = ((const kvrec_t *)current)->cas;
    if (current_cas) *current_cas = observed_cas;
    if (has_expected_cas && expected_cas != observed_cas) {
        rf_mutation_unlock(lock_key);
        return 1;
    }

    uint64_t next_cas = observed_cas + 1;
    size_t payload_size = sizeof(aof_kv_put_t) + (size_t)value_len;
    uint8_t payload[MAX_DATA_SIZE];
    aof_kv_put_t *header = (aof_kv_put_t *)payload;
    *header = (aof_kv_put_t){
        .skey = skey,
        .cas = next_cas,
        .expires_us = expires_us,
        .flags = 0,
        .value_len = value_len
    };
    if (value_len) memcpy(payload + sizeof(*header), value, value_len);

    int rc = rf_persist_apply_record(AOF_REC_KV_PUT, payload,
                                     (uint32_t)payload_size, timeout_ms, receipt);
    if (rc == 0) {
        uint8_t verify[MAX_DATA_SIZE];
        if (!shared_storage_get_fast(g_shared_storage, skey, verify, sizeof(verify))) {
            rc = -4;
        } else {
            const kvrec_t *stored = (const kvrec_t *)verify;
            if (stored->cas != next_cas || stored->flags != 0 ||
                stored->expires_us != expires_us || stored->value_len != value_len ||
                (value_len && memcmp(stored->value, value, value_len) != 0))
                rc = -4;
        }
    }
    if (new_cas) *new_cas = next_cas;
    rf_mutation_unlock(lock_key);
    return rc;
}

int rf_kv_delete_transaction(int32_t skey, int has_expected_cas,
                             uint64_t expected_cas, uint32_t timeout_ms,
                             uint64_t *new_cas, uint64_t *current_cas,
                             rf_write_receipt_t *receipt) {
    uint64_t lock_key = (uint64_t)(uint32_t)skey;
    if (rf_mutation_lock(lock_key) != 0) return -1;
    uint8_t current[MAX_DATA_SIZE];
    if (!shared_storage_get_fast(g_shared_storage, skey, current, sizeof(current)) ||
        (((const kvrec_t *)current)->flags & KV_FLAG_TOMBSTONE)) {
        rf_mutation_unlock(lock_key);
        return 2;
    }
    uint64_t observed_cas = ((const kvrec_t *)current)->cas;
    if (current_cas) *current_cas = observed_cas;
    if (has_expected_cas && expected_cas != observed_cas) {
        rf_mutation_unlock(lock_key);
        return 1;
    }

    uint64_t next_cas = observed_cas + 1;
    aof_kv_del_t record = {
        .skey = skey,
        .cas = next_cas,
        .flags = KV_FLAG_TOMBSTONE
    };
    int rc = rf_persist_apply_record(AOF_REC_KV_DEL, &record, sizeof(record),
                                     timeout_ms, receipt);
    if (rc == 0) {
        uint8_t verify[MAX_DATA_SIZE];
        if (!shared_storage_get_fast(g_shared_storage, skey, verify, sizeof(verify))) {
            rc = -4;
        } else {
            const kvrec_t *stored = (const kvrec_t *)verify;
            if (stored->cas != next_cas ||
                !(stored->flags & KV_FLAG_TOMBSTONE)) rc = -4;
        }
    }
    if (new_cas) *new_cas = next_cas;
    rf_mutation_unlock(lock_key);
    return rc;
}

int ha_fwd_store_response(const ha_fwd_write_rsp_t *response) {
    if (!g_shared_storage || !response || response->req_id == 0) return -1;
    return shared_storage_set(g_shared_storage, ha_fwd_rsp_key(response->req_id),
                              response, sizeof(*response));
}

int ha_fwd_send_checked(uint32_t type, const void *payload, uint32_t size,
                        uint32_t timeout_ms, ha_fwd_write_rsp_t *response) {
    if (!g_shared_storage || (!payload && size)) return -1;
    if (timeout_ms == 0) timeout_ms = 3000;
    size_t tot = sizeof(ha_fwd_rec_t) + size;
    void *buf = slab_alloc(tot);
    if (!buf) return -1;
    ha_fwd_rec_t *h = (ha_fwd_rec_t *) buf;
    h->req_id = ((uint64_t)(uint32_t)getpid() << 32) ^
                atomic_fetch_add(&g_ha_fwd_req_seq, 1);
    h->type = type;
    h->size = size;
    h->timeout_ms = timeout_ms;
    h->reserved = 0;
    uint64_t req_id = h->req_id;
    if (size) memcpy((uint8_t *) buf + sizeof(*h), payload, size);
    int rc = rf_bus_publish(AOF_REC_HA_FORWARD, buf, (uint32_t) tot);
    slab_free(buf);
    if (rc != 0) return rc;

    uint64_t start = now_us();
    uint64_t timeout_us = (uint64_t)timeout_ms * 1000ULL;
    for (;;) {
        ha_fwd_write_rsp_t found;
        if (shared_storage_try_get_fast(g_shared_storage, ha_fwd_rsp_key(req_id),
                                        &found, sizeof(found)) &&
            found.req_id == req_id) {
            if (response) *response = found;
            return found.rc;
        }
        if (now_us() - start >= timeout_us) return -5;
        usleep(200);
    }
}

int ha_fwd_send(uint32_t type, const void *payload, uint32_t size) {
    return ha_fwd_send_checked(type, payload, size, 3000, NULL);
}

static void *promoter_loop(void *arg){
    (void)arg;
    if (g_worker_id < 0 || (uint32_t)g_worker_id >= g_bus->worker_count) return NULL;
    uint64_t r = atomic_load_explicit(&g_bus->consumer_seq[g_worker_id], memory_order_acquire);
    tls_read_seq = r;
    for(;;){
        uint32_t idx = (uint32_t)r & g_bus->desc_mask;
        bus_desc_t *ds = &g_bus->desc[idx];

        if (atomic_load_explicit(&ds->valid, memory_order_acquire) && ds->seq == r){
            /* safe to read descriptor and payload */
            uint32_t id  = ds->rec_id;
            uint32_t sz  = ds->size;
            uint32_t off = ds->off;

            int apply_failed = 0;
            if ((uint64_t) off + sz <= g_bus->heap_size) {
                if (id == AOF_REC_HA_FORWARD) {
                    /* Only worker 0 handles HA forward → append to RAFT log, replicate */
                    if (g_worker_id == 0 && sz >= sizeof(ha_fwd_rec_t)) {
                        const ha_fwd_rec_t *f = (const ha_fwd_rec_t *) (g_bus->heap + off);
                        if (f->size == sz - sizeof(*f))
                            (void)RAMForge_HA_handle_write_forward(f, sz);
                    }
                } else if (id == AOF_REC_HA_MEMBERSHIP_FORWARD) {
                    if (g_worker_id == 0) {
                        (void)RAMForge_HA_handle_membership_forward(g_bus->heap + off, sz);
                    }
                } else if (rf_test_bus_apply_should_fail(id)) {
                    apply_failed = 1;
                } else {
                    int handled = RAMForge_HA_replay_record(id, g_bus->heap + off, sz);
                    if (handled < 0 ||
                        (handled == 0 &&
                         rf_broker_apply_aof_checked(id, g_bus->heap + off, sz) != 0))
                        apply_failed = 1;
                }
            } else {
                /* wrapped payload should never happen with reserve_heap() */
                apply_failed = 1;
            }
            if (apply_failed) {
                uint64_t expected = 0;
                (void)atomic_compare_exchange_strong_explicit(
                    &g_bus->consumer_failed_seq[g_worker_id], &expected, r + 1,
                    memory_order_release, memory_order_relaxed);
            }
            r++;
            tls_read_seq = r;
            atomic_store_explicit(&g_bus->consumer_data_tail[g_worker_id], ds->data_end,
                                  memory_order_release);
            atomic_store_explicit(&g_bus->consumer_seq[g_worker_id], r, memory_order_release);
            continue;
        }

        /* If producer lapped us, jump to latest window to avoid hot spin and mark drop */
        uint64_t head = atomic_load_explicit(&g_bus->desc_head, memory_order_acquire);
        if (head - r > (uint64_t)(g_bus->desc_mask + 1)){
            atomic_fetch_add_explicit(&g_bus->drops, head - r, memory_order_relaxed);
            uint64_t expected = 0;
            (void)atomic_compare_exchange_strong_explicit(
                &g_bus->consumer_failed_seq[g_worker_id], &expected, r + 1,
                memory_order_release, memory_order_relaxed);
            r = head;
            tls_read_seq = r;
            atomic_store_explicit(&g_bus->consumer_seq[g_worker_id], r, memory_order_release);
            atomic_store_explicit(&g_bus->consumer_data_tail[g_worker_id],
                                  atomic_load_explicit(&g_bus->data_head, memory_order_acquire),
                                  memory_order_release);
            continue;
        }

        /* tiny nap */
        struct timespec ts = {0, 200000}; /* 200µs */
        nanosleep(&ts, NULL);
    }
    return NULL;
}

void rf_bus_start_promoter(void){
    if (!g_bus) return;
    static _Atomic int started = 0;
    int expect = 0;
    if (!atomic_compare_exchange_strong(&started, &expect, 1)) return;
    init_worker_id_from_env();
    pthread_create(&g_promoter, NULL, promoter_loop, NULL);
    pthread_detach(g_promoter);
}

static inline void init_tls_aof_pool(size_t initial_size) {
    if (!tls_aof_pool.pool) {
        tls_aof_pool.pool_size = align_up_sz(initial_size, 64);
        tls_aof_pool.pool = aligned_alloc(64, tls_aof_pool.pool_size);
        atomic_store_explicit(&tls_aof_pool.pool_head, 0, memory_order_relaxed);
    }
}



// ═══════════════════════════════════════════════════════════════════════════════
// SIMD-Optimized String Operations
// ═══════════════════════════════════════════════════════════════════════════════

static inline uint64_t fnv1a64(const void *p, size_t n) {
    const uint8_t *s = (const uint8_t*)p;
    uint64_t h = 1469598103934665603ULL;
    for (size_t i = 0; i < n; i++) {
        h ^= s[i];
        h *= 1099511628211ULL;
    }
    return h;
}

static inline uint32_t simd_hash_djb2(const char *s, size_t len) {
    uint64_t h64 = fnv1a64(s, len);
    return (uint32_t) (h64 ^ (h64 >> 32));
}

// Fast string comparison using SIMD
static inline int simd_strcmp(const char *a, const char *b, size_t len) {
#if SIMD_ENABLED && defined(__AVX2__)
    if (len >= 32) {
        size_t i = 0;
        for (; i + 32 <= len; i += 32) {
            __m256i va = _mm256_loadu_si256((const __m256i*)(a + i));
            __m256i vb = _mm256_loadu_si256((const __m256i*)(b + i));
            __m256i cmp = _mm256_cmpeq_epi8(va, vb);
            uint32_t mask = _mm256_movemask_epi8(cmp);
            if (mask != 0xFFFFFFFF) return 1; // Not equal
        }
         /* compare the tail only */
        size_t rem = len - (len & ~((size_t)31));
        if (rem) return memcmp(a + (len - rem), b + (len - rem), rem);
        return 0;
    }
#endif
    return memcmp(a, b, len);
}


// ═══════════════════════════════════════════════════════════════════════════════
// Global State with Cache-Line Alignment
// ═══════════════════════════════════════════════════════════════════════════════

static rf_topic_t g_topics[RF_MAX_TOPICS] __attribute__((aligned(64)));
static _Atomic int g_topic_count = 0;
static _Atomic uint64_t g_idempotency_index_writes = 0;
static int g_default_partitions = 8; // Increased for better parallelism
static size_t g_ring_cap = 1048576;  // 1M slots, power of 2
static pthread_mutex_t g_local_topic_lock = PTHREAD_MUTEX_INITIALIZER;
static int init_partition_lockfree(rf_partition_t *p);
static rf_topic_t *find_topic_by_id_local(uint32_t id);
static int rf_topic_apply_local(const char *name, uint32_t id, int partitions);
static int rf_shared_topic_publish_replay(const aof_topic_rec_t *rec);
static rf_topic_t *rf_topic_require_by_id(uint32_t id);

// Fast topic lookup hash table
#define TOPIC_HASH_SIZE 2048
static _Atomic(rf_topic_t*) g_topic_hash[TOPIC_HASH_SIZE];

#if defined(__x86_64__) || defined(_M_X64) || defined(__i386) || defined(_M_IX86)
# include <x86intrin.h> // provides __rdtsc() and _mm_pause()
static inline uint64_t rdtsc(void) { return __rdtsc(); }
#else
static inline uint64_t rdtsc(void) {
    // Non-x86 fallback: return a steadily increasing timestamp in ns
    struct timespec ts;
    clock_gettime(CLOCK_MONOTONIC, &ts);
    return (uint64_t)ts.tv_sec * 1000000000ULL + (uint64_t)ts.tv_nsec;
}
#endif

// --- CPU relax (portable) ---
static inline void cpu_relax(void) {
#if defined(__x86_64__) || defined(_M_X64) || defined(__i386) || defined(_M_IX86)
    # if defined(__SSE2__) || defined(__AVX__) || defined(__AVX2__)
    _mm_pause();
# else
    __asm__ __volatile__("pause");
# endif
#elif defined(__aarch64__) || defined(__arm__)
    __asm__ __volatile__("yield");
#else
    sched_yield();
#endif
}



static inline uint64_t fetch_add_u64(_Atomic uint64_t *obj, uint64_t inc, memory_order mo) {
    uint64_t cur = atomic_load_explicit(obj, memory_order_relaxed);
    for (;;) {
        uint64_t next = cur + inc;
        if (atomic_compare_exchange_weak_explicit(
                obj, &cur, next, mo, memory_order_relaxed)) {
            return cur;
        }
        // On failure, 'cur' has been updated to the current value; retry.
    }
}

static inline int offset_skey(uint32_t topic_id, int partition) {
    // Combine topic_id and partition into a unique negative key
    // Negative keys won't collide with user's positive keys
    uint32_t combined = (topic_id ^ (topic_id >> 16)) ^ ((uint32_t)partition << 8);
    return -(int)(combined | 0x40000000);  // Ensure negative and non-zero
}

static inline void seed_shared_next_offset(uint32_t topic_id, int part, uint64_t want_next) {
    if (!g_shared_storage) return;

    int skey = offset_skey(topic_id, part);

    // Fetch current atomically (and usually also "creates" the counter if missing)
    uint64_t cur = shared_storage_atomic_add_u64(g_shared_storage, skey, 0);

    if (cur < want_next) {
        // Bump by the difference (may overshoot under races, which is OK; backwards is not)
        (void)shared_storage_atomic_add_u64(g_shared_storage, skey, want_next - cur);
    }
}


static inline uint64_t alloc_offset_shared(rf_topic_t *t, rf_partition_t *p, int partition) {
    if (g_shared_storage) {
        int skey = offset_skey(t->id, partition);
        uint64_t off = shared_storage_atomic_inc_u64(g_shared_storage, skey);
        uint64_t floor = atomic_load_explicit(&p->write_head, memory_order_acquire);
        uint64_t next_local = atomic_load_explicit(&p->next_offset, memory_order_acquire);
        if (next_local > floor) floor = next_local;
        if (off < floor) {
            uint64_t delta = floor - off;
            uint64_t prev = shared_storage_atomic_add_u64(g_shared_storage, skey, delta);
            off = prev + delta;
            if (off < floor) off = floor;
        }
        return off;
    }
    // Fallback for single-process mode without shared storage
    return fetch_add_u64(&p->next_offset, 1, memory_order_acq_rel);
}
static inline uint64_t alloc_offsets_shared(rf_topic_t *t, rf_partition_t *p, int partition, uint64_t count) {
    if (g_shared_storage) {
        int skey = offset_skey(t->id, partition);
        return shared_storage_atomic_add_u64(g_shared_storage, skey, count);
    }
    return fetch_add_u64(&p->next_offset, count, memory_order_acq_rel);
}

static inline uint64_t idemp_fp64(const char *topic, int part, const void *key, size_t key_len) {
    uint64_t h = fnv1a64(topic, strlen(topic));
    h = fnv1a64(&part, sizeof(part)) ^ (h * 16777619ULL);
    h = fnv1a64(key, key_len) ^ (h * 16777619ULL);
    return h;
}

static inline int idemp_hash32_fp(const char *topic, int part, uint64_t fp64) {
    char tmp[RF_MAX_TOPIC_LEN + 16];
    int n = snprintf(tmp, sizeof tmp, "__idemp|%s|%d|", topic, part);
    uint32_t h = simd_hash_djb2(tmp, n);
    uint32_t m = (uint32_t)(fp64 ^ (fp64 >> 32)); // fold 64→32
    h ^= m;
    return (int)(h ? h : 1);
}

static int idemp_get(const char *topic, int part, const void *unused_key, size_t unused_len, uint64_t fp64_in, uint64_t *out_offset) {
    (void)unused_key;
    (void)unused_len;
    if (!g_shared_storage) return 0;
    int skey = idemp_hash32_fp(topic, part, fp64_in);
    aof_idemp_val_t v;
    if (!shared_storage_get_fast(g_shared_storage, skey, &v, sizeof v)) return 0;
    if (v.fp64 != fp64_in) return 0; // guard against 32-bit collision
    if (out_offset) *out_offset = v.offset;
    return 1;
}

static int idemp_put(const char *topic, int part, const void *unused_key, size_t unused_len, uint64_t fp64_in, uint64_t offset) {
    (void)unused_key;
    (void)unused_len;
    if (!g_shared_storage) return 0;
    int skey = idemp_hash32_fp(topic, part, fp64_in);
    aof_idemp_val_t v = { .fp64 = fp64_in, .offset = offset };
    int rc = shared_storage_set(g_shared_storage, skey, &v, sizeof v);
    if (rc == 0)
        atomic_fetch_add_explicit(&g_idempotency_index_writes, 1, memory_order_relaxed);
    return rc == 0;
}

static int digest_u32(EVP_MD_CTX *ctx, uint32_t value) {
    uint8_t b[4] = {
        (uint8_t)(value >> 24), (uint8_t)(value >> 16),
        (uint8_t)(value >> 8), (uint8_t)value
    };
    return EVP_DigestUpdate(ctx, b, sizeof(b)) == 1 ? 0 : -1;
}

static int digest_u64(EVP_MD_CTX *ctx, uint64_t value) {
    uint8_t b[8];
    for (int i = 7; i >= 0; i--) {
        b[i] = (uint8_t)value;
        value >>= 8;
    }
    return EVP_DigestUpdate(ctx, b, sizeof(b)) == 1 ? 0 : -1;
}

static int idemp_key_digest(const char *topic, int part,
                            const void *key, size_t key_len, uint8_t out[32]) {
    static const char domain[] = "AYDER-IDEMP-KEY-v2";
    EVP_MD_CTX *ctx = EVP_MD_CTX_new();
    unsigned int out_len = 0;
    if (!ctx) return -1;
    int ok = EVP_DigestInit_ex(ctx, EVP_sha256(), NULL) == 1 &&
             EVP_DigestUpdate(ctx, domain, sizeof(domain) - 1) == 1 &&
             digest_u64(ctx, strlen(topic)) == 0 &&
             EVP_DigestUpdate(ctx, topic, strlen(topic)) == 1 &&
             digest_u32(ctx, (uint32_t)part) == 0 &&
             digest_u64(ctx, key_len) == 0 &&
             EVP_DigestUpdate(ctx, key, key_len) == 1 &&
             EVP_DigestFinal_ex(ctx, out, &out_len) == 1 && out_len == 32;
    EVP_MD_CTX_free(ctx);
    return ok ? 0 : -1;
}

static int idemp_payload_digest(const char *topic, int part,
                                const rf_msg_view_t *messages, size_t count,
                                uint8_t out[32]) {
    static const char domain[] = "AYDER-IDEMP-PAYLOAD-v2";
    EVP_MD_CTX *ctx = EVP_MD_CTX_new();
    unsigned int out_len = 0;
    if (!ctx) return -1;
    int ok = EVP_DigestInit_ex(ctx, EVP_sha256(), NULL) == 1 &&
             EVP_DigestUpdate(ctx, domain, sizeof(domain) - 1) == 1 &&
             digest_u64(ctx, strlen(topic)) == 0 &&
             EVP_DigestUpdate(ctx, topic, strlen(topic)) == 1 &&
             digest_u32(ctx, (uint32_t)part) == 0 &&
             digest_u64(ctx, count) == 0;
    for (size_t i = 0; ok && i < count; i++) {
        ok = digest_u64(ctx, messages[i].key_len) == 0 &&
             (!messages[i].key_len ||
              EVP_DigestUpdate(ctx, messages[i].key, messages[i].key_len) == 1) &&
             digest_u64(ctx, messages[i].val_len) == 0 &&
             (!messages[i].val_len ||
              EVP_DigestUpdate(ctx, messages[i].val, messages[i].val_len) == 1);
    }
    ok = ok && EVP_DigestFinal_ex(ctx, out, &out_len) == 1 && out_len == 32;
    EVP_MD_CTX_free(ctx);
    return ok ? 0 : -1;
}

static int idemp_state_key(const uint8_t digest[32]) {
    uint32_t folded = simd_hash_djb2((const char *)digest, 32);
    uint32_t key = (folded & 0x1fffffffu) | 0x20000000u;
    return -(int)key;
}

static int idemp_state_load(const uint8_t key_digest[32], uint32_t topic_id,
                            int part, rf_idemp_state_t *out) {
    if (!g_shared_storage || !out) return 0;
    memset(out, 0, sizeof(*out));
    if (!shared_storage_get_fast(g_shared_storage, idemp_state_key(key_digest),
                                 out, sizeof(*out))) return 0;
    if (out->magic != RF_IDEMP_V2_MAGIC || out->version != RF_IDEMP_V2_VERSION ||
        out->topic_id != topic_id || out->partition != (uint16_t)part ||
        memcmp(out->key_digest, key_digest, 32) != 0) return -1;
    return 1;
}

static int idemp_state_store(const rf_idemp_state_t *state) {
    if (!g_shared_storage || !state) return -1;
    const char *fail_committed = getenv("RF_TEST_IDEMP_FAIL_COMMITTED_STORE");
    if (rf_test_faults_enabled() && fail_committed &&
        strcmp(fail_committed, "1") == 0 &&
        state->status == RF_IDEMP_STATE_COMMITTED &&
        state->applied_all_workers) {
        errno = EIO;
        return -1;
    }
    int rc = shared_storage_set(g_shared_storage,
                                idemp_state_key(state->key_digest),
                                state, sizeof(*state));
    if (rc == 0 && state->status == RF_IDEMP_STATE_COMMITTED)
        atomic_fetch_add_explicit(&g_idempotency_index_writes, 1,
                                  memory_order_relaxed);
    return rc;
}

static void receipt_from_idemp_state(const rf_idemp_state_t *state,
                                     rf_write_receipt_t *receipt) {
    memset(receipt, 0, sizeof(*receipt));
    receipt->batch_id = state->batch_id;
    receipt->raft_index = state->raft_index;
    receipt->local_fsync = state->local_fsync != 0;
    receipt->quorum_committed = state->quorum_committed != 0;
    receipt->failover_safe = state->quorum_committed != 0;
    receipt->applied_all_workers = state->applied_all_workers != 0;
}

// ═══════════════════════════════════════════════════════════════════════════════
// Lock-Free Topic Management
// ═══════════════════════════════════════════════════════════════════════════════

uint32_t rf_topic_id(const char *name) {
    return simd_hash_djb2(name, strlen(name));
}

static rf_topic_t* find_topic_lockfree(const char *name, uint32_t id) {
    size_t nlen = strlen(name);
    uint32_t hash_idx = id & (TOPIC_HASH_SIZE - 1);

    for (int probe = 0; probe < 8; probe++) {
        uint32_t idx = (hash_idx + probe) & (TOPIC_HASH_SIZE - 1);
        rf_topic_t *t = atomic_load_explicit(&g_topic_hash[idx], memory_order_acquire);
        if (t && t->id == id) {
            size_t tlen = strnlen(t->name, RF_MAX_TOPIC_LEN);
            if (tlen == nlen && simd_strcmp(t->name, name, nlen) == 0) return t;
        }
        if (!t) break;
    }
    return NULL;
}


static inline void atomic_max_u64(_Atomic uint64_t *dst, uint64_t val) {
    uint64_t cur = atomic_load_explicit(dst, memory_order_acquire);
    while (cur < val &&
           !atomic_compare_exchange_weak_explicit(dst, &cur, val,
                                                  memory_order_acq_rel, memory_order_acquire)) {
        /* keep trying */
    }
}

static inline void rf_advance_write_head_after_publish_relaxed(rf_partition_t *p, uint64_t offset) {
    uint64_t spins = 0;
    uint64_t start = now_us();
    for (;;) {
        uint64_t wh = atomic_load_explicit(&p->write_head, memory_order_acquire);
        if (wh > offset) return;
        if (wh == offset) {
            uint64_t expect = offset;
            if (atomic_compare_exchange_weak_explicit(&p->write_head, &expect, offset + 1,
                                                      memory_order_acq_rel, memory_order_acquire)) {
                return;
            }
            continue;
        }
        /* fail-open after 500ms to avoid wedging producers behind a permanently missing offset */
        if ((now_us() - start) > 500000ULL) {
            atomic_max_u64(&p->write_head, offset + 1);
            return;
        }
        if ((++spins & 0x3FFu) == 0) sched_yield();
        else cpu_relax();
    }
}

/*
 * Strict publication path for successful appends:
 * do not skip missing predecessor offsets, so successful appends are observed
 * in contiguous offset order.
 */
static inline void rf_advance_write_head_after_publish_strict(rf_partition_t *p, uint64_t offset) {
    uint64_t spins = 0;
    for (;;) {
        uint64_t wh = atomic_load_explicit(&p->write_head, memory_order_acquire);
        if (wh > offset) return;
        if (wh == offset) {
            uint64_t expect = offset;
            if (atomic_compare_exchange_weak_explicit(&p->write_head, &expect, offset + 1,
                                                      memory_order_acq_rel, memory_order_acquire)) {
                return;
            }
            continue;
        }
        if ((++spins & 0x3FFu) == 0) sched_yield();
        else cpu_relax();
    }
}

static int init_partition_lockfree(rf_partition_t *p) {
    /* Ensure capacity is power of 2 */
    size_t cap = 1;
    while (cap < g_ring_cap) cap <<= 1;
    p->capacity = cap;
    p->capacity_mask = cap - 1;

    atomic_store_explicit(&p->ring, NULL, memory_order_relaxed);
    /* Initialize atomic variables */
    atomic_store_explicit(&p->write_head, 0, memory_order_relaxed);
    atomic_store_explicit(&p->read_tail, 0, memory_order_relaxed);
    atomic_store_explicit(&p->next_offset, 0, memory_order_relaxed);
    atomic_store_explicit(&p->subscribers, NULL, memory_order_relaxed);
    atomic_store_explicit(&p->sub_epoch, 0, memory_order_relaxed);
    atomic_store_explicit(&p->ttl_window_us, 0, memory_order_relaxed);
    atomic_store_explicit(&p->ttl_ms, 0, memory_order_relaxed);
    atomic_store_explicit(&p->last_ttl_gc_us, 0, memory_order_relaxed);
    atomic_store_explicit(&p->retain_max_bytes, 0, memory_order_relaxed);
    atomic_store_explicit(&p->live_bytes, 0, memory_order_relaxed);
    atomic_store_explicit(&p->last_size_gc_us, 0, memory_order_relaxed);
    atomic_store_explicit(&p->size_gc_cursor, 0, memory_order_relaxed);
    atomic_store_explicit(&p->delete_floor, 0, memory_order_relaxed);

    return 0;
}


int rf_broker_init(size_t ring_per_partition, int default_partitions) {
    // Round up to next power of 2
    if (ring_per_partition > 0) {
        size_t cap = 1;
        while (cap < ring_per_partition) cap <<= 1;
        g_ring_cap = cap;
    } else {
        g_ring_cap = 1 << 22;  // DEFAULT TO 4M, not 1M
    }
    g_default_partitions = (default_partitions > 0) ? default_partitions : g_default_partitions;

    atomic_store_explicit(&g_topic_count, 0, memory_order_relaxed);

    // Initialize hash table
    for (int i = 0; i < TOPIC_HASH_SIZE; i++) {
        atomic_store_explicit(&g_topic_hash[i], NULL, memory_order_relaxed);
    }
    return 0;
}




/* bounded GC from the oldest offsets until live_bytes <= retain_max_bytes */


int rf_set_ttl_ms(const char *topic, int part, uint64_t ttl_ms) {
    rf_topic_t *t = find_topic_lockfree(topic, rf_topic_id(topic));
    if (!t) return -1;

    int p0 = 0, p1 = t->partitions;
    if (part >= 0 && part < t->partitions) { p0 = part; p1 = part + 1; }

    uint64_t now = ttl_now_wall_us();

    for (int i = p0; i < p1; i++) {
        rf_partition_t *rp = &t->parts[i];
        /* set both: fixed TTL (other paths) and rolling window (read path) */
        rf_ttl_apply_to_partition(rp, ttl_ms);

        /* opportunistic sweep of a window (optional) */
        rf_msg_slot_t *ring = atomic_load_explicit(&rp->ring, memory_order_acquire);
        if (!ring) continue;
        uint64_t wh  = atomic_load_explicit(&rp->write_head, memory_order_acquire);

        uint64_t cap = rp->capacity;
        uint64_t start = (wh > cap) ? (wh - cap) : 0;

        for (uint64_t o = start; o < wh; o++) {
            rf_msg_slot_t *s = &ring[o & rp->capacity_mask];
            uint16_t f = atomic_load_explicit(&s->flags, memory_order_acquire);
            if (!(f & 1)) continue;

            uint64_t ts = atomic_load_explicit(&s->ts_us, memory_order_acquire);
            if (ttl_ms && (ts + ttl_ms*1000ULL) < now) {
                rf_invalidate_slot(rp, s);
            }
        }
    }
    return 0;
}

int rf_set_ttl_ms_all(uint64_t ttl_ms) {
    int n = atomic_load_explicit(&g_topic_count, memory_order_acquire);
    for (int i = 0; i < n; i++) {
        rf_topic_t *t = &g_topics[i];
        rf_set_ttl_ms(t->name, -1, ttl_ms);
    }
    return 0;
}

static inline void rf_ttl_sweep_if_due(rf_partition_t *p, uint64_t now_mono, uint64_t cutoff_wall) {
    if (!cutoff_wall) return;
    uint64_t last = atomic_load_explicit(&p->last_ttl_gc_us, memory_order_acquire);
    if (now_mono - last < 2000) return; /* throttle ~2ms; adjust as you like */
    if (!atomic_compare_exchange_strong(&p->last_ttl_gc_us, &last, now_mono)) return;

    /* scan a small tail window (e.g., 64 slots) behind write_head */
    const uint64_t wh = atomic_load_explicit(&p->write_head, memory_order_acquire);
    const uint64_t sweep = 64;
    const uint64_t start = (wh > sweep) ? (wh - sweep) : 0;

    for (uint64_t o = start; o < wh; o++) {
        rf_msg_slot_t *s = &p->ring[o & p->capacity_mask];
        uint16_t f = atomic_load_explicit(&s->flags, memory_order_acquire);
        if (!(f & 1)) continue; /* not valid */
        uint64_t ts = atomic_load_explicit(&s->ts_us, memory_order_acquire);
        if (ts && ts < cutoff_wall) rf_invalidate_slot(p, s);
    }
}

static void rf_size_apply_to_partition(rf_partition_t *p, uint64_t maxb) {
    atomic_store_explicit(&p->retain_max_bytes, maxb, memory_order_release);
    rf_size_sweep_if_over(p, now_us());
}

int rf_set_max_bytes(const char *topic, int part, uint64_t max_bytes) {
    rf_topic_t *t = find_topic_lockfree(topic, rf_topic_id(topic));
    if (!t) return -1;
    int p0 = 0, p1 = t->partitions;
    if (part >= 0 && part < t->partitions) { p0 = part; p1 = part + 1; }
    for (int i = p0; i < p1; i++) rf_size_apply_to_partition(&t->parts[i], max_bytes);
    return 0;
}

int rf_set_max_bytes_all(uint64_t max_bytes) {
    int n = atomic_load_explicit(&g_topic_count, memory_order_acquire);
    for (int i = 0; i < n; i++) {
        rf_topic_t *t = &g_topics[i];
        for (int p = 0; p < t->partitions; p++)
            rf_size_apply_to_partition(&t->parts[p], max_bytes);
    }
    return 0;
}

static inline int rf_msg_is_expired(rf_partition_t *p, rf_msg_slot_t *slot) {
    uint64_t ttl = atomic_load_explicit(&p->ttl_ms, memory_order_acquire);
    if (!ttl) return 0;
    uint64_t ts  = atomic_load_explicit(&slot->ts_us, memory_order_acquire);
    uint64_t now = ttl_now_wall_us();
    /* expire if message age exceeds TTL */
    return (ts + ttl * 1000ULL) < now;
}
int rf_topic_ensure(const char *name, int partitions) {
    if (!name || !*name || strnlen(name, RF_MAX_TOPIC_LEN) >= RF_MAX_TOPIC_LEN) return -1;
    uint32_t id = rf_topic_id(name);
    rf_topic_t *t = find_topic_lockfree(name, id);
    int requested_parts = partitions > 0 ? partitions : g_default_partitions;
    if (requested_parts <= 0 || requested_parts > RF_MAX_PARTITIONS) return -1;
    if (t) return (partitions <= 0 || t->partitions == requested_parts) ? 0 : -5;
    if (!g_bus) return -6;

    uint64_t start = now_us();
    rf_shared_topic_t *reserved = NULL;
    for (;;) {
        if (rf_lock_shared(&g_bus->topic_lock) != 0) return -6;
        rf_shared_topic_t *empty = NULL;
        rf_shared_topic_t *found = NULL;
        int collision = 0;
        for (uint32_t i = 0; i < RF_MAX_TOPICS; i++) {
            rf_shared_topic_t *candidate = &g_bus->topics[i];
            uint32_t state = atomic_load_explicit(&candidate->ready, memory_order_acquire);
            if (state == 0) {
                if (!empty) empty = candidate;
                continue;
            }
            if (candidate->id != id) continue;
            if (strncmp(candidate->name, name, RF_MAX_TOPIC_LEN) != 0) {
                collision = 1;
                break;
            }
            found = candidate;
            break;
        }
        if (collision) {
            pthread_mutex_unlock(&g_bus->topic_lock);
            return -5;
        }
        if (found) {
            uint32_t state = atomic_load_explicit(&found->ready, memory_order_acquire);
            int canonical_parts = found->partitions;
            pthread_mutex_unlock(&g_bus->topic_lock);
            if (state == RF_SHARED_TOPIC_READY) {
                if (partitions > 0 && canonical_parts != requested_parts) return -5;
                return rf_topic_apply_local(name, id, canonical_parts);
            }
            if ((now_us() - start) >= 5000000ULL) return -6;
            struct timespec ts = {0, 200000};
            nanosleep(&ts, NULL);
            continue;
        }
        if (!empty) {
            pthread_mutex_unlock(&g_bus->topic_lock);
            return -2;
        }
        empty->id = id;
        empty->partitions = (uint16_t)requested_parts;
        empty->reserved = 0;
        memset(empty->name, 0, sizeof(empty->name));
        memcpy(empty->name, name, strlen(name));
        atomic_store_explicit(&empty->ready, RF_SHARED_TOPIC_PENDING, memory_order_release);
        reserved = empty;
        pthread_mutex_unlock(&g_bus->topic_lock);
        break;
    }

    aof_topic_rec_t rec;
    memset(&rec, 0, sizeof(rec));
    rec.id = id;
    rec.parts = (uint16_t)requested_parts;
    memcpy(rec.name, name, strlen(name));

    rf_write_receipt_t receipt;
    int persist_rc = rf_persist_apply_record(AOF_REC_BROKER_TOPIC, &rec,
                                             (uint32_t)sizeof(rec), 5000,
                                             &receipt);
    int keep_committed = persist_rc == 0 || receipt.local_fsync ||
                         receipt.quorum_committed;

    if (rf_lock_shared(&g_bus->topic_lock) == 0) {
        if (keep_committed) {
            atomic_store_explicit(&reserved->ready, RF_SHARED_TOPIC_READY, memory_order_release);
            atomic_fetch_add_explicit(&g_bus->topic_count, 1, memory_order_relaxed);
        } else {
            memset(reserved->name, 0, sizeof(reserved->name));
            reserved->id = 0;
            reserved->partitions = 0;
            atomic_store_explicit(&reserved->ready, 0, memory_order_release);
        }
        pthread_mutex_unlock(&g_bus->topic_lock);
    } else {
        return -6;
    }
    return persist_rc == 0 ? 0 : -6;
}

int rf_delete_before_checked(const char *topic, int partition,
                             uint64_t before_offset, uint32_t timeout_ms,
                             uint64_t *deleted_count, uint64_t *freed_bytes,
                             rf_write_receipt_t *receipt)
{
    rf_write_receipt_t result;
    memset(&result, 0, sizeof(result));
    if (deleted_count) *deleted_count = 0;
    if (freed_bytes) *freed_bytes = 0;
    if (receipt) *receipt = result;
    if (!topic) return -1;

    rf_topic_t *t = find_topic_lockfree(topic, rf_topic_id(topic));
    if (!t || partition < 0 || partition >= t->partitions) return -2;

    rf_partition_t *p = &t->parts[partition];
    uint64_t transaction_key = ((uint64_t)t->id << 32) ^
                               (uint64_t)(uint32_t)partition;
    if (rf_mutation_lock(transaction_key) != 0) return -4;
    rf_msg_slot_t *ring = atomic_load_explicit(&p->ring, memory_order_acquire);
    uint64_t wh = atomic_load_explicit(&p->write_head, memory_order_acquire);

    /* Efficient bounds: only the live window matters */
    const uint64_t oldest = (wh > p->capacity) ? (wh - p->capacity) : 0;
    const uint64_t limit  = (before_offset < wh) ? before_offset : wh;

    uint64_t deleted = 0, freed = 0;
    if (ring && limit > oldest) {
        for (uint64_t o = oldest; o < limit; o++) {
            rf_msg_slot_t *s = &ring[o & p->capacity_mask];

            uint16_t f = atomic_load_explicit(&s->flags, memory_order_acquire);
            if (!(f & 1)) continue;

            uint64_t so = atomic_load_explicit(&s->offset, memory_order_acquire);
            if (so != o) continue; /* wrong logical occupant */

            freed += slot_payload_bytes(s);
            deleted++;
        }
    }

    aof_truncate_rec_t rec = {
            .topic_id = t->id,
            .partition = (uint16_t)partition,
            .reserved = 0,
            .before_offset = before_offset
    };
    int rc = rf_persist_apply_record(AOF_REC_BROKER_TRUNCATE, &rec,
                                     (uint32_t)sizeof(rec), timeout_ms, &result);
    rf_mutation_unlock(transaction_key);
    if (receipt) *receipt = result;
    if (rc != 0) return rc;

    if (deleted_count) *deleted_count = deleted;
    if (freed_bytes)   *freed_bytes   = freed;
    return 0;
}

int rf_set_retention_checked(const char *topic, int partition,
                             int has_ttl, uint64_t ttl_ms,
                             int has_max_bytes, uint64_t max_bytes,
                             uint32_t timeout_ms,
                             rf_write_receipt_t *receipt) {
    rf_write_receipt_t result;
    memset(&result, 0, sizeof(result));
    if (receipt) *receipt = result;
    if (!has_ttl && !has_max_bytes) return -1;

    int global = !topic || !*topic || strcmp(topic, "*") == 0;
    rf_topic_t *resolved = NULL;
    if (!global) {
        if (strnlen(topic, RF_MAX_TOPIC_LEN) >= RF_MAX_TOPIC_LEN) return -1;
        resolved = rf_topic_require_by_id(rf_topic_id(topic));
        if (!resolved || strcmp(resolved->name, topic) != 0 || partition < -1 ||
            partition >= resolved->partitions) return -2;
    } else if (partition != -1) {
        return -1;
    }

    aof_ttl_rec_t record;
    memset(&record, 0, sizeof(record));
    record.partition = global ? -1 : partition;
    record.ttl_ms = ttl_ms;
    record.max_bytes = max_bytes;
    record.present_flags = (has_ttl ? RF_RETENTION_HAS_TTL : 0) |
                           (has_max_bytes ? RF_RETENTION_HAS_MAX_BYTES : 0);
    if (global) {
        record.topic[0] = '*';
    } else {
        memcpy(record.topic, topic, strlen(topic) + 1);
    }

    uint64_t lock_key = UINT64_C(0x5254454e54494f4e); /* "RTENTION" */
    if (rf_mutation_lock(lock_key) != 0) return -4;
    int rc = rf_persist_apply_record(AOF_REC_BROKER_TTL, &record,
                                     (uint32_t)sizeof(record), timeout_ms,
                                     &result);
    rf_mutation_unlock(lock_key);
    if (receipt) *receipt = result;
    return rc;
}

int rf_delete_before(const char *topic, int partition, uint64_t before_offset,
                     uint64_t *deleted_count, uint64_t *freed_bytes) {
    return rf_delete_before_checked(topic, partition, before_offset, 0,
                                    deleted_count, freed_bytes, NULL);
}

// ═══════════════════════════════════════════════════════════════════════════════
// Zero-Copy Message Production with Lock-Free Ring Buffer
// ═══════════════════════════════════════════════════════════════════════════════

static inline int replay_one_msg_rec(const aof_msg_rec_t *r, const uint8_t *kv) {
    rf_topic_t *t = rf_topic_require_by_id(r->topic_id);
    if (!t || r->partition >= t->partitions) return -1;
    rf_partition_t *p = &t->parts[r->partition];

    if (rf_partition_ensure_ring(p) != 0) return -1;
    rf_msg_slot_t *ring = atomic_load_explicit(&p->ring, memory_order_acquire);
    if (!ring) return -1;

    uint64_t floor = atomic_load_explicit(&p->delete_floor, memory_order_acquire);
    if (r->offset < floor) return 0;  // honor truncate floor

    size_t total_size = r->key_len + r->val_len;
    uint64_t slot_idx = (r->offset) & p->capacity_mask;
    rf_msg_slot_t *slot = &ring[slot_idx];

    if (rf_slot_ready_for_offset(slot, r->offset)) return 0;
    void *large_blob = NULL;
    if (total_size > sizeof(slot->inline_data)) {
        large_blob = slab_alloc(total_size);
        if (!large_blob) return -1;
        memcpy(large_blob, kv, total_size);
    }
    uint64_t want = r->offset + 1;
    seed_shared_next_offset(r->topic_id, r->partition, want);
    atomic_max_u64(&p->next_offset, want);
    atomic_max_u64(&p->write_head, want);
    rf_slot_evict_if_needed(p, slot, r->offset);

    if (total_size <= sizeof(slot->inline_data)) {
        memcpy(slot->inline_data, kv, total_size);
        slot->blob = slot->inline_data;
    } else {
        slot->blob = large_blob;
    }

    /* Defensive: normalize ts so TTL won't purge immediately if it's 0 */
    uint64_t now_wall = ttl_now_wall_us();
    uint64_t ts_norm = normalize_persisted_ts_us(r->ts_us, r->reserved, now_wall);
    atomic_store_explicit(&slot->offset, r->offset, memory_order_release);
    atomic_store_explicit(&slot->ts_us, ts_norm, memory_order_release);
    atomic_store_explicit(&slot->key_len, r->key_len, memory_order_release);
    atomic_store_explicit(&slot->val_len, r->val_len, memory_order_release);
    atomic_store_explicit(&slot->partition, r->partition, memory_order_release);
    atomic_store_explicit(&slot->flags, (total_size <= sizeof(slot->inline_data)) ? 1 : 3, memory_order_release);

    /* size accounting on replay */
    atomic_fetch_add_explicit(&p->live_bytes, (uint64_t)total_size, memory_order_relaxed);
    return 0;
}

// ═══════════════════════════════════════════════════════════════════════════
// SEALED SINGLE MESSAGE PRODUCE - ZERO-LOSS GUARANTEE
// ═══════════════════════════════════════════════════════════════════════════



// ═══════════════════════════════════════════════════════════════════════════════
// Lock-Free High-Performance Consumer
// ═══════════════════════════════════════════════════════════════════════════════

size_t rf_consume(const char *topic, const char *group, int partition,
                  uint64_t from_exclusive, size_t max_msgs,
                  rf_msg_view_t *out, size_t out_cap, uint64_t *next_offset_out) {
    (void)group;
    rf_topic_t *t = find_topic_lockfree(topic, rf_topic_id(topic));
    if (!t || partition < 0 || partition >= t->partitions) return 0;
    rf_partition_t *p = &t->parts[partition];

    rf_msg_slot_t *ring = atomic_load_explicit(&p->ring, memory_order_acquire);
    if (!ring) {
        /* If nobody ever touched this partition in this worker, there is no ring yet */
        if (rf_partition_ensure_ring(p) != 0) return 0;
        ring = atomic_load_explicit(&p->ring, memory_order_acquire);
        if (!ring) return 0;
    }

    /* Bound reads to offsets that are currently published. */
    uint64_t visible_tail = atomic_load_explicit(&p->write_head, memory_order_acquire);
    uint64_t current_offset = from_exclusive + 1;
    /* live window lower bound due to capacity */
    uint64_t min_available = (visible_tail > p->capacity) ? (visible_tail - p->capacity) : 0;
    if (current_offset < min_available) current_offset = min_available;

    /* NEW: skip any offsets below the delete floor */
    uint64_t floor = atomic_load_explicit(&p->delete_floor, memory_order_acquire);
    if (current_offset < floor) current_offset = floor;

    uint64_t avail = (visible_tail > current_offset) ? (visible_tail - current_offset) : 0;
    size_t cap = (out_cap < max_msgs ? out_cap : max_msgs);

    /* TTL cutoff (rolling): expire if ts_us < now - window */
    uint64_t win_us = atomic_load_explicit(&p->ttl_window_us, memory_order_acquire);
    uint64_t now_wall = ttl_now_wall_us();
    uint64_t cutoff = win_us ? (now_wall - win_us) : 0;

    /* bounded, best-effort ring cleanup */
    rf_ttl_sweep_if_due(p, now_us(), cutoff);

    size_t produced = 0;
    uint64_t scanned = 0;

    while (produced < cap && scanned < avail) {
        uint64_t wh = atomic_load_explicit(&p->write_head, memory_order_acquire);
        uint64_t oldest = (wh > p->capacity) ? (wh - p->capacity) : 0;
        uint64_t slot_idx = current_offset & p->capacity_mask;
        rf_msg_slot_t *slot = &ring[slot_idx];

        uint16_t flags = atomic_load_explicit(&slot->flags, memory_order_acquire);
        uint64_t so    = atomic_load_explicit(&slot->offset, memory_order_acquire);

        if ((flags & RF_SLOT_FLAG_TOMBSTONE) && so == current_offset) {
            current_offset++;
            scanned++;
            continue;
        }

        if ((flags & RF_SLOT_FLAG_VALID) && so == current_offset) {
            if (so < oldest) {
                current_offset++;
                scanned++;
                continue;
            }
            if (cutoff) {
                uint64_t ts = atomic_load_explicit(&slot->ts_us, memory_order_acquire);
                if (ts && ts < cutoff) {
                    /* expired -> invalidate and skip without producing (also updates live_bytes) */
                    rf_invalidate_slot(p, slot);
                    current_offset++; scanned++;
                    continue;
                }
            }

            uint32_t klen = atomic_load_explicit(&slot->key_len, memory_order_acquire);
            uint32_t vlen = atomic_load_explicit(&slot->val_len, memory_order_acquire);

            out[produced] = (rf_msg_view_t){
                    .key       = slot->blob,
                    .key_len   = klen,
                    .val       = (uint8_t*)slot->blob + klen,
                    .val_len   = vlen,
                    .offset    = so,
                    .ts_us     = atomic_load_explicit(&slot->ts_us, memory_order_acquire),
                    .partition = partition
            };
            produced++;
            current_offset++;
            scanned++;
        } else {
            if (current_offset < oldest || so == UINT64_MAX) {
                current_offset++;
                scanned++;
                continue;
            }
            /* Preserve ordering: stop at unpublished/future offsets. */
            break;
        }
    }

    if (next_offset_out) *next_offset_out = current_offset;
    return produced;
}

// ═══════════════════════════════════════════════════════════════════════════════
// Optimized Offset Management
// ═══════════════════════════════════════════════════════════════════════════════

static int offset_key_hash(const char *topic, const char *group, int part) {
    char tmp[256];
    int n = snprintf(tmp, sizeof(tmp), "__cg|%s|%s|%d", topic, group ?: "", part);
    uint32_t h = simd_hash_djb2(tmp, n);
    return h ? (int)h : 1; // Avoid 0
}

int rf_offset_load(const char *topic, const char *group, int partition, uint64_t *out) {
    if (!g_shared_storage) {
        *out = 0;
        return 0;
    }
    int k = offset_key_hash(topic, group, partition);
    uint64_t val = 0;
    return shared_storage_get_fast(g_shared_storage, k, &val, sizeof(val)) ? (*out = val, 1) : 0;
}

int rf_offset_store(const char *topic, const char *group, int partition, uint64_t off) {
    if (!g_shared_storage) return -1;
    int k = offset_key_hash(topic, group, partition);
    return shared_storage_atomic_max_u64(g_shared_storage, k, off, NULL);
    return 0;
}

static rf_topic_t *find_topic_by_id_local(uint32_t id) {
    int n = atomic_load_explicit(&g_topic_count, memory_order_acquire);
    for (int i = 0; i < n; i++) {
        if (g_topics[i].id == id) return &g_topics[i];
    }
    return NULL;
}

static int rf_topic_apply_local(const char *name, uint32_t id, int partitions) {
    if (!name || !*name || strnlen(name, RF_MAX_TOPIC_LEN) >= RF_MAX_TOPIC_LEN) return -1;
    if (partitions <= 0 || partitions > RF_MAX_PARTITIONS) return -1;

    pthread_mutex_lock(&g_local_topic_lock);
    rf_topic_t *existing = find_topic_lockfree(name, id);
    if (existing) {
        int rc = existing->partitions == partitions ? 0 : -5;
        pthread_mutex_unlock(&g_local_topic_lock);
        return rc;
    }
    rf_topic_t *same_id = find_topic_by_id_local(id);
    if (same_id) {
        pthread_mutex_unlock(&g_local_topic_lock);
        return -5;
    }

    int idx = atomic_load_explicit(&g_topic_count, memory_order_relaxed);
    if (idx >= RF_MAX_TOPICS) {
        pthread_mutex_unlock(&g_local_topic_lock);
        return -2;
    }

    rf_topic_t *slot = &g_topics[idx];
    memset(slot, 0, sizeof(*slot));
    memcpy(slot->name, name, strlen(name) + 1);
    slot->id = id;
    slot->hash = id;
    slot->partitions = partitions;

    size_t parts_size = (size_t)partitions * sizeof(rf_partition_t);
    slot->parts = aligned_alloc(64, parts_size);
    if (!slot->parts) {
        pthread_mutex_unlock(&g_local_topic_lock);
        return -3;
    }
    memset(slot->parts, 0, parts_size);
    for (int i = 0; i < partitions; i++) {
        if (init_partition_lockfree(&slot->parts[i]) != 0) {
            free(slot->parts);
            slot->parts = NULL;
            pthread_mutex_unlock(&g_local_topic_lock);
            return -4;
        }
    }

    uint32_t hash_idx = id & (TOPIC_HASH_SIZE - 1);
    int inserted = 0;
    for (int probe = 0; probe < 8; probe++) {
        uint32_t idx_hash = (hash_idx + (uint32_t)probe) & (TOPIC_HASH_SIZE - 1);
        rf_topic_t *expected = NULL;
        if (atomic_compare_exchange_strong_explicit(
                &g_topic_hash[idx_hash], &expected, slot,
                memory_order_release, memory_order_relaxed)) {
            inserted = 1;
            break;
        }
    }
    if (!inserted) {
        free(slot->parts);
        slot->parts = NULL;
        memset(slot, 0, sizeof(*slot));
        pthread_mutex_unlock(&g_local_topic_lock);
        return -5;
    }

    atomic_store_explicit(&g_topic_count, idx + 1, memory_order_release);
    pthread_mutex_unlock(&g_local_topic_lock);
    return 0;
}

static int rf_shared_topic_find(uint32_t id, const char *name,
                                aof_topic_rec_t *out, uint32_t *state_out) {
    if (!g_bus) return 0;
    for (uint32_t i = 0; i < RF_MAX_TOPICS; i++) {
        uint32_t state = atomic_load_explicit(&g_bus->topics[i].ready, memory_order_acquire);
        if (state == 0) continue;
        rf_shared_topic_t *shared = &g_bus->topics[i];
        if (shared->id != id) continue;
        if (name && strncmp(shared->name, name, RF_MAX_TOPIC_LEN) != 0) return -1;
        if (out) {
            memset(out, 0, sizeof(*out));
            out->id = shared->id;
            out->parts = shared->partitions;
            memcpy(out->name, shared->name, RF_MAX_TOPIC_LEN);
        }
        if (state_out) *state_out = state;
        return 1;
    }
    return 0;
}

static int rf_shared_topic_publish_replay(const aof_topic_rec_t *rec) {
    if (!g_bus || !rec) return -1;
    if (rf_lock_shared(&g_bus->topic_lock) != 0) return -1;
    int found = rf_shared_topic_find(rec->id, rec->name, NULL, NULL);
    if (found < 0) {
        pthread_mutex_unlock(&g_bus->topic_lock);
        return -1;
    }
    if (found == 1) {
        for (uint32_t i = 0; i < RF_MAX_TOPICS; i++) {
            if (g_bus->topics[i].id == rec->id &&
                strncmp(g_bus->topics[i].name, rec->name, RF_MAX_TOPIC_LEN) == 0) {
                atomic_store_explicit(&g_bus->topics[i].ready, RF_SHARED_TOPIC_READY,
                                      memory_order_release);
                break;
            }
        }
        pthread_mutex_unlock(&g_bus->topic_lock);
        return 0;
    }
    for (uint32_t i = 0; i < RF_MAX_TOPICS; i++) {
        uint32_t expected = 0;
        if (!atomic_compare_exchange_strong_explicit(&g_bus->topics[i].ready, &expected,
                                                     RF_SHARED_TOPIC_PENDING,
                                                     memory_order_acq_rel,
                                                     memory_order_relaxed)) continue;
        g_bus->topics[i].id = rec->id;
        g_bus->topics[i].partitions = rec->parts;
        g_bus->topics[i].reserved = 0;
        memcpy(g_bus->topics[i].name, rec->name, RF_MAX_TOPIC_LEN);
        atomic_store_explicit(&g_bus->topics[i].ready, RF_SHARED_TOPIC_READY,
                              memory_order_release);
        atomic_fetch_add_explicit(&g_bus->topic_count, 1, memory_order_relaxed);
        pthread_mutex_unlock(&g_bus->topic_lock);
        return 0;
    }
    pthread_mutex_unlock(&g_bus->topic_lock);
    return -1;
}

static rf_topic_t *rf_topic_require_by_id(uint32_t id) {
    rf_topic_t *topic = find_topic_by_id_local(id);
    if (topic) return topic;

    aof_topic_rec_t rec;
    uint32_t state = 0;
    if (rf_shared_topic_find(id, NULL, &rec, &state) != 1 ||
        state != RF_SHARED_TOPIC_READY) return NULL;
    rec.name[RF_MAX_TOPIC_LEN - 1] = 0;
    if (rf_topic_apply_local(rec.name, rec.id, rec.parts) != 0) return NULL;
    return find_topic_by_id_local(id);
}

int rf_partition_snapshot(const char *topic, int partition, rf_partition_snapshot_t *out) {
    if (!topic || !out) return 0;
    memset(out, 0, sizeof(*out));
    rf_topic_t *t = find_topic_lockfree(topic, rf_topic_id(topic));
    if (!t || partition < 0 || partition >= t->partitions) return 0;
    rf_partition_t *p = &t->parts[partition];
    uint64_t floor = atomic_load_explicit(&p->delete_floor, memory_order_acquire);
    uint64_t high = atomic_load_explicit(&p->write_head, memory_order_acquire);
    out->delete_floor = floor;
    out->high_watermark = high;
    out->message_count = (high >= floor) ? (high - floor) : 0;
    out->idempotency_index_writes = atomic_load_explicit(&g_idempotency_index_writes, memory_order_relaxed);
    return 1;
}

int rf_commit_checked(const char *topic, const char *group, int partition,
                      uint64_t offset, uint32_t timeout_ms, uint64_t *batch_id,
                      rf_write_receipt_t *receipt) {
    rf_write_receipt_t result;
    memset(&result, 0, sizeof(result));
    if (receipt) *receipt = result;
    if (batch_id) *batch_id = 0;
    if (!topic || !*topic || !group || !*group || partition < 0 ||
        partition > UINT16_MAX ||
        strnlen(topic, RF_MAX_TOPIC_LEN) >= RF_MAX_TOPIC_LEN ||
        strnlen(group, RF_MAX_GROUP_LEN) >= RF_MAX_GROUP_LEN) return -1;
    rf_topic_t *existing = rf_topic_require_by_id(rf_topic_id(topic));
    if (!existing || strcmp(existing->name, topic) != 0 ||
        partition >= existing->partitions) return -1;

    aof_commit_rec_t c = {0};
    c.topic_id = existing->id;
    c.partition = (uint16_t)partition;
    c.offset = offset;
    strncpy(c.topic, topic, RF_MAX_TOPIC_LEN - 1);
    strncpy(c.group, group, RF_MAX_GROUP_LEN - 1);
    uint64_t lock_key = (uint64_t)(uint32_t)offset_key_hash(topic, group, partition);
    if (rf_mutation_lock(lock_key) != 0) return -4;
    int rc = rf_persist_apply_record(AOF_REC_BROKER_COMMIT, &c,
                                     (uint32_t)sizeof(c), timeout_ms, &result);
    uint64_t observed = 0;
    if (rc == 0 && (!rf_offset_load(topic, group, partition, &observed) ||
                    observed < offset)) rc = -4;
    rf_mutation_unlock(lock_key);
    if (batch_id) *batch_id = result.batch_id;
    if (receipt) *receipt = result;
    return rc;
}

int rf_commit(const char *topic, const char *group, int partition, uint64_t offset,
              uint64_t *batch_id) {
    return rf_commit_checked(topic, group, partition, offset, 0, batch_id, NULL);
}

void rf_broker_replay_aof(uint32_t rec_id, const void *data, size_t sz) {
    if (!data) {
        g_replay_apply_error = 1;
        return;
    }
    if (rec_id == AOF_REC_HA_READ_BARRIER) {
        if (sz != sizeof(ha_read_barrier_rec_t)) {
            g_replay_apply_error = 1;
            return;
        }
        const ha_read_barrier_rec_t *rb = (const ha_read_barrier_rec_t *)data;
        atomic_max_u64(&g_last_ha_read_barrier_req, rb->req_id);
        return;
    }

    if (rec_id == AOF_REC_KV_PUT) {
        /* payload: aof_kv_put_t + value bytes */
        if (sz < sizeof(aof_kv_put_t)) {
            g_replay_apply_error = 1;
            return;
        }
        const aof_kv_put_t *h = (const aof_kv_put_t *) data;
        size_t need = sizeof(kvrec_t) + h->value_len;
        if (sizeof(aof_kv_put_t) + (size_t)h->value_len != sz) {
            g_replay_apply_error = 1;
            return;
        }
        /* stack buffer is fine (values are bounded by MAX_DATA_SIZE upstream) */
        uint8_t tmp[MAX_DATA_SIZE];
        if (need > sizeof(tmp)) {
            g_replay_apply_error = 1;
            return;
        }
        kvrec_t *r = (kvrec_t *) tmp;
        r->cas = h->cas;
        r->expires_us = h->expires_us;
        r->flags = h->flags;
        r->value_len = h->value_len;
        if (h->value_len)
            memcpy(r->value, (const uint8_t *) data + sizeof(*h), h->value_len);
        /* write into shared table */
        if (shared_storage_set(g_shared_storage, h->skey, r, need) != 0)
            g_replay_apply_error = 1;
        return;
    }

    if (rec_id == AOF_REC_KV_DEL) {
        if (sz != sizeof(aof_kv_del_t)) {
            g_replay_apply_error = 1;
            return;
        }
        const aof_kv_del_t *d = (const aof_kv_del_t *) data;
        uint8_t tmp[sizeof(kvrec_t)];
        kvrec_t *r = (kvrec_t *) tmp;
        r->cas = d->cas;
        r->expires_us = 0;
        r->flags = KV_FLAG_TOMBSTONE;
        r->value_len = 0;
        if (shared_storage_set(g_shared_storage, d->skey, r, sizeof(tmp)) != 0)
            g_replay_apply_error = 1;
        return;
    }

    if (rec_id == AOF_REC_BROKER_TRUNCATE) {
        if (sz != sizeof(aof_truncate_rec_t)) {
            g_replay_apply_error = 1;
            return;
        }
        const aof_truncate_rec_t *r = (const aof_truncate_rec_t *) data;

        rf_topic_t *t = rf_topic_require_by_id(r->topic_id);
        if (!t || r->partition >= t->partitions) {
            g_replay_apply_error = 1;
            return;
        }

        rf_partition_t *p = &t->parts[r->partition];
        atomic_max_u64(&p->delete_floor, r->before_offset);

        /* Opportunistically invalidate any already-replayed slots below the floor */
        rf_msg_slot_t *ring = atomic_load_explicit(&p->ring, memory_order_acquire);
        uint64_t wh = atomic_load_explicit(&p->write_head, memory_order_acquire);
        uint64_t oldest = (wh > p->capacity) ? (wh - p->capacity) : 0;
        uint64_t limit = (r->before_offset < wh) ? r->before_offset : wh;
        if (ring && limit > oldest) {
            for (uint64_t o = oldest; o < limit; o++) {
                rf_msg_slot_t *s = &ring[o & p->capacity_mask];
                uint16_t f = atomic_load_explicit(&s->flags, memory_order_acquire);
                if (!(f & 1)) continue;
                uint64_t so = atomic_load_explicit(&s->offset, memory_order_acquire);
                if (so != o) continue;
                rf_invalidate_slot(p, s);
            }
        }
        return;
    }

    if (rec_id == AOF_REC_BROKER_TTL) {
        if (sz < offsetof(aof_ttl_rec_t, topic)) {
            g_replay_apply_error = 1;
            return;
        }
        aof_ttl_rec_t r = {0};
        size_t copy = sz;
        if (copy > sizeof(r)) copy = sizeof(r);
        memcpy(&r, data, copy);
        if (sz >= offsetof(aof_ttl_rec_t, topic) + RF_MAX_TOPIC_LEN &&
            r.topic[RF_MAX_TOPIC_LEN - 1] != '\0') {
            g_replay_apply_error = 1;
            return;
        }
        r.topic[RF_MAX_TOPIC_LEN - 1] = '\0';
        uint32_t flags = 0;
        if (sz >= offsetof(aof_ttl_rec_t, present_flags) + sizeof(uint32_t)) {
            flags = r.present_flags;
            if (flags == 0 ||
                (flags & ~(RF_RETENTION_HAS_TTL |
                           RF_RETENTION_HAS_MAX_BYTES)) != 0) {
                g_replay_apply_error = 1;
                return;
            }
        } else {
            if (r.ttl_ms) flags |= RF_RETENTION_HAS_TTL;
            if (sz >= offsetof(aof_ttl_rec_t, max_bytes) + sizeof(uint64_t))
                flags |= RF_RETENTION_HAS_MAX_BYTES;
        }
        int rc = 0;
        if (r.topic[0] == 0 || (r.topic[0] == '*' && r.topic[1] == 0)) {
            if (r.partition != -1) rc = -1;
            if (rc == 0 && (flags & RF_RETENTION_HAS_TTL))
                rc = rf_set_ttl_ms_all(r.ttl_ms);
            if (rc == 0 && (flags & RF_RETENTION_HAS_MAX_BYTES))
                rc = rf_set_max_bytes_all(r.max_bytes);
        } else {
            rf_topic_t *topic = rf_topic_require_by_id(rf_topic_id(r.topic));
            if (!topic || strcmp(topic->name, r.topic) != 0 || r.partition < -1 ||
                r.partition >= topic->partitions) rc = -1;
            if (rc == 0 && (flags & RF_RETENTION_HAS_TTL))
                rc = rf_set_ttl_ms(r.topic, r.partition, r.ttl_ms);
            if (rc == 0 && (flags & RF_RETENTION_HAS_MAX_BYTES))
                rc = rf_set_max_bytes(r.topic, r.partition, r.max_bytes);
        }
        if (rc != 0) g_replay_apply_error = 1;
        return;
    }
    // Handle batch messages first
    if (rec_id == AOF_REC_BROKER_MSG_BATCH) {
        if (replay_plain_batch_atomic(data, sz) != 0)
            g_replay_apply_error = 1;
        return;
    }

    // Original single message handling
    if (rec_id == AOF_REC_BROKER_TOPIC) {
        if (sz != sizeof(aof_topic_rec_t)) {
            g_replay_apply_error = 1;
            return;
        }
        aof_topic_rec_t rec;
        memcpy(&rec, data, sizeof(rec));
        if (rec.name[0] == '\0' || rec.name[RF_MAX_TOPIC_LEN - 1] != '\0' ||
            rec.id != rf_topic_id(rec.name) || rec.parts == 0 ||
            rec.parts > RF_MAX_PARTITIONS) {
            g_replay_apply_error = 1;
            return;
        }
        if (rf_shared_topic_publish_replay(&rec) != 0 ||
            rf_topic_apply_local(rec.name, rec.id, rec.parts) != 0)
            g_replay_apply_error = 1;
        return;
    }

    if (rec_id == AOF_REC_BROKER_MSG) {
        if (sz < sizeof(aof_msg_rec_t)) {
            g_replay_apply_error = 1;
            return;
        }
        const aof_msg_rec_t *r = (const aof_msg_rec_t*)data;
        size_t total_size = (size_t)r->key_len + (size_t)r->val_len;
        if (total_size != sz - sizeof(*r)) {
            g_replay_apply_error = 1;
            return;
        }
        const uint8_t *kv = (const uint8_t*)data + sizeof(*r);
        if (replay_one_msg_rec(r, kv) != 0)
            g_replay_apply_error = 1;
        return;
    }

    if (rec_id == AOF_REC_BROKER_COMMIT) {
        if (sz != sizeof(aof_commit_rec_t)) {
            g_replay_apply_error = 1;
            return;
        }
        const aof_commit_rec_t *c = (const aof_commit_rec_t*)data;
        if (c->topic[0] == '\0' || c->group[0] == '\0' ||
            c->topic[RF_MAX_TOPIC_LEN - 1] != '\0' ||
            c->group[RF_MAX_GROUP_LEN - 1] != '\0' ||
            c->topic_id != rf_topic_id(c->topic) ||
            rf_offset_store(c->topic, c->group, c->partition, c->offset) != 0)
            g_replay_apply_error = 1;
        return;
    }

    if (rec_id == AOF_REC_BROKER_MSG_IDEMP) {
        int v2 = replay_idemp_v2(rec_id, data, sz);
        if (v2 != 0) {
            if (v2 < 0) g_replay_apply_error = 1;
            return;
        }
        if (sz < sizeof(aof_msg_rec_t) + sizeof(uint64_t)) {
            g_replay_apply_error = 1;
            return;
        }
        const aof_msg_rec_t *r = (const aof_msg_rec_t*)data;
        const uint8_t *kv = (const uint8_t*)data + sizeof(*r);
        size_t tot = (size_t)r->key_len + (size_t)r->val_len;
        if (tot < (size_t)r->key_len ||
            sizeof(*r) + tot + sizeof(uint64_t) != sz) {
            g_replay_apply_error = 1;
            return;
        }
        uint64_t fp64;
        memcpy(&fp64, kv + tot, sizeof fp64);

        /* reuse your single-message replay logic */
        if (replay_one_msg_rec(r, kv) != 0) {
            g_replay_apply_error = 1;
            return;
        }

        /* install idempotency mapping */
        rf_topic_t *t = rf_topic_require_by_id(r->topic_id);
        if (!t || r->partition >= t->partitions ||
            !idemp_put(t->name, r->partition, &fp64, sizeof fp64, fp64,
                       r->offset))
            g_replay_apply_error = 1;
        return;
    }

    if (rec_id == AOF_REC_BROKER_MSG_BATCH_IDEMP) {
        int v2 = replay_idemp_v2(rec_id, data, sz);
        if (v2 != 0) {
            if (v2 < 0) g_replay_apply_error = 1;
            return;
        }
        /* walk messages, last 8 bytes are fp64 trailer */
        if (sz < sizeof(aof_msg_rec_t) + sizeof(uint64_t)) {
            g_replay_apply_error = 1;
            return;
        }
        size_t messages_size = sz - sizeof(uint64_t);
        const uint8_t *end = (const uint8_t*)data + messages_size;
        if (replay_plain_batch_atomic(data, messages_size) != 0) {
            g_replay_apply_error = 1;
            return;
        }

        /* grab fp64 trailer */
        uint64_t fp64;
        memcpy(&fp64, end, sizeof fp64);

        /* install mapping using the batch first_offset */
        /* We can find first_offset by scanning first message in the batch: */
        const aof_msg_rec_t *first = (const aof_msg_rec_t*)data;
        rf_topic_t *t = rf_topic_require_by_id(first->topic_id);
        if (!t || first->partition >= t->partitions ||
            !idemp_put(t->name, first->partition, &fp64, sizeof fp64, fp64,
                       first->offset))
            g_replay_apply_error = 1;
        return;
    }
    g_replay_apply_error = 1;
}

int rf_broker_apply_aof_checked(uint32_t rec_id, const void *data, size_t size) {
    g_replay_apply_error = 0;
    rf_broker_replay_aof(rec_id, data, size);
    return g_replay_apply_error ? -1 : 0;
}

// ═══════════════════════════════════════════════════════════════════════════════
// Memory Management and Cleanup
// ═══════════════════════════════════════════════════════════════════════════════

void rf_broker_shutdown(void) {
    int n = atomic_load_explicit(&g_topic_count, memory_order_acquire);
    for (int i = 0; i < n; i++) {
        rf_topic_t *topic = &g_topics[i];
        for (int p = 0; p < topic->partitions; p++) {
            rf_partition_t *part = &topic->parts[p];

            // Clean up ring buffer
            rf_msg_slot_t *ring = atomic_exchange_explicit(&part->ring, NULL, memory_order_acq_rel);
            if (ring) {
                /* Only scan the live logical window, not the entire capacity if you want: */
                uint64_t wh = atomic_load_explicit(&part->write_head, memory_order_acquire);
                uint64_t oldest = (wh > part->capacity) ? (wh - part->capacity) : 0;
                for (uint64_t o = oldest; o < wh; o++) {
                    rf_msg_slot_t *slot = &ring[o & part->capacity_mask];
                    uint16_t flags = atomic_load_explicit(&slot->flags, memory_order_acquire);
                    if ((flags & 1) && (flags & 2)) {
                        // Large message - free the blob
                        uint64_t so = atomic_load_explicit(&slot->offset, memory_order_acquire);
                        if (so != o) continue;
                        if (slot->blob && slot->blob != slot->inline_data) {
                            slab_free(slot->blob);
                        }
                    }
                }
                // Unmap or free the ring
                size_t ring_size = part->capacity * sizeof(rf_msg_slot_t);
                (void)munmap(ring, ring_size);
            }

            // Clean up subscribers
            rf_sub_t *sub = atomic_load_explicit(&part->subscribers, memory_order_acquire);
            while (sub) {
                rf_sub_t *next = atomic_load_explicit(&sub->next, memory_order_acquire);
                // Wait for ref count to drop
                while (atomic_load_explicit(&sub->ref_count, memory_order_acquire) > 1) {
                    cpu_relax();
                }
                slab_free(sub);
                sub = next;
            }
        }

        if (topic->parts) {
            free(topic->parts);
        }
    }

    // Clear hash table
    for (int i = 0; i < TOPIC_HASH_SIZE; i++) {
        atomic_store_explicit(&g_topic_hash[i], NULL, memory_order_release);
    }
    atomic_store_explicit(&g_topic_count, 0, memory_order_release);
}

// ═══════════════════════════════════════════════════════════════════════════════
// Batch Operations for Maximum Throughput
// ═══════════════════════════════════════════════════════════════════════════════


int rf_produce_sealed_checked(const char *topic, int partition,
                              const void *key, size_t key_len,
                              const void *val, size_t val_len,
                              uint32_t timeout_ms, uint64_t *out_offset,
                              uint64_t *batch_id) {
    memset(&g_last_write_receipt, 0, sizeof(g_last_write_receipt));
    if (out_offset) *out_offset = 0;
    if (batch_id) *batch_id = 0;
    if (!topic || (!key && key_len) || (!val && val_len)) return -1;
    if (rf_topic_ensure(topic, 0) != 0) return -2;
    rf_topic_t *t = find_topic_lockfree(topic, rf_topic_id(topic));
    if (!t) return -2;
    if (partition < 0)
        partition = simd_hash_djb2(topic, strlen(topic)) & (t->partitions - 1);
    if (partition >= t->partitions) return -3;
    rf_partition_t *p = &t->parts[partition];
    if (rf_partition_ensure_ring(p) != 0) return -4;

    size_t payload_size = key_len + val_len;
    size_t record_size = sizeof(aof_msg_rec_t) + payload_size;
    if (record_size < sizeof(aof_msg_rec_t) || record_size > UINT32_MAX)
        return -4;
    uint8_t *record = slab_alloc(record_size);
    if (!record) return -4;
    uint64_t transaction_key = ((uint64_t)t->id << 32) ^
                               (uint64_t)(uint32_t)partition;
    if (rf_mutation_lock(transaction_key) != 0) {
        slab_free(record);
        return -4;
    }
    uint64_t offset = alloc_offset_shared(t, p, partition);
    aof_msg_rec_t *header = (aof_msg_rec_t *)record;
    *header = (aof_msg_rec_t){
        .topic_id = t->id,
        .partition = (uint16_t)partition,
        .reserved = RF_TS_FLAG_WALL,
        .offset = offset,
        .ts_us = wall_us_raw(),
        .key_len = (uint32_t)key_len,
        .val_len = (uint32_t)val_len
    };
    uint8_t *writep = record + sizeof(*header);
    if (key_len) memcpy(writep, key, key_len);
    if (val_len) memcpy(writep + key_len, val, val_len);
    rf_write_receipt_t receipt;
    int rc = rf_persist_apply_record(AOF_REC_BROKER_MSG, record,
                                     (uint32_t)record_size, timeout_ms, &receipt);
    if (out_offset) *out_offset = offset;
    if (batch_id) *batch_id = receipt.batch_id;
    slab_free(record);
    rf_mutation_unlock(transaction_key);
    if (rc == 0) rf_size_sweep_maybe(p);
    return rc;
}

int rf_produce_sealed(const char *topic, int partition,
                      const void *key, size_t key_len,
                      const void *val, size_t val_len,
                      uint64_t *out_offset, uint64_t *batch_id) {
    return rf_produce_sealed_checked(topic, partition, key, key_len, val,
                                     val_len, 0, out_offset, batch_id);
}

int rf_produce_batch_sealed_checked(const char *topic, int partition,
                                    const rf_msg_view_t *messages, size_t count,
                                    uint32_t timeout_ms, uint64_t *first_offset,
                                    uint64_t *batch_id) {
    memset(&g_last_write_receipt, 0, sizeof(g_last_write_receipt));
    if (first_offset) *first_offset = 0;
    if (batch_id) *batch_id = 0;
    if (!topic || !messages || count == 0) return -1;
    if (rf_topic_ensure(topic, 0) != 0) return -2;
    rf_topic_t *t = find_topic_lockfree(topic, rf_topic_id(topic));
    if (!t) return -2;
    if (partition < 0)
        partition = simd_hash_djb2(topic, strlen(topic)) & (t->partitions - 1);
    if (partition >= t->partitions) return -3;
    rf_partition_t *p = &t->parts[partition];
    if (rf_partition_ensure_ring(p) != 0) return -4;

    size_t record_size = 0;
    for (size_t i = 0; i < count; i++) {
        if ((!messages[i].key && messages[i].key_len) ||
            (!messages[i].val && messages[i].val_len)) return -1;
        size_t bytes = sizeof(aof_msg_rec_t) + messages[i].key_len +
                       messages[i].val_len;
        if (bytes < sizeof(aof_msg_rec_t) || SIZE_MAX - record_size < bytes)
            return -4;
        record_size += bytes;
    }
    if (record_size > UINT32_MAX) return -4;
    uint8_t *record = slab_alloc(record_size);
    if (!record) return -4;
    uint64_t transaction_key = ((uint64_t)t->id << 32) ^
                               (uint64_t)(uint32_t)partition;
    if (rf_mutation_lock(transaction_key) != 0) {
        slab_free(record);
        return -4;
    }
    uint64_t offset = alloc_offsets_shared(t, p, partition, count);
    uint64_t timestamp = wall_us_raw();
    uint8_t *writep = record;
    for (size_t i = 0; i < count; i++) {
        aof_msg_rec_t *header = (aof_msg_rec_t *)writep;
        *header = (aof_msg_rec_t){
            .topic_id = t->id,
            .partition = (uint16_t)partition,
            .reserved = RF_TS_FLAG_WALL,
            .offset = offset + i,
            .ts_us = timestamp,
            .key_len = (uint32_t)messages[i].key_len,
            .val_len = (uint32_t)messages[i].val_len
        };
        writep += sizeof(*header);
        if (messages[i].key_len) {
            memcpy(writep, messages[i].key, messages[i].key_len);
            writep += messages[i].key_len;
        }
        if (messages[i].val_len) {
            memcpy(writep, messages[i].val, messages[i].val_len);
            writep += messages[i].val_len;
        }
    }
    rf_write_receipt_t receipt;
    int rc = rf_persist_apply_record(AOF_REC_BROKER_MSG_BATCH, record,
                                     (uint32_t)record_size, timeout_ms, &receipt);
    if (first_offset) *first_offset = offset;
    if (batch_id) *batch_id = receipt.batch_id;
    slab_free(record);
    rf_mutation_unlock(transaction_key);
    if (rc == 0) rf_size_sweep_maybe(p);
    return rc;
}

int rf_produce_batch_sealed(const char *topic, int partition,
                            const rf_msg_view_t *messages, size_t count,
                            uint64_t *first_offset, uint64_t *batch_id) {
    return rf_produce_batch_sealed_checked(topic, partition, messages, count,
                                           0, first_offset, batch_id);
}

static int idemp_existing_result(rf_idemp_state_t *state,
                                 const uint8_t payload_digest[32],
                                 uint64_t *offset, uint64_t *batch_id,
                                 int *is_duplicate, uint32_t timeout_ms) {
    if (memcmp(state->payload_digest, payload_digest, 32) != 0)
        return RF_ERR_IDEMP_CONFLICT;
    if (offset) *offset = state->first_offset;
    if (batch_id) *batch_id = state->batch_id;
    if (is_duplicate) *is_duplicate = 1;
    if (state->status != RF_IDEMP_STATE_COMMITTED)
        return RF_ERR_IDEMP_PENDING;
    if (!state->local_fsync ||
        ((state->flags & RF_IDEMP_FLAG_HA) && !state->quorum_committed))
        return RF_ERR_IDEMP_PENDING;

    if (!state->applied_all_workers) {
        ha_read_barrier_rec_t barrier = {
            .req_id = ((uint64_t)state->topic_id << 32) ^ state->first_offset,
            .issued_us = now_us()
        };
        if (rf_bus_publish_wait_except(AOF_REC_HA_READ_BARRIER, &barrier,
                                       sizeof(barrier), timeout_ms,
                                       g_worker_id, NULL) != 0)
            return RF_ERR_IDEMP_PENDING;
        state->applied_all_workers = 1;
        if (idemp_state_store(state) != 0)
            return RF_ERR_IDEMP_PENDING;
    }
    rf_write_receipt_t receipt;
    receipt_from_idemp_state(state, &receipt);
    g_last_write_receipt = receipt;
    return 0;
}

int rf_produce_sealed_idemp_checked(const char *topic, int partition,
                                    const void *key, size_t key_len,
                                    const void *val, size_t val_len,
                                    const void *idemp_key, size_t idemp_key_len,
                                    uint32_t timeout_ms, uint64_t *out_offset,
                                    uint64_t *batch_id, int *is_duplicate) {
    memset(&g_last_write_receipt, 0, sizeof(g_last_write_receipt));
    if (out_offset) *out_offset = 0;
    if (batch_id) *batch_id = 0;
    if (is_duplicate) *is_duplicate = 0;
    if (!topic || (!key && key_len) || (!val && val_len) ||
        !idemp_key || idemp_key_len == 0) return -1;
    if (rf_topic_ensure(topic, 0) != 0) return -2;
    rf_topic_t *t = find_topic_lockfree(topic, rf_topic_id(topic));
    if (!t) return -2;
    if (partition < 0)
        partition = simd_hash_djb2(topic, strlen(topic)) & (t->partitions - 1);
    if (partition >= t->partitions) return -3;
    rf_partition_t *p = &t->parts[partition];
    if (rf_partition_ensure_ring(p) != 0) return -4;

    rf_msg_view_t message = {
        .key = key, .key_len = key_len, .val = val, .val_len = val_len
    };
    uint8_t key_digest[32], payload_digest[32];
    if (idemp_key_digest(topic, partition, idemp_key, idemp_key_len,
                         key_digest) != 0 ||
        idemp_payload_digest(topic, partition, &message, 1,
                             payload_digest) != 0) return -4;

    uint64_t transaction_key = ((uint64_t)t->id << 32) ^
                               (uint64_t)(uint32_t)partition;
    if (rf_mutation_lock(transaction_key) != 0) return -4;
    rf_idemp_state_t state;
    int found = idemp_state_load(key_digest, t->id, partition, &state);
    if (found > 0 && state.status == RF_IDEMP_STATE_ABORTED) found = 0;
    if (found != 0) {
        int rc = found < 0 ? -7 :
                 idemp_existing_result(&state, payload_digest, out_offset,
                                       batch_id, is_duplicate, timeout_ms);
        rf_mutation_unlock(transaction_key);
        return rc;
    }

    /* Legacy entries cannot prove payload binding.  Preserve at-most-once by
     * refusing to execute an ambiguous retry. */
    uint64_t legacy_offset = 0;
    uint64_t legacy_fp = idemp_fp64(topic, partition, idemp_key, idemp_key_len);
    if (idemp_get(topic, partition, idemp_key, idemp_key_len,
                  legacy_fp, &legacy_offset)) {
        if (out_offset) *out_offset = legacy_offset;
        if (is_duplicate) *is_duplicate = 1;
        rf_mutation_unlock(transaction_key);
        return RF_ERR_IDEMP_PENDING;
    }

    size_t message_bytes = sizeof(aof_msg_rec_t) + key_len + val_len;
    if (message_bytes < sizeof(aof_msg_rec_t) ||
        message_bytes > UINT32_MAX - sizeof(aof_idemp_v2_trailer_t)) {
        rf_mutation_unlock(transaction_key);
        return -4;
    }
    size_t record_bytes = message_bytes + sizeof(aof_idemp_v2_trailer_t);
    uint8_t *record = slab_alloc(record_bytes);
    if (!record) {
        rf_mutation_unlock(transaction_key);
        return -4;
    }
    uint64_t offset = alloc_offset_shared(t, p, partition);
    aof_msg_rec_t *message_record = (aof_msg_rec_t *)record;
    *message_record = (aof_msg_rec_t){
        .topic_id = t->id,
        .partition = (uint16_t)partition,
        .reserved = RF_TS_FLAG_WALL,
        .offset = offset,
        .ts_us = wall_us_raw(),
        .key_len = (uint32_t)key_len,
        .val_len = (uint32_t)val_len
    };
    uint8_t *payload = record + sizeof(*message_record);
    if (key_len) memcpy(payload, key, key_len);
    if (val_len) memcpy(payload + key_len, val, val_len);
    aof_idemp_v2_trailer_t *trailer =
        (aof_idemp_v2_trailer_t *)(record + message_bytes);
    memset(trailer, 0, sizeof(*trailer));
    trailer->magic = RF_IDEMP_V2_MAGIC;
    trailer->version = RF_IDEMP_V2_VERSION;
    trailer->flags = RAMForge_HA_is_enabled() ? RF_IDEMP_FLAG_HA : 0;
    trailer->message_count = 1;
    memcpy(trailer->key_digest, key_digest, 32);
    memcpy(trailer->payload_digest, payload_digest, 32);

    memset(&state, 0, sizeof(state));
    state.magic = RF_IDEMP_V2_MAGIC;
    state.version = RF_IDEMP_V2_VERSION;
    state.status = RF_IDEMP_STATE_PENDING;
    state.topic_id = t->id;
    state.partition = (uint16_t)partition;
    state.flags = trailer->flags;
    state.message_count = 1;
    memcpy(state.key_digest, key_digest, 32);
    memcpy(state.payload_digest, payload_digest, 32);
    state.first_offset = offset;
    if (idemp_state_store(&state) != 0) {
        slab_free(record);
        rf_mutation_unlock(transaction_key);
        return -7;
    }

    rf_write_receipt_t receipt;
    int rc = rf_persist_apply_record(AOF_REC_BROKER_MSG_IDEMP, record,
                                     (uint32_t)record_bytes, timeout_ms, &receipt);
    state.batch_id = receipt.batch_id;
    state.raft_index = receipt.raft_index;
    state.local_fsync = receipt.local_fsync ? 1u : 0u;
    state.quorum_committed = receipt.quorum_committed ? 1u : 0u;
    state.applied_all_workers = receipt.applied_all_workers ? 1u : 0u;
    if (rc == 0) state.status = RF_IDEMP_STATE_COMMITTED;
    int final_state_rc = idemp_state_store(&state);
    if (out_offset) *out_offset = offset;
    if (batch_id) *batch_id = receipt.batch_id;
    slab_free(record);
    rf_mutation_unlock(transaction_key);
    if (final_state_rc != 0) return RF_ERR_IDEMP_PENDING;
    if (rc != 0 && (receipt.quorum_committed || receipt.raft_index != 0 ||
                    receipt.batch_id != 0 || receipt.local_fsync))
        return RF_ERR_IDEMP_PENDING;
    return rc;
}

int rf_produce_sealed_idemp(const char *topic, int partition,
                            const void *key, size_t key_len,
                            const void *val, size_t val_len,
                            const void *idemp_key, size_t idemp_key_len,
                            uint64_t *out_offset, uint64_t *batch_id,
                            int *is_duplicate) {
    return rf_produce_sealed_idemp_checked(
        topic, partition, key, key_len, val, val_len,
        idemp_key, idemp_key_len, 0, out_offset, batch_id, is_duplicate);
}

int rf_produce_batch_sealed_idemp_checked(const char *topic, int partition,
                                          const rf_msg_view_t *messages,
                                          size_t count, const void *idemp_key,
                                          size_t idemp_key_len,
                                          uint32_t timeout_ms,
                                          uint64_t *first_offset,
                                          uint64_t *batch_id,
                                          int *is_duplicate) {
    memset(&g_last_write_receipt, 0, sizeof(g_last_write_receipt));
    if (first_offset) *first_offset = 0;
    if (batch_id) *batch_id = 0;
    if (is_duplicate) *is_duplicate = 0;
    if (!topic || !messages || count == 0 || count > UINT32_MAX ||
        !idemp_key || idemp_key_len == 0) return -1;
    if (rf_topic_ensure(topic, 0) != 0) return -2;
    rf_topic_t *t = find_topic_lockfree(topic, rf_topic_id(topic));
    if (!t) return -2;
    if (partition < 0)
        partition = simd_hash_djb2(topic, strlen(topic)) & (t->partitions - 1);
    if (partition >= t->partitions) return -3;
    rf_partition_t *p = &t->parts[partition];
    if (rf_partition_ensure_ring(p) != 0) return -4;

    uint8_t key_digest[32], payload_digest[32];
    if (idemp_key_digest(topic, partition, idemp_key, idemp_key_len,
                         key_digest) != 0 ||
        idemp_payload_digest(topic, partition, messages, count,
                             payload_digest) != 0) return -4;
    uint64_t transaction_key = ((uint64_t)t->id << 32) ^
                               (uint64_t)(uint32_t)partition;
    if (rf_mutation_lock(transaction_key) != 0) return -4;
    rf_idemp_state_t state;
    int found = idemp_state_load(key_digest, t->id, partition, &state);
    if (found > 0 && state.status == RF_IDEMP_STATE_ABORTED) found = 0;
    if (found != 0) {
        int rc = found < 0 ? -7 :
                 idemp_existing_result(&state, payload_digest, first_offset,
                                       batch_id, is_duplicate, timeout_ms);
        rf_mutation_unlock(transaction_key);
        return rc;
    }
    uint64_t legacy_offset = 0;
    uint64_t legacy_fp = idemp_fp64(topic, partition, idemp_key, idemp_key_len);
    if (idemp_get(topic, partition, idemp_key, idemp_key_len,
                  legacy_fp, &legacy_offset)) {
        if (first_offset) *first_offset = legacy_offset;
        if (is_duplicate) *is_duplicate = 1;
        rf_mutation_unlock(transaction_key);
        return RF_ERR_IDEMP_PENDING;
    }

    size_t messages_bytes = 0;
    for (size_t i = 0; i < count; i++) {
        if ((!messages[i].key && messages[i].key_len) ||
            (!messages[i].val && messages[i].val_len)) {
            rf_mutation_unlock(transaction_key);
            return -1;
        }
        size_t bytes = sizeof(aof_msg_rec_t) + messages[i].key_len +
                       messages[i].val_len;
        if (bytes < sizeof(aof_msg_rec_t) || SIZE_MAX - messages_bytes < bytes) {
            rf_mutation_unlock(transaction_key);
            return -4;
        }
        messages_bytes += bytes;
    }
    if (messages_bytes > UINT32_MAX - sizeof(aof_idemp_v2_trailer_t)) {
        rf_mutation_unlock(transaction_key);
        return -4;
    }
    size_t record_bytes = messages_bytes + sizeof(aof_idemp_v2_trailer_t);
    uint8_t *record = slab_alloc(record_bytes);
    if (!record) {
        rf_mutation_unlock(transaction_key);
        return -4;
    }
    uint64_t offset = alloc_offsets_shared(t, p, partition, count);
    uint64_t timestamp = wall_us_raw();
    uint8_t *writep = record;
    for (size_t i = 0; i < count; i++) {
        aof_msg_rec_t *message_record = (aof_msg_rec_t *)writep;
        *message_record = (aof_msg_rec_t){
            .topic_id = t->id,
            .partition = (uint16_t)partition,
            .reserved = RF_TS_FLAG_WALL,
            .offset = offset + i,
            .ts_us = timestamp,
            .key_len = (uint32_t)messages[i].key_len,
            .val_len = (uint32_t)messages[i].val_len
        };
        writep += sizeof(*message_record);
        if (messages[i].key_len) {
            memcpy(writep, messages[i].key, messages[i].key_len);
            writep += messages[i].key_len;
        }
        if (messages[i].val_len) {
            memcpy(writep, messages[i].val, messages[i].val_len);
            writep += messages[i].val_len;
        }
    }
    aof_idemp_v2_trailer_t *trailer = (aof_idemp_v2_trailer_t *)writep;
    memset(trailer, 0, sizeof(*trailer));
    trailer->magic = RF_IDEMP_V2_MAGIC;
    trailer->version = RF_IDEMP_V2_VERSION;
    trailer->flags = RAMForge_HA_is_enabled() ? RF_IDEMP_FLAG_HA : 0;
    trailer->message_count = (uint32_t)count;
    memcpy(trailer->key_digest, key_digest, 32);
    memcpy(trailer->payload_digest, payload_digest, 32);

    memset(&state, 0, sizeof(state));
    state.magic = RF_IDEMP_V2_MAGIC;
    state.version = RF_IDEMP_V2_VERSION;
    state.status = RF_IDEMP_STATE_PENDING;
    state.topic_id = t->id;
    state.partition = (uint16_t)partition;
    state.flags = trailer->flags;
    state.message_count = (uint32_t)count;
    memcpy(state.key_digest, key_digest, 32);
    memcpy(state.payload_digest, payload_digest, 32);
    state.first_offset = offset;
    if (idemp_state_store(&state) != 0) {
        slab_free(record);
        rf_mutation_unlock(transaction_key);
        return -7;
    }

    rf_write_receipt_t receipt;
    int rc = rf_persist_apply_record(AOF_REC_BROKER_MSG_BATCH_IDEMP, record,
                                     (uint32_t)record_bytes, timeout_ms, &receipt);
    state.batch_id = receipt.batch_id;
    state.raft_index = receipt.raft_index;
    state.local_fsync = receipt.local_fsync ? 1u : 0u;
    state.quorum_committed = receipt.quorum_committed ? 1u : 0u;
    state.applied_all_workers = receipt.applied_all_workers ? 1u : 0u;
    if (rc == 0) state.status = RF_IDEMP_STATE_COMMITTED;
    int final_state_rc = idemp_state_store(&state);
    if (first_offset) *first_offset = offset;
    if (batch_id) *batch_id = receipt.batch_id;
    slab_free(record);
    rf_mutation_unlock(transaction_key);
    if (final_state_rc != 0) return RF_ERR_IDEMP_PENDING;
    if (rc != 0 && (receipt.quorum_committed || receipt.raft_index != 0 ||
                    receipt.batch_id != 0 || receipt.local_fsync))
        return RF_ERR_IDEMP_PENDING;
    return rc;
}

int rf_produce_batch_sealed_idemp(const char *topic, int partition,
                                  const rf_msg_view_t *messages, size_t count,
                                  const void *idemp_key, size_t idemp_key_len,
                                  uint64_t *first_offset, uint64_t *batch_id,
                                  int *is_duplicate) {
    return rf_produce_batch_sealed_idemp_checked(
        topic, partition, messages, count, idemp_key, idemp_key_len, 0,
        first_offset, batch_id, is_duplicate);
}

static int replay_message_set_atomic(const aof_msg_rec_t *const *records,
                                     const rf_msg_view_t *messages,
                                     size_t count) {
    if (!records || !messages || count == 0) return -1;
    const aof_msg_rec_t *first = records[0];
    rf_topic_t *topic = rf_topic_require_by_id(first->topic_id);
    if (!topic || first->partition >= topic->partitions) return -1;
    rf_partition_t *partition = &topic->parts[first->partition];
    if (rf_partition_ensure_ring(partition) != 0) return -1;
    rf_msg_slot_t *ring = atomic_load_explicit(&partition->ring, memory_order_acquire);
    if (!ring) return -1;

    void **large_blobs = calloc(count, sizeof(*large_blobs));
    if (!large_blobs) return -1;
    uint64_t floor = atomic_load_explicit(&partition->delete_floor,
                                          memory_order_acquire);
    for (size_t i = 0; i < count; i++) {
        const aof_msg_rec_t *record = records[i];
        if (record->topic_id != first->topic_id ||
            record->partition != first->partition) goto fail;
        if (record->offset < floor) continue;
        rf_msg_slot_t *slot = &ring[record->offset & partition->capacity_mask];
        if (rf_slot_ready_for_offset(slot, record->offset)) continue;
        size_t total = messages[i].key_len + messages[i].val_len;
        if (total > sizeof(slot->inline_data)) {
            large_blobs[i] = slab_alloc(total);
            if (!large_blobs[i]) goto fail;
            if (messages[i].key_len)
                memcpy(large_blobs[i], messages[i].key, messages[i].key_len);
            if (messages[i].val_len)
                memcpy((uint8_t *)large_blobs[i] + messages[i].key_len,
                       messages[i].val, messages[i].val_len);
        }
    }

    for (size_t i = 0; i < count; i++) {
        const aof_msg_rec_t *record = records[i];
        if (record->offset < floor) {
            if (large_blobs[i]) slab_free(large_blobs[i]);
            continue;
        }
        rf_msg_slot_t *slot = &ring[record->offset & partition->capacity_mask];
        if (rf_slot_ready_for_offset(slot, record->offset)) {
            if (large_blobs[i]) slab_free(large_blobs[i]);
            continue;
        }
        size_t total = messages[i].key_len + messages[i].val_len;
        uint64_t want = record->offset + 1;
        seed_shared_next_offset(record->topic_id, record->partition, want);
        atomic_max_u64(&partition->next_offset, want);
        atomic_max_u64(&partition->write_head, want);
        rf_slot_evict_if_needed(partition, slot, record->offset);
        if (total <= sizeof(slot->inline_data)) {
            if (messages[i].key_len)
                memcpy(slot->inline_data, messages[i].key, messages[i].key_len);
            if (messages[i].val_len)
                memcpy(slot->inline_data + messages[i].key_len,
                       messages[i].val, messages[i].val_len);
            slot->blob = slot->inline_data;
        } else {
            slot->blob = large_blobs[i];
            large_blobs[i] = NULL;
        }
        atomic_store_explicit(&slot->offset, record->offset, memory_order_release);
        atomic_store_explicit(&slot->ts_us,
                              normalize_persisted_ts_us(record->ts_us,
                                                        record->reserved,
                                                        ttl_now_wall_us()),
                              memory_order_release);
        atomic_store_explicit(&slot->key_len, record->key_len, memory_order_release);
        atomic_store_explicit(&slot->val_len, record->val_len, memory_order_release);
        atomic_store_explicit(&slot->partition, record->partition, memory_order_release);
        atomic_store_explicit(&slot->flags,
                              total <= sizeof(slot->inline_data)
                              ? RF_SLOT_FLAG_VALID
                              : (RF_SLOT_FLAG_VALID | RF_SLOT_FLAG_LARGE),
                              memory_order_release);
        atomic_fetch_add_explicit(&partition->live_bytes, total, memory_order_relaxed);
    }
    free(large_blobs);
    return 0;

fail:
    for (size_t i = 0; i < count; i++)
        if (large_blobs[i]) slab_free(large_blobs[i]);
    free(large_blobs);
    return -1;
}

static int replay_plain_batch_atomic(const void *data, size_t size) {
    if (!data || size < sizeof(aof_msg_rec_t)) return -1;
    const uint8_t *begin = (const uint8_t *)data;
    const uint8_t *end = begin + size;
    const uint8_t *p = begin;
    size_t count = 0;

    while (p < end) {
        size_t remaining = (size_t)(end - p);
        if (remaining < sizeof(aof_msg_rec_t)) return -1;
        const aof_msg_rec_t *record = (const aof_msg_rec_t *)p;
        size_t payload = (size_t)record->key_len + (size_t)record->val_len;
        if (payload < (size_t)record->key_len) return -1;
        size_t bytes = sizeof(*record) + payload;
        if (bytes < sizeof(*record) || bytes > remaining) return -1;
        if (count == SIZE_MAX) return -1;
        count++;
        p += bytes;
    }
    if (p != end || count == 0 ||
        count > SIZE_MAX / sizeof(aof_msg_rec_t *) ||
        count > SIZE_MAX / sizeof(rf_msg_view_t)) return -1;

    const aof_msg_rec_t **records = calloc(count, sizeof(*records));
    rf_msg_view_t *messages = calloc(count, sizeof(*messages));
    if (!records || !messages) {
        free(records);
        free(messages);
        return -1;
    }

    p = begin;
    for (size_t i = 0; i < count; i++) {
        const aof_msg_rec_t *record = (const aof_msg_rec_t *)p;
        size_t payload = (size_t)record->key_len + (size_t)record->val_len;
        records[i] = record;
        messages[i].key = p + sizeof(*record);
        messages[i].key_len = record->key_len;
        messages[i].val = p + sizeof(*record) + record->key_len;
        messages[i].val_len = record->val_len;
        p += sizeof(*record) + payload;
    }
    int rc = replay_message_set_atomic(records, messages, count);
    free(records);
    free(messages);
    return rc;
}

static int replay_idemp_v2(uint32_t rec_id, const void *data, size_t size) {
    if (size < sizeof(aof_msg_rec_t) + sizeof(aof_idemp_v2_trailer_t)) return 0;
    const uint8_t *begin = (const uint8_t *)data;
    const aof_idemp_v2_trailer_t *trailer =
        (const aof_idemp_v2_trailer_t *)(begin + size - sizeof(*trailer));
    if (trailer->magic != RF_IDEMP_V2_MAGIC ||
        trailer->version != RF_IDEMP_V2_VERSION) return 0;
    if (trailer->message_count == 0 ||
        (rec_id == AOF_REC_BROKER_MSG_IDEMP && trailer->message_count != 1))
        return -1;

    size_t count = trailer->message_count;
    if (count > (size - sizeof(*trailer)) / sizeof(aof_msg_rec_t)) return -1;
    const aof_msg_rec_t **records = calloc(count, sizeof(*records));
    rf_msg_view_t *messages = calloc(count, sizeof(*messages));
    if (!records || !messages) {
        free(records);
        free(messages);
        return -1;
    }
    const uint8_t *p = begin;
    const uint8_t *end = (const uint8_t *)trailer;
    int rc = -1;
    for (size_t i = 0; i < count; i++) {
        if ((size_t)(end - p) < sizeof(aof_msg_rec_t)) goto done;
        const aof_msg_rec_t *record = (const aof_msg_rec_t *)p;
        size_t payload = (size_t)record->key_len + (size_t)record->val_len;
        size_t bytes = sizeof(*record) + payload;
        if (bytes < sizeof(*record) || bytes > (size_t)(end - p)) goto done;
        records[i] = record;
        messages[i].key = p + sizeof(*record);
        messages[i].key_len = record->key_len;
        messages[i].val = p + sizeof(*record) + record->key_len;
        messages[i].val_len = record->val_len;
        p += bytes;
    }
    if (p != end) goto done;
    const aof_msg_rec_t *first = records[0];
    rf_topic_t *topic = rf_topic_require_by_id(first->topic_id);
    if (!topic || first->partition >= topic->partitions) goto done;
    uint8_t payload_digest[32];
    if (idemp_payload_digest(topic->name, first->partition, messages, count,
                             payload_digest) != 0 ||
        memcmp(payload_digest, trailer->payload_digest, 32) != 0) goto done;
    if (replay_message_set_atomic(records, messages, count) != 0) goto done;

    rf_idemp_state_t state;
    memset(&state, 0, sizeof(state));
    state.magic = RF_IDEMP_V2_MAGIC;
    state.version = RF_IDEMP_V2_VERSION;
    state.status = RF_IDEMP_STATE_COMMITTED;
    state.topic_id = first->topic_id;
    state.partition = first->partition;
    state.flags = trailer->flags;
    state.message_count = trailer->message_count;
    memcpy(state.key_digest, trailer->key_digest, 32);
    memcpy(state.payload_digest, trailer->payload_digest, 32);
    state.first_offset = first->offset;
    state.local_fsync = 1;
    state.quorum_committed = (trailer->flags & RF_IDEMP_FLAG_HA) ? 1u : 0u;
    if (idemp_state_store(&state) != 0) goto done;
    rc = 1;

done:
    free(records);
    free(messages);
    return rc;
}

static int idemp_state_from_raft_record(uint32_t rec_id, const void *data,
                                        size_t size, rf_idemp_state_t *state) {
    if (rec_id != AOF_REC_BROKER_MSG_IDEMP &&
        rec_id != AOF_REC_BROKER_MSG_BATCH_IDEMP) return 0;
    if (!data || !state ||
        size < sizeof(aof_msg_rec_t) + sizeof(aof_idemp_v2_trailer_t)) return -1;
    const uint8_t *begin = (const uint8_t *)data;
    const aof_idemp_v2_trailer_t *trailer =
        (const aof_idemp_v2_trailer_t *)(begin + size - sizeof(*trailer));
    if (trailer->magic != RF_IDEMP_V2_MAGIC ||
        trailer->version != RF_IDEMP_V2_VERSION) return 0;
    if (trailer->message_count == 0 ||
        (rec_id == AOF_REC_BROKER_MSG_IDEMP && trailer->message_count != 1))
        return -1;

    const uint8_t *p = begin;
    const uint8_t *end = (const uint8_t *)trailer;
    const aof_msg_rec_t *first = NULL;
    for (uint32_t i = 0; i < trailer->message_count; i++) {
        if ((size_t)(end - p) < sizeof(aof_msg_rec_t)) return -1;
        const aof_msg_rec_t *record = (const aof_msg_rec_t *)p;
        size_t payload = (size_t)record->key_len + (size_t)record->val_len;
        size_t bytes = sizeof(*record) + payload;
        if (bytes < sizeof(*record) || bytes > (size_t)(end - p)) return -1;
        if (!first) first = record;
        else if (record->topic_id != first->topic_id ||
                 record->partition != first->partition) return -1;
        p += bytes;
    }
    if (!first || p != end) return -1;

    memset(state, 0, sizeof(*state));
    state->magic = RF_IDEMP_V2_MAGIC;
    state->version = RF_IDEMP_V2_VERSION;
    state->status = RF_IDEMP_STATE_PENDING;
    state->topic_id = first->topic_id;
    state->partition = first->partition;
    state->flags = trailer->flags;
    state->message_count = trailer->message_count;
    memcpy(state->key_digest, trailer->key_digest, 32);
    memcpy(state->payload_digest, trailer->payload_digest, 32);
    state->first_offset = first->offset;
    return 1;
}

int rf_idemp_restore_raft_state(uint32_t rec_id, const void *data, size_t size,
                                uint64_t raft_index, int quorum_committed,
                                int applied) {
    rf_idemp_state_t restored;
    int parsed = idemp_state_from_raft_record(rec_id, data, size, &restored);
    if (parsed <= 0) return parsed;
    restored.raft_index = raft_index;
    restored.quorum_committed = quorum_committed ? 1u : 0u;
    restored.local_fsync = applied ? 1u : 0u;
    restored.status = applied ? RF_IDEMP_STATE_COMMITTED : RF_IDEMP_STATE_PENDING;

    rf_idemp_state_t existing;
    int found = idemp_state_load(restored.key_digest, restored.topic_id,
                                  restored.partition, &existing);
    if (found < 0) return -1;
    if (found > 0 &&
        memcmp(existing.payload_digest, restored.payload_digest,
               sizeof(existing.payload_digest)) != 0) return -1;
    if (found > 0 && existing.status == RF_IDEMP_STATE_COMMITTED) {
        if (applied) {
            existing.raft_index = raft_index;
            existing.local_fsync = 1;
            existing.quorum_committed = quorum_committed ? 1u : 0u;
            if (idemp_state_store(&existing) != 0) return -1;
        }
        return 0;
    }
    if (found > 0 && existing.status == RF_IDEMP_STATE_PENDING &&
        existing.raft_index != 0 && existing.raft_index != raft_index) return -1;
    return idemp_state_store(&restored);
}

void rf_idemp_abort_raft_state(uint32_t rec_id, const void *data, size_t size,
                               uint64_t raft_index) {
    rf_idemp_state_t parsed;
    if (idemp_state_from_raft_record(rec_id, data, size, &parsed) != 1) return;
    rf_idemp_state_t existing;
    if (idemp_state_load(parsed.key_digest, parsed.topic_id, parsed.partition,
                         &existing) != 1) return;
    if (existing.status != RF_IDEMP_STATE_PENDING ||
        (existing.raft_index != 0 && existing.raft_index != raft_index)) return;
    existing.status = RF_IDEMP_STATE_ABORTED;
    existing.raft_index = 0;
    existing.batch_id = 0;
    existing.local_fsync = 0;
    existing.quorum_committed = 0;
    existing.applied_all_workers = 0;
    (void)idemp_state_store(&existing);
}
