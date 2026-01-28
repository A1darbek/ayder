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
#include <stdatomic.h>
#include <time.h>
#include <sys/mman.h>
#include <unistd.h>
#include <sched.h>
#include <stddef.h>   // max_align_t
#include <stdalign.h> // _Alignof
#include <stdbool.h>

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


static void rf_ttl_apply_to_partition(rf_partition_t *p, uint64_t ttl_ms) {
    uint64_t win = ttl_ms ? (ttl_ms * 1000ULL) : 0;
    atomic_store_explicit(&p->ttl_window_us, win, memory_order_release);
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


static uint32_t reserve_heap(size_t sz){
    if (sz == 0) return 0;

    /* If a payload can never fit, fail early (prevents infinite loop). */
    if (sz > g_bus->heap_size) return UINT32_MAX;

    const size_t A = RF_BUS_HEAP_ALIGN;
    const size_t sz_a = align_up_sz(sz, A);

    for(;;){
        uint64_t cur = atomic_load_explicit(&g_bus->data_head, memory_order_relaxed);

        uint32_t raw = (uint32_t)(cur % g_bus->heap_size);
        uint32_t off = (uint32_t)align_up_sz(raw, A);

        uint64_t adv;
        uint32_t out;

        if ((uint64_t)off + sz <= g_bus->heap_size) {
            /* pad up to alignment + reserve aligned payload */
            adv = (uint64_t)(off - raw) + (uint64_t)sz_a;
            out = off;
        } else {
            /* wrap: consume remainder, then reserve from 0 (already aligned) */
            adv = (uint64_t)(g_bus->heap_size - raw) + (uint64_t)sz_a;
            out = 0;
        }

        if (atomic_compare_exchange_weak_explicit(&g_bus->data_head, &cur, cur + adv,
                                                  memory_order_acq_rel, memory_order_relaxed))
            return out;
    }
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

    atomic_store(&g_bus->data_head, 0);
    atomic_store(&g_bus->desc_head, 0);
    atomic_store(&g_bus->drops, 0);

    return 0;
}


int rf_bus_publish(int rec_id, const void *payload, uint32_t size){
    if (!g_bus || !payload || !size) return -1;

    /* reserve payload space and copy */
    uint32_t off = reserve_heap(size);
    if (off == UINT32_MAX) return -1;
    memcpy(g_bus->heap + off, payload, size);

    /* claim descriptor slot */
    uint64_t d = atomic_fetch_add_explicit(&g_bus->desc_head, 1, memory_order_acq_rel);
    uint32_t idx = (uint32_t)d & g_bus->desc_mask;
    bus_desc_t *ds = &g_bus->desc[idx];

    /* overwrite is allowed (broadcast bus), consumers must keep up */
    ds->rec_id = (uint32_t)rec_id;
    ds->size   = size;
    ds->off    = off;
    ds->seq    = d;

    /* publish */
    atomic_thread_fence(memory_order_release);
    atomic_store_explicit(&ds->valid, 1, memory_order_release);
    return 0;
}

int ha_fwd_send(uint32_t type, const void *payload, uint32_t size) {
    size_t tot = sizeof(ha_fwd_rec_t) + size;
    void *buf = slab_alloc(tot);
    if (!buf) return -1;
    ha_fwd_rec_t *h = (ha_fwd_rec_t *) buf;
    h->type = type;
    h->size = size;
    if (size) memcpy((uint8_t *) buf + sizeof(*h), payload, size);
    int rc = rf_bus_publish(AOF_REC_HA_FORWARD, buf, (uint32_t) tot);
    slab_free(buf);
    return rc;
}

static void *promoter_loop(void *arg){
    (void)arg;
    uint64_t r = tls_read_seq; /* start from 0 */
    for(;;){
        uint32_t idx = (uint32_t)r & g_bus->desc_mask;
        bus_desc_t *ds = &g_bus->desc[idx];

        if (atomic_load_explicit(&ds->valid, memory_order_acquire) && ds->seq == r){
            /* safe to read descriptor and payload */
            uint32_t id  = ds->rec_id;
            uint32_t sz  = ds->size;
            uint32_t off = ds->off;

            if ((uint64_t) off + sz <= g_bus->heap_size) {
                if (id == AOF_REC_HA_FORWARD) {
                    /* Only worker 0 handles HA forward → append to RAFT log, replicate */
                    if (g_worker_id == 0) {
                        const ha_fwd_rec_t *f = (const ha_fwd_rec_t *) (g_bus->heap + off);
                        const uint8_t *p = (const uint8_t *) f + sizeof(*f);
                        /* Call into HA: append locally + replicate (async) */
                        extern int HA_forward_append_and_replicate(uint32_t type, const void *pl, size_t psz);
                        (void) HA_forward_append_and_replicate(f->type, p, f->size);
                    }
                } else {
                    rf_broker_replay_aof(id, g_bus->heap + off, sz);
                }
            } else {
                /* wrapped payload should never happen with reserve_heap() */
            }
            r++;
            tls_read_seq = r;
            continue;
        }

        /* If producer lapped us, jump to latest window to avoid hot spin and mark drop */
        uint64_t head = atomic_load_explicit(&g_bus->desc_head, memory_order_acquire);
        if (head - r > (uint64_t)(g_bus->desc_mask + 1)){
            atomic_fetch_add_explicit(&g_bus->drops, head - r, memory_order_relaxed);
            r = head;
            tls_read_seq = r;
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

#define MAX_TOPICS 1024
static rf_topic_t g_topics[MAX_TOPICS] __attribute__((aligned(64)));
static _Atomic int g_topic_count = 0;
static int g_default_partitions = 8; // Increased for better parallelism
static size_t g_ring_cap = 1048576;  // 1M slots, power of 2

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
        return shared_storage_atomic_inc_u64(g_shared_storage, skey);
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
    return shared_storage_set(g_shared_storage, skey, &v, sizeof v);
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
        uint64_t us = ttl_ms ? ttl_ms * 1000ULL : 0;
        atomic_store_explicit(&rp->ttl_ms, us, memory_order_release);
        atomic_store_explicit(&rp->ttl_window_us, us, memory_order_release);

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
    if (!name || !*name) return -1;
    uint32_t id = rf_topic_id(name);
    rf_topic_t *t = find_topic_lockfree(name, id);
    if (t) return 0; // Already exists

    /* Reserve a unique slot atomically (fixes race with weak CAS) */
    int idx = (int) atomic_fetch_add_explicit(&g_topic_count, 1, memory_order_acq_rel);
    if (idx >= MAX_TOPICS) {
        /* roll back the reservation */
        atomic_fetch_sub_explicit(&g_topic_count, 1, memory_order_acq_rel);
        return -2;
    }

    rf_topic_t *slot = &g_topics[idx];
    memset(slot, 0, sizeof(*slot));
    strncpy(slot->name, name, RF_MAX_TOPIC_LEN - 1);
    slot->id = id;
    slot->hash = id; // Precomputed for fast lookup
    slot->partitions = partitions > 0 ? partitions : g_default_partitions;

    // Allocate partitions with cache-line alignment
    size_t parts_size = slot->partitions * sizeof(rf_partition_t);
    slot->parts = aligned_alloc(64, parts_size);
    if (!slot->parts) return -3;
    memset(slot->parts, 0, parts_size);
    for (int i = 0; i < slot->partitions; i++) {
        if (init_partition_lockfree(&slot->parts[i]) != 0) return -4;
    }

    // Insert into hash table
    uint32_t hash_idx = id & (TOPIC_HASH_SIZE - 1);
    for (int probe = 0; probe < 8; probe++) {
        uint32_t idx_hash = (hash_idx + probe) & (TOPIC_HASH_SIZE - 1);
        rf_topic_t *expected = NULL;
        if (atomic_compare_exchange_weak_explicit(
                &g_topic_hash[idx_hash], &expected, slot,
                memory_order_release, memory_order_relaxed)) {
            break;
        }
    }

    // Persist topic metadata
    struct {
        uint32_t id;
        uint16_t parts;
        char name[RF_MAX_TOPIC_LEN];
    } rec = { .id = slot->id, .parts = (uint16_t)slot->partitions };
    strncpy(rec.name, slot->name, RF_MAX_TOPIC_LEN - 1);
    AOF_append_sealed(AOF_REC_BROKER_TOPIC, &rec, sizeof(rec));
    if (RAMForge_HA_is_enabled() && RAMForge_HA_is_leader()) {
        (void) RAMForge_HA_replicate_write(AOF_REC_BROKER_TOPIC, &rec, sizeof(rec));
    }
    return 0;
}

int rf_delete_before(const char *topic, int partition, uint64_t before_offset,
                     uint64_t *deleted_count, uint64_t *freed_bytes)
{
    if (!topic) return -1;

    rf_topic_t *t = find_topic_lockfree(topic, rf_topic_id(topic));
    if (!t || partition < 0 || partition >= t->partitions) return -2;

    rf_partition_t *p = &t->parts[partition];
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

            freed   += slot_payload_bytes(s);
            rf_invalidate_slot(p, s);
            deleted++;
        }
    }

    /* Raise the hard floor so readers/replay won’t surface old data */
    atomic_max_u64(&p->delete_floor, before_offset);

    /* Persist truncation so it survives crashes */
    aof_truncate_rec_t rec = {
            .topic_id = t->id,
            .partition = (uint16_t)partition,
            .reserved = 0,
            .before_offset = before_offset
    };
    AOF_append_sealed(AOF_REC_BROKER_TRUNCATE, &rec, sizeof(rec));

    if (RAMForge_HA_is_enabled()) {
        RAMForge_HA_replicate_write(AOF_REC_BROKER_TRUNCATE, &rec, sizeof(rec));
    }

    if (deleted_count) *deleted_count = deleted;
    if (freed_bytes)   *freed_bytes   = freed;
    return 0;
}

// ═══════════════════════════════════════════════════════════════════════════════
// Zero-Copy Message Production with Lock-Free Ring Buffer
// ═══════════════════════════════════════════════════════════════════════════════

static inline void replay_one_msg_rec(const aof_msg_rec_t *r, const uint8_t *kv) {
    rf_topic_t *t = NULL;
    int n = atomic_load_explicit(&g_topic_count, memory_order_acquire);
    for (int i = 0; i < n; i++) {
        if (g_topics[i].id == r->topic_id) {
            t = &g_topics[i];
            break;
        }
    }
    if (!t || r->partition >= t->partitions) return;
    rf_partition_t *p = &t->parts[r->partition];

    if (rf_partition_ensure_ring(p) != 0) return;
    rf_msg_slot_t *ring = atomic_load_explicit(&p->ring, memory_order_acquire);
    if (!ring) return;

    uint64_t floor = atomic_load_explicit(&p->delete_floor, memory_order_acquire);
    if (r->offset < floor) return;  // NEW: honor truncate floor

    size_t total_size = r->key_len + r->val_len;
    uint64_t slot_idx = (r->offset) & p->capacity_mask;
    rf_msg_slot_t *slot = &ring[slot_idx];

    uint64_t want = r->offset + 1;
    seed_shared_next_offset(r->topic_id, r->partition, want);
    atomic_max_u64(&p->next_offset, want);
    atomic_max_u64(&p->write_head, want);
    if (rf_slot_ready_for_offset(slot, r->offset)) return;
    rf_slot_evict_if_needed(p, slot, r->offset);

    if (total_size <= sizeof(slot->inline_data)) {
        memcpy(slot->inline_data, kv, total_size);
        slot->blob = slot->inline_data;
    } else {
        void *blob = slab_alloc(total_size);
        if (blob) {
            memcpy(blob, kv, total_size);
            slot->blob = blob;
        }
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

}

// ═══════════════════════════════════════════════════════════════════════════
// SEALED SINGLE MESSAGE PRODUCE - ZERO-LOSS GUARANTEE
// ═══════════════════════════════════════════════════════════════════════════

int rf_produce_sealed(const char *topic, int partition,
                      const void *key, size_t key_len,
                      const void *val, size_t val_len,
                      uint64_t *out_offset, uint64_t *batch_id) {
    if (!topic || (!val && val_len)) return -1;
    rf_topic_ensure(topic, 0);
    rf_topic_t *t = find_topic_lockfree(topic, rf_topic_id(topic));
    if (!t) return -2;

    if (partition < 0) {
        partition = simd_hash_djb2(topic, strlen(topic)) & (t->partitions - 1);
    }
    if (partition >= t->partitions) return -3;

    rf_partition_t *p = &t->parts[partition];

    if (rf_partition_ensure_ring(p) != 0) return -4;

    rf_msg_slot_t *ring = atomic_load_explicit(&p->ring, memory_order_acquire);
    if (!ring) return -4;
    // Lock-free slot allocation
    uint64_t offset = alloc_offset_shared(t, p, partition);
    uint64_t slot_idx = offset & p->capacity_mask;
    rf_msg_slot_t *slot = &ring[slot_idx];
    rf_slot_evict_if_needed(p, slot, offset);
    /* ring is addressed by offset; write_head trails logical "next free" */
    atomic_max_u64(&p->next_offset, offset + 1);
    atomic_max_u64(&p->write_head, offset + 1);

    size_t total_size = key_len + val_len;
    uint64_t ts_wall = wall_us_raw();

    // Zero-copy storage in ring buffer
    if (total_size <= sizeof(slot->inline_data)) {
        // Inline storage
        if (key && key_len) {
            memcpy(slot->inline_data, key, key_len);
        }
        if (val && val_len) {
            memcpy(slot->inline_data + key_len, val, val_len);
        }
        slot->blob = slot->inline_data;
    } else {
        // Large message
        void *blob = slab_alloc(total_size);
        if (!blob) return -4;
        if (key && key_len) memcpy(blob, key, key_len);
        if (val && val_len) memcpy((uint8_t*)blob + key_len, val, val_len);
        slot->blob = blob;
    }

    // Store metadata atomically
    atomic_store_explicit(&slot->offset, offset, memory_order_release);
    atomic_store_explicit(&slot->ts_us, ts_wall, memory_order_release);
    atomic_store_explicit(&slot->key_len, key_len, memory_order_release);
    atomic_store_explicit(&slot->val_len, val_len, memory_order_release);
    atomic_store_explicit(&slot->partition, partition, memory_order_release);
    atomic_store_explicit(&slot->flags, (total_size <= sizeof(slot->inline_data)) ? 1 : 3, memory_order_release);

    /* size accounting + opportunistic sweep */
    atomic_fetch_add_explicit(&p->live_bytes, (uint64_t)total_size, memory_order_relaxed);
    rf_size_sweep_maybe(p);

    // SEALED: Build AOF record for immediate durability
    size_t aof_size = sizeof(aof_msg_rec_t) + total_size;
    void *sealed_buffer = slab_alloc(aof_size);
    if (!sealed_buffer) return -5;

    aof_msg_rec_t *aof_rec = (aof_msg_rec_t*)sealed_buffer;
    aof_rec->topic_id = t->id;
    aof_rec->partition = (uint16_t)partition;
    aof_rec->reserved = RF_TS_FLAG_WALL;
    aof_rec->offset = offset;
    aof_rec->ts_us = ts_wall;
    aof_rec->key_len = key_len;
    aof_rec->val_len = val_len;

    // Copy data to sealed buffer
    memcpy((uint8_t*)sealed_buffer + sizeof(aof_msg_rec_t), slot->blob, total_size);

    /* === ROCKET SWITCH ===
       RF_ROCKET unset/0  -> durable sealed append (existing behavior)
       RF_ROCKET set(!=0) -> in-memory broadcast to all workers via rf_bus_publish(),
                             batch_id=0 (non-durable) */
    uint64_t out_bid = 0;
    const char *rocket_env = getenv("RF_ROCKET");
    const int rocket_on = (rocket_env && rocket_env[0] != '\0' && rocket_env[0] != '0');

    if (rocket_on) {
        /* fast cross-worker promotion; no disk/fsync on the hot path */
        /* NOTE: rf_bus_publish() should be best-effort; if you prefer a fallback to durable path
                 on error, check its return and call AOF_append_sealed() instead. */
        (void)rf_bus_publish(AOF_REC_BROKER_MSG, sealed_buffer, (uint32_t)aof_size);
        out_bid = 0; /* signals "not durable" to callers */
    } else {
        /* original sealed (durable) path */
        out_bid = AOF_append_sealed(AOF_REC_BROKER_MSG, sealed_buffer, aof_size);


        (void)rf_bus_publish(AOF_REC_BROKER_MSG, sealed_buffer, (uint32_t)aof_size);
    }

    if (RAMForge_HA_is_enabled()) {
        RAMForge_HA_replicate_write(AOF_REC_BROKER_MSG, sealed_buffer, aof_size);
    }

    if (batch_id)   *batch_id   = out_bid;
    if (out_offset) *out_offset = offset;

    slab_free(sealed_buffer);
    return 0;
}

// ═══════════════════════════════════════════════════════════════════════════════
// Lock-Free High-Performance Consumer
// ═══════════════════════════════════════════════════════════════════════════════

size_t rf_consume(const char *topic, const char *group, int partition,
                  uint64_t from_exclusive, size_t max_msgs,
                  rf_msg_view_t *out, size_t out_cap, uint64_t *next_offset_out) {
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

    /* Upper bound must be the set of offsets that have been assigned. */
    uint64_t assigned_tail = atomic_load_explicit(&p->next_offset, memory_order_acquire);
    uint64_t current_offset = from_exclusive + 1;
    /* live window lower bound due to capacity */
    uint64_t min_available = (assigned_tail > p->capacity) ? (assigned_tail - p->capacity) : 0;
    if (current_offset < min_available) current_offset = min_available;

    /* NEW: skip any offsets below the delete floor */
    uint64_t floor = atomic_load_explicit(&p->delete_floor, memory_order_acquire);
    if (current_offset < floor) current_offset = floor;

    uint64_t avail = (assigned_tail > current_offset) ? (assigned_tail - current_offset) : 0;
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
        uint64_t slot_idx = current_offset & p->capacity_mask;
        rf_msg_slot_t *slot = &ring[slot_idx];

        uint16_t flags = atomic_load_explicit(&slot->flags, memory_order_acquire);
        uint64_t so    = atomic_load_explicit(&slot->offset, memory_order_acquire);

        if ((flags & 1) && so == current_offset) {
            uint64_t wh = atomic_load_explicit(&p->write_head, memory_order_acquire);
            uint64_t oldest = (wh > p->capacity) ? (wh - p->capacity) : 0;

            if (so < oldest) {
                current_offset++;
                scanned++;
                continue;
            }
            if (cutoff) {
                uint64_t ts = atomic_load_explicit(&slot->ts_us, memory_order_acquire);
                if (ts && ts < cutoff) {
                    /* expired → invalidate and skip without producing (also updates live_bytes) */
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
            /* Decide what kind of "hole" this is. */
            if (so != current_offset) {
                /* Definitely not our logical slot (evicted/overwritten/tombstoned) → skip it. */
                current_offset++;
                scanned++;
                continue;
            }
            /* so == current_offset but not valid yet:
   either still being published or has just been invalidated.
   If it’s still being published, skipping would drop it → stop here. */
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
    if (!g_shared_storage) return 0;
    int k = offset_key_hash(topic, group, partition);
    return shared_storage_set(g_shared_storage, k, &off, sizeof(off));
}

int rf_commit(const char *topic, const char *group, int partition, uint64_t offset) {
    if (!topic || !group) return -1;

    uint64_t prev = 0;
    if (rf_offset_load(topic, group, partition, &prev) && offset < prev) {
        return 0; /* ignore backwards commit */
    }
    rf_offset_store(topic, group, partition, offset);

    aof_commit_rec_t c = {0};
    c.topic_id = rf_topic_id(topic);
    c.partition = (uint16_t)partition;
    c.offset = offset;
    strncpy(c.topic, topic, RF_MAX_TOPIC_LEN - 1);
    strncpy(c.group, group, RF_MAX_GROUP_LEN - 1);
    AOF_append_sealed(AOF_REC_BROKER_COMMIT, &c, sizeof(c));

    return 0;
}

// ═══════════════════════════════════════════════════════════════════════════════
// Optimized AOF Replay with Batch Processing
// ═══════════════════════════════════════════════════════════════════════════════

static void rf_broker_replay_aof_batch(uint32_t rec_id, const void *data, size_t sz) {
    (void)rec_id;
    if (sz < sizeof(aof_msg_rec_t)) return;

    const uint8_t *ptr = (const uint8_t*)data;
    const uint8_t *end = ptr + sz;
    uint64_t now_wall = ttl_now_wall_us();

    // We’ll track per-(topic,partition) max offset we see in this batch
    // (small cache, linear scan is fine for small topic counts)
    while (ptr + sizeof(aof_msg_rec_t) <= end) {
        const aof_msg_rec_t *r = (const aof_msg_rec_t*)ptr;
        size_t record_size = sizeof(aof_msg_rec_t) + r->key_len + r->val_len;
        if (ptr + record_size > end) break; // truncated tail → stop

        // Find topic by id
        rf_topic_t *t = NULL;
        int n = atomic_load_explicit(&g_topic_count, memory_order_acquire);
        for (int i = 0; i < n; i++) {
            if (g_topics[i].id == r->topic_id) {
                t = &g_topics[i];
                break;
            }
        }

        if (!t || r->partition >= t->partitions) {
            ptr += record_size;
            continue;
        }

        rf_partition_t *p = &t->parts[r->partition];
        if (rf_partition_ensure_ring(p) != 0) { ptr += record_size; continue; }
        rf_msg_slot_t *ring = atomic_load_explicit(&p->ring, memory_order_acquire);
        if (!ring) { ptr += record_size; continue; }
        uint64_t floor = atomic_load_explicit(&p->delete_floor, memory_order_acquire);
        if (r->offset < floor) {
            /* skip replaying messages below the floor */
            ptr += record_size;
            continue;
        }
        const uint8_t *kv = ptr + sizeof(*r);
        size_t total_size = r->key_len + r->val_len;

        // *** KEY CHANGE: index ring by logical offset, not by write_head ***
        uint64_t slot_idx = (r->offset) & p->capacity_mask;
        rf_msg_slot_t *slot = &ring[slot_idx];

        uint64_t want = r->offset + 1;
        seed_shared_next_offset(r->topic_id, r->partition, want);
        atomic_max_u64(&p->next_offset, want);
        atomic_max_u64(&p->write_head, want);
        if (rf_slot_ready_for_offset(slot, r->offset)) {
            ptr += record_size;
            continue;
        }
        rf_slot_evict_if_needed(p, slot, r->offset);

        if (total_size <= sizeof(slot->inline_data)) {
            memcpy(slot->inline_data, kv, total_size);
            slot->blob = slot->inline_data;
        } else {
            void *blob = slab_alloc(total_size);
            if (blob) {
                memcpy(blob, kv, total_size);
                slot->blob = blob;
            }
        }

        // Publish metadata (release stores)
        uint64_t ts_norm = normalize_persisted_ts_us(r->ts_us, r->reserved, now_wall);
        atomic_store_explicit(&slot->offset, r->offset, memory_order_release);
        atomic_store_explicit(&slot->ts_us, ts_norm, memory_order_release);
        atomic_store_explicit(&slot->key_len, r->key_len, memory_order_release);
        atomic_store_explicit(&slot->val_len, r->val_len, memory_order_release);
        atomic_store_explicit(&slot->partition, r->partition, memory_order_release);
        atomic_store_explicit(&slot->flags, (total_size <= sizeof(slot->inline_data)) ? 1 : 3, memory_order_release);

        atomic_fetch_add_explicit(&p->live_bytes, (uint64_t)total_size, memory_order_relaxed);

        ptr += record_size;
    }
}

void rf_broker_replay_aof(uint32_t rec_id, const void *data, size_t sz) {
    if (rec_id == AOF_REC_KV_PUT) {
        /* payload: aof_kv_put_t + value bytes */
        if (sz < sizeof(aof_kv_put_t)) return;
        const aof_kv_put_t *h = (const aof_kv_put_t *) data;
        size_t need = sizeof(kvrec_t) + h->value_len;
        if (sizeof(aof_kv_put_t) + h->value_len > sz) return;
        /* stack buffer is fine (values are bounded by MAX_DATA_SIZE upstream) */
        uint8_t tmp[MAX_DATA_SIZE];
        if (need > sizeof(tmp)) return; /* defensive */
        kvrec_t *r = (kvrec_t *) tmp;
        r->cas = h->cas;
        r->expires_us = h->expires_us;
        r->flags = h->flags;
        r->value_len = h->value_len;
        if (h->value_len)
            memcpy(r->value, (const uint8_t *) data + sizeof(*h), h->value_len);
        /* write into shared table */
        (void) shared_storage_set(g_shared_storage, h->skey, r, need);
        return;
    }

    if (rec_id == AOF_REC_KV_DEL) {
        if (sz < sizeof(aof_kv_del_t)) return;
        const aof_kv_del_t *d = (const aof_kv_del_t *) data;
        uint8_t tmp[sizeof(kvrec_t)];
        kvrec_t *r = (kvrec_t *) tmp;
        r->cas = d->cas;
        r->expires_us = 0;
        r->flags = KV_FLAG_TOMBSTONE;
        r->value_len = 0;
        (void) shared_storage_set(g_shared_storage, d->skey, r, sizeof(tmp));
        return;
    }

    if (rec_id == AOF_REC_BROKER_TRUNCATE) {
        if (sz < sizeof(aof_truncate_rec_t)) return;
        const aof_truncate_rec_t *r = (const aof_truncate_rec_t *) data;

        rf_topic_t *t = NULL;
        int n = atomic_load_explicit(&g_topic_count, memory_order_acquire);
        for (int i = 0; i < n; i++) {
            if (g_topics[i].id == r->topic_id) {
                t = &g_topics[i];
                break;
            }
        }
        if (!t || r->partition >= t->partitions) return;

        rf_partition_t *p = &t->parts[r->partition];


        atomic_max_u64(&p->delete_floor, r->before_offset);

        /* Opportunistically invalidate any already-replayed slots below the floor */
        uint64_t wh = atomic_load_explicit(&p->write_head, memory_order_acquire);
        uint64_t oldest = (wh > p->capacity) ? (wh - p->capacity) : 0;
        uint64_t limit = (r->before_offset < wh) ? r->before_offset : wh;
        if (limit > oldest) {
            for (uint64_t o = oldest; o < limit; o++) {
                rf_msg_slot_t *s = &p->ring[o & p->capacity_mask];
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
        if (sz < offsetof(aof_ttl_rec_t, topic)) return;
        aof_ttl_rec_t r = {0};
        size_t copy = sz;
        if (copy > sizeof(r)) copy = sizeof(r);
        memcpy(&r, data, copy);

        r.topic[RF_MAX_TOPIC_LEN - 1] = 0;
        uint64_t ttl_ms = r.ttl_ms;
        uint64_t maxb = (sz >= offsetof(aof_ttl_rec_t, max_bytes) + sizeof(uint64_t)) ? r.max_bytes : 0;

        if (r.topic[0] == 0 || (r.topic[0] == '*' && r.topic[1] == 0)) {
            if (ttl_ms) rf_set_ttl_ms_all(ttl_ms);
            rf_set_max_bytes_all(maxb);
        } else {
            if (ttl_ms) rf_set_ttl_ms(r.topic, r.partition, ttl_ms);
            rf_set_max_bytes(r.topic, r.partition, maxb);
        }
        return;
    }
    // Handle batch messages first
    if (rec_id == AOF_REC_BROKER_MSG_BATCH) {
        rf_broker_replay_aof_batch(rec_id, data, sz);
        return;
    }

    // Original single message handling
    if (rec_id == AOF_REC_BROKER_TOPIC) {
        if (sz < sizeof(uint32_t) + sizeof(uint16_t)) return;
        struct {
            uint32_t id;
            uint16_t parts;
            char name[RF_MAX_TOPIC_LEN];
        } rec;
        memcpy(&rec, data, MIN(sz, sizeof(rec)));
        rf_topic_ensure(rec.name, rec.parts);
        return;
    }

    if (rec_id == AOF_REC_BROKER_MSG) {
        if (sz < sizeof(aof_msg_rec_t)) return;
        const aof_msg_rec_t *r = (const aof_msg_rec_t*)data;
        uint64_t now_wall = ttl_now_wall_us();

        rf_topic_t *t = NULL;
        int n = atomic_load_explicit(&g_topic_count, memory_order_acquire);
        for (int i = 0; i < n; i++) {
            if (g_topics[i].id == r->topic_id) {
                t = &g_topics[i];
                break;
            }
        }
        if (!t || r->partition >= t->partitions) return;

        rf_partition_t *p = &t->parts[r->partition];
        if (rf_partition_ensure_ring(p) != 0) return;
        rf_msg_slot_t *ring = atomic_load_explicit(&p->ring, memory_order_acquire);
        if (!ring) return;

        /* ===== NEW: honor persisted truncate floor ===== */
        uint64_t floor = atomic_load_explicit(&p->delete_floor, memory_order_acquire);
        if (r->offset < floor) return;
        const uint8_t *kv = (const uint8_t*)data + sizeof(*r);
        size_t total_size = r->key_len + r->val_len;

        uint64_t slot_idx = (r->offset) & p->capacity_mask; // <-- change
        rf_msg_slot_t *slot = &ring[slot_idx];

        uint64_t want = r->offset + 1;
        seed_shared_next_offset(r->topic_id, r->partition, want);
        atomic_max_u64(&p->next_offset, want);
        atomic_max_u64(&p->write_head, want);
        if (rf_slot_ready_for_offset(slot, r->offset)) return;
        rf_slot_evict_if_needed(p, slot, r->offset);

        if (total_size <= sizeof(slot->inline_data)) {
            memcpy(slot->inline_data, kv, total_size);
            slot->blob = slot->inline_data;
        } else {
            void *blob = slab_alloc(total_size);
            if (blob) {
                memcpy(blob, kv, total_size);
                slot->blob = blob;
            }
        }

        atomic_store_explicit(&slot->offset, r->offset, memory_order_release);
        uint64_t ts_norm = normalize_persisted_ts_us(r->ts_us, r->reserved, now_wall);
        atomic_store_explicit(&slot->ts_us, ts_norm, memory_order_release);
        atomic_store_explicit(&slot->key_len, r->key_len, memory_order_release);
        atomic_store_explicit(&slot->val_len, r->val_len, memory_order_release);
        atomic_store_explicit(&slot->partition, r->partition, memory_order_release);
        atomic_store_explicit(&slot->flags, (total_size <= sizeof(slot->inline_data)) ? 1 : 3, memory_order_release);

        atomic_fetch_add_explicit(&p->live_bytes, (uint64_t)total_size, memory_order_relaxed);

        /* tails already bumped above */
        return;
    }

    if (rec_id == AOF_REC_BROKER_COMMIT) {
        if (sz < sizeof(aof_commit_rec_t)) return;
        const aof_commit_rec_t *c = (const aof_commit_rec_t*)data;
        rf_offset_store(c->topic, c->group, c->partition, c->offset);
        return;
    }

    if (rec_id == AOF_REC_BROKER_MSG_IDEMP) {
        if (sz < sizeof(aof_msg_rec_t) + sizeof(uint64_t)) return;
        const aof_msg_rec_t *r = (const aof_msg_rec_t*)data;
        const uint8_t *kv = (const uint8_t*)data + sizeof(*r);
        size_t tot = r->key_len + r->val_len;
        if (sizeof(*r) + tot + sizeof(uint64_t) > sz) return;
        uint64_t fp64;
        memcpy(&fp64, kv + tot, sizeof fp64);

        /* reuse your single-message replay logic */
        replay_one_msg_rec(r, kv);

        /* install idempotency mapping */
        rf_topic_t *t = NULL;
        int n = atomic_load_explicit(&g_topic_count, memory_order_acquire);
        for (int i=0;i<n;i++)
            if (g_topics[i].id == r->topic_id) { t = &g_topics[i]; break; }
        if (t) {
            idemp_put(t->name, r->partition, &fp64, sizeof fp64, fp64, r->offset);
        }
        return;
    }

    if (rec_id == AOF_REC_BROKER_MSG_BATCH_IDEMP) {
        /* walk messages, last 8 bytes are fp64 trailer */
        if (sz < sizeof(uint64_t)) return;
        const uint8_t *ptr = (const uint8_t*)data;
        const uint8_t *end = (const uint8_t*)data + sz - sizeof(uint64_t);

        /* Rebuild messages */
        while (ptr + sizeof(aof_msg_rec_t) <= end) {
            const aof_msg_rec_t *r = (const aof_msg_rec_t*)ptr;
            size_t tot = sizeof(*r) + r->key_len + r->val_len;
            if (ptr + tot > end) break;
            const uint8_t *kv = ptr + sizeof(*r);
            replay_one_msg_rec(r, kv);
            ptr += tot;
        }

        /* grab fp64 trailer */
        uint64_t fp64;
        memcpy(&fp64, end, sizeof fp64);

        /* install mapping using the batch first_offset */
        /* We can find first_offset by scanning first message in the batch: */
        const aof_msg_rec_t *first = (const aof_msg_rec_t*)data;
        rf_topic_t *t = NULL;
        int n = atomic_load_explicit(&g_topic_count, memory_order_acquire);
        for (int i=0;i<n;i++)
            if (g_topics[i].id == first->topic_id) { t = &g_topics[i]; break; }
        if (t) {
            idemp_put(t->name, first->partition, &fp64, sizeof fp64, fp64, first->offset);
        }
        return;
    }
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

            // Wait for all operations to complete
            uint64_t write_head = atomic_load_explicit(&part->write_head, memory_order_acquire);

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

int rf_produce_batch_sealed(const char *topic, int partition,
                            const rf_msg_view_t *messages, size_t count,
                            uint64_t *first_offset, uint64_t *batch_id) {
    if (!topic || !messages || count == 0) return -1;
    rf_topic_ensure(topic, 0);
    rf_topic_t *t = find_topic_lockfree(topic, rf_topic_id(topic));
    if (!t) return -2;

    if (partition < 0) {
        partition = simd_hash_djb2(topic, strlen(topic)) & (t->partitions - 1);
    }
    if (partition >= t->partitions) return -3;

    rf_partition_t *p = &t->parts[partition];

    if (rf_partition_ensure_ring(p) != 0) return -4;
    rf_msg_slot_t *ring = atomic_load_explicit(&p->ring, memory_order_acquire);
    if (!ring) return -4;

    /* Reserve slots atomically */
    uint64_t offset_start = alloc_offsets_shared(t, p, partition, (uint64_t)count);
    if (first_offset) *first_offset = offset_start;
    uint64_t ts_wall = wall_us_raw();

    /* Calculate total AOF size for sealed batch */
    size_t total_aof_size = 0;
    for (size_t i = 0; i < count; i++) {
        total_aof_size += sizeof(aof_msg_rec_t) + messages[i].key_len + messages[i].val_len;
    }

    /* Allocate sealed batch buffer */
    void *sealed_batch_buffer = slab_alloc(total_aof_size);
    if (!sealed_batch_buffer) return -4;
    uint8_t *aof_write_ptr = (uint8_t*)sealed_batch_buffer;

    /* Process messages with minimal overhead - store in ring + build AOF */
    for (size_t i = 0; i < count; i++) {
        uint64_t msg_offset = offset_start + i;
        uint64_t slot_idx = msg_offset & p->capacity_mask;
        rf_msg_slot_t *slot = &ring[slot_idx];
        rf_slot_evict_if_needed(p, slot, msg_offset);
        const rf_msg_view_t *msg = &messages[i];

        size_t total_size = msg->key_len + msg->val_len;
        /* Zero-copy message storage in ring buffer */
        if (total_size <= sizeof(slot->inline_data)) {
            /* Inline storage - true zero-copy */
            if (msg->key && msg->key_len) {
                memcpy(slot->inline_data, msg->key, msg->key_len);
            }
            if (msg->val && msg->val_len) {
                memcpy(slot->inline_data + msg->key_len, msg->val, msg->val_len);
            }
            slot->blob = slot->inline_data;
        } else {
            /* Large message - use slab allocator */
            void *blob = slab_alloc(total_size);
            if (!blob) {
                slab_free(sealed_batch_buffer);
                return -5;
            }
            if (msg->key && msg->key_len) memcpy(blob, msg->key, msg->key_len);
            if (msg->val && msg->val_len) memcpy((uint8_t*)blob + msg->key_len, msg->val, msg->val_len);
            slot->blob = blob;
        }

        /* Store message metadata atomically */
        atomic_store_explicit(&slot->offset, msg_offset, memory_order_release);
        atomic_store_explicit(&slot->ts_us, ts_wall, memory_order_release);
        atomic_store_explicit(&slot->key_len, msg->key_len, memory_order_release);
        atomic_store_explicit(&slot->val_len, msg->val_len, memory_order_release);
        atomic_store_explicit(&slot->partition, partition, memory_order_release);
        atomic_store_explicit(&slot->flags, (total_size <= sizeof(slot->inline_data)) ? 1 : 3, memory_order_release);
        atomic_fetch_add_explicit(&p->live_bytes, (uint64_t)total_size, memory_order_relaxed);

        /* Build AOF record in sealed batch buffer */
        aof_msg_rec_t *aof_rec = (aof_msg_rec_t*)aof_write_ptr;
        aof_rec->topic_id = t->id;
        aof_rec->partition = (uint16_t)partition;
        aof_rec->reserved = RF_TS_FLAG_WALL;
        aof_rec->offset = msg_offset;
        aof_rec->ts_us = ts_wall;
        aof_rec->key_len = msg->key_len;
        aof_rec->val_len = msg->val_len;
        aof_write_ptr += sizeof(aof_msg_rec_t);

        /* Copy key/value data to AOF record */
        if (total_size <= sizeof(slot->inline_data)) {
            memcpy(aof_write_ptr, slot->inline_data, total_size);
        } else {
            memcpy(aof_write_ptr, slot->blob, total_size);
        }
        aof_write_ptr += total_size;
    }

    atomic_max_u64(&p->next_offset, offset_start + count);
    atomic_max_u64(&p->write_head, offset_start + count);

    /* SEALED APPEND: Write to AOF with zero-loss guarantee */
    /* ROCKET MODE: promote to all workers' memory without fsync */
#if 1
    const char *rocket = getenv("RF_ROCKET");
    if (rocket && atoi(rocket) != 0) {
        rf_bus_publish(AOF_REC_BROKER_MSG_BATCH, sealed_batch_buffer, (uint32_t)total_aof_size);
        if (batch_id) *batch_id = 0; /* no durable batch id */
    } else
#endif
    {
        /* default durable path (unchanged) */
        uint64_t aof_batch_id = AOF_append_sealed(AOF_REC_BROKER_MSG_BATCH, sealed_batch_buffer, total_aof_size);

        (void)rf_bus_publish(AOF_REC_BROKER_MSG_BATCH, sealed_batch_buffer, (uint32_t)total_aof_size);
        if (batch_id) *batch_id = aof_batch_id;
    }

    /* Clean up */
    if (RAMForge_HA_is_enabled()) {
        RAMForge_HA_replicate_write(AOF_REC_BROKER_MSG_BATCH, sealed_batch_buffer, total_aof_size);
    }
    slab_free(sealed_batch_buffer);

    rf_size_sweep_maybe(p);
    return 0; /* Success - batch is sealed and will be synced by background thread */
}

int rf_produce_sealed_idemp(const char *topic, int partition,
                            const void *key, size_t key_len,
                            const void *val, size_t val_len,
                            const void *idemp_key, size_t idemp_key_len,
                            uint64_t *out_offset, uint64_t *batch_id, int *is_duplicate) {
    *is_duplicate = 0;
    rf_topic_ensure(topic, 0);
    rf_topic_t *t = find_topic_lockfree(topic, rf_topic_id(topic));
    if (!t) return -2;
    if (partition < 0) partition = simd_hash_djb2(topic, strlen(topic)) & (t->partitions - 1);
    if (partition >= t->partitions) return -3;

    /* fast pre-check in shmem (cross-worker) */
    uint64_t fp64 = idemp_fp64(topic, partition, idemp_key, idemp_key_len);
    uint64_t existing_off = 0;
    if (idemp_get(topic, partition, idemp_key, idemp_key_len, fp64, &existing_off)) {
        if (out_offset) *out_offset = existing_off;
        *is_duplicate = 1;
        return 0; /* Early return - no offset consumed */
    }

    /* Produce into ring + build sealed payload that ALSO carries fp64 */
    rf_partition_t *p = &t->parts[partition];
    if (rf_partition_ensure_ring(p) != 0) return -4;
    rf_msg_slot_t *ring = atomic_load_explicit(&p->ring, memory_order_acquire);
    if (!ring) return -4;
    uint64_t offset = alloc_offset_shared(t, p, partition);
    uint64_t slot_idx = offset & p->capacity_mask;

    rf_msg_slot_t *slot = &ring[slot_idx];
    rf_slot_evict_if_needed(p, slot, offset);

    size_t total = key_len + val_len;
    uint64_t ts_wall = wall_us_raw();

    if (total <= sizeof(slot->inline_data)) {
        if (key && key_len) memcpy(slot->inline_data, key, key_len);
        if (val && val_len) memcpy(slot->inline_data + key_len, val, val_len);
        slot->blob = slot->inline_data;
    } else {
        void *blob = slab_alloc(total);
        if (!blob) return -4;
        if (key && key_len) memcpy(blob, key, key_len);
        if (val && val_len) memcpy((uint8_t*)blob + key_len, val, val_len);
        slot->blob = blob;
    }

    atomic_store_explicit(&slot->offset, offset, memory_order_release);
    atomic_store_explicit(&slot->ts_us, ts_wall, memory_order_release);
    atomic_store_explicit(&slot->key_len, key_len, memory_order_release);
    atomic_store_explicit(&slot->val_len, val_len, memory_order_release);
    atomic_store_explicit(&slot->partition, partition, memory_order_release);
    atomic_store_explicit(&slot->flags, (total <= sizeof(slot->inline_data)) ? 1 : 3, memory_order_release);

    atomic_max_u64(&p->next_offset, offset + 1);
    atomic_max_u64(&p->write_head, offset + 1);

    atomic_fetch_add_explicit(&p->live_bytes, (uint64_t) total, memory_order_relaxed);
    rf_size_sweep_maybe(p);

    /* Build sealed record: [aof_msg_rec_t][kv][uint64_t fp64] */
    size_t aof_sz = sizeof(aof_msg_rec_t) + total + sizeof(uint64_t);
    void *buf = slab_alloc(aof_sz);
    if (!buf) return -5;

    aof_msg_rec_t *r = (aof_msg_rec_t*)buf;
    r->topic_id = t->id;
    r->partition = (uint16_t)partition;
    r->reserved = RF_TS_FLAG_WALL;
    r->offset = offset;
    r->ts_us = ts_wall;
    r->key_len = key_len;
    r->val_len = val_len;

    uint8_t *w = (uint8_t*)buf + sizeof(*r);
    if (total <= sizeof(slot->inline_data)) memcpy(w, slot->inline_data, total);
    else memcpy(w, slot->blob, total);
    memcpy(w + total, &fp64, sizeof(fp64));

    uint64_t bid = AOF_append_sealed(AOF_REC_BROKER_MSG_IDEMP, buf, aof_sz);

    (void)rf_bus_publish(AOF_REC_BROKER_MSG_IDEMP, buf, (uint32_t)aof_sz);

    if (RAMForge_HA_is_enabled()) {
        RAMForge_HA_replicate_write(AOF_REC_BROKER_MSG_IDEMP, buf, aof_sz);
    }
    slab_free(buf);

    if (batch_id) *batch_id = bid;
    if (out_offset) *out_offset = offset;

    /* Best-effort insert immediately so concurrent retries dedupe; crash-safe rebuild happens via sealed replay anyway. */
    idemp_put(topic, partition, idemp_key, idemp_key_len, fp64, offset);
    return 0;
}

int rf_produce_batch_sealed_idemp(const char *topic, int partition, const rf_msg_view_t *messages, size_t count,
                                  const void *idemp_key, size_t idemp_key_len, uint64_t *first_offset,
                                  uint64_t *batch_id, int *is_duplicate) {
    *is_duplicate = 0;
    rf_topic_ensure(topic, 0);
    rf_topic_t *t = find_topic_lockfree(topic, rf_topic_id(topic));
    if (!t) return -2;
    if (partition < 0) partition = simd_hash_djb2(topic, strlen(topic)) & (t->partitions - 1);
    if (partition >= t->partitions) return -3;
    uint64_t fp64 = idemp_fp64(topic, partition, idemp_key, idemp_key_len);
    uint64_t existing_first = 0;
    if (idemp_get(topic, partition, idemp_key, idemp_key_len, fp64, &existing_first)) {
        if (first_offset) *first_offset = existing_first;
        *is_duplicate = 1;
        return 0;
    }
    rf_partition_t *p = &t->parts[partition];

    if (rf_partition_ensure_ring(p) != 0) return -4;
    rf_msg_slot_t *ring = atomic_load_explicit(&p->ring, memory_order_acquire);
    if (!ring) return -4;
    uint64_t off0 = alloc_offsets_shared(t, p, partition, (uint64_t)count);
    if (first_offset) *first_offset = off0;
    uint64_t ts_wall = wall_us_raw();
    /* Compute sealed payload size: for each msg: [aof_msg_rec_t][kv] and after the last msg, append fp64 once */ size_t total_aof = sizeof(uint64_t);
    /* fp64 trailer */ for (size_t i = 0; i < count; i++) total_aof += sizeof(aof_msg_rec_t) + messages[i].key_len +
                                                                       messages[i].val_len;
    void *buf = slab_alloc(total_aof);
    if (!buf) return -4;
    uint8_t *w = (uint8_t *) buf;
    uint64_t batch_bytes = 0;  /* ---- NEW: accumulate for live_bytes ---- */
    for (size_t i = 0; i < count; i++) {
        uint64_t o        = off0 + i;
        uint64_t slot_idx = o & p->capacity_mask;
        rf_msg_slot_t *slot = &ring[slot_idx];
        rf_slot_evict_if_needed(p, slot, o);
        const rf_msg_view_t *m = &messages[i];
        size_t tot = m->key_len + m->val_len;
        if (tot <= sizeof(slot->inline_data)) {
            if (m->key && m->key_len) memcpy(slot->inline_data, m->key, m->key_len);
            if (m->val && m->val_len) memcpy(slot->inline_data + m->key_len, m->val, m->val_len);
            slot->blob = slot->inline_data;
        } else {
            void *blob = slab_alloc(tot);
            if (!blob) {
                slab_free(buf);
                return -5;
            }
            if (m->key && m->key_len) memcpy(blob, m->key, m->key_len);
            if (m->val && m->val_len) memcpy((uint8_t *) blob + m->key_len, m->val, m->val_len);
            slot->blob = blob;
        }
        atomic_store_explicit(&slot->offset, o, memory_order_release);
        atomic_store_explicit(&slot->ts_us, ts_wall, memory_order_release);
        atomic_store_explicit(&slot->key_len, m->key_len, memory_order_release);
        atomic_store_explicit(&slot->val_len, m->val_len, memory_order_release);
        atomic_store_explicit(&slot->partition, partition, memory_order_release);
        atomic_store_explicit(&slot->flags, (tot <= sizeof(slot->inline_data)) ? 1 : 3, memory_order_release);


        batch_bytes += (uint64_t)tot;  /* ---- NEW ---- */
        aof_msg_rec_t *r = (aof_msg_rec_t *) w;
        r->topic_id = t->id;
        r->partition = (uint16_t) partition;
        r->reserved = RF_TS_FLAG_WALL;
        r->offset = o;
        r->ts_us = ts_wall;
        r->key_len = m->key_len;
        r->val_len = m->val_len;
        w += sizeof(*r);
        if (tot <= sizeof(slot->inline_data)) {
            memcpy(w, slot->inline_data, tot);
        } else {
            memcpy(w, slot->blob, tot);
        }
        w += tot;
    }

    atomic_max_u64(&p->next_offset, off0 + count);
    atomic_max_u64(&p->write_head, off0 + count);
    memcpy(w, &fp64, sizeof fp64);
    uint64_t bid = AOF_append_sealed(AOF_REC_BROKER_MSG_BATCH_IDEMP, buf, total_aof);

    (void)rf_bus_publish(AOF_REC_BROKER_MSG_BATCH_IDEMP, buf, (uint32_t)total_aof);
    if (RAMForge_HA_is_enabled()) {
        RAMForge_HA_replicate_write(AOF_REC_BROKER_MSG_BATCH_IDEMP, buf, total_aof);
    }

    slab_free(buf);
    if (batch_id) *batch_id = bid;

    atomic_fetch_add_explicit(&p->live_bytes, batch_bytes, memory_order_relaxed);
    rf_size_sweep_maybe(p);

    /* immediate best-effort insert for live dedupe; crash-safe via replay */
    idemp_put(topic, partition, idemp_key,
              idemp_key_len, fp64, off0);
    return 0;
}
