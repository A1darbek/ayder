// rf_broker.h
#pragma once
#include <stddef.h>
#include <stdint.h>
#include <pthread.h>

#define RF_MAX_TOPIC_LEN     64
#define RF_MAX_GROUP_LEN     64
#define RF_MAX_PARTITIONS    128

// AOF record types for broker
#define AOF_REC_BROKER_MSG      0xB10C0001u
#define AOF_REC_BROKER_COMMIT   0xB10C0002u
#define AOF_REC_BROKER_TOPIC    0xB10C0003u
#define AOF_REC_BROKER_MSG_IDEMP      0xB10C1001u
#define AOF_REC_BROKER_MSG_BATCH_IDEMP   0xB10C1002u
#define AOF_REC_IDEMP_INDEX 0xB10C1003u
#define AOF_REC_BROKER_TRUNCATE 0x4254 /* 'BT' */
#define AOF_REC_HA_FORWARD 0xA0F00D  /* arbitrary new bus record id */
#define AOF_REC_HA_MEMBERSHIP_FORWARD 0xA0F00E /* agent->worker0 membership control */

typedef struct {
    uint32_t type;   /* e.g. AOF_REC_BROKER_MSG or _BATCH */
    uint32_t size;   /* payload bytes */
    /* uint8_t data[size] follows */
} ha_fwd_rec_t;

typedef enum {
    HA_MEM_FWD_OP_ADD = 1,
    HA_MEM_FWD_OP_PROMOTE = 2,
    HA_MEM_FWD_OP_REMOVE = 3
} ha_mem_fwd_op_t;

typedef struct {
    uint64_t req_id;
    uint32_t op;            /* ha_mem_fwd_op_t */
    uint32_t timeout_ms;
    int32_t priority;
    int32_t reserved;
    char node_id[64];
    char advertise_addr[256];
    char http_addr[256];
} __attribute__((packed)) ha_mem_fwd_req_t;

typedef struct {
    uint64_t req_id;
    int32_t rc;
    int32_t out_index;
} __attribute__((packed)) ha_mem_fwd_rsp_t;

int ha_fwd_send(uint32_t type, const void *payload, uint32_t size);

#pragma pack(push,1)
typedef struct {
    int32_t  skey;        /* kv_int_key(ns,key) computed by writer */
    uint64_t cas;
    uint64_t expires_us;
    uint32_t flags;
    uint32_t value_len;   /* followed by value bytes */
} aof_kv_put_t;

typedef struct {
    int32_t  skey;
    uint64_t cas;         /* new CAS (tombstone record carries cas = prev+1) */
    uint32_t flags;       /* will be KV_FLAG_TOMBSTONE */
} aof_kv_del_t;
#pragma pack(pop)


typedef struct {
    uint64_t fp64;     /* full fingerprint of (topic,part,id_key) */
    uint64_t offset;   /* original offset we committed */
} __attribute__((packed)) aof_idemp_val_t;



typedef struct {
    uint32_t topic_id;
    uint16_t partition;
    uint16_t reserved;
    uint64_t idk_fp;        // 64-bit fingerprint of idempotency_key
    uint64_t first_offset;  // for single: offset; for batch: first_offset
    uint64_t ts_us;         // optional (for debugging/metrics)
} __attribute__((packed)) aof_idemp_index_t;

typedef struct {
    uint32_t topic_id;
    uint16_t partition;
    uint16_t reserved;
    uint64_t offset;
    uint64_t ts_us;
    uint32_t key_len;
    uint32_t val_len;
    /* key | value bytes follow */
} __attribute__((packed)) aof_msg_rec_t;

typedef struct {
    const void *key; size_t key_len;
    const void *val; size_t val_len;
    uint64_t    offset;
    uint64_t    ts_us;
    int         partition;
} rf_msg_view_t;

// ── Producer idempotency tuple
typedef struct {
    uint64_t producer_id;   // 0 => idempotency disabled
    uint32_t epoch;
    uint64_t seq;
} rf_idemp_t;

// ── On-disk idempotent message header (prepended to aof_msg_rec_t)
typedef struct {
    uint64_t producer_id;
    uint32_t epoch;
    uint32_t _pad;
    uint64_t seq;
} __attribute__((packed)) aof_idemp_hdr_t;

// ── Watermark record persisted to main AOF (tiny)
typedef struct {
    uint64_t producer_id;
    uint32_t epoch;
    uint32_t _pad0;
    uint32_t topic_id;
    uint16_t partition;
    uint16_t _pad1;
    uint64_t next_committed_seq;  // first not-yet-committed
} __attribute__((packed)) aof_idemp_wm_t;

typedef struct {
    int32_t  partition;         // -1 = all partitions of the topic; ignored if topic=="*"
    uint64_t ttl_ms;            // 0 disables TTL
    uint64_t max_bytes;                  /* 0 = disabled (ADDED; optional on old replays) */
    char     topic[RF_MAX_TOPIC_LEN]; // "*" or empty => global
} __attribute__((packed)) aof_ttl_rec_t;

typedef struct {
    uint32_t topic_id;
    uint16_t partition;
    uint16_t reserved;
    uint64_t before_offset; // delete all offsets < before_offset
} __attribute__((packed)) aof_truncate_rec_t;

typedef struct {
    _Atomic uint32_t valid;     /* 0=empty, 1=ready */
    uint32_t         rec_id;
    uint32_t         size;
    uint32_t         off;       /* offset into heap */
    uint64_t         seq;       /* global sequence (monotonic) */
} bus_desc_t;

typedef struct {
    /* ring of descriptors */
    bus_desc_t *desc;
    uint32_t    desc_mask;

    /* shared heap for payload bytes (wrap-around) */
    uint8_t    *heap;
    uint32_t    heap_size;

    /* producers advance these monotonically (wrap logic on consumers) */
    _Atomic uint64_t data_head;
    _Atomic uint64_t desc_head;

    /* (optional) stats */
    _Atomic uint64_t drops;
} rf_bus_t;


typedef struct {
    uint32_t topic_id;
    uint16_t partition;
    uint16_t reserved;
    uint64_t offset;
    char topic[RF_MAX_TOPIC_LEN];
    char group[RF_MAX_GROUP_LEN];
} __attribute__((packed)) aof_commit_rec_t;
typedef void (*rf_push_fn)(const rf_msg_view_t *msg, void *user);

// Zero-copy message slot using memory mapping for large messages
typedef struct rf_msg_slot {
    _Atomic uint64_t offset;
    _Atomic uint64_t ts_us;
    _Atomic uint32_t key_len;
    _Atomic uint32_t val_len;
    _Atomic uint16_t partition;
    _Atomic uint16_t flags;      // bit 0: valid, bit 1: large_msg
    void *blob;                  // [key | val] - either inline or mmap'd
    char inline_data[280];       // Small messages inline (zero-copy)
} rf_msg_slot_t __attribute__((aligned(64))); // Cache line aligned



// Lock-free subscriber list using RCU-style updates
typedef struct rf_sub {
    rf_push_fn cb;
    void *user;
    _Atomic(struct rf_sub*) next;
    _Atomic uint32_t ref_count;
} rf_sub_t;

typedef struct {
    // Producer side (single writer)
    _Atomic uint64_t write_head;
    _Atomic uint64_t next_offset;
    char _pad1[56]; // Avoid false sharing

    // Consumer side (multiple readers)
    _Atomic uint64_t read_tail;
    char _pad2[56]; // Avoid false sharing

    // Ring buffer - power of 2 size for fast modulo
    _Atomic(rf_msg_slot_t*) ring;
    uint64_t capacity_mask; // capacity - 1 for fast modulo
    size_t capacity;

    // Subscriber management (RCU-style)
    _Atomic(rf_sub_t*) subscribers;
    _Atomic uint32_t sub_epoch;

    // Memory pool for large messages
    void *large_msg_pool;
    _Atomic uint32_t pool_head;

    /* Retention / TTL */
    _Atomic uint64_t ttl_window_us;  /* 0 = disabled; else messages older than now-ttl_window_us are expired */
    _Atomic uint64_t ttl_ms;        /* 0 = disabled (rolling TTL) */
    _Atomic uint64_t last_ttl_gc_us;  /* last time we ran TTL GC (best-effort) */

    _Atomic uint64_t retain_max_bytes;  /* 0 = disabled */
    _Atomic uint64_t live_bytes;        /* bytes currently visible (payload only) */
    _Atomic uint64_t last_size_gc_us;   /* throttle size sweeps */
    _Atomic uint64_t size_gc_cursor;    /* where last size sweep stopped (offset relative to oldest) */

    /* NEW: hard floor – offsets strictly less than this are deleted */
    _Atomic uint64_t delete_floor;
} rf_partition_t __attribute__((aligned(64)));

typedef struct {
    char name[RF_MAX_TOPIC_LEN];
    uint32_t id;
    uint32_t hash; // Precomputed hash for fast lookup
    int partitions;
    rf_partition_t *parts;
} rf_topic_t;

/* Init/Shutdown */
int  rf_broker_init(size_t ring_per_partition, int default_partitions);
void rf_broker_shutdown(void);

/* Call once in the parent *before forking* (so the mapping is shared).   */
int  rf_bus_init_pre_fork(size_t heap_bytes /* e.g. 64MB */, size_t desc_count /* power of 2 e.g. 8192 */);

/* Call in every worker after startup to begin promoting bus payloads locally. */
void rf_bus_start_promoter(void);

/* Publish one record: rec_id is one of AOF_REC_*; payload is your existing packed blob. */
int  rf_bus_publish(int rec_id, const void *payload, uint32_t size);

int rf_delete_before(const char *topic, int partition, uint64_t before_offset,
                     uint64_t *deleted_count, uint64_t *freed_bytes);
/* Topics */
uint32_t rf_topic_id(const char *name);                // stable hash
int      rf_topic_ensure(const char *name, int partitions); // idempotent

/* Produce (AOF persisted) */
int rf_produce_sealed(const char *topic, int partition,
                      const void *key, size_t key_len,
                      const void *val, size_t val_len,
                      uint64_t *out_offset, uint64_t *batch_id);
int rf_produce_sealed_idemp(const char *topic, int partition,
                            const void *key, size_t key_len,
                            const void *val, size_t val_len,
                            const void *idemp_key, size_t idemp_key_len,
                            uint64_t *out_offset, uint64_t *batch_id,
                            int *is_duplicate);

int rf_produce_batch_sealed_idemp(const char *topic, int partition,
                                  const rf_msg_view_t *messages, size_t count,
                                  const void *idemp_key, size_t idemp_key_len,
                                  uint64_t *first_offset, uint64_t *batch_id,
                                  int *is_duplicate);

/* Consume (pull) */
size_t rf_consume(const char *topic, const char *group, int partition,
                  uint64_t from_exclusive, size_t max_msgs,
                  rf_msg_view_t *out, size_t out_cap,
                  uint64_t *next_offset);

/* Commit offset (records to shared_storage + AOF) */
int rf_commit(const char *topic, const char *group, int partition, uint64_t offset);

/* AOF replay hook */
void rf_broker_replay_aof(uint32_t rec_id, const void *data, size_t sz);

/* Offset helpers (shared_storage) */
int  rf_offset_load (const char *topic, const char *group, int partition, uint64_t *out);
int  rf_offset_store(const char *topic, const char *group, int partition, uint64_t off);
int rf_produce_batch_sealed(const char *topic, int partition,
                            const rf_msg_view_t *messages, size_t count,
                            uint64_t *first_offset, uint64_t *batch_id);

int rf_set_ttl_ms(const char *topic, int partition, uint64_t ttl_ms);
/* apply TTL to ALL partitions of ALL topics */
int rf_set_ttl_ms_all(uint64_t ttl_ms);
int rf_set_max_bytes_all(uint64_t max_bytes);
int rf_set_max_bytes(const char *topic, int part, uint64_t max_bytes);
