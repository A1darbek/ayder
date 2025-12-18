#ifndef AOF_BATCH_H
#define AOF_BATCH_H

#include <stddef.h>
#include "storage.h"
#include "rf_broker.h"

#ifndef AOF_REC_BROKER_TTL
#define AOF_REC_BROKER_TTL 0x54544C31  /* 'TTL1' */
#endif
#define AOF_REC_BROKER_RETENTION 0x5252

#define AOF_REC_KV_PUT 0x4B560001
#define AOF_REC_KV_DEL 0x4B560002


#define KV_FLAG_TOMBSTONE  (1u<<0)

#pragma pack(push,1)
typedef struct {
    uint64_t cas;            // 0 if never written
    uint64_t expires_us;     // 0 = no ttl
    uint32_t flags;          // tombstone, etc
    uint32_t value_len;      // payload length
    uint8_t  value[];        // payload bytes
} kvrec_t;
#pragma pack(pop)


void AOF_init(const char *path,
              size_t ring_capacity,
              unsigned flush_interval_ms);

void AOF_prepare_for_rotation(void);
int AOF_rotate_file(const char *new_path);
/// Synchronously replay the existing AOF file into `storage`.
void AOF_load(struct Storage *storage);
/// Enqueue one command (id + data blob) for batched fsync.
void AOF_append(int id, const void *data, size_t sz);

/// Flush any pending entries, stop the writer thread, close the file.
void AOF_shutdown(void);

/* Asynchronous rewrite: returns 0 if launch OK, -1 otherwise */
int AOF_begin_rewrite(const char *source_path);

/* Check if segment rewrite is in progress */
int AOF_segment_rewrite_in_progress(void);

/* blocking rewrite (legacy / admin compact) */
void AOF_rewrite(Storage *storage);
/* Low-level record writing (used internally) */
int aof_write_record(int fd, int id, const void *data, uint32_t size);
void AOF_sealed_follower_start(int worker_id);
size_t AOF_sealed_queue_depth(void);


/* Check if non-blocking rewrite is in progress */
int AOF_rewrite_in_progress(void);
void AOF_sync(void);
size_t AOF_pending_writes(void);
uint64_t AOF_append_sealed(int id, const void *data, size_t sz);
uint64_t AOF_sealed_last_synced_id(void);


/* Replays <aof_path>.sealed (idempotent). Returns #records applied. */
size_t   AOF_sealed_replay(const char *aof_path);
/* Non-blocking rewrite - returns 0 on success, -1 if already running */
int AOF_rewrite_nonblocking(Storage *st);

void AOF_sealed_test_set_hold(int on);
void AOF_sealed_test_release(void);
void AOF_sealed_test_set_delay_us(int us);
int AOF_sealed_gc(uint64_t max_age_ms, uint64_t max_keep_bytes, uint64_t *bytes_after);
void AOF_live_follow_start(void);
void AOF_live_follow_stop(void);

#endif // AOF_BATCH_H
