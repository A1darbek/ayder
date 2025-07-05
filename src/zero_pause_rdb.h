#ifndef ZERO_PAUSE_RDB_H
#define ZERO_PAUSE_RDB_H

#include <stddef.h>
#include <stdint.h>

typedef struct Storage Storage;
extern Storage *g_storage_ref;
extern char    *g_rdb_path;

// Statistics structure for monitoring
typedef struct {
    int      active_snapshot;
    uint64_t current_generation;
    size_t   entries_written;
    size_t   snapshot_table_entries;
    size_t   bitmap_capacity;
    double   last_snapshot_duration_ms;
    uint64_t total_snapshots;
    uint64_t chaos_events_injected;
} ZeroPauseStats;

// Core API
void ZeroPauseRDB_init(const char *rdb_path, Storage *storage,
                       size_t max_keys, unsigned snapshot_interval_sec);

void ZeroPauseRDB_mark_dirty(int key_id, const void *old_data, size_t old_size);
void ZeroPauseRDB_snapshot(void);
void ZeroPauseRDB_stats(ZeroPauseStats *stats);
void ZeroPauseRDB_shutdown(void);
int ZeroPauseRDB_snapshot_wait(void);

// Chaos testing API
void ZeroPauseRDB_chaos_test(int enable);
void ZeroPauseRDB_chaos_inject_latency(int min_us, int max_us);
void ZeroPauseRDB_chaos_inject_memory_pressure(size_t bytes);
void ZeroPauseRDB_chaos_simulate_slow_disk(int enable);

#endif
