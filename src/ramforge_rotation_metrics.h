#ifndef RAMFORGE_RAMFORGE_ROTATION_METRICS_H
#define RAMFORGE_RAMFORGE_ROTATION_METRICS_H

#include <stdarg.h>
#include <stdio.h>
#include <string.h>
#include <stdint.h>
#include <stddef.h>
#include <stdatomic.h>

typedef struct {
    char*  buffer;
    size_t capacity;
    size_t length;
} metrics_buffer_t;

typedef struct {
    _Atomic uint64_t total_operations;
    _Atomic uint64_t total_latency_us;
    _Atomic uint64_t min_latency_us;
    _Atomic uint64_t max_latency_us;
    _Atomic uint64_t p99_latency_us;
    _Atomic uint64_t p95_latency_us;
    _Atomic uint64_t snapshots_created;
    _Atomic uint64_t snapshots_rotated;
    _Atomic uint64_t snapshot_total_time_us;
    _Atomic uint64_t snapshot_min_time_us;
    _Atomic uint64_t snapshot_max_time_us;
    _Atomic uint64_t rotations_by_size;
    _Atomic uint64_t rotations_by_age;
    _Atomic uint64_t rotations_by_performance;
    _Atomic uint64_t rotations_by_chaos;
    _Atomic uint64_t rotations_forced;
    _Atomic uint64_t aof_rotations_total;
    _Atomic uint64_t zp_snapshots;
    _Atomic uint64_t zp_snapshot_us;
    _Atomic uint64_t rotation_failures;
    _Atomic uint64_t rdb_files_total;
    _Atomic uint64_t rdb_size_total_mb;
    _Atomic uint64_t aof_files_total;
    _Atomic uint64_t aof_size_total_mb;
    _Atomic uint64_t aof_generation;
    _Atomic uint64_t aof_switches;
    _Atomic uint64_t write_failures;
    _Atomic uint64_t successful_writes;
    _Atomic uint64_t disk_space_freed_mb;
    _Atomic uint64_t beast_mode_ops;
    _Atomic uint64_t sub_1ms_operations;
    _Atomic uint64_t sub_100us_operations;
    _Atomic uint64_t sub_10us_operations;
    _Atomic uint64_t sub_1us_operations;
    _Atomic uint64_t crc_validations;
    _Atomic uint64_t crc_failures;
    _Atomic uint64_t recovery_attempts;
    _Atomic uint64_t recovery_successes;
    _Atomic uint64_t zp_restores;
    _Atomic uint64_t zp_restore_failures;
    _Atomic uint64_t zp_restore_us;
    _Atomic uint64_t zp_restore_last_success_unix;
    _Atomic uint64_t zp_restore_inflight;
} RAMForgeMetrics;

/* header-only helpers: safe to include everywhere, no unused warnings */
static inline void metrics_buf_init(metrics_buffer_t* buf, char* buffer, size_t cap)
    __attribute__((unused));

static inline void metrics_buf_init(metrics_buffer_t* buf, char* buffer, size_t cap) {
    buf->buffer = buffer;
    buf->capacity = cap;
    buf->length = 0;
    if (cap > 0) buf->buffer[0] = '\0';
}

static inline int metrics_buf_printf(metrics_buffer_t* buf, const char* fmt, ...)
    __attribute__((unused, format(printf, 2, 3)));

static inline int metrics_buf_printf(metrics_buffer_t* buf, const char* fmt, ...) {
    if (!buf || buf->length >= buf->capacity) return 0;

    va_list args;
    va_start(args, fmt);
    int written = vsnprintf(buf->buffer + buf->length,
                            buf->capacity - buf->length,
                            fmt, args);
    va_end(args);

    if (written < 0) return 0;

    if ((size_t)written >= buf->capacity - buf->length) {
        buf->length = buf->capacity - 1;
    } else {
        buf->length += (size_t)written;
    }
    return written;
}

void ZeroPauseRDB_metrics_inc(uint64_t usec);
void ZeroPauseRDB_restore_metrics_inc(uint64_t usec, int ok);
void RAMForge_force_rotation(void);
void RAMForge_record_crc_validation(int success);
void RAMForge_export_prometheus_metrics_buffer(char* buffer, size_t capacity);
void RAMForge_record_recovery_attempt(int success);

#endif
