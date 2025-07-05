#ifndef AOF_BATCH_H
#define AOF_BATCH_H

#include <stddef.h>
#include "storage.h"

/// Initialize the AOF batcher:
///   path               – file to append to
///   ring_capacity      – size of the ring buffer (power of two)
///   flush_interval_ms  – group-commit interval
void AOF_init(const char *path,
              size_t ring_capacity,
              unsigned flush_interval_ms);

/// Synchronously replay the existing AOF file into `storage`.
void AOF_load(struct Storage *storage);

/// Enqueue one command (id + data blob) for batched fsync.
int AOF_append(int id, const void *data, size_t size);

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

/* Check if non-blocking rewrite is in progress */
int AOF_rewrite_in_progress(void);

/* Non-blocking rewrite - returns 0 on success, -1 if already running */
int AOF_rewrite_nonblocking(Storage *st);

#endif // AOF_BATCH_H
