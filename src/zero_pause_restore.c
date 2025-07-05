#include "zero_pause_restore.h"
#include "zero_pause_rdb.h"
#include "storage.h"
#include "shared_storage.h"
#include "crc32c.h"
#include "ramforge_rotation_metrics.h"
#include "log.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <sys/stat.h>
#include <errno.h>
#include <uv.h>
#include <inttypes.h>
#include "globals.h"
// External references from your existing code
extern char *g_rdb_path;

// Restore context for background loading
typedef struct {
    FILE *rdb_file;
    Storage *target_storage;
    SharedStorage *shared_storage;
    uint64_t entries_loaded;
    uint64_t expected_crc;
    uint64_t calculated_crc;
    uint64_t restore_generation;
    uv_thread_t restore_thread;
    _Atomic(int) active;
    _Atomic(int) success;
    char error_msg[256];
} RestoreContext;

static RestoreContext g_restore_ctx = {0};

static inline uint64_t now_us(void) {
    struct timespec ts;
    clock_gettime(CLOCK_MONOTONIC, &ts);
    return (uint64_t)ts.tv_sec * 1000000ULL + ts.tv_nsec / 1000;
}

// Validate RDB file format and integrity
static int validate_rdb_file(FILE *file, uint64_t *generation, uint64_t *expected_crc) {
    uint64_t magic;
    if (fread(&magic, sizeof(magic), 1, file) != 1) {
        return -1;
    }

    if (magic != 0x52414D460001ULL) { // 'RAMF'0001
        return -1;
    }

    if (fread(generation, sizeof(*generation), 1, file) != 1) {
        return -1;
    }

    // Seek to end to read CRC
    if (fseek(file, -sizeof(uint64_t), SEEK_END) != 0) {
        return -1;
    }

    if (fread(expected_crc, sizeof(*expected_crc), 1, file) != 1) {
        return -1;
    }

    // Return to data section
    if (fseek(file, sizeof(magic) + sizeof(*generation), SEEK_SET) != 0) {
        return -1;
    }

    return 0;
}

// Background restore thread
static void restore_worker_thread(void *arg) {
    RestoreContext *ctx = (RestoreContext *)arg;
    uint64_t t0 = now_us();

    LOGD("Starting zero-pause restore from generation %" PRIu64 "\n",
         ctx->restore_generation);

    ctx->calculated_crc = 0;
    ctx->entries_loaded = 0;

    // Read entries from RDB file
    while (!feof(ctx->rdb_file)) {
        int key_id;
        size_t data_size;

        if (fread(&key_id, sizeof(key_id), 1, ctx->rdb_file) != 1) {
            if (feof(ctx->rdb_file)) break;
            snprintf(ctx->error_msg, sizeof(ctx->error_msg),
                     "Failed to read key ID at entry %" PRIu64, ctx->entries_loaded);
            goto error;
        }

        if (fread(&data_size, sizeof(data_size), 1, ctx->rdb_file) != 1) {
            if (feof(ctx->rdb_file)) break;        /* normal end-of-file */
            snprintf(ctx->error_msg, sizeof(ctx->error_msg),
                     "Failed to read data size for key %d", key_id);
            goto error;
        }

        // Sanity check data size
        if (data_size > 16 * 1024 * 1024) { // 16MB limit
            snprintf(ctx->error_msg, sizeof(ctx->error_msg),
                     "Data size too large: %zu bytes for key %d", data_size, key_id);
            goto error;
        }

        // Allocate and read data
        void *data = malloc(data_size);
        if (!data) {
            snprintf(ctx->error_msg, sizeof(ctx->error_msg),
                     "Failed to allocate %zu bytes for key %d", data_size, key_id);
            goto error;
        }

        if (fread(data, data_size, 1, ctx->rdb_file) != 1) {
            free(data);
            snprintf(ctx->error_msg, sizeof(ctx->error_msg),
                     "Failed to read data for key %d", key_id);
            goto error;
        }

        // Update CRC calculation
        ctx->calculated_crc = crc32c(ctx->calculated_crc, &key_id, sizeof(key_id));
        ctx->calculated_crc = crc32c(ctx->calculated_crc, &data_size, sizeof(data_size));
        ctx->calculated_crc = crc32c(ctx->calculated_crc, data, data_size);

        // Store in both local and shared storage
        if (ctx->target_storage) {
            storage_save(ctx->target_storage, key_id, data, data_size);
        }

        if (ctx->shared_storage) {
            shared_storage_set(ctx->shared_storage, key_id, data, data_size);
        }

        free(data);
        ctx->entries_loaded++;

        // Yield CPU periodically to maintain responsiveness
        if (ctx->entries_loaded % 1000 == 0) {
            usleep(100); // 100 microseconds
        }
    }

    // Validate CRC
    if (ctx->calculated_crc != ctx->expected_crc) {
        snprintf(ctx->error_msg, sizeof(ctx->error_msg),
                 "CRC mismatch: expected %08" PRIx64 ", got %08" PRIx64,
                ctx->expected_crc, ctx->calculated_crc);
        goto error;
    }

    uint64_t dur = now_us() - t0;

    // Record successful recovery
    RAMForge_record_recovery_attempt(1);

    LOGD("Completed zero-pause restore: %" PRIu64 " entries loaded in %" PRIu64 "us\n",
         ctx->entries_loaded, dur);

    atomic_store(&ctx->success, 1);
    atomic_store(&ctx->active, 0);
    return;

    error:
    // Record failed recovery
    RAMForge_record_recovery_attempt(0);

    LOGE("Zero-pause restore failed: %s\n", ctx->error_msg);
    atomic_store(&ctx->success, 0);
    atomic_store(&ctx->active, 0);
}

// Public API: Start zero-pause restore
int ZeroPauseRDB_restore(const char *rdb_path, Storage *target_storage,
                         SharedStorage *shared_storage) {
    if (atomic_load(&g_restore_ctx.active)) {
        return -1; // Restore already in progress
    }

    // Reset context
    memset(&g_restore_ctx, 0, sizeof(g_restore_ctx));
    g_restore_ctx.target_storage = target_storage;
    g_restore_ctx.shared_storage = shared_storage;

    // Open RDB file
    g_restore_ctx.rdb_file = fopen(rdb_path, "rb");
    if (!g_restore_ctx.rdb_file) {
        snprintf(g_restore_ctx.error_msg, sizeof(g_restore_ctx.error_msg),
                 "Failed to open RDB file: %s", strerror(errno));
        return -1;
    }

    // Validate file format
    if (validate_rdb_file(g_restore_ctx.rdb_file, &g_restore_ctx.restore_generation,
                          &g_restore_ctx.expected_crc) != 0) {
        fclose(g_restore_ctx.rdb_file);
        snprintf(g_restore_ctx.error_msg, sizeof(g_restore_ctx.error_msg),
                 "Invalid RDB file format");
        return -1;
    }

    // Start background restore
    atomic_store(&g_restore_ctx.active, 1);
    atomic_store(&g_restore_ctx.success, 0);

    if (uv_thread_create(&g_restore_ctx.restore_thread, restore_worker_thread,
                         &g_restore_ctx) != 0) {
        fclose(g_restore_ctx.rdb_file);
        atomic_store(&g_restore_ctx.active, 0);
        snprintf(g_restore_ctx.error_msg, sizeof(g_restore_ctx.error_msg),
                 "Failed to create restore thread");
        return -1;
    }

    return 0;
}

// Check restore status
int ZeroPauseRDB_restore_status(ZeroPauseRestoreStatus *status) {
    if (!status) return -1;

    status->active = atomic_load(&g_restore_ctx.active);
    status->success = atomic_load(&g_restore_ctx.success);
    status->entries_loaded = g_restore_ctx.entries_loaded;
    status->restore_generation = g_restore_ctx.restore_generation;
    strncpy(status->error_msg, g_restore_ctx.error_msg, sizeof(status->error_msg) - 1);
    status->error_msg[sizeof(status->error_msg) - 1] = '\0';

    return 0;
}

// Wait for restore completion
int ZeroPauseRDB_restore_wait(void) {
    if (!atomic_load(&g_restore_ctx.active)) {
        return atomic_load(&g_restore_ctx.success) ? 0 : -1;
    }

    uv_thread_join(&g_restore_ctx.restore_thread);
    fclose(g_restore_ctx.rdb_file);

    return atomic_load(&g_restore_ctx.success) ? 0 : -1;
}

// ──────────────────────────────────────────────────────────────
// HTTP Endpoint Handlers
// ──────────────────────────────────────────────────────────────

// Trigger zero-pause restore
int zp_restore_handler(Request *req, Response *res) {
    (void)req;

    // Use default RDB path if not specified
    const char *rdb_path = g_rdb_path ? g_rdb_path : "./zp_dump.rdb";

    // Start restore operation
    int result = ZeroPauseRDB_restore(rdb_path, g_storage_ref, g_shared_storage);

    if (result == 0) {
        memcpy(res->buffer, "{\"status\":\"started\",\"message\":\"Zero-pause restore initiated\"}\n", 63);
        return 0;
    } else if (atomic_load(&g_restore_ctx.active)) {
        memcpy(res->buffer, "{\"status\":\"error\",\"message\":\"Restore already in progress\"}\n", 59);
        return 0;
    } else {
        int len = snprintf(res->buffer, sizeof(res->buffer),
                           "{\"status\":\"error\",\"message\":\"%.200s\"}\n",
                           g_restore_ctx.error_msg);
        return len > 0 ? 0 : -1;
    }
}

// Get restore status
int zp_restore_status_handler(Request *req, Response *res) {
    (void)req;

    ZeroPauseRestoreStatus status;
    if (ZeroPauseRDB_restore_status(&status) != 0) {
        memcpy(res->buffer, "{\"status\":\"error\",\"message\":\"Failed to get status\"}\n", 51);
        return 0;
    }

    int len = snprintf(res->buffer, sizeof(res->buffer),
                       "{"
                       "\"active\":%s,"
                       "\"success\":%s,"
    "\"entries_loaded\":%" PRIu64 ","
                                  "\"generation\":%" PRIu64 ","
                                                            "\"error_msg\":\"%.200s\""
                                                            "}\n",
            status.active ? "true" : "false",
            status.success ? "true" : "false",
            status.entries_loaded,
            status.restore_generation,
            status.error_msg);

    return len > 0 ? 0 : -1;
}

// Synchronous restore (waits for completion)
int zp_restore_sync_handler(Request *req, Response *res) {
    (void)req;

    // Use default RDB path if not specified
    const char *rdb_path = g_rdb_path ? g_rdb_path : "./zp_dump.rdb";

    // Start restore operation
    int result = ZeroPauseRDB_restore(rdb_path, g_storage_ref, g_shared_storage);

    if (result != 0) {
        if (atomic_load(&g_restore_ctx.active)) {
            memcpy(res->buffer, "{\"status\":\"error\",\"message\":\"Restore already in progress\"}\n", 59);
        } else {
            int len = snprintf(res->buffer, sizeof(res->buffer),
                               "{\"status\":\"error\",\"message\":\"%.200s\"}\n",
                               g_restore_ctx.error_msg);
            if (len <= 0) return -1;
        }
        return 0;
    }

    // Wait for completion
    result = ZeroPauseRDB_restore_wait();

    if (result == 0) {
        int len = snprintf(res->buffer, sizeof(res->buffer),
        "{\"status\":\"success\",\"entries_loaded\":%" PRIu64 "}\n",
                g_restore_ctx.entries_loaded);
        return len > 0 ? 0 : -1;
    } else {
        int len = snprintf(res->buffer, sizeof(res->buffer),
                           "{\"status\":\"error\",\"message\":\"%.200s\"}\n",
                           g_restore_ctx.error_msg);
        return len > 0 ? 0 : -1;
    }
}
