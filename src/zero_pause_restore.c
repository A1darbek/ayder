#include "zero_pause_restore.h"
#include "zero_pause_rdb.h"
#include "storage.h"
#include "shared_storage.h"
#include "crc32c.h"
//#include "ramforge_rotation_metrics.h"
#include "log.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <sys/stat.h>
#include <errno.h>
#include <uv.h>
#include <inttypes.h>
#include <glob.h>
#include "globals.h"
#include "storage_thread_safeguard.h"
#include "ramforge_rotation_metrics.h"

// External references from your existing code
extern char *g_rdb_path;

// Restore context for background loading
typedef struct {
    FILE *rdb_file;
    Storage *target_storage;
    SharedStorage *shared_storage;
    uint32_t entries_loaded;
    uint32_t expected_crc;
    uint32_t calculated_crc;
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
static int validate_rdb_file(FILE *file, uint64_t *generation, uint32_t *expected_crc) {
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
    if (fseek(file, -(long)sizeof(uint32_t), SEEK_END) != 0) {
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
            pthread_rwlock_wrlock(&g_storage_lock);
            storage_save(ctx->target_storage, key_id, data, data_size);
            pthread_rwlock_unlock(&g_storage_lock);
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
                 "CRC mismatch: expected %08" PRIx32 ", got %08" PRIx32,
                 ctx->expected_crc, ctx->calculated_crc);
        goto error;
    }
    {
        uint64_t dur = now_us() - t0;

//         Record successful recovery
        RAMForge_record_recovery_attempt(1);
        ZeroPauseRDB_restore_metrics_inc(dur, 1);

        LOGD("Completed zero-pause restore: %" PRIu64 " entries loaded in %" PRIu64 "us\n",
             ctx->entries_loaded, dur);

        atomic_store(&ctx->success, 1);
    }
    finish:
    atomic_store(&ctx->active, 0);
    return;

    error:
    // Record failed recovery
    ZeroPauseRDB_restore_metrics_inc(0, 0);
    RAMForge_record_recovery_attempt(0);

    LOGE("Zero-pause restore failed: %s\n", ctx->error_msg);
    atomic_store(&ctx->success, 0);
    atomic_store(&ctx->active, 0);
    goto finish;
}

static FILE *open_with_backoff(const char *path)
{
    for (int i = 0; i < 50; ++i) {          // wait up to 50 ms
        FILE *f = fopen(path, "rb");
        if (f) return f;
        usleep(1000);
    }
    return NULL;
}

// Find the most recent RDB file (current or timestamped)
static char *find_latest_rdb_file(const char *base_path) {
    static char latest_path[512];

    // First, try the standard current file
    snprintf(latest_path, sizeof(latest_path), "%s", base_path);
    if (access(latest_path, F_OK) == 0) {
        return latest_path;
    }

    // If not found, search for timestamped files
    char prefix[512];
    strncpy(prefix, base_path, sizeof(prefix) - 1);
    prefix[sizeof(prefix) - 1] = '\0';

    size_t n = strlen(prefix);
    if (n > 4 && strcmp(prefix + n - 4, ".rdb") == 0)
        prefix[n - 4] = '\0';          /* remove .rdb */

    char pattern[512];
    snprintf(pattern, sizeof pattern, "%s_*.rdb", prefix);

    glob_t glob_result;
    if (glob(pattern, GLOB_TILDE, NULL, &glob_result) != 0) {
        return NULL;  // No files found
    }

    if (glob_result.gl_pathc == 0) {
        globfree(&glob_result);
        return NULL;
    }

    // Find the most recent file by modification time
    time_t latest_mtime = 0;
    int latest_index = -1;

    for (size_t i = 0; i < glob_result.gl_pathc; i++) {
        struct stat st;
        if (stat(glob_result.gl_pathv[i], &st) == 0) {
            if (st.st_mtime > latest_mtime) {
                latest_mtime = st.st_mtime;
                latest_index = i;
            }
        }
    }

    if (latest_index >= 0) {
        strncpy(latest_path, glob_result.gl_pathv[latest_index], sizeof(latest_path) - 1);
        latest_path[sizeof(latest_path) - 1] = '\0';
        globfree(&glob_result);
        return latest_path;
    }

    globfree(&glob_result);
    return NULL;
}

// Public API: Start zero-pause restore
int ZeroPauseRDB_restore(const char *rdb_path,
                         Storage *target_storage,
                         SharedStorage *shared_storage)
{
    if (atomic_load(&g_restore_ctx.active))
        return -1;                     /* restore already running */

    /* 0. reset context */
    memset(&g_restore_ctx, 0, sizeof g_restore_ctx);
    g_restore_ctx.target_storage  = target_storage;
    g_restore_ctx.shared_storage  = shared_storage;

    /* 1. open the file – retry once in case it was rotated away
           between discovery and fopen()                            */
    const char *base_path = rdb_path;          /* keep original   */
    FILE *f = open_with_backoff(rdb_path);     /* first attempt   */
    if (!f) {                                  /* maybe rotated?  */
        rdb_path = find_latest_rdb_file(base_path);
        f = rdb_path ? open_with_backoff(rdb_path) : NULL;
    }
    if (!f) {
        snprintf(g_restore_ctx.error_msg, sizeof g_restore_ctx.error_msg,
                 "Failed to open latest RDB file after rotation: %s",
                 strerror(errno));
        return -1;
    }
    g_restore_ctx.rdb_file = f;                /* remember handle */

    /* 2. validate header / CRC */
    if (validate_rdb_file(g_restore_ctx.rdb_file,
                          &g_restore_ctx.restore_generation,
                          &g_restore_ctx.expected_crc) != 0)
    {
        fclose(g_restore_ctx.rdb_file);
        snprintf(g_restore_ctx.error_msg, sizeof g_restore_ctx.error_msg,
                 "Invalid or corrupt RDB file");
        return -1;
    }

    /* 3. spawn background worker */
    atomic_store(&g_restore_ctx.active,  1);
    atomic_store(&g_restore_ctx.success, 0);

    if (uv_thread_create(&g_restore_ctx.restore_thread,
                         restore_worker_thread,
                         &g_restore_ctx) != 0)
    {
        fclose(g_restore_ctx.rdb_file);
        atomic_store(&g_restore_ctx.active, 0);
        snprintf(g_restore_ctx.error_msg, sizeof g_restore_ctx.error_msg,
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
// Updated restore handlers with smart file discovery
int zp_restore_handler(Request *req, Response *res) {
    (void)req;

    // Smart RDB file discovery
    const char *base_rdb_path = g_rdb_path ? g_rdb_path : "./zp_dump.rdb";
    char *rdb_path = find_latest_rdb_file(base_rdb_path);

    if (!rdb_path) {
        memcpy(res->buffer, "{\"status\":\"error\",\"message\":\"No RDB file found\"}\n", 48);
        return 0;
    }

    printf("🔍 Using RDB file: %s\n", rdb_path);

    // Start restore operation
    int result = ZeroPauseRDB_restore(rdb_path, g_storage_ref, g_shared_storage);

    if (result == 0) {
        int len = snprintf(res->buffer, sizeof(res->buffer),
                           "{\"status\":\"started\",\"message\":\"Zero-pause restore initiated from %s\"}\n",
                           rdb_path);
        return len > 0 ? 0 : -1;
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

    // Smart RDB file discovery
    const char *base_rdb_path = g_rdb_path ? g_rdb_path : "./zp_dump.rdb";
    char *rdb_path = find_latest_rdb_file(base_rdb_path);

    if (!rdb_path) {
        memcpy(res->buffer, "{\"status\":\"error\",\"message\":\"No RDB file found\"}\n", 48);
        return 0;
    }

    printf("🔍 Using RDB file for sync restore: %s\n", rdb_path);

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
                           "{\"status\":\"success\",\"entries_loaded\":%" PRIu64 ",\"file\":\"%s\"}\n",
                           g_restore_ctx.entries_loaded, rdb_path);
        return len > 0 ? 0 : -1;
    } else {
        int len = snprintf(res->buffer, sizeof(res->buffer),
                           "{\"status\":\"error\",\"message\":\"%.200s\"}\n",
                           g_restore_ctx.error_msg);
        return len > 0 ? 0 : -1;
    }
}
