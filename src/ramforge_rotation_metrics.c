#define _GNU_SOURCE

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdatomic.h>
#include <pthread.h>
#include <time.h>
#include <sys/stat.h>
#include <unistd.h>
#include <glob.h>
#include <uv.h>
#include <inttypes.h>
#include <libgen.h>
#include "ramforge_rotation_metrics.h"
#include "log.h"
#include "aof_batch.h"
#include "zero_pause_rdb.h"
#include "metrics_shared.h"
#include <fcntl.h>
#include <dlfcn.h>
#include <sys/statvfs.h>

#define HN 10

uint64_t RAMForge_http_2xx(void);

uint64_t RAMForge_http_4xx(void);

uint64_t RAMForge_http_401(void);

uint64_t RAMForge_http_429(void);

uint64_t RAMForge_http_5xx(void);

uint64_t RAMForge_http_503(void);

uint64_t RAMForge_http_total(void);

uint64_t RAMForge_http_active_connections(void);

// from aof_batch.c
void AOF_fsync_hist_snapshot(uint64_t *b, size_t n, double *sum_s, uint64_t *count);

void AOF_seal_latency_hist_snapshot(uint64_t *b, size_t n, double *sum_s, uint64_t *count);

const double *AOF_fsync_bucket_bounds(int *n);

const double *AOF_seal_bucket_bounds(int *n);

int AOF_fs_stats(uint64_t *bytes_total, uint64_t *bytes_free, uint64_t *inodes_total, uint64_t *inodes_free);

size_t AOF_sealed_queue_depth(void);

size_t AOF_pending_writes(void);

typedef struct Storage Storage;

extern void storage_iterate(Storage *st, void (*cb)(int, const void *, size_t, void *), void *ud);

typedef struct {
    size_t max_rdb_size_mb;           // Rotate when RDB exceeds this size
    size_t max_aof_size_mb;           // Rotate when AOF exceeds this size
    time_t max_age_hours;             // Rotate when files are older than this
    time_t rotation_interval_sec;     // Force rotation every N seconds
    int max_snapshots_to_keep;        // Keep only N most recent RDB files
    int max_aof_segments_to_keep;     // Keep only N most recent AOF segments
    double max_snapshot_time_ms;      // Rotate if snapshot takes too long
    size_t min_dirty_keys_threshold;  // Don't rotate unless enough keys changed
    int chaos_rotation_enabled;       // Enable random rotations for testing
    int chaos_rotation_chance_pct;    // Percentage chance of chaos rotation
} RotationPolicy;
#define LATENCY_BUCKETS 1000
typedef struct {
    uint64_t buckets[LATENCY_BUCKETS];
    uint64_t bucket_boundaries[LATENCY_BUCKETS];
    _Atomic uint64_t total_samples;
    pthread_mutex_t lock;
} LatencyHistogram;

static RotationPolicy g_policy = {
        .max_rdb_size_mb = 100,              // 100MB default
        .max_aof_size_mb = 50,               // 50MB default
        .max_age_hours = 24,                 // 24 hours
        .rotation_interval_sec = 3600,       // 1 hour
        .max_snapshots_to_keep = 10,         // Keep 10 RDB files
        .max_aof_segments_to_keep = 5,       // Keep 5 AOF segments
        .max_snapshot_time_ms = 100.0,       // 100ms snapshot limit
        .min_dirty_keys_threshold = 1000,    // 1000 keys changed
        .chaos_rotation_enabled = 0,         // Disabled by default
        .chaos_rotation_chance_pct = 5       // 5% chance
};
static LatencyHistogram g_latency_hist = {0};
static char *g_rdb_base_path = NULL;
static char *g_aof_base_path = NULL;
static uv_timer_t g_rotation_timer;
static uv_timer_t g_metrics_timer;
static pthread_mutex_t g_rotation_lock = PTHREAD_MUTEX_INITIALIZER;
static int g_is_rotation_manager = 0;

void ZeroPauseRDB_metrics_inc(uint64_t usec) {
    atomic_fetch_add(&g_metrics.zp_snapshots, 1);
    atomic_fetch_add(&g_metrics.zp_snapshot_us, usec);
}

void ZeroPauseRDB_restore_metrics_inc(uint64_t usec, int ok) {
    if (ok) {
        atomic_fetch_add(&g_metrics.zp_restores, 1);
        atomic_fetch_add(&g_metrics.zp_restore_us, usec);
        atomic_store (&g_metrics.zp_restore_last_success_unix, time(NULL));
    } else {
        atomic_fetch_add(&g_metrics.zp_restore_failures, 1);
    }
}


static void init_latency_histogram(void) {
    pthread_mutex_init(&g_latency_hist.lock, NULL);
    g_latency_hist.bucket_boundaries[0] = 1;
    for (int i = 1; i < LATENCY_BUCKETS; i++) {
        if (i % 3 == 1) {
            g_latency_hist.bucket_boundaries[i] = g_latency_hist.bucket_boundaries[i - 1] * 2;
        } else if (i % 3 == 2) {
            g_latency_hist.bucket_boundaries[i] = g_latency_hist.bucket_boundaries[i - 1] * 2.5;
        } else {
            g_latency_hist.bucket_boundaries[i] = g_latency_hist.bucket_boundaries[i - 1] * 2;
        }
    }
}


static size_t get_file_size_mb(const char *path) {
    struct stat st;
    if (stat(path, &st) != 0) return 0;
    return st.st_size / (1024 * 1024);
}

static time_t get_file_age_hours(const char *path) {
    struct stat st;
    if (stat(path, &st) != 0) return 0;
    return (time(NULL) - st.st_mtime) / 3600;
}

static int count_files_with_pattern(const char *pattern) {
    glob_t glob_result;
    int count = 0;
    if (glob(pattern, GLOB_TILDE, NULL, &glob_result) == 0) {
        count = glob_result.gl_pathc;
        globfree(&glob_result);
    }
    return count;
}

static void cleanup_old_files_by_count(const char *pattern, int keep_count) {
    glob_t glob_result;
    if (glob(pattern, GLOB_TILDE, NULL, &glob_result) != 0) {
        return;
    }
    if ((int) glob_result.gl_pathc <= keep_count) {
        globfree(&glob_result);
        return;
    }
    for (size_t i = 0; i < glob_result.gl_pathc - 1; i++) {
        for (size_t j = i + 1; j < glob_result.gl_pathc; j++) {
            struct stat st1, st2;
            stat(glob_result.gl_pathv[i], &st1);
            stat(glob_result.gl_pathv[j], &st2);
            if (st1.st_mtime < st2.st_mtime) {
                char *tmp = glob_result.gl_pathv[i];
                glob_result.gl_pathv[i] = glob_result.gl_pathv[j];
                glob_result.gl_pathv[j] = tmp;
            }
        }
    }
    for (int i = keep_count; i < (int) glob_result.gl_pathc; i++) {
        size_t size_mb = get_file_size_mb(glob_result.gl_pathv[i]);
        printf("🗑️  Removing old file: %s (%zu MB)\n", glob_result.gl_pathv[i], size_mb);
        if (unlink(glob_result.gl_pathv[i]) == 0) {
            atomic_fetch_add(&g_metrics.disk_space_freed_mb, size_mb);
        }
    }
    globfree(&glob_result);
}

typedef enum {
    ROTATION_NONE,
    ROTATION_SIZE,
    ROTATION_AGE,
    ROTATION_PERFORMANCE,
    ROTATION_CHAOS,
    ROTATION_FORCED
} RotationReason;

static RotationReason should_rotate_rdb(void) {
    char current_rdb[512];
    snprintf(current_rdb, sizeof(current_rdb), "%s.rdb", g_rdb_base_path);
    size_t size_mb = get_file_size_mb(current_rdb);
    if (size_mb > g_policy.max_rdb_size_mb) {
        printf("📏 RDB rotation triggered by size: %zu MB > %zu MB\n",
               size_mb, g_policy.max_rdb_size_mb);
        return ROTATION_SIZE;
    }
    time_t age_hours = get_file_age_hours(current_rdb);
    if (age_hours > g_policy.max_age_hours) {
        printf("⏰ RDB rotation triggered by age: %ld hours > %ld hours\n",
               age_hours, g_policy.max_age_hours);
        return ROTATION_AGE;
    }
    uint64_t snapshots = atomic_load(&g_metrics.snapshots_created);
    if (snapshots > 0) {
        uint64_t avg_time_us = atomic_load(&g_metrics.snapshot_total_time_us) / snapshots;
        double avg_time_ms = avg_time_us / 1000.0;
        if (avg_time_ms > g_policy.max_snapshot_time_ms) {
            printf("🐌 RDB rotation triggered by performance: %.2f ms > %.2f ms\n",
                   avg_time_ms, g_policy.max_snapshot_time_ms);
            return ROTATION_PERFORMANCE;
        }
    }
    if (g_policy.chaos_rotation_enabled) {
        int chaos_roll = rand() % 100;
        if (chaos_roll < g_policy.chaos_rotation_chance_pct) {
            printf("🎲 RDB rotation triggered by chaos: %d%% chance hit!\n",
                   g_policy.chaos_rotation_chance_pct);
            return ROTATION_CHAOS;
        }
    }
    return ROTATION_NONE;
}

static void execute_rdb_rotation(RotationReason reason) {
    char live[512];
    snprintf(live, sizeof(live), "%s.rdb", g_rdb_base_path);

    /* ① bail out quietly if the writer hasn't produced the file yet */
    if (access(live, F_OK) != 0) return;            /* ← new guard */
    pthread_mutex_lock(&g_rotation_lock);
    struct timespec start, end;
    clock_gettime(CLOCK_MONOTONIC, &start);
    time_t now = time(NULL);
    struct tm *tm_info = localtime(&now);
    char timestamp[64];
    strftime(timestamp, sizeof(timestamp), "%Y%m%d_%H%M%S", tm_info);
    char old_path[512], new_path[512];
    snprintf(old_path, sizeof(old_path), "%s.rdb", g_rdb_base_path);
    snprintf(new_path, sizeof(new_path), "%s_%s.rdb", g_rdb_base_path, timestamp);
    if (rename(old_path, new_path) == 0) {
        printf("🔄 RDB rotated: %s → %s\n", old_path, new_path);
        atomic_fetch_add(&g_metrics.snapshots_rotated, 1);
        switch (reason) {
            case ROTATION_SIZE:
                atomic_fetch_add(&g_metrics.rotations_by_size, 1);
                break;
            case ROTATION_AGE:
                atomic_fetch_add(&g_metrics.rotations_by_age, 1);
                break;
            case ROTATION_PERFORMANCE:
                atomic_fetch_add(&g_metrics.rotations_by_performance, 1);
                break;
            case ROTATION_CHAOS:
                atomic_fetch_add(&g_metrics.rotations_by_chaos, 1);
                break;
            case ROTATION_FORCED:
                atomic_fetch_add(&g_metrics.rotations_forced, 1);
                break;
            default:
                break;
        }
        char pattern[512];
        snprintf(pattern, sizeof(pattern), "%s_*.rdb", g_rdb_base_path);
        cleanup_old_files_by_count(pattern, g_policy.max_snapshots_to_keep);
        atomic_store(&g_metrics.rdb_files_total, count_files_with_pattern(pattern));
    } else {
        printf("❌ RDB rotation failed: %s\n", strerror(errno));
        atomic_fetch_add(&g_metrics.rotation_failures, 1);
    }
    clock_gettime(CLOCK_MONOTONIC, &end);
    uint64_t rotation_time_us = (end.tv_sec - start.tv_sec) * 1000000 +
                                (end.tv_nsec - start.tv_nsec) / 1000;
    printf("⚡ RDB rotation completed in %"PRIu64" μs\n", rotation_time_us);
    pthread_mutex_unlock(&g_rotation_lock);
}

static void rotation_timer_cb(uv_timer_t *timer) {
    (void) timer;
    RotationReason reason = should_rotate_rdb();
    if (reason != ROTATION_NONE) {
        execute_rdb_rotation(reason);
    }
}

static off_t file_size(const char *p) {
    struct stat st;
    return stat(p, &st) == 0 ? st.st_size : -1;
}

static RotationReason should_rotate_aof(void) {
    /* Check if file exists first */
    if (access(g_aof_base_path, F_OK) != 0) {
        return ROTATION_NONE;  /* File doesn't exist yet */
    }

    struct stat st;
    if (stat(g_aof_base_path, &st) != 0) {
        return ROTATION_NONE;  /* Can't stat file */
    }

    /* Skip tiny files */
    if (st.st_size < 1024) {
        return ROTATION_NONE;
    }

    size_t size_mb = st.st_size / (1024 * 1024);
    if (size_mb > g_policy.max_aof_size_mb) {
        printf("📏 AOF rotation triggered by size: %zu MB > %zu MB\n",
               size_mb, g_policy.max_aof_size_mb);
        return ROTATION_SIZE;
    }

    time_t age_hours = (time(NULL) - st.st_mtime) / 3600;
    if (age_hours > g_policy.max_age_hours) {
        printf("⏰ AOF rotation triggered by age: %ld hours > %ld hours\n",
               age_hours, g_policy.max_age_hours);
        return ROTATION_AGE;
    }

    /* Check if AOF system is under stress */
    uint64_t pending = AOF_pending_writes();
    if (pending > 10000) {  /* Very high pending writes */
        printf("🚨 AOF rotation triggered by high pending writes: %" PRIu64 "\n", pending);
        return ROTATION_PERFORMANCE;
    }

    return ROTATION_NONE;
}

static void execute_aof_rotation(RotationReason reason) {
    pthread_mutex_lock(&g_rotation_lock);

    printf("🔄 Starting enhanced AOF rotation (reason: %s)\n",
           reason == ROTATION_SIZE ? "size" :
           reason == ROTATION_AGE ? "age" : "forced");

    struct timespec rotation_start, rotation_end;
    clock_gettime(CLOCK_MONOTONIC, &rotation_start);

    /* Wait for AOF system to be ready */
    AOF_prepare_for_rotation();

    /* Check if we should skip rotation for empty/tiny files */
    off_t current_size = file_size(g_aof_base_path);
    if (current_size <= 0) {
        printf("⏭️  Skipping rotation of empty/missing AOF file\n");
        pthread_mutex_unlock(&g_rotation_lock);
        return;
    }

    if (current_size < 1024) { /* Less than 1KB */
        printf("⏭️  Skipping rotation of tiny AOF file (%lld bytes)\n", (long long) current_size);
        pthread_mutex_unlock(&g_rotation_lock);
        return;
    }

    /* Generate timestamped path */
    char ts_path[512], ts[32];
    time_t now = time(NULL);
    strftime(ts, sizeof(ts), "%Y%m%d%H%M%S", localtime(&now));
    snprintf(ts_path, sizeof(ts_path), "%s_%s", g_aof_base_path, ts);

    /* Use safe rotation if available, fallback to original */
    int rotation_result;
    if (AOF_rotate_file) {  /* Function pointer check */
        rotation_result = AOF_rotate_file(ts_path);
    } else {
        rotation_result = AOF_rotate_file(ts_path);
    }

    if (rotation_result != 0) {
        fprintf(stderr, "❌ AOF rotation failed\n");
        atomic_fetch_add(&g_metrics.rotation_failures, 1);
        pthread_mutex_unlock(&g_rotation_lock);
        return;
    }

    /* Ensure directory sync for crash consistency */
    {
        char dirbuf[512];
        strncpy(dirbuf, ts_path, sizeof(dirbuf) - 1);
        dirbuf[sizeof(dirbuf) - 1] = '\0';
        char *dir = dirname(dirbuf);
        int dfd = open(dir ? dir : ".", O_RDONLY | O_DIRECTORY | O_CLOEXEC);
        if (dfd >= 0) {
            fsync(dfd);
            close(dfd);
        }
    }

    /* Update metrics before cleanup */
    switch (reason) {
        case ROTATION_SIZE:
            atomic_fetch_add(&g_metrics.rotations_by_size, 1);
            break;
        case ROTATION_AGE:
            atomic_fetch_add(&g_metrics.rotations_by_age, 1);
            break;
        case ROTATION_FORCED:
            atomic_fetch_add(&g_metrics.rotations_forced, 1);
            break;
        default:
            break;
    }
    atomic_fetch_add(&g_metrics.aof_rotations_total, 1);

    /* Get final file size for logging */
    off_t final_size = file_size(ts_path);
    printf("✅ AOF rotated: %s → %s (size=%lld bytes)\n",
           g_aof_base_path, ts_path, (long long) final_size);

    /* Start background rewrite if file is substantial */
    if (final_size > 1024) {  /* Only rewrite files > 1KB */
        if (AOF_begin_rewrite(ts_path) == 0) {
            printf("📝 AOF segment rewrite started on %s\n", ts_path);
        } else {
            printf("⚠️  AOF segment rewrite skipped (already running)\n");
        }
    } else {
        printf("⏭️  Skipping rewrite for small segment %s (%lld bytes)\n",
               ts_path, (long long) final_size);
    }

    /* Cleanup old segments */
    char pattern[512];
    snprintf(pattern, sizeof(pattern), "%s_*", g_aof_base_path);
    cleanup_old_files_by_count(pattern, g_policy.max_aof_segments_to_keep);
    atomic_store(&g_metrics.aof_files_total, count_files_with_pattern(pattern));

    /* Record rotation timing */
    clock_gettime(CLOCK_MONOTONIC, &rotation_end);
    uint64_t rotation_time_us = (rotation_end.tv_sec - rotation_start.tv_sec) * 1000000 +
                                (rotation_end.tv_nsec - rotation_start.tv_nsec) / 1000;


    printf("⚡ AOF rotation completed in %"PRIu64" μs\n", rotation_time_us);

    pthread_mutex_unlock(&g_rotation_lock);
    printf("🎯 Enhanced AOF rotation complete\n");
}

static void enhanced_metrics_timer_cb(uv_timer_t *timer) {
    (void) timer;

    /* RDB rotation check */
    RotationReason rdb_reason = should_rotate_rdb();
    if (rdb_reason != ROTATION_NONE) {
        execute_rdb_rotation(rdb_reason);
    }

    /* AOF rotation check with additional safety */
    RotationReason aof_reason = should_rotate_aof();
    if (aof_reason != ROTATION_NONE) {
        /* Check if AOF system is ready */
        if (access(g_aof_base_path, F_OK) == 0) {
            execute_aof_rotation(aof_reason);
        } else {
            printf("⚠️  AOF file not found, skipping rotation: %s\n", g_aof_base_path);
        }
    }

    /* Additional health checks */
    static uint64_t last_check_time = 0;
    uint64_t now = time(NULL);

    if (now - last_check_time > 300) {  /* Every 5 minutes */
        last_check_time = now;

        /* Check for stale io_uring operations */
        uint64_t pending = AOF_pending_writes();
        if (pending > 1000) {  /* Arbitrary threshold */
            printf("⚠️  High pending write count: %" PRIu64 "\n", pending);
        }

        /* Log rotation health */
        uint64_t rotation_failures = atomic_load(&g_metrics.rotation_failures);
        uint64_t total_rotations = atomic_load(&g_metrics.aof_rotations_total) +
                                   atomic_load(&g_metrics.snapshots_rotated);

        if (total_rotations > 0) {
            double failure_rate = (double) rotation_failures / total_rotations * 100.0;
            if (failure_rate > 5.0) {  /* More than 5% failure rate */
                printf("🚨 High rotation failure rate: %.1f%% (%"PRIu64"/%"PRIu64")\n",
                       failure_rate, rotation_failures, total_rotations);
            }
        }
    }
}

static int (*AOF_rotate_file_safe)(const char *) = NULL;

void RAMForge_rotation_init(const char *rdb_base_path, const char *aof_base_path) {
    g_is_rotation_manager = 1;
    g_rdb_base_path = strdup(rdb_base_path);
    g_aof_base_path = strdup(aof_base_path);

    /* Detect if safe rotation is available */
    AOF_rotate_file_safe = dlsym(RTLD_DEFAULT, "AOF_rotate_file_safe");
    if (AOF_rotate_file_safe) {
        printf("✅ Safe AOF rotation detected and enabled\n");
    } else {
        printf("⚠️  Using standard AOF rotation (safe rotation not available)\n");
    }

    init_latency_histogram();

    /* Use enhanced timer callback */
    uv_timer_init(uv_default_loop(), &g_metrics_timer);
    uv_timer_start(&g_metrics_timer, enhanced_metrics_timer_cb, 5000, 10000); /* 5s initial, 10s interval */

    /* Keep original RDB timer for compatibility */
    uv_timer_init(uv_default_loop(), &g_rotation_timer);
    uv_timer_start(&g_rotation_timer, rotation_timer_cb, 30000, 30000);

    printf("🚀 Enhanced RAMForge Auto-Rotation + Metrics initialized\n");
    printf("📊 RDB Max Size: %zu MB | Max Age: %ld hours | Keep: %d files\n",
           g_policy.max_rdb_size_mb, g_policy.max_age_hours, g_policy.max_snapshots_to_keep);
    printf("📊 AOF Max Size: %zu MB | Max Age: %ld hours | Keep: %d segments\n",
           g_policy.max_aof_size_mb, g_policy.max_age_hours, g_policy.max_aof_segments_to_keep);
}

void RAMForge_configure_rotation_policy(size_t max_rdb_mb, time_t max_age_hours,
                                        int keep_count, int chaos_enabled) {
    g_policy.max_rdb_size_mb = max_rdb_mb;
    g_policy.max_age_hours = max_age_hours;
    g_policy.max_snapshots_to_keep = keep_count;
    g_policy.chaos_rotation_enabled = chaos_enabled;
    printf("⚙️  Rotation policy updated: Size=%zuMB, Age=%ldhrs, Keep=%d, Chaos=%s\n",
           max_rdb_mb, max_age_hours, keep_count, chaos_enabled ? "ON" : "OFF");
}

void RAMForge_force_rotation(void) {
    printf("🔧 Forcing RDB rotation...\n");
    printf("🔧 Forcing AOF rotation...\n");
    execute_rdb_rotation(ROTATION_FORCED);
    execute_aof_rotation(ROTATION_FORCED);
}

void RAMForge_record_snapshot_time(uint64_t time_us) {
    atomic_fetch_add(&g_metrics.snapshots_created, 1);
    atomic_fetch_add(&g_metrics.snapshot_total_time_us, time_us);

    uint64_t current_min = atomic_load(&g_metrics.snapshot_min_time_us);
    while (current_min == 0 || time_us < current_min) {
        if (atomic_compare_exchange_weak(&g_metrics.snapshot_min_time_us, &current_min, time_us)) {
            break;
        }
    }
    uint64_t current_max = atomic_load(&g_metrics.snapshot_max_time_us);
    while (time_us > current_max) {
        if (atomic_compare_exchange_weak(&g_metrics.snapshot_max_time_us, &current_max, time_us)) {
            break;
        }
    }
}

typedef struct {
    time_t last_rotation;
    uint64_t operations_since_rotation;
    double performance_score;
    int consecutive_slow_operations;
} RotationScheduler;
static RotationScheduler g_scheduler = {0};

static void update_rotation_scheduler(uint64_t latency_us) {
    g_scheduler.operations_since_rotation++;
    if (latency_us > 1000) { // > 1ms
        g_scheduler.consecutive_slow_operations++;
    } else {
        g_scheduler.consecutive_slow_operations = 0;
    }
    uint64_t sub_1ms = atomic_load(&g_metrics.sub_1ms_operations);
    uint64_t total = atomic_load(&g_metrics.total_operations);
    g_scheduler.performance_score = total > 0 ? (double) sub_1ms / total : 0.0;

    if (!g_is_rotation_manager) return;   /* workers exit early */

    if (g_scheduler.consecutive_slow_operations > 100 &&
        g_scheduler.performance_score < 0.95) { // Less than 95% sub-1ms
        printf("📉 Performance-based rotation triggered: %.1f%% sub-1ms performance\n",
               g_scheduler.performance_score * 100);
        execute_rdb_rotation(ROTATION_PERFORMANCE);
        g_scheduler.last_rotation = time(NULL);
        g_scheduler.operations_since_rotation = 0;
        g_scheduler.consecutive_slow_operations = 0;
    }
}

void RAMForge_record_crc_validation(int success) {
    atomic_fetch_add(&g_metrics.crc_validations, 1);
    if (!success) {
        atomic_fetch_add(&g_metrics.crc_failures, 1);
    }
}

void RAMForge_record_recovery_attempt(int success) {
    atomic_fetch_add(&g_metrics.recovery_attempts, 1);
    if (success) {
        atomic_fetch_add(&g_metrics.recovery_successes, 1);
    }
}

void RAMForge_get_beast_mode_stats(double *sub_1ms_pct, double *sub_1us_pct, uint64_t *total_ops) {
    *total_ops = atomic_load(&g_metrics.total_operations);
    if (*total_ops > 0) {
        *sub_1ms_pct = (double) atomic_load(&g_metrics.sub_1ms_operations) / *total_ops * 100.0;
        *sub_1us_pct = (double) atomic_load(&g_metrics.sub_1us_operations) / *total_ops * 100.0;
    } else {
        *sub_1ms_pct = *sub_1us_pct = 0.0;
    }
}

void RAMForge_enhanced_operation_record(uint64_t latency_us) {
    update_rotation_scheduler(latency_us);
}

void RAMForge_cleanup_rotation_system(void) {
    if (g_rdb_base_path) {
        free(g_rdb_base_path);
        g_rdb_base_path = NULL;
    }
    if (g_aof_base_path) {
        free(g_aof_base_path);
        g_aof_base_path = NULL;
    }
    uv_timer_stop(&g_rotation_timer);
    uv_timer_stop(&g_metrics_timer);
    pthread_mutex_destroy(&g_latency_hist.lock);
    pthread_mutex_destroy(&g_rotation_lock);
    printf("🧹 RAMForge rotation system cleaned up\n");
}

void RAMForge_export_prometheus_metrics_buffer(char *buffer, size_t capacity) {
    metrics_buffer_t buf;
    metrics_buf_init(&buf, buffer, capacity);
    uint64_t total_ops = atomic_load(&g_metrics.total_operations);

    metrics_buf_printf(&buf, "# HELP ayder_aof_files_total Current AOF segment count\n");
    metrics_buf_printf(&buf, "# TYPE ayder_aof_files_total gauge\n");
    metrics_buf_printf(&buf, "ayder_aof_files_total %" PRIu64 "\n\n",
                       atomic_load(&g_metrics.aof_files_total));


    metrics_buf_printf(&buf, "# TYPE ayder_aof_generation gauge\n");
    metrics_buf_printf(&buf, "ayder_aof_generation %" PRIu64 "\n\n",
                       atomic_load(&g_metrics.aof_generation));

    /* ---------- HTTP counters ---------- */
    metrics_buf_printf(&buf, "# HELP ayder_http_requests_total Total HTTP requests (all statuses)\n");
    metrics_buf_printf(&buf, "# TYPE ayder_http_requests_total counter\n");
    metrics_buf_printf(&buf, "ayder_http_requests_total %" PRIu64 "\n\n", RAMForge_http_total());

    metrics_buf_printf(&buf, "# HELP ayder_http_responses_total HTTP responses by class/code\n");
    metrics_buf_printf(&buf, "# TYPE ayder_http_responses_total counter\n");
    metrics_buf_printf(&buf, "ayder_http_responses_total{code=\"2xx\"} %" PRIu64 "\n", RAMForge_http_2xx());
    metrics_buf_printf(&buf, "ayder_http_responses_total{code=\"4xx\"} %" PRIu64 "\n", RAMForge_http_4xx());
    metrics_buf_printf(&buf, "ayder_http_responses_total{code=\"401\"} %" PRIu64 "\n", RAMForge_http_401());
    metrics_buf_printf(&buf, "ayder_http_responses_total{code=\"429\"} %" PRIu64 "\n", RAMForge_http_429());
    metrics_buf_printf(&buf, "ayder_http_responses_total{code=\"5xx\"} %" PRIu64 "\n", RAMForge_http_5xx());
    metrics_buf_printf(&buf, "ayder_http_responses_total{code=\"503\"} %" PRIu64 "\n\n", RAMForge_http_503());

    metrics_buf_printf(&buf, "# HELP ayder_http_active_connections Active connections\n");
    metrics_buf_printf(&buf, "# TYPE ayder_http_active_connections gauge\n");
    metrics_buf_printf(&buf, "ayder_http_active_connections %" PRIu64 "\n\n", RAMForge_http_active_connections());

    /* ---------- Queue depths ---------- */
    metrics_buf_printf(&buf, "# HELP ayder_aof_pending_writes Pending AOF writes (ring + uring)\n");
    metrics_buf_printf(&buf, "# TYPE ayder_aof_pending_writes gauge\n");
    metrics_buf_printf(&buf, "ayder_aof_pending_writes %zu\n\n", AOF_pending_writes());

    metrics_buf_printf(&buf, "# HELP ayder_sealed_queue_depth Pending sealed batches ready for fsync\n");
    metrics_buf_printf(&buf, "# TYPE ayder_sealed_queue_depth gauge\n");
    metrics_buf_printf(&buf, "ayder_sealed_queue_depth %zu\n\n", AOF_sealed_queue_depth());


    /* ---------- Histograms ---------- */
    int nfs = 0, nsl = 0;
    const double *fsle = AOF_fsync_bucket_bounds(&nfs);
    const double *slele = AOF_seal_bucket_bounds(&nsl);
    uint64_t fsc[HN] = {0}, slc[HN] = {0};
    double fssum = 0.0, slsum = 0.0;
    uint64_t fcnt = 0, scnt = 0;
    AOF_fsync_hist_snapshot(fsc, HN, &fssum, &fcnt);
    AOF_seal_latency_hist_snapshot(slc, HN, &slsum, &scnt);

    // fsync histogram
    metrics_buf_printf(&buf, "# HELP ayder_fsync_duration_seconds fsync group duration\n");
    metrics_buf_printf(&buf, "# TYPE ayder_fsync_duration_seconds histogram\n");
    uint64_t cum = 0;
    for (int i = 0; i < nfs; i++) {
        cum += fsc[i];
        metrics_buf_printf(&buf, "ayder_fsync_duration_seconds_bucket{le=\"%.6f\"} %" PRIu64 "\n", fsle[i], cum);
    }
    metrics_buf_printf(&buf, "ayder_fsync_duration_seconds_bucket{le=\"+Inf\"} %" PRIu64 "\n", cum);
    metrics_buf_printf(&buf, "ayder_fsync_duration_seconds_sum %.9f\n", fssum);
    metrics_buf_printf(&buf, "ayder_fsync_duration_seconds_count %" PRIu64 "\n\n", fcnt);

    // seal durable latency histogram
    metrics_buf_printf(&buf, "# HELP ayder_sealed_durable_latency_seconds time from append to durable fsync\n");
    metrics_buf_printf(&buf, "# TYPE ayder_sealed_durable_latency_seconds histogram\n");
    cum = 0;
    for (int i = 0; i < nsl; i++) {
        cum += slc[i];
        metrics_buf_printf(&buf, "ayder_sealed_durable_latency_seconds_bucket{le=\"%.6f\"} %" PRIu64 "\n", slele[i],
                           cum);
    }
    metrics_buf_printf(&buf, "ayder_sealed_durable_latency_seconds_bucket{le=\"+Inf\"} %" PRIu64 "\n", cum);
    metrics_buf_printf(&buf, "ayder_sealed_durable_latency_seconds_sum %.9f\n", slsum);
    metrics_buf_printf(&buf, "ayder_sealed_durable_latency_seconds_count %" PRIu64 "\n\n", scnt);


    if (buf.capacity > 0)
        buf.buffer[(buf.length < buf.capacity) ? buf.length : buf.capacity - 1] = '\0';
}
