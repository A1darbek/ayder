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
#include "ramforge_rotation_metrics.h"
#include "log.h"
#include "aof_batch.h"
#include "zero_pause_rdb.h"
#include "metrics_shared.h"

typedef struct Storage Storage;
extern void storage_iterate(Storage *st, void (*cb)(int, const void*, size_t, void*), void *ud);
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

void ZeroPauseRDB_metrics_inc(uint64_t usec)
{
    atomic_fetch_add(&g_metrics.zp_snapshots, 1);
    atomic_fetch_add(&g_metrics.zp_snapshot_us, usec);
}

static void init_latency_histogram(void) {
    pthread_mutex_init(&g_latency_hist.lock, NULL);
    g_latency_hist.bucket_boundaries[0] = 1;
    for (int i = 1; i < LATENCY_BUCKETS; i++) {
        if (i % 3 == 1) {
            g_latency_hist.bucket_boundaries[i] = g_latency_hist.bucket_boundaries[i-1] * 2;
        } else if (i % 3 == 2) {
            g_latency_hist.bucket_boundaries[i] = g_latency_hist.bucket_boundaries[i-1] * 2.5;
        } else {
            g_latency_hist.bucket_boundaries[i] = g_latency_hist.bucket_boundaries[i-1] * 2;
        }
    }
}
static void record_latency(uint64_t latency_us) {
    pthread_mutex_lock(&g_latency_hist.lock);
    int bucket = 0;
    for (int i = 0; i < LATENCY_BUCKETS - 1; i++) {
        if (latency_us <= g_latency_hist.bucket_boundaries[i]) {
            bucket = i;
            break;
        }
    }
    if (bucket == 0 && latency_us > g_latency_hist.bucket_boundaries[LATENCY_BUCKETS-1]) {
        bucket = LATENCY_BUCKETS - 1;
    }
    g_latency_hist.buckets[bucket]++;
    atomic_fetch_add(&g_latency_hist.total_samples, 1);
    pthread_mutex_unlock(&g_latency_hist.lock);
}
static uint64_t calculate_percentile(double percentile) {
    pthread_mutex_lock(&g_latency_hist.lock);
    uint64_t total = atomic_load(&g_latency_hist.total_samples);
    if (total == 0) {
        pthread_mutex_unlock(&g_latency_hist.lock);
        return 0;
    }
    uint64_t target = (uint64_t)(total * percentile / 100.0);
    uint64_t cumulative = 0;
    for (int i = 0; i < LATENCY_BUCKETS; i++) {
        cumulative += g_latency_hist.buckets[i];
        if (cumulative >= target) {
            pthread_mutex_unlock(&g_latency_hist.lock);
            return g_latency_hist.bucket_boundaries[i];
        }
    }
    pthread_mutex_unlock(&g_latency_hist.lock);
    return g_latency_hist.bucket_boundaries[LATENCY_BUCKETS-1];
}
void RAMForge_record_operation_latency(uint64_t latency_us) {
    atomic_fetch_add(&g_metrics.total_operations, 1);
    atomic_fetch_add(&g_metrics.total_latency_us, latency_us);
    uint64_t current_min = atomic_load(&g_metrics.min_latency_us);
    while (current_min == 0 || latency_us < current_min) {
        if (atomic_compare_exchange_weak(&g_metrics.min_latency_us, &current_min, latency_us)) {
            break;
        }
    }
    uint64_t current_max = atomic_load(&g_metrics.max_latency_us);
    while (latency_us > current_max) {
        if (atomic_compare_exchange_weak(&g_metrics.max_latency_us, &current_max, latency_us)) {
            break;
        }
    }
    atomic_fetch_add(&g_metrics.beast_mode_ops, 1);

    if (latency_us < 1000) {     // < 1ms
        atomic_fetch_add(&g_metrics.sub_1ms_operations, 1);
    }
    if (latency_us < 100) {      // < 100μs
        atomic_fetch_add(&g_metrics.sub_100us_operations, 1);
    }
    if (latency_us < 10) {       // < 10μs
        atomic_fetch_add(&g_metrics.sub_10us_operations, 1);
    }
    if (latency_us < 1) {        // < 1μs (the holy grail!)
        atomic_fetch_add(&g_metrics.sub_1us_operations, 1);
    }
    record_latency(latency_us);
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
    if ((int)glob_result.gl_pathc <= keep_count) {
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
    for (int i = keep_count; i < (int)glob_result.gl_pathc; i++) {
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
    (void)timer;
    RotationReason reason = should_rotate_rdb();
    if (reason != ROTATION_NONE) {
        execute_rdb_rotation(reason);
    }
}
static void update_percentile_metrics(void) {
    atomic_store(&g_metrics.p99_latency_us, calculate_percentile(99.0));
    atomic_store(&g_metrics.p95_latency_us, calculate_percentile(95.0));
}


static void metrics_timer_cb(uv_timer_t *timer) {
    (void)timer;
    update_percentile_metrics();
    uint64_t total_ops = atomic_load(&g_metrics.total_operations);
    if (total_ops == 0) return;
    uint64_t avg_latency = atomic_load(&g_metrics.total_latency_us) / total_ops;
    uint64_t sub_1ms = atomic_load(&g_metrics.sub_1ms_operations);
    uint64_t sub_100us = atomic_load(&g_metrics.sub_100us_operations);
    uint64_t sub_10us = atomic_load(&g_metrics.sub_10us_operations);
    uint64_t sub_1us = atomic_load(&g_metrics.sub_1us_operations);
    LOGD("\n🔥 ═══ RAMFORGE BEAST MODE METRICS ═══\n");
    LOGD("⚡ Operations: %"PRIu64" | Avg: %"PRIu64"μs | Min: %"PRIu64"μs | Max: %"PRIu64"μs\n",
           total_ops, avg_latency, atomic_load(&g_metrics.min_latency_us),
           atomic_load(&g_metrics.max_latency_us));
    LOGD("🎯 P95: %"PRIu64"μs | P99: %"PRIu64"μs\n",
           atomic_load(&g_metrics.p95_latency_us), atomic_load(&g_metrics.p99_latency_us));
    LOGD("🚀 Beast Performance Breakdown:\n");
    LOGD("   < 1ms:   %"PRIu64" ops (%.1f%%)\n", sub_1ms, (double)sub_1ms/total_ops*100);
    LOGD("   < 100μs: %"PRIu64" ops (%.1f%%)\n", sub_100us, (double)sub_100us/total_ops*100);
    LOGD("   < 10μs:  %"PRIu64" ops (%.1f%%)\n", sub_10us, (double)sub_10us/total_ops*100);
    LOGD("   < 1μs:   %"PRIu64" ops (%.1f%%) 🏆\n", sub_1us, (double)sub_1us/total_ops*100);
    LOGD("📸 Snapshots: %"PRIu64" created | %"PRIu64" rotated\n",
           atomic_load(&g_metrics.snapshots_created), atomic_load(&g_metrics.snapshots_rotated));
    LOGD("🔄 Rotations: Size=%"PRIu64" | Age=%"PRIu64" | Perf=%"PRIu64" | Chaos=%"PRIu64"\n",
           atomic_load(&g_metrics.rotations_by_size), atomic_load(&g_metrics.rotations_by_age),
           atomic_load(&g_metrics.rotations_by_performance), atomic_load(&g_metrics.rotations_by_chaos));
    LOGD("💾 Storage: RDB=%"PRIu64" files | AOF=%"PRIu64" files | Freed=%"PRIu64"MB\n",
           atomic_load(&g_metrics.rdb_files_total), atomic_load(&g_metrics.aof_files_total),
           atomic_load(&g_metrics.disk_space_freed_mb));
    LOGD("═══════════════════════════════════════\n\n");
}

static int g_dashboard_mode = 0;
void RAMForge_enable_dashboard_mode(int enable) {
    g_dashboard_mode = enable;
    printf("%s Live dashboard mode\n", enable ? "🔥 Enabled" : "📊 Disabled");
}

static void display_live_dashboard(void) {
    printf("\033[2J\033[H"); // Clear screen and move to top
    uint64_t total_ops = atomic_load(&g_metrics.total_operations);
    if (total_ops == 0) return;
    uint64_t avg_latency = atomic_load(&g_metrics.total_latency_us) / total_ops;
    uint64_t sub_1ms = atomic_load(&g_metrics.sub_1ms_operations);
    uint64_t sub_1us = atomic_load(&g_metrics.sub_1us_operations);
    double sub_1ms_pct = (double)sub_1ms / total_ops * 100;
    double sub_1us_pct = (double)sub_1us / total_ops * 100;
    printf("🔥 ═══════════════════════════════════════════════════════════════\n");
    printf("🚀 RAMFORGE BEAST MODE - LIVE PERFORMANCE DASHBOARD\n");
    printf("🔥 ═══════════════════════════════════════════════════════════════\n\n");
    printf("⚡ OPERATIONS: %'llu total | AVG: %llu μs | MIN: %llu μs | MAX: %llu μs\n",
           (unsigned long long)total_ops, (unsigned long long)avg_latency,
           (unsigned long long)atomic_load(&g_metrics.min_latency_us),
           (unsigned long long)atomic_load(&g_metrics.max_latency_us));
    printf("🎯 PERCENTILES: P95: %llu μs | P99: %llu μs\n",
           (unsigned long long)atomic_load(&g_metrics.p95_latency_us),
           (unsigned long long)atomic_load(&g_metrics.p99_latency_us));
    printf("\n🏆 BEAST MODE PERFORMANCE:\n");
    printf("   Sub-1ms:  %'llu ops (%.1f%%) ", (unsigned long long)sub_1ms, sub_1ms_pct);
    if (sub_1ms_pct > 99.0) printf("🔥🔥🔥");
    else if (sub_1ms_pct > 95.0) printf("🔥🔥");
    else if (sub_1ms_pct > 90.0) printf("🔥");
    printf("\n");
    printf("   Sub-1μs:  %'llu ops (%.1f%%) ", (unsigned long long)sub_1us, sub_1us_pct);
    if (sub_1us_pct > 50.0) printf("👑🏆");
    else if (sub_1us_pct > 25.0) printf("🏆");
    else if (sub_1us_pct > 10.0) printf("⭐");
    printf("\n\n");
    printf("🎪 PERFORMANCE VISUALIZATION:\n");
    printf("   Sub-1ms: [");
    int bars = (int)(sub_1ms_pct / 2); // 50 chars max
    for (int i = 0; i < 50; i++) {
        if (i < bars) printf("█");
        else printf("░");
    }
    printf("] %.1f%%\n", sub_1ms_pct);
    printf("   Sub-1μs: [");
    bars = (int)(sub_1us_pct / 2);
    for (int i = 0; i < 50; i++) {
        if (i < bars) printf("█");
        else printf("░");
    }
    printf("] %.1f%%\n\n", sub_1us_pct);
    printf("💾 STORAGE: RDB=%llu files | AOF=%llu files | Freed=%llu MB\n",
           (unsigned long long)atomic_load(&g_metrics.rdb_files_total),
           (unsigned long long)atomic_load(&g_metrics.aof_files_total),
           (unsigned long long)atomic_load(&g_metrics.disk_space_freed_mb));
    printf("🔄 ROTATIONS: Size=%llu | Age=%llu | Perf=%llu | Chaos=%llu\n",
           (unsigned long long)atomic_load(&g_metrics.rotations_by_size),
           (unsigned long long)atomic_load(&g_metrics.rotations_by_age),
           (unsigned long long)atomic_load(&g_metrics.rotations_by_performance),
           (unsigned long long)atomic_load(&g_metrics.rotations_by_chaos));
    printf("\n🛡️  DURABILITY: CRC OK=%llu | Failures=%llu | Recovery=%llu/%llu\n",
           (unsigned long long)atomic_load(&g_metrics.crc_validations),
           (unsigned long long)atomic_load(&g_metrics.crc_failures),
           (unsigned long long)atomic_load(&g_metrics.recovery_successes),
           (unsigned long long)atomic_load(&g_metrics.recovery_attempts));
    time_t now = time(NULL);
    printf("\n🕒 Last updated: %s", ctime(&now));
    printf("═══════════════════════════════════════════════════════════════\n");
}

static RotationReason should_rotate_aof(void) {
    char current_aof[512];
    snprintf(current_aof, sizeof(current_aof), "%s", g_aof_base_path);
    size_t size_mb = get_file_size_mb(current_aof);
    if (size_mb > g_policy.max_aof_size_mb) {
        printf("📏 AOF rotation triggered by size: %zu MB > %zu MB\n",
               size_mb, g_policy.max_aof_size_mb);
        return ROTATION_SIZE;
    }
    time_t age_hours = get_file_age_hours(current_aof);
    if (age_hours > g_policy.max_age_hours) {
        printf("⏰ AOF rotation triggered by age: %ld hours > %ld hours\n",
               age_hours, g_policy.max_age_hours);
        return ROTATION_AGE;
    }
    return ROTATION_NONE;
}
static void execute_aof_rotation(RotationReason reason) {
    pthread_mutex_lock(&g_rotation_lock);

    /* ── 1. build timestamped name and move the live file ────────── */
    char ts_path[512];  // Fixed variable name
    char ts[32];
    strftime(ts, sizeof(ts), "%Y%m%d%H%M%S", localtime(&(time_t){time(NULL)}));
    snprintf(ts_path, sizeof(ts_path), "%s_%s", g_aof_base_path, ts);  // Fixed variable name

    /* rename append.aof → append.aof_YYYYMMDD_HHMMSS */
    if (rename(g_aof_base_path, ts_path) != 0) {
        perror("rename AOF");
        pthread_mutex_unlock(&g_rotation_lock);
        return;
    }

    /* reopen a fresh, empty append.aof */
    FILE *new_aof = fopen(g_aof_base_path, "wb+");  // Fixed pointer declaration
    if (!new_aof) {
        perror("fopen new AOF");
        /* best effort – keep going; we still have the rotated file */
    } else {
        fclose(new_aof);
    }

    /* ── 2. start background rewrite of the rotated segment ─────── */
    if (AOF_begin_rewrite(ts_path) == 0) {  // Fixed function call - just pass string
        LOGI("📝 AOF rewrite started on %s\n", ts_path);
    } else {
        LOGW("⚠️ AOF rewrite skipped (already running)\n");
    }

    /* ── 3. take a zero-pause snapshot to pair with fresh AOF ────── */
    ZeroPauseRDB_snapshot(); /* non-blocking; <5 ms */

    /* ── 4. book-keeping & Prometheus counters ─────────────────── */
    atomic_fetch_add(&g_metrics.rotations_by_size, 1);
    atomic_fetch_add(&g_metrics.aof_rotations_total, 1);   /* <── count per-segment */

    char pattern[512];
    snprintf(pattern, sizeof(pattern), "%s_*", g_aof_base_path);
    cleanup_old_files_by_count(pattern, g_policy.max_aof_segments_to_keep);

    atomic_store(&g_metrics.aof_files_total, count_files_with_pattern(pattern));
    pthread_mutex_unlock(&g_rotation_lock);
}
static void enhanced_metrics_timer_cb(uv_timer_t *timer) {
    (void)timer;
    update_percentile_metrics();
    if (g_dashboard_mode) {
        display_live_dashboard();
    } else {
        metrics_timer_cb(timer);
    }
    RotationReason rdb_reason = should_rotate_rdb();
    if (rdb_reason != ROTATION_NONE) {
        execute_rdb_rotation(rdb_reason);
    }

    RotationReason aof_reason = should_rotate_aof();
    if (aof_reason != ROTATION_NONE) {
        execute_aof_rotation(aof_reason);
    }
}

void RAMForge_rotation_init(const char *rdb_base_path, const char *aof_base_path) {
    g_rdb_base_path = strdup(rdb_base_path);
    g_aof_base_path = strdup(aof_base_path);
    init_latency_histogram();
    uv_timer_init(uv_default_loop(), &g_rotation_timer);
    uv_timer_start(&g_rotation_timer, rotation_timer_cb, 30000, 30000);
    uv_timer_init(uv_default_loop(), &g_metrics_timer);
    uv_timer_start(&g_metrics_timer, enhanced_metrics_timer_cb, 10000, 10000);
    printf("🚀 RAMForge Auto-Rotation + Metrics initialized\n");
    printf("📊 RDB Max Size: %zu MB | Max Age: %ld hours | Keep: %d files\n",
           g_policy.max_rdb_size_mb, g_policy.max_age_hours, g_policy.max_snapshots_to_keep);
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
    g_scheduler.performance_score = total > 0 ? (double)sub_1ms / total : 0.0;
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
        *sub_1ms_pct = (double)atomic_load(&g_metrics.sub_1ms_operations) / *total_ops * 100.0;
        *sub_1us_pct = (double)atomic_load(&g_metrics.sub_1us_operations) / *total_ops * 100.0;
    } else {
        *sub_1ms_pct = *sub_1us_pct = 0.0;
    }
}
void RAMForge_enhanced_operation_record(uint64_t latency_us) {
    RAMForge_record_operation_latency(latency_us);
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

void RAMForge_export_prometheus_metrics_buffer(char* buffer, size_t capacity) {
    metrics_buffer_t buf;
    metrics_buf_init(&buf, buffer, capacity);
    uint64_t total_ops = atomic_load(&g_metrics.total_operations);
    uint64_t avg_latency = total_ops > 0 ? atomic_load(&g_metrics.total_latency_us) / total_ops : 0;
    metrics_buf_printf(&buf, "# HELP ramforge_operations_total Total number of operations processed\n");
    metrics_buf_printf(&buf, "# TYPE ramforge_operations_total counter\n");
    metrics_buf_printf(&buf, "ramforge_operations_total %" PRIu64 "\n\n", total_ops);
    metrics_buf_printf(&buf, "# HELP ramforge_latency_microseconds Operation latency in microseconds\n");
    metrics_buf_printf(&buf, "# TYPE ramforge_latency_microseconds summary\n");
    metrics_buf_printf(&buf, "ramforge_latency_microseconds{quantile=\"0.95\"} %" PRIu64 "\n", atomic_load(&g_metrics.p95_latency_us));
    metrics_buf_printf(&buf, "ramforge_latency_microseconds{quantile=\"0.99\"} %" PRIu64 "\n", atomic_load(&g_metrics.p99_latency_us));
    metrics_buf_printf(&buf, "ramforge_latency_microseconds_sum %" PRIu64 "\n", atomic_load(&g_metrics.total_latency_us));
    metrics_buf_printf(&buf, "ramforge_latency_microseconds_count %" PRIu64 "\n\n", total_ops);
    metrics_buf_printf(&buf, "# HELP ramforge_beast_mode_operations Operations under specific latency thresholds\n");
    metrics_buf_printf(&buf, "# TYPE ramforge_beast_mode_operations counter\n");
    metrics_buf_printf(&buf, "ramforge_beast_mode_operations{threshold=\"1ms\"} %" PRIu64 "\n", atomic_load(&g_metrics.sub_1ms_operations));
    metrics_buf_printf(&buf, "ramforge_beast_mode_operations{threshold=\"100us\"} %" PRIu64 "\n", atomic_load(&g_metrics.sub_100us_operations));
    metrics_buf_printf(&buf, "ramforge_beast_mode_operations{threshold=\"10us\"} %" PRIu64 "\n", atomic_load(&g_metrics.sub_10us_operations));
    metrics_buf_printf(&buf, "ramforge_beast_mode_operations{threshold=\"1us\"} %" PRIu64 "\n\n", atomic_load(&g_metrics.sub_1us_operations));
    metrics_buf_printf(&buf, "# HELP ramforge_snapshots_total Total snapshots created\n");
    metrics_buf_printf(&buf, "# TYPE ramforge_snapshots_total counter\n");
    metrics_buf_printf(&buf, "ramforge_snapshots_total %" PRIu64 "\n\n", atomic_load(&g_metrics.snapshots_created));
    metrics_buf_printf(&buf, "# HELP ramforge_rotations_total File rotations by reason\n");
    metrics_buf_printf(&buf, "# TYPE ramforge_rotations_total counter\n");
    metrics_buf_printf(&buf, "ramforge_rotations_total{reason=\"size\"} %" PRIu64 "\n", atomic_load(&g_metrics.rotations_by_size));
    metrics_buf_printf(&buf, "ramforge_rotations_total{reason=\"age\"} %" PRIu64 "\n", atomic_load(&g_metrics.rotations_by_age));
    metrics_buf_printf(&buf, "ramforge_rotations_total{reason=\"performance\"} %" PRIu64 "\n", atomic_load(&g_metrics.rotations_by_performance));
    metrics_buf_printf(&buf, "ramforge_rotations_total{reason=\"chaos\"} %" PRIu64 "\n\n", atomic_load(&g_metrics.rotations_by_chaos));
    metrics_buf_printf(&buf, "# HELP ramforge_crc_validations_total CRC validations performed\n");
    metrics_buf_printf(&buf, "# TYPE ramforge_crc_validations_total counter\n");
    metrics_buf_printf(&buf, "ramforge_crc_validations_total %" PRIu64 "\n\n", atomic_load(&g_metrics.crc_validations));
    metrics_buf_printf(&buf, "# HELP ramforge_beast_mode_percentage Percentage of sub-1us operations\n");
    metrics_buf_printf(&buf, "# TYPE ramforge_beast_mode_percentage gauge\n");
    metrics_buf_printf(&buf, "ramforge_beast_mode_percentage %.2f\n\n",
                       total_ops > 0 ? (double)atomic_load(&g_metrics.sub_1us_operations)/total_ops*100 : 0.0);

    metrics_buf_printf(&buf,
                       "# HELP zero_pause_snapshots_total Total zero-pause snapshots\n");
    metrics_buf_printf(&buf,
                       "# TYPE zero_pause_snapshots_total counter\n");
    metrics_buf_printf(&buf,
                       "zero_pause_snapshots_total %" PRIu64 "\n\n",
                       atomic_load(&g_metrics.zp_snapshots));

    metrics_buf_printf(&buf,
                       "# HELP zero_pause_snapshot_latency_microseconds Snapshot latency\n");
    metrics_buf_printf(&buf,
                       "# TYPE zero_pause_snapshot_latency_microseconds summary\n");
    metrics_buf_printf(&buf,
                       "zero_pause_snapshot_latency_microseconds{quantile=\"0.99\"} %" PRIu64 "\n\n",
                       atomic_load(&g_metrics.zp_snapshot_us));


    metrics_buf_printf(&buf, "# HELP ramforge_aof_files_total Current AOF segment count\n");
    metrics_buf_printf(&buf, "# TYPE ramforge_aof_files_total gauge\n");
    metrics_buf_printf(&buf, "ramforge_aof_files_total %" PRIu64 "\n\n",
            atomic_load(&g_metrics.aof_files_total));

    metrics_buf_printf(&buf, "# HELP ramforge_aof_rotations_total Total AOF rotations performed\n");
    metrics_buf_printf(&buf, "# TYPE ramforge_aof_rotations_total counter\n");
    metrics_buf_printf(&buf, "ramforge_aof_rotations_total %"PRIu64"\n\n",
                       atomic_load(&g_metrics.aof_rotations_total));

    if (buf.capacity > 0)
        buf.buffer[(buf.length < buf.capacity) ? buf.length : buf.capacity - 1] = '\0';
}
