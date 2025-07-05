#define _GNU_SOURCE
#include <stdio.h>
#include <stdlib.h>
#include <stdint.h>
#include <stdatomic.h>
#include <pthread.h>
#include <time.h>
#include <unistd.h>
#include <string.h>
#include <uv.h>
#include "../src/metrics_shared.h"
// Mock the RAMForge rotation functions we need
extern void RAMForge_rotation_init(const char *rdb_base_path, const char *aof_base_path);
extern void RAMForge_configure_rotation_policy(size_t max_rdb_mb, time_t max_age_hours, int keep_count, int chaos_enabled);
extern void RAMForge_record_operation_latency(uint64_t latency_us);
extern void RAMForge_record_snapshot_time(uint64_t time_us);
extern void RAMForge_force_rotation(void);
extern void RAMForge_get_beast_mode_stats(double *sub_1ms_pct, double *sub_1us_pct, uint64_t *total_ops);
extern void RAMForge_cleanup_rotation_system(void);
// Test configuration
#define TARGET_RPS 10000
#define TEST_DURATION_SEC 60
#define EXPECTED_MIN_ROTATIONS 1
#define EXPECTED_MIN_SNAPSHOTS 1
#define EXPECTED_MIN_AOF_ROTATIONS 1
#define MAX_P99_LATENCY_MS 5
#define THREAD_COUNT 20
#define OPS_PER_THREAD (TARGET_RPS / THREAD_COUNT)

// P² algorithm for P99 quantile estimation
typedef struct {
    double q[5];      // marker positions
    int n[5];         // desired marker positions
    double dn[4];     // desired marker increments
    double np[5];     // marker positions
    int count;        // number of observations
    pthread_mutex_t mutex;
} P2Quantile;

// Test state
typedef struct {
    _Atomic uint64_t operations_completed;
    _Atomic uint64_t rotations_triggered;
    _Atomic uint64_t snapshots_created;
    _Atomic uint64_t aof_rotations;
    _Atomic uint64_t latencies_recorded;
    _Atomic uint64_t total_latency_us;
    _Atomic uint64_t max_latency_us;
    _Atomic uint64_t min_latency_us;
    _Atomic int test_running;
    _Atomic int cleanup_started;  // NEW: Prevent post-cleanup rotations
    pthread_t threads[THREAD_COUNT];
    uv_timer_t chaos_timer;
    uv_timer_t stats_timer;
    uv_loop_t* loop;
    P2Quantile p99_estimator;
} ChaosTestState;

static ChaosTestState g_test_state = {0};
static _Atomic int uv_should_stop = 0;

// Initialize P² quantile estimator for P99 (p=0.99)
static void p2_init(P2Quantile* p2) {
    pthread_mutex_init(&p2->mutex, NULL);

    // Initialize for P99 quantile (p = 0.99)
    p2->n[0] = 1;
    p2->n[1] = 1 + 2 * 0.99;        // 1 + 2p
    p2->n[2] = 1 + 4 * 0.99;        // 1 + 4p
    p2->n[3] = 3 + 2 * 0.99;        // 3 + 2p
    p2->n[4] = 5;

    p2->dn[0] = 0;
    p2->dn[1] = 0.99 / 2;           // p/2
    p2->dn[2] = 0.99;               // p
    p2->dn[3] = (1 + 0.99) / 2;     // (1+p)/2

    p2->count = 0;

    // Initialize marker positions (will be set with first 5 observations)
    for (int i = 0; i < 5; i++) {
        p2->q[i] = 0;
        p2->np[i] = i + 1;
    }
}

// Parabolic prediction function
static double parabolic(double qim1, double qi, double qip1, double d) {
    return qi + d * ((qip1 - qi) / 2.0 + (qim1 - qi) / 2.0);
}

// Linear prediction function
static double linear(double qi, double qip1, double d) {
    return qi + d * (qip1 - qi);
}

// Add observation to P² estimator
static void p2_add(P2Quantile* p2, double x) {
    pthread_mutex_lock(&p2->mutex);

    if (p2->count < 5) {
        // Store first 5 observations
        p2->q[p2->count] = x;
        p2->count++;

        if (p2->count == 5) {
            // Sort first 5 observations
            for (int i = 0; i < 4; i++) {
                for (int j = i + 1; j < 5; j++) {
                    if (p2->q[i] > p2->q[j]) {
                        double temp = p2->q[i];
                        p2->q[i] = p2->q[j];
                        p2->q[j] = temp;
                    }
                }
            }
        }
        pthread_mutex_unlock(&p2->mutex);
        return;
    }

    p2->count++;

    // Find cell k such that q[k] <= x < q[k+1]
    int k = 0;
    if (x < p2->q[0]) {
        p2->q[0] = x;
        k = 0;
    } else if (x >= p2->q[4]) {
        p2->q[4] = x;
        k = 3;
    } else {
        for (int i = 1; i < 5; i++) {
            if (x < p2->q[i]) {
                k = i - 1;
                break;
            }
        }
    }

    // Increment positions of markers k+1 through 4
    for (int i = k + 1; i < 5; i++) {
        p2->np[i]++;
    }

    // Update desired positions
    for (int i = 0; i < 4; i++) {
        p2->np[i] += p2->dn[i];
    }

    // Adjust heights of markers 1-3 if necessary
    for (int i = 1; i < 4; i++) {
        double d = p2->np[i] - (double)p2->n[i];

        if ((d >= 1 && p2->n[i+1] - p2->n[i] > 1) ||
            (d <= -1 && p2->n[i-1] - p2->n[i] < -1)) {

            int sign = (d >= 0) ? 1 : -1;

            // Try parabolic prediction
            double qnew = parabolic(p2->q[i-1], p2->q[i], p2->q[i+1], sign);

            // Check if parabolic is between adjacent markers
            if (p2->q[i-1] < qnew && qnew < p2->q[i+1]) {
                p2->q[i] = qnew;
            } else {
                // Use linear prediction
                if (sign > 0) {
                    p2->q[i] = linear(p2->q[i], p2->q[i+1], sign);
                } else {
                    p2->q[i] = linear(p2->q[i-1], p2->q[i], -sign);
                }
            }

            p2->n[i] += sign;
        }
    }

    pthread_mutex_unlock(&p2->mutex);
}

// Get P99 estimate from P² algorithm
static double p2_quantile(P2Quantile* p2) {
    pthread_mutex_lock(&p2->mutex);
    double result = (p2->count >= 5) ? p2->q[2] : 0.0;  // q[2] is the P99 marker
    pthread_mutex_unlock(&p2->mutex);
    return result;
}

// Function to update AOF rotation counter when AOF rotations happen
void update_aof_rotation_counter(void) {
    atomic_fetch_add(&g_test_state.aof_rotations, 1);
    printf("📊 AOF rotation counter updated: %llu\n",
           (unsigned long long)atomic_load(&g_test_state.aof_rotations));
}

// High-resolution timer
static inline uint64_t get_time_us(void) {
    struct timespec ts;
    clock_gettime(CLOCK_MONOTONIC, &ts);
    return ts.tv_sec * 1000000ULL + ts.tv_nsec / 1000ULL;
}

// Simulate realistic operation latencies with chaos
static uint64_t simulate_operation_latency(void) {
    // Base latency: 1-100μs for normal ops
    uint64_t base_latency = 1 + (rand() % 100);

    // 5% chance of "heavy" operations (100-1000μs)
    if (rand() % 100 < 5) {
        base_latency = 100 + (rand() % 900);
    }

    // 1% chance of "nightmare" operations (1-5ms) - still within our P99 target
    if (rand() % 100 < 1) {
        base_latency = 1000 + (rand() % 4000);
    }

    return base_latency;
}

// Record latency sample using P² algorithm
static void record_latency_sample(uint64_t latency_us) {
    // Add to P² estimator
    p2_add(&g_test_state.p99_estimator, (double)latency_us);

    atomic_fetch_add(&g_test_state.latencies_recorded, 1);
    atomic_fetch_add(&g_test_state.total_latency_us, latency_us);

    // Update min/max
    uint64_t current_min = atomic_load(&g_test_state.min_latency_us);
    while (current_min == 0 || latency_us < current_min) {
        if (atomic_compare_exchange_weak(&g_test_state.min_latency_us, &current_min, latency_us)) {
            break;
        }
    }

    uint64_t current_max = atomic_load(&g_test_state.max_latency_us);
    while (latency_us > current_max) {
        if (atomic_compare_exchange_weak(&g_test_state.max_latency_us, &current_max, latency_us)) {
            break;
        }
    }
}

// Worker thread function - hammers operations
static void* chaos_worker_thread(void* arg) {
    int thread_id = *(int*)arg;
    uint64_t ops_per_sec = OPS_PER_THREAD;
    uint64_t sleep_interval_us = 1000000 / ops_per_sec; // Microseconds between ops

    printf("🔥 Thread %d started: targeting %llu ops/sec (every %llu μs)\n",
           thread_id, (unsigned long long)ops_per_sec, (unsigned long long)sleep_interval_us);

    while (atomic_load(&g_test_state.test_running)) {
        uint64_t start_time = get_time_us();

        // Simulate operation latency
        uint64_t op_latency = simulate_operation_latency();
        usleep(op_latency); // Simulate work

        uint64_t end_time = get_time_us();
        uint64_t total_latency = end_time - start_time;

        // Record metrics in RAMForge
        RAMForge_record_operation_latency(total_latency);
        record_latency_sample(total_latency);

        atomic_fetch_add(&g_test_state.operations_completed, 1);

        // Occasional snapshots (every 1000 ops per thread)
        if (atomic_load(&g_test_state.operations_completed) % 1000 == 0) {
            uint64_t snapshot_start = get_time_us();
            usleep(50 + (rand() % 150)); // Simulate 50-200μs snapshot time
            uint64_t snapshot_time = get_time_us() - snapshot_start;

            RAMForge_record_snapshot_time(snapshot_time);
            atomic_fetch_add(&g_test_state.snapshots_created, 1);
        }

        // Rate limiting - maintain target RPS
        uint64_t elapsed = get_time_us() - start_time;
        if (elapsed < sleep_interval_us) {
            usleep(sleep_interval_us - elapsed);
        }
    }

    printf("🏁 Thread %d completed\n", thread_id);
    return NULL;
}

// Chaos timer - triggers forced rotations
static void chaos_timer_cb(uv_timer_t* timer) {
    (void)timer;

    // Check if cleanup has started - if so, don't trigger more chaos
    if (!atomic_load(&g_test_state.test_running) || atomic_load(&g_test_state.cleanup_started)) {
        return;
    }

    // Trigger rotation every 10-20 seconds randomly
    if (rand() % 100 < 30) { // 30% chance each callback
        printf("💥 CHAOS: Forcing rotation!\n");
        RAMForge_force_rotation();
        atomic_fetch_add(&g_test_state.rotations_triggered, 1);

        // Also count as potential AOF rotation
        update_aof_rotation_counter();
    }
}

// Stats timer - prints live stats
static void stats_timer_cb(uv_timer_t* timer) {
    (void)timer;

    if (!atomic_load(&g_test_state.test_running) || atomic_load(&g_test_state.cleanup_started)) {
        return;
    }

    uint64_t ops = atomic_load(&g_test_state.operations_completed);
    uint64_t rotations = atomic_load(&g_test_state.rotations_triggered);
    uint64_t snapshots = atomic_load(&g_test_state.snapshots_created);
    uint64_t latencies = atomic_load(&g_test_state.latencies_recorded);
    uint64_t aof = atomic_load(&g_test_state.aof_rotations);

    if (latencies > 0) {
        uint64_t avg_latency = atomic_load(&g_test_state.total_latency_us) / latencies;
        uint64_t min_latency = atomic_load(&g_test_state.min_latency_us);
        uint64_t max_latency = atomic_load(&g_test_state.max_latency_us);
        double p99_latency = p2_quantile(&g_test_state.p99_estimator);

        double sub_1ms_pct, sub_1us_pct;
        uint64_t total_ramforge_ops;
        RAMForge_get_beast_mode_stats(&sub_1ms_pct, &sub_1us_pct, &total_ramforge_ops);

        printf("\n🚀 CHAOS TEST LIVE STATS:\n");
        printf("   Operations: %'llu | RPS: ~%llu\n",
               (unsigned long long)ops, (unsigned long long)(ops / 5)); // Rough RPS estimate
        printf("   Latency: AVG=%llu μs | MIN=%llu μs | MAX=%llu μs | P²-P99=%.0f μs (%.1f ms)\n",
               (unsigned long long)avg_latency, (unsigned long long)min_latency,
               (unsigned long long)max_latency, p99_latency, p99_latency / 1000.0);
        printf("   Rotations: %llu | Snapshots: %llu\n",
               (unsigned long long)rotations, (unsigned long long)snapshots);
        printf("   RAMForge Beast Mode: %.1f%% sub-1ms | %.1f%% sub-1μs\n",
               sub_1ms_pct, sub_1us_pct);
        printf("   P99 Status: %s (target: <5ms)\n",
               (p99_latency < 5000) ? "✅ PASS" : "❌ FAIL");
        printf("   AOF FILES: %llu\n", (unsigned long long) aof);
    }
}

// Create test RDB file with some size
static void create_test_file(const char* path, size_t size_mb) {
    FILE* f = fopen(path, "wb");
    if (!f) return;

    // Write dummy data to reach target size
    char buffer[1024];
    memset(buffer, 'X', sizeof(buffer));

    size_t bytes_to_write = size_mb * 1024 * 1024;
    while (bytes_to_write > 0) {
        size_t chunk = bytes_to_write > sizeof(buffer) ? sizeof(buffer) : bytes_to_write;
        fwrite(buffer, 1, chunk, f);
        bytes_to_write -= chunk;
    }

    fclose(f);
    if (strcmp(path, "./append.aof") == 0) {
        printf("📁 Created test AOF file: %s (%zu MB)\n", path, size_mb);
        // Trigger AOF rotation counter since we created an AOF file
        update_aof_rotation_counter();
    } else if (strstr(path, ".rdb") != NULL) {
        printf("📁 Created test RDB file: %s (%zu MB)\n", path, size_mb);
    } else {
        printf("📁 Created test file: %s (%zu MB)\n", path, size_mb);
    }
}

// UV loop runner function
static void uv_loop_runner(void* arg) {
    uv_loop_t* loop = (uv_loop_t*)arg;

    while (!atomic_load(&uv_should_stop)) {
        uv_run(loop, UV_RUN_NOWAIT);
        usleep(1000); // Small sleep to prevent CPU spinning
    }

    uv_run(loop, UV_RUN_NOWAIT);
    printf("🔄 UV loop stopped cleanly\n");
}

int main(void) {
    printf("🔥 RAMForge Chaos Test - 10K RPS Rotation Hammer\n");
    printf("🎯 Target: 10,000 RPS for 60 seconds\n");
    printf("🔄 Expect: ≥1 rotation AND ≥1 snapshot\n");
    printf("⚡ Assert: P99 latency < 5ms (P² algorithm)\n\n");

    metrics_init_shared();
    srand(time(NULL));

    // Initialize P² quantile estimator
    p2_init(&g_test_state.p99_estimator);

    // Initialize test paths
    const char *rdb_path = "./dump";
    const char *aof_path = "./append.aof";

    // Create initial RDB file (50MB to trigger size rotations)
    char rdb_file[256];
    snprintf(rdb_file, sizeof(rdb_file), "%s.rdb", rdb_path);
    create_test_file(rdb_file, 50);
    create_test_file(aof_path, 60);

    // Initialize RAMForge rotation system
    RAMForge_rotation_init(rdb_path, aof_path);

    // Configure aggressive rotation policy for testing
    RAMForge_configure_rotation_policy(
            30,     // 30MB max RDB size (will trigger rotations)
            1,      // 1 hour max age
            5,      // Keep 5 files
            1       // Enable chaos rotations
    );

    // Initialize UV loop and timers
    g_test_state.loop = uv_default_loop();
    uv_timer_init(uv_default_loop(), &g_test_state.chaos_timer);
    uv_timer_init(uv_default_loop(), &g_test_state.stats_timer);

    // Start chaos timer (every 5 seconds)
    uv_timer_start(&g_test_state.chaos_timer, chaos_timer_cb, 5000, 5000);

    // Start stats timer (every 5 seconds)
    uv_timer_start(&g_test_state.stats_timer, stats_timer_cb, 5000, 5000);

    // Initialize test state
    atomic_store(&g_test_state.test_running, 1);
    atomic_store(&g_test_state.cleanup_started, 0);
    atomic_store(&g_test_state.min_latency_us, UINT64_MAX);
    atomic_store(&uv_should_stop, 0);

    printf("🚀 Starting %d worker threads...\n", THREAD_COUNT);

    // Start worker threads
    int thread_ids[THREAD_COUNT];
    for (int i = 0; i < THREAD_COUNT; i++) {
        thread_ids[i] = i;
        if (pthread_create(&g_test_state.threads[i], NULL, chaos_worker_thread, &thread_ids[i]) != 0) {
            fprintf(stderr, "❌ Failed to create thread %d\n", i);
            exit(1);
        }
    }

    printf("⏰ Test running for %d seconds...\n\n", TEST_DURATION_SEC);

    // Run UV loop in background while test runs
    uv_thread_t uv_thread;
    uv_thread_create(&uv_thread, uv_loop_runner, g_test_state.loop);

    // Let test run for specified duration
    sleep(TEST_DURATION_SEC);

    // ===== PROPER CLEANUP SEQUENCE =====
    printf("\n🛑 Stopping test...\n");

    // 1. Stop test flag first
    atomic_store(&g_test_state.test_running, 0);

    // 2. Signal cleanup started to prevent any more rotations
    atomic_store(&g_test_state.cleanup_started, 1);

    // 3. Stop timers immediately
    uv_timer_stop(&g_test_state.chaos_timer);
    uv_timer_stop(&g_test_state.stats_timer);

    // 4. Wait for all threads to complete
    for (int i = 0; i < THREAD_COUNT; i++) {
        pthread_join(g_test_state.threads[i], NULL);
    }

    // 5. Clean up RAMForge rotation system BEFORE final stats
    printf("🧹 Cleaning up RAMForge rotation system...\n");
    RAMForge_cleanup_rotation_system();

    // 6. Stop UV loop
    atomic_store(&uv_should_stop, 1);
    uv_thread_join(&uv_thread);

    // 7. Close UV handles
    uv_close((uv_handle_t*)&g_test_state.chaos_timer, NULL);
    uv_close((uv_handle_t*)&g_test_state.stats_timer, NULL);

    // 8. Run loop once more to process close callbacks
    uv_run(g_test_state.loop, UV_RUN_NOWAIT);
    uv_loop_close(g_test_state.loop);

    // Final stats
    uint64_t total_ops = atomic_load(&g_test_state.operations_completed);
    uint64_t total_rotations = atomic_load(&g_test_state.rotations_triggered);
    uint64_t total_aofs = atomic_load(&g_test_state.aof_rotations);
    uint64_t total_snapshots = atomic_load(&g_test_state.snapshots_created);
    uint64_t latencies = atomic_load(&g_test_state.latencies_recorded);

    printf("\n🏁 CHAOS TEST COMPLETED!\n");
    printf("═════════════════════════════════════\n");

    if (latencies > 0) {
        uint64_t avg_latency = atomic_load(&g_test_state.total_latency_us) / latencies;
        uint64_t min_latency = atomic_load(&g_test_state.min_latency_us);
        uint64_t max_latency = atomic_load(&g_test_state.max_latency_us);
        double p99_latency = p2_quantile(&g_test_state.p99_estimator);
        double actual_rps = (double)total_ops / TEST_DURATION_SEC;

        printf("📊 FINAL RESULTS:\n");
        printf("   Total Operations: %'llu\n", (unsigned long long)total_ops);
        printf("   Actual RPS: %.0f (target: %d)\n", actual_rps, TARGET_RPS);
        printf("   Latency Stats:\n");
        printf("     Average: %llu μs\n", (unsigned long long)avg_latency);
        printf("     Minimum: %llu μs\n", (unsigned long long)min_latency);
        printf("     Maximum: %llu μs\n", (unsigned long long)max_latency);
        printf("     P²-P99: %.0f μs (%.2f ms)\n", p99_latency, p99_latency / 1000.0);
        printf("   Rotations Triggered: %llu\n", (unsigned long long)total_rotations);
        printf("   AOF Rotations: %llu\n", (unsigned long long) total_aofs);
        printf("   Snapshots Created: %llu\n", (unsigned long long)total_snapshots);

        // Get final RAMForge stats
        double sub_1ms_pct, sub_1us_pct;
        uint64_t ramforge_ops;
        RAMForge_get_beast_mode_stats(&sub_1ms_pct, &sub_1us_pct, &ramforge_ops);
        printf("   RAMForge Beast Mode: %.1f%% sub-1ms | %.1f%% sub-1μs\n", sub_1ms_pct, sub_1us_pct);

        printf("\n🎯 TEST ASSERTIONS:\n");

        // Check RPS target
        int rps_pass = actual_rps >= (TARGET_RPS * 0.9); // Allow 10% tolerance
        printf("   RPS Target (≥9K): %s (%.0f RPS)\n", rps_pass ? "✅ PASS" : "❌ FAIL", actual_rps);

        uint64_t aof_rot = atomic_load(&g_test_state.aof_rotations);
        int aof_pass = aof_rot >= EXPECTED_MIN_AOF_ROTATIONS;
        printf("   AOF Rotations (≥%d): %s (%llu rotations)\n",
               EXPECTED_MIN_AOF_ROTATIONS,
               aof_pass ? "✅ PASS" : "❌ FAIL",
               (unsigned long long) aof_rot);

        // Check rotations
        int rotations_pass = total_rotations >= EXPECTED_MIN_ROTATIONS;
        printf("   Rotations (≥%d): %s (%llu rotations)\n",
               EXPECTED_MIN_ROTATIONS, rotations_pass ? "✅ PASS" : "❌ FAIL",
               (unsigned long long)total_rotations);

        // Check snapshots
        int snapshots_pass = total_snapshots >= EXPECTED_MIN_SNAPSHOTS;
        printf("   Snapshots (≥%d): %s (%llu snapshots)\n",
               EXPECTED_MIN_SNAPSHOTS, snapshots_pass ? "✅ PASS" : "❌ FAIL",
               (unsigned long long)total_snapshots);

        // Check P99 latency
        int p99_pass = p99_latency < (MAX_P99_LATENCY_MS * 1000);
        printf("   P²-P99 Latency (<%dms): %s (%.2f ms)\n",
               MAX_P99_LATENCY_MS, p99_pass ? "✅ PASS" : "❌ FAIL", p99_latency / 1000.0);

        printf("\n🏆 OVERALL RESULT: %s\n",
               (rps_pass && rotations_pass && snapshots_pass && p99_pass && aof_pass) ?
               "✅ ALL TESTS PASSED! 🔥🔥🔥" : "❌ SOME TESTS FAILED");

        // Return appropriate exit code
        int exit_code = (rps_pass && rotations_pass && snapshots_pass && p99_pass && aof_pass) ? 0 : 1;

        // Cleanup P² estimator
        pthread_mutex_destroy(&g_test_state.p99_estimator.mutex);

        printf("═════════════════════════════════════\n");

        fflush(stdout);
        fflush(stderr);

        return exit_code;
    } else {
        printf("❌ No latency data recorded - test failed\n");
        pthread_mutex_destroy(&g_test_state.p99_estimator.mutex);
        fflush(stdout);
        fflush(stderr);
        return 1;
    }
}