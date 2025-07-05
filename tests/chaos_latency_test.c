#include "../src/zero_pause_rdb.h"
#include "../src/storage.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <unistd.h>
#include <pthread.h>
#include <sys/time.h>
#include <inttypes.h>

// Test configuration
typedef struct {
    int    duration_seconds;
    int    write_threads;
    int    read_threads;
    int    writes_per_second;
    int    chaos_enabled;
    size_t data_size_min;
    size_t data_size_max;
    int    snapshot_interval_sec;
} ChaosTestConfig;

// Thread context for load generation
typedef struct {
    Storage *storage;
    int      thread_id;
    int      operations;
    double   avg_latency_us;
    double   max_latency_us;
    int      running;
} ThreadContext;

// Latency measurement utilities
static inline uint64_t get_timestamp_us(void) {
    struct timeval tv;
    gettimeofday(&tv, NULL);
    return tv.tv_sec * 1000000ULL + tv.tv_usec;
}

// Writer thread - generates continuous write load
static void* chaos_writer_thread(void *arg) {
    ThreadContext *ctx = (ThreadContext*)arg;
    char data_buffer[8192];
    uint64_t total_latency = 0;
    uint64_t max_latency = 0;
    int ops = 0;

    printf("🔥 Writer thread %d started\n", ctx->thread_id);

    while (ctx->running) {
        int key_id = rand() % 100000;
        size_t data_size = 64 + (rand() % 1024);  // 64B to 1KB

        // Fill with random data
        for (size_t i = 0; i < data_size; i++) {
            data_buffer[i] = rand() & 0xFF;
        }

        uint64_t start = get_timestamp_us();
        storage_save(ctx->storage, key_id, data_buffer, data_size);
        uint64_t end = get_timestamp_us();

        uint64_t latency = end - start;
        total_latency += latency;
        if (latency > max_latency) max_latency = latency;
        ops++;

        // Throttle to target ops/sec
        usleep(1000000 / 1000);  // ~1000 ops/sec per thread
    }

    ctx->operations = ops;
    ctx->avg_latency_us = ops > 0 ? (double)total_latency / ops : 0;
    ctx->max_latency_us = max_latency;

    printf("📊 Writer thread %d: %d ops, avg: %.1fμs, max: %.1fμs\n",
           ctx->thread_id, ops, ctx->avg_latency_us, ctx->max_latency_us);
    return NULL;
}

// Reader thread - generates read load and measures latency
static void* chaos_reader_thread(void *arg) {
    ThreadContext *ctx = (ThreadContext*)arg;
    char read_buffer[8192];
    uint64_t total_latency = 0;
    uint64_t max_latency = 0;
    int ops = 0;

    printf("📖 Reader thread %d started\n", ctx->thread_id);

    while (ctx->running) {
        int key_id = rand() % 100000;

        uint64_t start = get_timestamp_us();
        int found = storage_get(ctx->storage, key_id, read_buffer, sizeof(read_buffer));
        uint64_t end = get_timestamp_us();

        uint64_t latency = end - start;
        total_latency += latency;
        if (latency > max_latency) max_latency = latency;
        ops++;

        (void)found;  // Suppress unused warning
        usleep(2000);  // ~500 reads/sec per thread
    }

    ctx->operations = ops;
    ctx->avg_latency_us = ops > 0 ? (double)total_latency / ops : 0;
    ctx->max_latency_us = max_latency;

    printf("📊 Reader thread %d: %d ops, avg: %.1fμs, max: %.1fμs\n",
           ctx->thread_id, ops, ctx->avg_latency_us, ctx->max_latency_us);
    return NULL;
}

// Monitor thread - tracks snapshot progress and latency spikes
static void* chaos_monitor_thread(void *arg) {
    ThreadContext *ctx = (ThreadContext*)arg;
    ZeroPauseStats stats;
    uint64_t last_generation = 0;

    printf("📡 Monitor thread started\n");

    while (ctx->running) {
        ZeroPauseRDB_stats(&stats);

        if (stats.current_generation != last_generation) {
            printf("📸 Snapshot generation %" PRIu64 " → %" PRIu64 " (%s)\n",
                    last_generation, stats.current_generation,
                    stats.active_snapshot ? "ACTIVE" : "COMPLETE");
            last_generation = stats.current_generation;
        }

        if (stats.active_snapshot) {
            printf("⚡ Snapshot in progress: %" PRIu64 " entries, %" PRIu64 " COW entries\n",
                    (uint64_t)stats.entries_written, (uint64_t)stats.snapshot_table_entries);
        }

        sleep(5);
    }

    return NULL;
}

// Main chaos test runner
int chaos_latency_test_run(ChaosTestConfig *config) {
    printf("🎭 Starting RAMForge Chaos Latency Test\n");
    printf("   Duration: %ds, Writers: %d, Readers: %d\n",
           config->duration_seconds, config->write_threads, config->read_threads);
    printf("   Chaos: %s, Snapshots: every %ds\n",
           config->chaos_enabled ? "ENABLED" : "DISABLED",
           config->snapshot_interval_sec);

    // Initialize storage and zero-pause RDB
    Storage storage;
    storage_init(&storage);

    ZeroPauseRDB_init("test_chaos.rdb", &storage, 200000,
                      config->snapshot_interval_sec);

    if (config->chaos_enabled) {
        ZeroPauseRDB_chaos_test(1);
    }

    // Create worker threads
    int total_threads = config->write_threads + config->read_threads + 1;
    pthread_t *threads = malloc(total_threads * sizeof(pthread_t));
    ThreadContext *contexts = calloc(total_threads, sizeof(ThreadContext));

    int thread_idx = 0;

    // Start writer threads
    for (int i = 0; i < config->write_threads; i++) {
        contexts[thread_idx].storage = &storage;
        contexts[thread_idx].thread_id = thread_idx;
        contexts[thread_idx].running = 1;
        pthread_create(&threads[thread_idx], NULL,
                       chaos_writer_thread, &contexts[thread_idx]);
        thread_idx++;
    }

    // Start reader threads
    for (int i = 0; i < config->read_threads; i++) {
        contexts[thread_idx].storage = &storage;
        contexts[thread_idx].thread_id = thread_idx;
        contexts[thread_idx].running = 1;
        pthread_create(&threads[thread_idx], NULL,
                       chaos_reader_thread, &contexts[thread_idx]);
        thread_idx++;
    }

    // Start monitor thread
    contexts[thread_idx].running = 1;
    pthread_create(&threads[thread_idx], NULL, chaos_monitor_thread, &contexts[thread_idx]);

    // Let the chaos run!
    printf("🚀 Test running for %d seconds...\n", config->duration_seconds);
    sleep(config->duration_seconds);

    // Shutdown all threads
    printf("🛑 Stopping test...\n");
    for (int i = 0; i < total_threads; i++) {
        contexts[i].running = 0;
    }

    for (int i = 0; i < total_threads; i++) {
        pthread_join(threads[i], NULL);
    }

    // Final statistics
    printf("\n📈 FINAL RESULTS:\n");
    double total_avg_write_latency = 0;
    double max_write_latency = 0;
    int total_write_ops = 0;

    for (int i = 0; i < config->write_threads; i++) {
        total_avg_write_latency += contexts[i].avg_latency_us;
        if (contexts[i].max_latency_us > max_write_latency) {
            max_write_latency = contexts[i].max_latency_us;
        }
        total_write_ops += contexts[i].operations;
    }

    printf("✍️  Write Performance:\n");
    printf("    Total operations: %d\n", total_write_ops);
    printf("    Avg latency: %.1fμs\n", total_avg_write_latency / config->write_threads);
    printf("    Max latency: %.1fμs\n", max_write_latency);
    printf("    Ops/sec: %.1f\n", (double)total_write_ops / config->duration_seconds);

    ZeroPauseStats final_stats;
    ZeroPauseRDB_stats(&final_stats);
    printf("📸 Snapshot Performance:\n");
    printf("    Total snapshots: %" PRIu64 "\n", final_stats.total_snapshots);
    printf("    Final generation: %" PRIu64 "\n", final_stats.current_generation);

    // Cleanup
    ZeroPauseRDB_shutdown();
    storage_destroy(&storage);
    free(threads);
    free(contexts);

    printf("🎯 Chaos test completed successfully!\n");
    return 0;
}

// CLI entry point
int main(int argc, char **argv) {
    srand(time(NULL));

    ChaosTestConfig config = {
            .duration_seconds = 60,
            .write_threads = 4,
            .read_threads = 2,
            .writes_per_second = 1000,
            .chaos_enabled = 1,
            .data_size_min = 64,
            .data_size_max = 1024,
            .snapshot_interval_sec = 10
    };

    // Parse command line arguments
    for (int i = 1; i < argc; i++) {
        if (strcmp(argv[i], "--duration") == 0 && i + 1 < argc) {
            config.duration_seconds = atoi(argv[++i]);
        } else if (strcmp(argv[i], "--writers") == 0 && i + 1 < argc) {
            config.write_threads = atoi(argv[++i]);
        } else if (strcmp(argv[i], "--readers") == 0 && i + 1 < argc) {
            config.read_threads = atoi(argv[++i]);
        } else if (strcmp(argv[i], "--no-chaos") == 0) {
            config.chaos_enabled = 0;
        } else if (strcmp(argv[i], "--snapshot-interval") == 0 && i + 1 < argc) {
            config.snapshot_interval_sec = atoi(argv[++i]);
        }
    }

    return chaos_latency_test_run(&config);
}
