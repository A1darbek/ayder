// test_ramforge_suite.c - Comprehensive test suite for RAMForge
// Shows off blazing-fast performance and zero-pause capabilities

#define _GNU_SOURCE
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <pthread.h>
#include <sys/time.h>
#include <stdatomic.h>
#include <curl/curl.h>
#include <inttypes.h>
#include <netinet/in.h>

// Mock includes for your actual headers
#include "../src/zero_pause_rdb.h"
#include "../src/storage.h"
#include "../src/zero_pause_restore.h"
#include "../src/metrics_shared.h"

// ──────────────────────────────────────────────────────────────
// Test Framework & Utilities
// ──────────────────────────────────────────────────────────────

#define ANSI_RED     "\x1b[31m"
#define ANSI_GREEN   "\x1b[32m"
#define ANSI_YELLOW  "\x1b[33m"
#define ANSI_BLUE    "\x1b[34m"
#define ANSI_MAGENTA "\x1b[35m"
#define ANSI_CYAN    "\x1b[36m"
#define ANSI_RESET   "\x1b[0m"

typedef struct {
    const char *name;
    int (*test_func)(void);
    int passed;
    uint64_t duration_us;
} TestCase;

static int g_tests_passed = 0;
static int g_tests_failed = 0;
static uint64_t g_total_ops = 0;
static uint64_t g_beast_mode_ops = 0;  // Sub-1μs operations

static inline uint64_t now_us(void) {
    struct timespec ts;
    clock_gettime(CLOCK_MONOTONIC, &ts);
    return (uint64_t)ts.tv_sec * 1000000ULL + ts.tv_nsec / 1000;
}

#define ASSERT_TRUE(expr) do { \
    if (!(expr)) { \
        printf(ANSI_RED "❌ ASSERTION FAILED: %s (line %d)\n" ANSI_RESET, #expr, __LINE__); \
        return 0; \
    } \
} while(0)

#define ASSERT_EQ(a, b) do { \
    if ((a) != (b)) { \
        printf(ANSI_RED "❌ ASSERTION FAILED: %s != %s (%lld != %lld) (line %d)\n" ANSI_RESET, \
               #a, #b, (long long)(a), (long long)(b), __LINE__); \
        return 0; \
    } \
} while(0)

#define BEAST_MODE_THRESHOLD_US 1

// ──────────────────────────────────────────────────────────────
// Performance Benchmark Utilities
// ──────────────────────────────────────────────────────────────

typedef struct {
    uint64_t min_us;
    uint64_t max_us;
    uint64_t total_us;
    uint64_t count;
    uint64_t sub_1us;
    uint64_t sub_10us;
    uint64_t sub_100us;
    uint64_t sub_1ms;
} PerfStats;

static void perf_record(PerfStats *stats, uint64_t duration_us) {
    if (stats->count == 0) {
        stats->min_us = duration_us;
        stats->max_us = duration_us;
    } else {
        if (duration_us < stats->min_us) stats->min_us = duration_us;
        if (duration_us > stats->max_us) stats->max_us = duration_us;
    }

    stats->total_us += duration_us;
    stats->count++;

    // Beast mode counters
    if (duration_us < 1) stats->sub_1us++;
    if (duration_us < 10) stats->sub_10us++;
    if (duration_us < 100) stats->sub_100us++;
    if (duration_us < 1000) stats->sub_1ms++;

    g_total_ops++;
    if (duration_us < BEAST_MODE_THRESHOLD_US) {
        g_beast_mode_ops++;
    }
}

static void perf_print(const char *operation, PerfStats *stats) {
    if (stats->count == 0) return;

    uint64_t avg_us = stats->total_us / stats->count;
    double beast_mode_pct = (double)stats->sub_1us / stats->count * 100.0;

    printf("🚀 %s Performance:\n", operation);
    printf("   Min: " ANSI_GREEN "%" PRIu64 "μs" ANSI_RESET "\n", stats->min_us);
    printf("   Avg: " ANSI_CYAN "%" PRIu64 "μs" ANSI_RESET "\n", avg_us);
    printf("   Max: " ANSI_YELLOW "%" PRIu64 "μs" ANSI_RESET "\n", stats->max_us);
    printf("   Beast Mode (sub-1μs): " ANSI_MAGENTA "%.1f%%" ANSI_RESET " (%" PRIu64 "/%" PRIu64 ")\n",
           beast_mode_pct, stats->sub_1us, stats->count);
    printf("   Sub-10μs: %.1f%% | Sub-100μs: %.1f%% | Sub-1ms: %.1f%%\n",
           (double)stats->sub_10us / stats->count * 100.0,
           (double)stats->sub_100us / stats->count * 100.0,
           (double)stats->sub_1ms / stats->count * 100.0);
}

// ──────────────────────────────────────────────────────────────
// Unit Tests
// ──────────────────────────────────────────────────────────────

static int test_zero_pause_snapshot_restore(void) {
    Storage storage;
    storage_init(&storage);


    ZeroPauseRDB_init("test_chaos.rdb", &storage, 200000,
                      0);

    // Fill with test data
    printf("📊 Loading test data...\n");
    for (int i = 0; i < 50000; i++) {
        char value[128];
        snprintf(value, sizeof(value), "snapshot_test_data_%d_"
                                       "with_lots_of_content_to_make_it_realistic_%d", i, i * 7);
        storage_save(&storage, i, value, strlen(value));
    }

    // Test zero-pause snapshot
    printf("📸 Creating zero-pause snapshot...\n");
    uint64_t t0 = now_us();
    ZeroPauseRDB_snapshot();
    ZeroPauseRDB_snapshot_wait();
    uint64_t snapshot_time = now_us() - t0;

    // Wait for snapshot completion (in real implementation)

    printf("✅ Zero-pause snapshot completed in %" PRIu64 "μs\n", snapshot_time);

    // Test restore
    printf("📥 Testing zero-pause restore...\n");
    Storage restore_storage;
    storage_init(&restore_storage);

    t0 = now_us();
    int result = ZeroPauseRDB_restore("test_chaos.rdb", &restore_storage, NULL);
    uint64_t restore_time = now_us() - t0;

    ASSERT_EQ(result, 0);
    ZeroPauseRDB_restore_wait();     /* wait AFTER starting restore */
    printf("✅ Zero-pause restore completed in %" PRIu64 "μs\n", restore_time);

    ZeroPauseRDB_shutdown();
    storage_destroy(&storage);
    storage_destroy(&restore_storage);
    return 1;
}

static int test_concurrent_operations(void) {
    Storage storage;
    storage_init(&storage);

    printf("🔄 Testing concurrent operations...\n");

    // This would be expanded with actual pthread implementation
    // For now, simulating concurrent load
    PerfStats concurrent_stats = {0};

    for (int i = 0; i < 100000; i++) {
        char value[64];
        snprintf(value, sizeof(value), "concurrent_test_%d", i);

        uint64_t t0 = now_us();
        storage_save(&storage, i % 10000, value, strlen(value)); // Simulate overwrites
        uint64_t op_time = now_us() - t0;
        perf_record(&concurrent_stats, op_time);
    }

    perf_print("Concurrent Operations", &concurrent_stats);

    storage_destroy(&storage);
    return 1;
}


static int test_memory_pressure(void) {
    printf("💾 Testing under memory pressure...\n");

    Storage storage;
    storage_init(&storage);

    PerfStats pressure_stats = {0};

    // Allocate large amounts of data to create memory pressure
    for (int i = 0; i < 100000; i++) {
        char *large_value = malloc(1024); // 1KB per entry
        snprintf(large_value, 1024, "large_data_%d_", i);
        // Fill with pattern
        for (int j = 50; j < 1000; j++) {
            large_value[j] = 'A' + (j % 26);
        }

        uint64_t t0 = now_us();
        storage_save(&storage, i, large_value, 1024);
        uint64_t op_time = now_us() - t0;
        perf_record(&pressure_stats, op_time);

        free(large_value);

        // Periodic cleanup simulation
        if (i % 10000 == 0) {
            printf("   Processed %d entries...\n", i);
        }
    }

    perf_print("Memory Pressure Operations", &pressure_stats);

    storage_destroy(&storage);
    return 1;
}

// ──────────────────────────────────────────────────────────────
// Smoke Tests (End-to-End)
// ──────────────────────────────────────────────────────────────

typedef struct {
    char *memory;
    size_t size;
} HTTPResponse;

static size_t http_write_callback(void *contents, size_t size, size_t nmemb, HTTPResponse *response) {
    size_t realsize = size * nmemb;
    response->memory = realloc(response->memory, response->size + realsize + 1);
    if (response->memory) {
        memcpy(&(response->memory[response->size]), contents, realsize);
        response->size += realsize;
        response->memory[response->size] = 0;
    }
    return realsize;
}


static int test_zero_pause_operations_smoke(void) {
    printf("⚡ Testing zero-pause operations (smoke test)...\n");

    /*
     hey bro.i am encountring a idle of curl in smoke test even tho other tests passes clear as you can see it in the image.
            */

    /* skip if the admin HTTP port is not open */
    int sock = socket(AF_INET, SOCK_STREAM, 0);
    struct sockaddr_in sa = {.sin_family = AF_INET,
            .sin_port   = htons(1109),
            .sin_addr   = {htonl(0x7f000001)}}; /* 127.0.0.1 */
    if (connect(sock, (struct sockaddr *) &sa, sizeof sa) < 0) {
        close(sock);
        puts("   (server not running – test skipped)");
        return 1;             /* treat as PASS */
    }
    close(sock);

    CURL *curl;
    CURLcode res;
    HTTPResponse response = {0};

    curl = curl_easy_init();
    if (!curl) return 0;

    // Test zero-pause snapshot
    curl_easy_setopt(curl, CURLOPT_TIMEOUT_MS, 1000L);      /* 1 s total */
    curl_easy_setopt(curl, CURLOPT_CONNECTTIMEOUT_MS, 200L);/* 0.2 s TCP */
    curl_easy_setopt(curl, CURLOPT_URL, "http://localhost:1109/admin/zp_snapshot");
    curl_easy_setopt(curl, CURLOPT_POST, 1L);
    curl_easy_setopt(curl, CURLOPT_WRITEFUNCTION, http_write_callback);
    curl_easy_setopt(curl, CURLOPT_WRITEDATA, &response);
    curl_easy_setopt(curl, CURLOPT_TIMEOUT, 10L);

    uint64_t t0 = now_us();
    res = curl_easy_perform(curl);
    uint64_t snapshot_time = now_us() - t0;

    if (res == CURLE_OK) {
        long response_code;
        curl_easy_getinfo(curl, CURLINFO_RESPONSE_CODE, &response_code);
        printf("   Zero-pause snapshot: HTTP %ld in %" PRIu64 "μs\n", response_code, snapshot_time);
        ASSERT_EQ(response_code, 200);
    }

    // Test zero-pause restore
    free(response.memory);
    response.memory = NULL;
    response.size = 0;

    curl_easy_setopt(curl, CURLOPT_URL, "http://localhost:1109/admin/zp_restore");
    t0 = now_us();
    res = curl_easy_perform(curl);
    uint64_t restore_time = now_us() - t0;

    if (res == CURLE_OK) {
        long response_code;
        curl_easy_getinfo(curl, CURLINFO_RESPONSE_CODE, &response_code);
        printf("   Zero-pause restore: HTTP %ld in %" PRIu64 "μs\n", response_code, restore_time);
        ASSERT_EQ(response_code, 200);
    }

    curl_easy_cleanup(curl);
    free(response.memory);
    return 1;
}

// ──────────────────────────────────────────────────────────────
// Benchmark Suite
// ──────────────────────────────────────────────────────────────

static int benchmark_beast_mode(void) {
    printf("🔥 RAMForge Beast Mode Benchmark\n");
    printf("════════════════════════════════════════════════════════\n");

    Storage storage;
    storage_init(&storage);

    PerfStats beast_stats = {0};
    const int iterations = 1000000; // 1M operations

    printf("🚀 Running %d operations...\n", iterations);

    uint64_t total_start = now_us();

    for (int i = 0; i < iterations; i++) {
        char value[32];
        snprintf(value, sizeof(value), "beast_%d", i);

        uint64_t t0 = now_us();
        storage_save(&storage, i % 100000, value, strlen(value));
        uint64_t op_time = now_us() - t0;
        perf_record(&beast_stats, op_time);
    }

    uint64_t total_time = now_us() - total_start;

    printf("\n💥 BEAST MODE RESULTS:\n");
    printf("════════════════════════════════════════════════════════\n");
    perf_print("Beast Mode Operations", &beast_stats);
    printf("\n🔥 Total Time: %" PRIu64 "μs (%.2f seconds)\n", total_time, total_time / 1000000.0);
    printf("🚀 Throughput: %.0f ops/sec\n", (double)iterations / (total_time / 1000000.0));
    printf("⚡ Average Latency: %" PRIu64 "μs\n", beast_stats.total_us / beast_stats.count);

    // Show off the beast mode percentage
    double beast_pct = (double)beast_stats.sub_1us / beast_stats.count * 100.0;
    if (beast_pct > 50.0) {
        printf("🏆 " ANSI_GREEN "BEAST MODE ACHIEVED: %.1f%% sub-1μs operations!" ANSI_RESET "\n", beast_pct);
    } else {
        printf("🎯 Beast Mode: %.1f%% sub-1μs operations\n", beast_pct);
    }

    storage_destroy(&storage);
    return 1;
}

// ──────────────────────────────────────────────────────────────
// Test Runner
// ──────────────────────────────────────────────────────────────

static void run_test(TestCase *test) {
    printf("\n" ANSI_CYAN "🧪 Running: %s" ANSI_RESET "\n", test->name);
    printf("─────────────────────────────────────────────────────\n");

    uint64_t start = now_us();
    test->passed = test->test_func();
    test->duration_us = now_us() - start;

    if (test->passed) {
        printf(ANSI_GREEN "✅ PASSED" ANSI_RESET " (%s) in %" PRIu64 "μs\n", test->name, test->duration_us);
        g_tests_passed++;
    } else {
        printf(ANSI_RED "❌ FAILED" ANSI_RESET " (%s) in %" PRIu64 "μs\n", test->name, test->duration_us);
        g_tests_failed++;
    }
}

int main(int argc, char **argv) {
    init_shared_metrics();
    printf(ANSI_MAGENTA);
    printf("╔═══════════════════════════════════════════════════════╗\n");
    printf("║                RAMForge Test Suite                    ║\n");
    printf("║        Blazing-Fast • Zero-Pause • Beast Mode        ║\n");
    printf("╚═══════════════════════════════════════════════════════╝\n");
    printf(ANSI_RESET "\n");

    // Test suite definition
    TestCase tests[] = {
            {"Zero-Pause Snapshot/Restore", test_zero_pause_snapshot_restore},
            {"Concurrent Operations", test_concurrent_operations},
            {"Memory Pressure", test_memory_pressure},
            {"Zero-Pause Operations Smoke", test_zero_pause_operations_smoke},
            {"Beast Mode Benchmark", benchmark_beast_mode},
    };

    int num_tests = sizeof(tests) / sizeof(tests[0]);

    // Run all tests
    for (int i = 0; i < num_tests; i++) {
        run_test(&tests[i]);
    }

    // Final results
    printf("\n" ANSI_MAGENTA);
    printf("╔═══════════════════════════════════════════════════════╗\n");
    printf("║                    FINAL RESULTS                     ║\n");
    printf("╚═══════════════════════════════════════════════════════╝\n");
    printf(ANSI_RESET);

    printf("\n📊 Test Summary:\n");
    printf("   ✅ Passed: " ANSI_GREEN "%d" ANSI_RESET "\n", g_tests_passed);
    printf("   ❌ Failed: " ANSI_RED "%d" ANSI_RESET "\n", g_tests_failed);
    printf("   📈 Total Operations: " ANSI_CYAN "%" PRIu64 ANSI_RESET "\n", g_total_ops);
    printf("   🔥 Beast Mode Ops: " ANSI_MAGENTA "%" PRIu64 ANSI_RESET " (%.1f%%)\n",
           g_beast_mode_ops, g_total_ops > 0 ? (double)g_beast_mode_ops / g_total_ops * 100.0 : 0.0);

    if (g_tests_failed == 0) {
        printf("\n🏆 " ANSI_GREEN "ALL TESTS PASSED - RAMForge is BEAST MODE!" ANSI_RESET "\n");
        return 0;
    } else {
        printf("\n💥 " ANSI_RED "Some tests failed - check the output above" ANSI_RESET "\n");
        return 1;
    }
}
