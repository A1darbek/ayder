#define _GNU_SOURCE

#include <stdint.h>
#include <stdio.h>

#include "../../src/persistence_zp.c"

int g_log_level = LOG_DEBUG;
int g_log_timestamp = 0;
__thread char g_log_worker_id[8] = {0};
static RAMForgeMetrics test_metrics;
RAMForgeMetrics *g_metrics_ptr = &test_metrics;

static int follower_calls;

void AOF_init(const char *path, size_t ring_capacity, unsigned flush_interval_ms) {
    (void)path;
    (void)ring_capacity;
    (void)flush_interval_ms;
}

void AOF_load(Storage *storage) {
    (void)storage;
}

size_t AOF_sealed_replay(const char *path) {
    (void)path;
    return SIZE_MAX;
}

void AOF_live_follow_start(void) {
    follower_calls++;
}

int AOF_sealed_is_healthy(void) {
    return 1;
}

int main(void) {
    Storage storage = {0};
    Persistence_zp_init("corrupt.aof", &storage, 10);

    if (Persistence_is_ready() || follower_calls != 0 ||
        atomic_load(&test_metrics.persistence_failures_total) != 1 ||
        atomic_load(&test_metrics.write_failures) != 1) {
        fprintf(stderr,
                "corrupt replay did not hold readiness closed: ready=%d follower=%d failures=%lu writes=%lu\n",
                Persistence_is_ready(), follower_calls,
                (unsigned long)atomic_load(&test_metrics.persistence_failures_total),
                (unsigned long)atomic_load(&test_metrics.write_failures));
        return 1;
    }

    puts("PASS sealed replay corruption prevents persistence readiness");
    return 0;
}
