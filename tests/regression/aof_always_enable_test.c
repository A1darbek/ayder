#define _GNU_SOURCE

#include <stdio.h>

#include "../../src/persistence_zp.c"

int g_log_level = LOG_DEBUG;
int g_log_timestamp = 0;
__thread char g_log_worker_id[8] = {0};
static RAMForgeMetrics test_metrics;
RAMForgeMetrics *g_metrics_ptr = &test_metrics;

static int init_calls;
static int load_calls;
static int replay_calls;
static int follower_calls;
static unsigned observed_flush_ms = 99;

void AOF_init(const char *path, size_t ring_capacity, unsigned flush_interval_ms) {
    (void)path;
    (void)ring_capacity;
    init_calls++;
    observed_flush_ms = flush_interval_ms;
}

void AOF_load(Storage *storage) {
    (void)storage;
    load_calls++;
}

size_t AOF_sealed_replay(const char *path) {
    (void)path;
    replay_calls++;
    return 0;
}

void AOF_live_follow_start(void) {
    follower_calls++;
}

int AOF_sealed_is_healthy(void) {
    return 1;
}

int main(void) {
    Storage storage = {0};
    Persistence_zp_init("test.aof", &storage, 0);

    if (init_calls != 1 || load_calls != 1 || replay_calls != 1 ||
        follower_calls != 1 || observed_flush_ms != 0 ||
        !Persistence_is_ready()) {
        fprintf(stderr,
                "always mode did not initialize persistence: init=%d load=%d "
                "replay=%d follower=%d flush_ms=%u ready=%d\n",
                init_calls, load_calls, replay_calls, follower_calls,
                observed_flush_ms, Persistence_is_ready());
        return 1;
    }

    puts("PASS zero flush interval enables appendfsync-always persistence");
    return 0;
}
