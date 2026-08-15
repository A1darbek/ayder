#define _GNU_SOURCE

#include <stdint.h>
#include <stdio.h>

#include "../../src/rf_broker.c"

SharedStorage *g_shared_storage = (SharedStorage *)(uintptr_t)1;

static int event_log[8];
static size_t event_count;
static int ha_result;
static uint64_t append_result = 77;
static int max_result;
static uint64_t max_seen;

int RAMForge_HA_replicate_write(uint32_t record_type, const void *data,
                                uint32_t size) {
    const aof_commit_rec_t *commit = data;
    if (record_type != AOF_REC_BROKER_COMMIT || size != sizeof(*commit)) return -99;
    event_log[event_count++] = 1;
    return ha_result;
}

uint64_t AOF_append_sealed(int id, const void *data, size_t size) {
    const aof_commit_rec_t *commit = data;
    if ((uint32_t)id != AOF_REC_BROKER_COMMIT || size != sizeof(*commit)) return 0;
    event_log[event_count++] = 2;
    return append_result;
}

int shared_storage_atomic_max_u64(SharedStorage *storage, int key,
                                  uint64_t value, uint64_t *stored_value) {
    (void)storage;
    (void)key;
    if (stored_value) *stored_value = value > max_seen ? value : max_seen;
    if (value > max_seen) max_seen = value;
    event_log[event_count++] = 3;
    return max_result;
}

static void reset(void) {
    event_count = 0;
    ha_result = 0;
    append_result = 77;
    max_result = 0;
}

int main(void) {
    uint64_t batch_id = 0;

    reset();
    ha_result = -1;
    if (rf_commit("orders", "billing", 2, 10, &batch_id) != -3 ||
        batch_id != 0 || event_count != 1 || event_log[0] != 1) {
        fprintf(stderr, "commit did not stop on HA failure\n");
        return 1;
    }

    reset();
    append_result = 0;
    if (rf_commit("orders", "billing", 2, 11, &batch_id) != -2 ||
        batch_id != 0 || event_count != 2 || event_log[0] != 1 ||
        event_log[1] != 2) {
        fprintf(stderr, "commit did not stop before publishing an unqueued offset\n");
        return 1;
    }

    reset();
    max_result = -1;
    if (rf_commit("orders", "billing", 2, 12, &batch_id) != -4 ||
        batch_id != 77 || event_count != 3 || event_log[0] != 1 ||
        event_log[1] != 2 || event_log[2] != 3) {
        fprintf(stderr, "commit did not surface shared offset-store failure\n");
        return 1;
    }

    reset();
    if (rf_commit("orders", "billing", 2, 100, &batch_id) != 0 ||
        batch_id != 77 || event_count != 3 || event_log[0] != 1 ||
        event_log[1] != 2 || event_log[2] != 3 || max_seen != 100) {
        fprintf(stderr, "commit success path ordering/proof mismatch\n");
        return 1;
    }
    reset();
    if (rf_commit("orders", "billing", 2, 9, &batch_id) != 0 ||
        max_seen != 100) {
        fprintf(stderr, "commit helper allowed a backwards offset\n");
        return 1;
    }

    puts("PASS commit requires HA, sealed enqueue, and monotonic shared publication");
    return 0;
}
