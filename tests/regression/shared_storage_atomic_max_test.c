#define _GNU_SOURCE

#include <pthread.h>
#include <stddef.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "../../src/shared_storage.h"

enum { TEST_CAPACITY = 1024, TEST_KEY = 0x13572468 };

typedef struct {
    SharedStorage *storage;
    uint64_t base;
} writer_arg_t;

static void *writer(void *opaque) {
    writer_arg_t *arg = opaque;
    for (uint64_t i = 0; i < 1000; i++) {
        uint64_t value = arg->base + ((i * 7919u) % 1000u);
        if (shared_storage_atomic_max_u64(arg->storage, TEST_KEY, value, NULL) != 0)
            return (void *)1;
    }
    return NULL;
}

int main(void) {
    size_t bytes = offsetof(SharedStorage, entries) +
                   TEST_CAPACITY * sizeof(SharedEntry);
    SharedStorage *storage = calloc(1, bytes);
    if (!storage) return 1;
    atomic_store(&storage->capacity, TEST_CAPACITY);
    for (int i = 0; i < SHARD_COUNT; i++) {
        if (pthread_mutex_init(&storage->shards[i].lock, NULL) != 0) return 1;
    }

    uint64_t stored = 0;
    if (shared_storage_atomic_max_u64(storage, TEST_KEY, 100, &stored) != 0 ||
        stored != 100 ||
        shared_storage_atomic_max_u64(storage, TEST_KEY, 7, &stored) != 0 ||
        stored != 100) {
        fprintf(stderr, "atomic max regressed sequentially: stored=%lu\n",
                (unsigned long)stored);
        return 1;
    }

    pthread_t threads[4];
    writer_arg_t args[4];
    for (size_t i = 0; i < 4; i++) {
        args[i].storage = storage;
        args[i].base = (uint64_t)i * 1000u;
        if (pthread_create(&threads[i], NULL, writer, &args[i]) != 0) return 1;
    }
    for (size_t i = 0; i < 4; i++) {
        void *result = NULL;
        if (pthread_join(threads[i], &result) != 0 || result != NULL) return 1;
    }

    uint64_t loaded = 0;
    if (!shared_storage_get_fast(storage, TEST_KEY, &loaded, sizeof(loaded)) ||
        loaded != 3999) {
        fprintf(stderr, "atomic max lost/regressed concurrent value: loaded=%lu\n",
                (unsigned long)loaded);
        return 1;
    }

    for (int i = 0; i < SHARD_COUNT; i++)
        pthread_mutex_destroy(&storage->shards[i].lock);
    free(storage);
    puts("PASS shared commit offset storage is monotonic under concurrency");
    return 0;
}
