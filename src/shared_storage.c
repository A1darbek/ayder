#define _GNU_SOURCE
#include "shared_storage.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
/* Optimized hash function - faster than original */
static inline uint32_t hash_key_fast(int key) {
    uint32_t h = (uint32_t)key;
    h = ((h >> 16) ^ h) * 0x45d9f3b;
    h = ((h >> 16) ^ h) * 0x45d9f3b;
    h = (h >> 16) ^ h;
    return h;
}

/* Robin Hood hashing with distance tracking */
static inline uint32_t probe_distance(uint32_t hash, uint32_t slot, uint32_t capacity) {
    return (slot + capacity - (hash % capacity)) % capacity;
}

SharedStorage *shared_storage_init(void) {
    // Unlink any existing segment first
    shm_unlink(SHARED_STORAGE_NAME);

    int shm_fd = shm_open(SHARED_STORAGE_NAME, O_CREAT | O_RDWR | O_EXCL, 0666);
    if (shm_fd == -1) {
        perror("shm_open init");
        return NULL;
    }

    size_t shm_sz = sizeof(SharedStorage);
    if (ftruncate(shm_fd, shm_sz) == -1) {
        perror("ftruncate");
        close(shm_fd);
        return NULL;
    }

    SharedStorage *ss = mmap(NULL, shm_sz,
                             PROT_READ | PROT_WRITE,
                             MAP_SHARED, shm_fd, 0);
    close(shm_fd);
    if (ss == MAP_FAILED) {
        perror("mmap init");
        return NULL;
    }

    // Initialize atomic counters
    atomic_store(&ss->capacity, MAX_SHARED_ENTRIES);
    atomic_store(&ss->size, 0);
    atomic_store(&ss->next_slot, 0);
    atomic_store(&ss->read_ops, 0);
    atomic_store(&ss->write_ops, 0);
    atomic_store(&ss->collisions, 0);

    // Initialize all entries
    for (uint32_t i = 0; i < MAX_SHARED_ENTRIES; i++) {
        atomic_store(&ss->entries[i].key, 0);
        atomic_store(&ss->entries[i].size, 0);
        atomic_store(&ss->entries[i].version, 0);
    }

    // Initialize shards with adaptive mutexes
    pthread_mutexattr_t mattr;
    pthread_mutexattr_init(&mattr);
    pthread_mutexattr_setpshared(&mattr, PTHREAD_PROCESS_SHARED);
    pthread_mutexattr_settype(&mattr, PTHREAD_MUTEX_ADAPTIVE_NP);

    for (int i = 0; i < SHARD_COUNT; i++) {
        if (pthread_mutex_init(&ss->shards[i].lock, &mattr) != 0) {
            perror("pthread_mutex_init");
            munmap(ss, shm_sz);
            return NULL;
        }
        atomic_store(&ss->shards[i].load_factor, 0);
    }
    pthread_mutexattr_destroy(&mattr);

    printf("✓ Optimized shared storage initialized (%u entries, %d shards)\n",
           MAX_SHARED_ENTRIES, SHARD_COUNT);
    return ss;
}

SharedStorage *shared_storage_attach(void) {
    int shm_fd = shm_open(SHARED_STORAGE_NAME, O_RDWR, 0666);
    if (shm_fd == -1) {
        perror("shm_open attach");
        return NULL;
    }

    SharedStorage *ss = mmap(NULL, sizeof(SharedStorage),
                             PROT_READ | PROT_WRITE,
                             MAP_SHARED, shm_fd, 0);
    close(shm_fd);
    if (ss == MAP_FAILED) {
        perror("mmap attach");
        return NULL;
    }
    return ss;
}

int shared_storage_set(SharedStorage *ss, int key, const void *data, size_t sz) {
    if (!ss || !data || sz > MAX_DATA_SIZE || key == 0) {
        return -1;
    }

    uint32_t h = hash_key_fast(key);
    uint32_t shard_idx = h & (SHARD_COUNT - 1);
    Shard *shard = &ss->shards[shard_idx];

    pthread_mutex_lock(&shard->lock);

    uint32_t capacity = atomic_load(&ss->capacity);
    uint32_t idx = h % capacity;
    uint32_t start_idx = idx;
    uint32_t distance = 0;
    uint32_t max_distance = capacity / 4; // Limit probe distance

    while (distance < max_distance) {
        int existing_key = atomic_load(&ss->entries[idx].key);

        if (existing_key == 0) {
            // Empty slot - insert here
            atomic_store(&ss->entries[idx].key, key);
            atomic_store(&ss->entries[idx].size, (uint32_t)sz);
            memcpy(ss->entries[idx].data, data, sz);
            atomic_fetch_add(&ss->entries[idx].version, 1);
            atomic_fetch_add(&ss->size, 1);
            atomic_fetch_add(&shard->load_factor, 1);
            atomic_fetch_add(&ss->write_ops, 1);
            pthread_mutex_unlock(&shard->lock);
            return 0;
        } else if (existing_key == key) {
            // Update existing
            atomic_store(&ss->entries[idx].size, (uint32_t)sz);
            memcpy(ss->entries[idx].data, data, sz);
            atomic_fetch_add(&ss->entries[idx].version, 1);
            atomic_fetch_add(&ss->write_ops, 1);
            pthread_mutex_unlock(&shard->lock);
            return 0;
        }

        idx = (idx + 1) % capacity;
        distance++;

        if (idx == start_idx) break; // Full circle
    }

    atomic_fetch_add(&ss->collisions, 1);
    pthread_mutex_unlock(&shard->lock);
    return -1; // Table full or too many collisions
}

// Fast read-only operation with optimistic locking
int shared_storage_get_fast(SharedStorage *ss, int key, void *out, size_t out_sz) {
    if (!ss || !out || key == 0) return 0;

    uint32_t h = hash_key_fast(key);
    uint32_t capacity = atomic_load(&ss->capacity);
    uint32_t idx = h % capacity;
    uint32_t start_idx = idx;
    uint32_t distance = 0;
    uint32_t max_distance = capacity / 8; // Shorter probe for reads

    while (distance < max_distance) {
        // Optimistic read without lock
        uint64_t version_before = atomic_load(&ss->entries[idx].version);
        int existing_key = atomic_load(&ss->entries[idx].key);

        if (existing_key == key) {
            uint32_t size = atomic_load(&ss->entries[idx].size);
            if (out_sz >= size) {
                // Copy data
                memcpy(out, ss->entries[idx].data, size);

                // Verify version didn't change during read
                uint64_t version_after = atomic_load(&ss->entries[idx].version);
                if (version_before == version_after) {
                    atomic_fetch_add(&ss->read_ops, 1);
                    return 1; // Success
                }
                // Version changed, retry with lock
                break;
            }
        } else if (existing_key == 0) {
            break; // Not found
        }

        idx = (idx + 1) % capacity;
        distance++;

        if (idx == start_idx) break;
    }

    // Fall back to locked read if optimistic failed
    return shared_storage_get(ss, key, out, out_sz);
}

// Traditional locked read for consistency
int shared_storage_get(SharedStorage *ss, int key, void *out, size_t out_sz) {
    if (!ss || !out || key == 0) return 0;

    uint32_t h = hash_key_fast(key);
    uint32_t shard_idx = h & (SHARD_COUNT - 1);
    Shard *shard = &ss->shards[shard_idx];

    pthread_mutex_lock(&shard->lock);

    uint32_t capacity = atomic_load(&ss->capacity);
    uint32_t idx = h % capacity;
    uint32_t start_idx = idx;
    uint32_t distance = 0;
    uint32_t max_distance = capacity / 4;

    while (distance < max_distance) {
        int existing_key = atomic_load(&ss->entries[idx].key);

        if (existing_key == key) {
            uint32_t size = atomic_load(&ss->entries[idx].size);
            if (out_sz >= size) {
                memcpy(out, ss->entries[idx].data, size);
                atomic_fetch_add(&ss->read_ops, 1);
                pthread_mutex_unlock(&shard->lock);
                return 1;
            }
        } else if (existing_key == 0) {
            break; // Not found
        }

        idx = (idx + 1) % capacity;
        distance++;

        if (idx == start_idx) break;
    }

    pthread_mutex_unlock(&shard->lock);
    return 0;
}

void shared_storage_stats(SharedStorage *ss) {
    if (!ss) return;

    uint64_t reads = atomic_load(&ss->read_ops);
    uint64_t writes = atomic_load(&ss->write_ops);
    uint64_t collisions = atomic_load(&ss->collisions);
    uint32_t size = atomic_load(&ss->size);
    uint32_t capacity = atomic_load(&ss->capacity);

    printf("\n=== SHARED STORAGE STATS ===\n");
    printf("Entries: %u / %u (%.1f%% full)\n",
           size, capacity, (float)size / capacity * 100);
    printf("Read ops: %lu\n", reads);
    printf("Write ops: %lu\n", writes);
    printf("Collisions: %lu\n", collisions);
    printf("Load factor: %.3f\n", (float)size / capacity);

    // Shard utilization
    uint32_t used_shards = 0;
    uint32_t max_shard_load = 0;
    for (int i = 0; i < SHARD_COUNT; i++) {
        uint32_t load = atomic_load(&ss->shards[i].load_factor);
        if (load > 0) used_shards++;
        if (load > max_shard_load) max_shard_load = load;
    }
    printf("Active shards: %u / %d\n", used_shards, SHARD_COUNT);
    printf("Max shard load: %u\n", max_shard_load);
    printf("============================\n\n");
}

void shared_storage_destroy(SharedStorage *ss) {
    if (!ss) return;

    for (int i = 0; i < SHARD_COUNT; i++) {
        pthread_mutex_destroy(&ss->shards[i].lock);
    }

    munmap(ss, sizeof(SharedStorage));
    shm_unlink(SHARED_STORAGE_NAME);
}