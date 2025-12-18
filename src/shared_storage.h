#ifndef OPTIMIZED_SHARED_STORAGE_H
#define OPTIMIZED_SHARED_STORAGE_H

#include <sys/mman.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <stdint.h>
#include <pthread.h>
#include <stdatomic.h>

#define SHARED_STORAGE_NAME "/ramforge_storage_v2"
#define MAX_SHARED_ENTRIES 2097152  // 2M entries (power of 2)
#define SHARD_COUNT 1024           // 1024 shards for better distribution
#define CACHE_LINE_SIZE 64
#define MAX_DATA_SIZE 256

#ifndef RF_SHM_DEFAULT
#define RF_SHM_DEFAULT "/ramforge_shared"
#endif


// Align structures to cache lines to prevent false sharing
typedef struct __attribute__((aligned(CACHE_LINE_SIZE))) {
    atomic_int key;
    atomic_uint_fast32_t size;
    uint8_t data[MAX_DATA_SIZE];
    atomic_uint_fast64_t version;  // For optimistic locking
} SharedEntry;

typedef struct __attribute__((aligned(CACHE_LINE_SIZE))) {
    pthread_mutex_t lock;
    atomic_uint_fast32_t load_factor;  // Track shard utilization
    char padding[CACHE_LINE_SIZE - sizeof(pthread_mutex_t) - sizeof(atomic_uint_fast32_t)];
} Shard;

typedef struct {
    atomic_uint_fast32_t capacity;
    atomic_uint_fast32_t size;
    atomic_uint_fast32_t next_slot;

    // Separate read/write counters for monitoring
    atomic_uint_fast64_t read_ops;
    atomic_uint_fast64_t write_ops;
    atomic_uint_fast64_t collisions;

    Shard shards[SHARD_COUNT];
    SharedEntry entries[MAX_SHARED_ENTRIES];
} SharedStorage;

// Function declarations
SharedStorage* shared_storage_init(void);
uint64_t shared_storage_atomic_inc_u64(SharedStorage *ss, int key);
uint64_t shared_storage_atomic_add_u64(SharedStorage *ss, int key, uint64_t delta);
SharedStorage* shared_storage_attach(void);
SharedStorage *shared_storage_init_named(const char *name);     // create or fail if exists
SharedStorage *shared_storage_attach_named(const char *name);   // attach to existing
int shared_storage_set(SharedStorage *ss, int key, const void *data, size_t size);
int shared_storage_get(SharedStorage *ss, int key, void *out, size_t out_size);
int shared_storage_get_fast(SharedStorage *ss, int key, void *out, size_t out_size);
void shared_storage_destroy(SharedStorage *ss);
void shared_storage_stats(SharedStorage *ss);
/* New – iterate over every occupied slot (no order guaranteed) */
void shared_storage_iterate(SharedStorage *ss,
                            void (*cb)(int, const void *, size_t, void *),
                            void *udata);

// PUBLIC API


#endif