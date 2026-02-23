#ifndef OPTIMIZED_SHARED_STORAGE_H
#define OPTIMIZED_SHARED_STORAGE_H

#include <pthread.h>
#include <stdatomic.h>
#include <stddef.h>
#include <stdint.h>

#define SHARED_STORAGE_NAME "/ramforge_storage_v2"

#define MAX_SHARED_ENTRIES_MAX 2097152u /* 2M */

/* Default capacity if RF_SHARED_ENTRIES is not set */
#define DEFAULT_SHARED_ENTRIES MAX_SHARED_ENTRIES_MAX

#define SHARD_COUNT 1024
#define CACHE_LINE_SIZE 64
#define MAX_DATA_SIZE 256

#if (SHARD_COUNT & (SHARD_COUNT - 1)) != 0
#  error "SHARD_COUNT must be a power of two"
#endif

typedef struct __attribute__((aligned(CACHE_LINE_SIZE))) {
  atomic_int key;
  atomic_uint_fast32_t size;
  uint8_t data[MAX_DATA_SIZE];
  atomic_uint_fast64_t version; // optimistic locking
} SharedEntry;

typedef struct __attribute__((aligned(CACHE_LINE_SIZE))) {
  pthread_mutex_t lock;
  atomic_uint_fast32_t load_factor;
  char padding[CACHE_LINE_SIZE - sizeof(pthread_mutex_t) - sizeof(atomic_uint_fast32_t)];
} Shard;

typedef struct {
  /* for correct munmap() */
  uint64_t shm_bytes;

  atomic_uint_fast32_t capacity;
  atomic_uint_fast32_t size;
  atomic_uint_fast32_t next_slot;

  atomic_uint_fast64_t read_ops;
  atomic_uint_fast64_t write_ops;
  atomic_uint_fast64_t collisions;

  Shard shards[SHARD_COUNT];

  /* runtime-sized */
  SharedEntry entries[];
} SharedStorage;

/* API */
SharedStorage *shared_storage_init(void);                     /* fresh create (unlinks old) */
SharedStorage *shared_storage_attach(void);                   /* attach default name */
SharedStorage *shared_storage_init_named(const char *name);   /* create or fail if exists */
SharedStorage *shared_storage_attach_named(const char *name); /* attach existing */

int shared_storage_set(SharedStorage *ss, int key, const void *data, size_t size);
int shared_storage_get(SharedStorage *ss, int key, void *out, size_t out_size);
int shared_storage_get_fast(SharedStorage *ss, int key, void *out, size_t out_size);

uint64_t shared_storage_atomic_inc_u64(SharedStorage *ss, int key);
uint64_t shared_storage_atomic_add_u64(SharedStorage *ss, int key, uint64_t delta);

void shared_storage_iterate(SharedStorage *ss, void (*cb)(int, const void *, size_t, void *), void *udata);

void shared_storage_destroy(SharedStorage *ss);
void shared_storage_stats(SharedStorage *ss);

#endif
