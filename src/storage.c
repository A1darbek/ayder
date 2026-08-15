// storage.c
#include "storage.h"
#include "storage_thread_safeguard.h"
#include <stdlib.h>
#include <string.h>
#include <stdint.h>
#include <stdatomic.h>

/// Simple 32-bit integer mix for hashing
static inline uint32_t mix32(uint32_t x) {
    x ^= x >> 16;
    x *= 0x7feb352dU;
    x ^= x >> 15;
    x *= 0x846ca68bU;
    x ^= x >> 16;
    return x;
}

/// Initialize with a small power-of-two capacity.
void storage_init(Storage *st) {
    if (!st) return;
    memset(st, 0, sizeof(*st));
    const char *env_cap = getenv("RAMFORGE_INIT_CAPACITY");
    size_t requested = env_cap ? (size_t)strtoull(env_cap, NULL, 10) : 131072;
    if (requested < 16) requested = 16;
    size_t capacity = 16;
    while (capacity < requested && capacity <= SIZE_MAX / 2) capacity <<= 1;
    if (capacity < requested) return;
    st->capacity = capacity;
    st->size     = 0;
    st->flags    = calloc(st->capacity, sizeof(uint8_t));
    st->keys     = calloc(st->capacity, sizeof(int));
    st->values   = calloc(st->capacity, sizeof(void*));
    st->val_sizes= calloc(st->capacity, sizeof(size_t));
    if (!st->flags || !st->keys || !st->values || !st->val_sizes) {
        free(st->flags);
        free(st->keys);
        free(st->values);
        free(st->val_sizes);
        memset(st, 0, sizeof(*st));
    }
}

/// Free all data blocks and arrays.
void storage_destroy(Storage *st) {
    if (st == NULL) return;
    atomic_thread_fence(memory_order_seq_cst);   /* let writer threads finish */

    // Free all occupied buckets
    for (size_t i = 0; st->flags && i < st->capacity; i++) {
        if (st->flags[i] == BUCKET_OCCUPIED && st->values[i] != NULL) {
            free(st->values[i]);
            st->values[i] = NULL;  // Prevent double-free
        }
    }

    // Free the arrays
    free(st->flags);
    free(st->keys);
    free(st->values);
    free(st->val_sizes);

    // Reset the structure to prevent accidental reuse
    st->flags = NULL;
    st->keys = NULL;
    st->values = NULL;
    st->val_sizes = NULL;
    st->capacity = 0;
    st->size = 0;
}

static void storage_insert_owned(Storage *st, int key, void *value, size_t size) {
    size_t mask = st->capacity - 1;
    size_t index = mix32((uint32_t)key) & mask;
    size_t distance = 0;
    for (;;) {
        if (st->flags[index] != BUCKET_OCCUPIED) {
            st->flags[index] = BUCKET_OCCUPIED;
            st->keys[index] = key;
            st->values[index] = value;
            st->val_sizes[index] = size;
            st->size++;
            return;
        }
        uint32_t occupant_hash = mix32((uint32_t)st->keys[index]);
        size_t occupant_distance =
            (index + st->capacity - (occupant_hash & mask)) & mask;
        if (occupant_distance < distance) {
            int displaced_key = st->keys[index];
            void *displaced_value = st->values[index];
            size_t displaced_size = st->val_sizes[index];
            st->keys[index] = key;
            st->values[index] = value;
            st->val_sizes[index] = size;
            key = displaced_key;
            value = displaced_value;
            size = displaced_size;
            distance = occupant_distance;
        }
        index = (index + 1) & mask;
        distance++;
    }
}

/// Rehash into a new table twice as large without changing the old table on OOM.
static int storage_rehash(Storage *st) {
    if (!st || st->capacity > SIZE_MAX / 2) return -1;
    Storage next = {.capacity = st->capacity * 2};
    next.flags = calloc(next.capacity, sizeof(uint8_t));
    next.keys = calloc(next.capacity, sizeof(int));
    next.values = calloc(next.capacity, sizeof(void *));
    next.val_sizes = calloc(next.capacity, sizeof(size_t));
    if (!next.flags || !next.keys || !next.values || !next.val_sizes) {
        free(next.flags);
        free(next.keys);
        free(next.values);
        free(next.val_sizes);
        return -1;
    }
    for (size_t i = 0; i < st->capacity; i++) {
        if (st->flags[i] == BUCKET_OCCUPIED)
            storage_insert_owned(&next, st->keys[i], st->values[i],
                                 st->val_sizes[i]);
    }
    free(st->flags);
    free(st->keys);
    free(st->values);
    free(st->val_sizes);
    *st = next;
    return 0;
}

/// Insert or update via Robin-Hood hashing
int storage_save(Storage *st, int id, const void *data, size_t size) {
    if (!st || (!data && size) || !st->flags || !st->keys || !st->values ||
        !st->val_sizes || st->capacity == 0) return -1;

    // Grow if load factor > 0.7
    if ((double)(st->size + 1) / st->capacity > 0.7) {
        if (storage_rehash(st) != 0) return -1;
    }

    uint32_t hash = mix32((uint32_t)id);
    size_t  mask = st->capacity - 1;
    size_t  idx  = hash & mask;
    size_t  dist = 0;

    int    cur_key;
    void  *cur_val;
    size_t cur_sz;
    uint8_t cur_flag;

    // Prepare new entry
    int    new_key = id;
    void  *new_val = malloc(size ? size : 1);
    if (new_val == NULL) return -1;
    if (size) memcpy(new_val, data, size);
    size_t new_sz  = size;

    for (;;) {
        cur_flag = st->flags[idx];
        if (cur_flag != BUCKET_OCCUPIED) {
            // Empty or deleted: place here
            st->flags[idx]     = BUCKET_OCCUPIED;
            st->keys[idx]      = new_key;
            st->values[idx]    = new_val;
            st->val_sizes[idx] = new_sz;
            st->size++;

            return 0;
        }

        // Check if this is the same key (update case)
        if (st->keys[idx] == new_key) {
            free(st->values[idx]);
            st->values[idx] = new_val;
            st->val_sizes[idx] = new_sz;
            return 0;
        }

        // Compute existing entry's probe distance
        uint32_t cur_hash = mix32((uint32_t)st->keys[idx]);
        size_t  cur_dist = (idx + st->capacity - (cur_hash & mask)) & mask;

        if (cur_dist < dist) {
            // Robin-Hood swap
            cur_key   = st->keys[idx];
            cur_val   = st->values[idx];
            cur_sz    = st->val_sizes[idx];

            st->keys[idx]      = new_key;
            st->values[idx]    = new_val;
            st->val_sizes[idx] = new_sz;

            new_key   = cur_key;
            new_val   = cur_val;
            new_sz    = cur_sz;
            dist      = cur_dist;
        }

        // Next slot
        idx = (idx + 1) & mask;
        dist++;
    }
}

/// Retrieve the data for `id` if present.
int storage_get(Storage *st, int id, void *out, size_t out_sz) {
    if (st == NULL || out == NULL) return 0;

    uint32_t hash = mix32((uint32_t)id);
    size_t  mask = st->capacity - 1;
    size_t  idx  = hash & mask;

    for (size_t dist = 0; dist < st->capacity; dist++) {
        if (st->flags[idx] == BUCKET_EMPTY) {
            return 0;  // not found
        }
        if (st->flags[idx] == BUCKET_OCCUPIED && st->keys[idx] == id) {
            if (out_sz < st->val_sizes[idx]) return 0;
            memcpy(out, st->values[idx], st->val_sizes[idx]);
            return 1;
        }
        idx = (idx + 1) & mask;
    }
    return 0;
}
void storage_iterate(Storage *st, void (*fn)(int, const void *, size_t, void *), void *udata) {
    if (st == NULL || fn == NULL) return;

    // Access the flags/keys/values arrays directly (they're already in storage.c)
    for (size_t i = 0; i < st->capacity; i++) {
        if (st->flags[i] == BUCKET_OCCUPIED) {
            fn(st->keys[i],
               st->values[i],
               st->val_sizes[i],
               udata);
        }
    }
}
