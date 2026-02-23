#define _GNU_SOURCE
#include "shared_storage.h"
#include "log.h"

#include <errno.h>
#include <fcntl.h>
#include <stddef.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <sys/statvfs.h>
#include <unistd.h>

/* --- Fast hash (same as yours) --- */
static inline uint32_t hash_key_fast(int key) {
  uint32_t h = (uint32_t)key;
  h          = ((h >> 16) ^ h) * 0x45d9f3b;
  h          = ((h >> 16) ^ h) * 0x45d9f3b;
  h          = (h >> 16) ^ h;
  return h;
}

static inline uint32_t floor_pow2_u32(uint64_t v) {
  if (v == 0)
    return 0;
  uint64_t p = 1;
  while ((p << 1) <= v)
    p <<= 1;
  return (uint32_t)p;
}

static inline uint32_t round_pow2_u32(uint64_t v) {
  if (v == 0)
    return 0;
  uint64_t p = 1;
  while (p < v)
    p <<= 1;
  if (p > 0xFFFFFFFFULL)
    p = 0xFFFFFFFFULL;
  return (uint32_t)p;
}

static inline uint32_t clamp_cap_pow2(uint32_t cap) {
  /* pick a sane minimum; tweak if you want */
  const uint32_t MIN_CAP = 65536u;

  if (cap < MIN_CAP)
    cap = MIN_CAP;
  if (cap > MAX_SHARED_ENTRIES_MAX)
    cap = MAX_SHARED_ENTRIES_MAX;

  /* ensure pow2 */
  if (cap & (cap - 1))
    cap = floor_pow2_u32(cap);
  if (cap < MIN_CAP)
    cap = MIN_CAP;
  return cap;
}

static inline uint32_t read_env_capacity(void) {
  /* 1) explicit override */
  const char *e = getenv("RF_SHARED_ENTRIES");
  if (e && *e) {
    char *end            = NULL;
    errno                = 0;
    unsigned long long v = strtoull(e, &end, 10);
    if (errno == 0 && end != e) {
      uint32_t cap = round_pow2_u32(v);
      return clamp_cap_pow2(cap);
    }
    /* bad env value -> fall through to auto/default */
  }

  /* 2) auto-pick from /dev/shm free space */
  struct statvfs vfs;
  if (statvfs("/dev/shm", &vfs) == 0) {
    unsigned long long freeb = (unsigned long long)vfs.f_bavail * (unsigned long long)vfs.f_frsize;

    /* Safety margin so we don't consume all shm (leave room for others). */
    unsigned long long budget = (freeb * 60ULL) / 100ULL; /* 60% */

    /* Fixed overhead (header + shards), then entries[] */
    unsigned long long overhead = (unsigned long long)offsetof(SharedStorage, entries);

    if (budget > overhead + (unsigned long long)sizeof(SharedEntry)) {
      unsigned long long avail_for_entries = budget - overhead;
      unsigned long long want              = avail_for_entries / (unsigned long long)sizeof(SharedEntry);

      uint32_t cap = floor_pow2_u32(want);
      cap          = clamp_cap_pow2(cap);

      /* If clamp forced it too big for budget, step down until it fits. */
      while (cap > 0) {
        unsigned long long need = overhead + (unsigned long long)cap * (unsigned long long)sizeof(SharedEntry);
        if (need <= budget)
          break;
        cap >>= 1;
      }

      if (cap)
        return clamp_cap_pow2(cap);
    }
  }

  /* 3) fallback */
  return clamp_cap_pow2(DEFAULT_SHARED_ENTRIES);
}

static inline size_t shared_storage_bytes(uint32_t cap) {
  return offsetof(SharedStorage, entries) + (size_t)cap * sizeof(SharedEntry);
}

static int shm_has_space(size_t need_bytes) {
  struct statvfs v;
  if (statvfs("/dev/shm", &v) != 0)
    return 1; /* if unknown, don't block */
  unsigned long long freeb = (unsigned long long)v.f_bavail * (unsigned long long)v.f_frsize;
  return freeb >= (unsigned long long)need_bytes;
}

static int init_mutex_attrs(pthread_mutexattr_t *mattr) {
  if (pthread_mutexattr_init(mattr) != 0)
    return -1;
  if (pthread_mutexattr_setpshared(mattr, PTHREAD_PROCESS_SHARED) != 0)
    return -1;

#ifdef PTHREAD_MUTEX_ADAPTIVE_NP
  (void)pthread_mutexattr_settype(mattr, PTHREAD_MUTEX_ADAPTIVE_NP);
#else
  (void)pthread_mutexattr_settype(mattr, PTHREAD_MUTEX_NORMAL);
#endif
  return 0;
}

/* create-or-fail */
SharedStorage *shared_storage_init_named(const char *name) {
  if (!name || !*name)
    name = SHARED_STORAGE_NAME;

  uint32_t cap  = read_env_capacity();
  size_t shm_sz = shared_storage_bytes(cap);

  if (!shm_has_space(shm_sz)) {
    LOGE("/dev/shm too small for SharedStorage (%zu bytes ≈ %.1f MiB). "
         "Fix by increasing shm (docker: shm_size) or set RF_SHARED_ENTRIES smaller.",
         shm_sz, shm_sz / (1024.0 * 1024.0));
    return NULL;
  }

  int shm_fd = shm_open(name, O_CREAT | O_RDWR | O_EXCL, 0666);
  if (shm_fd == -1) {
    perror("hm_open init_named");
    return NULL;
  }

  if (ftruncate(shm_fd, (off_t)shm_sz) == -1) {
    perror("ftruncate");
    close(shm_fd);
    shm_unlink(name);
    return NULL;
  }

  SharedStorage *ss = mmap(NULL, shm_sz, PROT_READ | PROT_WRITE, MAP_SHARED, shm_fd, 0);
  close(shm_fd);
  if (ss == MAP_FAILED) {
    perror("mmap init_named");
    shm_unlink(name);
    return NULL;
  }

  /* Fresh shm is zeroed, so DO NOT pre-touch millions of entries. */
  ss->shm_bytes = shm_sz;

  atomic_store(&ss->capacity, cap);
  atomic_store(&ss->size, 0);
  atomic_store(&ss->next_slot, 0);
  atomic_store(&ss->read_ops, 0);
  atomic_store(&ss->write_ops, 0);
  atomic_store(&ss->collisions, 0);

  pthread_mutexattr_t mattr;
  if (init_mutex_attrs(&mattr) != 0) {
    LOGE("pthread_mutexattr init failed");
    munmap(ss, shm_sz);
    shm_unlink(name);
    return NULL;
  }

  for (int i = 0; i < SHARD_COUNT; i++) {
    if (pthread_mutex_init(&ss->shards[i].lock, &mattr) != 0) {
      perror("pthread_mutex_init");
      pthread_mutexattr_destroy(&mattr);
      munmap(ss, shm_sz);
      shm_unlink(name);
      return NULL;
    }
    atomic_store(&ss->shards[i].load_factor, 0);
  }
  pthread_mutexattr_destroy(&mattr);

  return ss;
}

/* fresh create (keeps your old behavior: unlink then create) */
SharedStorage *shared_storage_init(void) {
  shm_unlink(SHARED_STORAGE_NAME);
  return shared_storage_init_named(SHARED_STORAGE_NAME);
}

/* attach existing */
SharedStorage *shared_storage_attach_named(const char *name) {
  if (!name || !*name)
    name = SHARED_STORAGE_NAME;

  int shm_fd = shm_open(name, O_RDWR, 0666);
  if (shm_fd == -1) {
    perror("hm_open attach_named");
    return NULL;
  }

  struct stat st;
  if (fstat(shm_fd, &st) != 0) {
    perror("fstat shm");
    close(shm_fd);
    return NULL;
  }

  size_t shm_sz     = (size_t)st.st_size;
  SharedStorage *ss = mmap(NULL, shm_sz, PROT_READ | PROT_WRITE, MAP_SHARED, shm_fd, 0);
  close(shm_fd);

  if (ss == MAP_FAILED) {
    perror("mmap attach_named");
    return NULL;
  }

  /* In case older segments didn’t have shm_bytes set correctly, prefer st_size. */
  ss->shm_bytes = shm_sz;
  return ss;
}

SharedStorage *shared_storage_attach(void) { return shared_storage_attach_named(SHARED_STORAGE_NAME); }

static inline uint32_t capacity_load(const SharedStorage *ss) {
  uint32_t cap = atomic_load(&ss->capacity);
  return cap ? cap : 1024;
}

int shared_storage_set(SharedStorage *ss, int key, const void *data, size_t sz) {
  if (!ss || !data || sz > MAX_DATA_SIZE || key == 0)
    return -1;

  uint32_t h         = hash_key_fast(key);
  uint32_t shard_idx = h & (SHARD_COUNT - 1);
  Shard *shard       = &ss->shards[shard_idx];

  pthread_mutex_lock(&shard->lock);

  uint32_t cap          = capacity_load(ss);
  uint32_t idx          = h % cap;
  uint32_t start_idx    = idx;
  uint32_t distance     = 0;
  uint32_t max_distance = cap / 4;

  while (distance < max_distance) {
    int existing_key = atomic_load(&ss->entries[idx].key);

    if (existing_key == 0) {
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
      atomic_store(&ss->entries[idx].size, (uint32_t)sz);
      memcpy(ss->entries[idx].data, data, sz);
      atomic_fetch_add(&ss->entries[idx].version, 1);
      atomic_fetch_add(&ss->write_ops, 1);

      pthread_mutex_unlock(&shard->lock);
      return 0;
    }

    idx = (idx + 1) % cap;
    distance++;
    if (idx == start_idx)
      break;
  }

  atomic_fetch_add(&ss->collisions, 1);
  pthread_mutex_unlock(&shard->lock);
  return -1;
}

int shared_storage_get_fast(SharedStorage *ss, int key, void *out, size_t out_sz) {
  if (!ss || !out || key == 0)
    return 0;

  uint32_t h            = hash_key_fast(key);
  uint32_t cap          = capacity_load(ss);
  uint32_t idx          = h % cap;
  uint32_t start_idx    = idx;
  uint32_t distance     = 0;
  uint32_t max_distance = cap / 8;

  while (distance < max_distance) {
    uint64_t version_before = atomic_load(&ss->entries[idx].version);
    int existing_key        = atomic_load(&ss->entries[idx].key);

    if (existing_key == key) {
      uint32_t size = atomic_load(&ss->entries[idx].size);
      if (out_sz >= size) {
        memcpy(out, ss->entries[idx].data, size);
        uint64_t version_after = atomic_load(&ss->entries[idx].version);
        if (version_before == version_after) {
          atomic_fetch_add(&ss->read_ops, 1);
          return 1;
        }
        break; /* version changed, fall back to locked */
      }
    } else if (existing_key == 0) {
      break;
    }

    idx = (idx + 1) % cap;
    distance++;
    if (idx == start_idx)
      break;
  }

  /* fallback */
  return shared_storage_get(ss, key, out, out_sz);
}

int shared_storage_get(SharedStorage *ss, int key, void *out, size_t out_sz) {
  if (!ss || !out || key == 0)
    return 0;

  uint32_t h         = hash_key_fast(key);
  uint32_t shard_idx = h & (SHARD_COUNT - 1);
  Shard *shard       = &ss->shards[shard_idx];

  pthread_mutex_lock(&shard->lock);

  uint32_t cap          = capacity_load(ss);
  uint32_t idx          = h % cap;
  uint32_t start_idx    = idx;
  uint32_t distance     = 0;
  uint32_t max_distance = cap / 4;

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
      break;
    }

    idx = (idx + 1) % cap;
    distance++;
    if (idx == start_idx)
      break;
  }

  pthread_mutex_unlock(&shard->lock);
  return 0;
}

void shared_storage_iterate(SharedStorage *ss, void (*cb)(int, const void *, size_t, void *), void *ud) {
  if (!ss || !cb)
    return;

  uint32_t cap = capacity_load(ss);
  for (uint32_t i = 0; i < cap; ++i) {
    int k       = atomic_load(&ss->entries[i].key);
    uint32_t sz = atomic_load(&ss->entries[i].size);
    if (k && sz)
      cb(k, ss->entries[i].data, sz, ud);
  }
}

uint64_t shared_storage_atomic_inc_u64(SharedStorage *ss, int key) { return shared_storage_atomic_add_u64(ss, key, 1); }

uint64_t shared_storage_atomic_add_u64(SharedStorage *ss, int key, uint64_t delta) {
  if (!ss || key == 0)
    return 0;

  uint32_t h         = hash_key_fast(key);
  uint32_t shard_idx = h & (SHARD_COUNT - 1);
  Shard *shard       = &ss->shards[shard_idx];

  pthread_mutex_lock(&shard->lock);

  uint32_t cap          = capacity_load(ss);
  uint32_t idx          = h % cap;
  uint32_t start_idx    = idx;
  uint32_t distance     = 0;
  uint32_t max_distance = cap / 4;

  while (distance < max_distance) {
    int existing_key = atomic_load(&ss->entries[idx].key);

    if (existing_key == key) {
      uint64_t old_val;
      memcpy(&old_val, ss->entries[idx].data, sizeof(old_val));
      uint64_t new_val = old_val + delta;
      memcpy(ss->entries[idx].data, &new_val, sizeof(new_val));

      atomic_fetch_add(&ss->entries[idx].version, 1);
      pthread_mutex_unlock(&shard->lock);
      return old_val;
    } else if (existing_key == 0) {
      atomic_store(&ss->entries[idx].key, key);
      atomic_store(&ss->entries[idx].size, (uint32_t)sizeof(uint64_t));

      uint64_t init_val = delta;
      memcpy(ss->entries[idx].data, &init_val, sizeof(init_val));

      atomic_fetch_add(&ss->entries[idx].version, 1);
      atomic_fetch_add(&ss->size, 1);
      atomic_fetch_add(&shard->load_factor, 1);

      pthread_mutex_unlock(&shard->lock);
      return 0;
    }

    idx = (idx + 1) % cap;
    distance++;
    if (idx == start_idx)
      break;
  }

  pthread_mutex_unlock(&shard->lock);
  LOGE("shared_storage_atomic_add_u64: no slot for key %d", key);
  return 0;
}

void shared_storage_destroy(SharedStorage *ss) {
  if (!ss)
    return;

  /* best-effort mutex destroy */
  for (int i = 0; i < SHARD_COUNT; i++) {
    pthread_mutex_destroy(&ss->shards[i].lock);
  }

  size_t bytes = ss->shm_bytes ? (size_t)ss->shm_bytes : (size_t)offsetof(SharedStorage, entries);
  munmap(ss, bytes);

  /* keep old behavior: unlink default segment name */
  shm_unlink(SHARED_STORAGE_NAME);
}

void shared_storage_stats(SharedStorage *ss) {
  if (!ss)
    return;
  uint32_t cap = capacity_load(ss);
  LOGI("SharedStorage: cap=%u size=%u reads=%llu writes=%llu collisions=%llu shm=%.1f MiB", cap,
       (unsigned)atomic_load(&ss->size), (unsigned long long)atomic_load(&ss->read_ops),
       (unsigned long long)atomic_load(&ss->write_ops), (unsigned long long)atomic_load(&ss->collisions),
       (double)ss->shm_bytes / (1024.0 * 1024.0));
}
