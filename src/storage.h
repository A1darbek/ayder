// storage.h
#ifndef STORAGE_H
#define STORAGE_H
#include <stddef.h>
#include <stdint.h>
#define BUCKET_EMPTY    0
#define BUCKET_OCCUPIED 1
#define BUCKET_DELETED  2
/// Opaque iteration callback (for JSON serializers, RDB dumps, etc.)
typedef void (*storage_iter_fn)(
        int           id,
        const void  *data,
        size_t        size,
        void        *udata
);
/// The Storage type: SwissTable Robin-Hood hash map
typedef struct Storage {
    size_t     capacity;    ///< always power of two
    size_t     size;        ///< number of OCCUPIED entries
    uint8_t   *flags;       ///< BUCKET_* per slot
    int       *keys;        ///< key per slot
    void     **values;      ///< data pointer per slot
    size_t    *val_sizes;   ///< size of each data block
} Storage;
/// Initialize a Storage.  Must call once before use.
void storage_init(Storage *st);
/// Destroy a Storage, freeing all memory.
void storage_destroy(Storage *st);
/// Save or update entry `id` with a copy of `data` (size bytes).
void storage_save(Storage *st, int id, const void *data, size_t size);
/// Retrieve entry `id` into `out` (must be ≥ size bytes). Returns 1 if found.
int  storage_get(Storage *st, int id, void *out, size_t size);
/// Remove entry `id`, freeing its data.
void storage_remove(Storage *st, int id);
/// Iterate over all occupied entries in arbitrary order.
void storage_iterate(Storage *st,
                     storage_iter_fn fn,
                     void           *udata);
#endif // STORAGE_H
