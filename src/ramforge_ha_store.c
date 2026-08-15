#define _GNU_SOURCE
#include "ramforge_ha_store.h"

#include "crc32c.h"
#include "ramforge_ha_config.h"

#include <errno.h>
#include <fcntl.h>
#include <limits.h>
#include <pthread.h>
#include <stddef.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>

#define HA_STATE_MAGIC 0x48535431u /* HST1 */
#define HA_LOG_MAGIC   0x484C4731u /* HLG1 */
#define HA_STORE_VERSION 1u

typedef struct {
    uint32_t magic;
    uint16_t version;
    uint16_t bytes;
    uint64_t generation;
    uint64_t term;
    int32_t voted_for;
    uint32_t reserved;
    uint64_t commit_index;
    uint64_t applied_index;
    uint32_t crc;
    uint32_t padding;
} __attribute__((packed)) ha_disk_state_t;

typedef struct {
    uint32_t magic;
    uint16_t version;
    uint16_t header_bytes;
    uint64_t index;
    uint64_t term;
    uint64_t timestamp_us;
    uint32_t type;
    uint32_t payload_size;
    uint32_t payload_crc;
    uint32_t header_crc;
} __attribute__((packed)) ha_disk_log_header_t;

struct ha_store {
    int state_fd;
    int log_fd;
    uint32_t max_payload;
    uint64_t generation;
    ha_stable_state_t last_state;
    int has_state;
    uint64_t last_log_index;
    uint64_t last_log_term;
    int failed;
    char state_path[PATH_MAX];
    char log_path[PATH_MAX];
    pthread_mutex_t lock;
};

static int write_full(int fd, const void *buf, size_t size) {
    const uint8_t *p = (const uint8_t *)buf;
    size_t done = 0;
    while (done < size) {
        ssize_t n = write(fd, p + done, size - done);
        if (n < 0) {
            if (errno == EINTR) continue;
            return -1;
        }
        if (n == 0) {
            errno = EIO;
            return -1;
        }
        done += (size_t)n;
    }
    return 0;
}

static int pread_full_or_eof(int fd, void *buf, size_t size, off_t offset,
                             size_t *bytes_out) {
    uint8_t *p = (uint8_t *)buf;
    size_t done = 0;
    while (done < size) {
        ssize_t n = pread(fd, p + done, size - done, offset + (off_t)done);
        if (n < 0) {
            if (errno == EINTR) continue;
            return -1;
        }
        if (n == 0) break;
        done += (size_t)n;
    }
    if (bytes_out) *bytes_out = done;
    return 0;
}

static int fsync_parent_dir(const char *path) {
    char dir[PATH_MAX];
    size_t n = strnlen(path, sizeof(dir));
    if (n == 0 || n >= sizeof(dir)) return -1;
    memcpy(dir, path, n + 1);
    char *slash = strrchr(dir, '/');
    if (!slash) {
        memcpy(dir, ".", 2);
    } else if (slash == dir) {
        slash[1] = '\0';
    } else {
        *slash = '\0';
    }
    int fd = open(dir, O_RDONLY | O_DIRECTORY | O_CLOEXEC);
    if (fd < 0) return -1;
    int rc = fsync(fd);
    int saved = errno;
    close(fd);
    errno = saved;
    return rc;
}

static int open_durable_file(const char *path, int *created_out) {
    struct stat before;
    int existed = stat(path, &before) == 0;
    int fd = open(path, O_CREAT | O_RDWR | O_CLOEXEC, 0600);
    if (fd < 0) return -1;
    if (!existed && fsync_parent_dir(path) != 0) {
        int saved = errno;
        close(fd);
        errno = saved;
        return -1;
    }
    if (created_out) *created_out = !existed;
    return fd;
}

static uint32_t state_crc(const ha_disk_state_t *record) {
    return crc32c(0, record, offsetof(ha_disk_state_t, crc));
}

static uint32_t log_header_crc(const ha_disk_log_header_t *header) {
    return crc32c(0, header, offsetof(ha_disk_log_header_t, header_crc));
}

static void sanitize_node_id(const char *node_id, char *out, size_t cap) {
    size_t used = 0;
    if (!node_id || !*node_id) node_id = "node";
    for (const unsigned char *p = (const unsigned char *)node_id;
         *p && used + 1 < cap; p++) {
        unsigned char c = *p;
        out[used++] = ((c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') ||
                       (c >= '0' && c <= '9') || c == '-' || c == '_')
                    ? (char)c : '_';
    }
    out[used] = '\0';
}

static int choose_path(char *out, size_t cap, const char *specific_env,
                       const char *data_dir, const char *node_id,
                       const char *suffix) {
    const char *configured = getenv(specific_env);
    int n;
    if (configured && *configured) {
        n = snprintf(out, cap, "%s", configured);
    } else {
        n = snprintf(out, cap, "%s/ha-%s.%s", data_dir, node_id, suffix);
    }
    if (n < 0 || (size_t)n >= cap) {
        errno = ENAMETOOLONG;
        return -1;
    }
    return 0;
}

static int load_state(ha_store_t *store, ha_stable_state_t *out) {
    memset(out, 0, sizeof(*out));
    out->voted_for = -1;
    struct stat st;
    if (fstat(store->state_fd, &st) != 0) return -1;
    off_t offset = 0;
    uint64_t last_generation = 0;
    while (offset < st.st_size) {
        ha_disk_state_t disk;
        size_t got = 0;
        if (pread_full_or_eof(store->state_fd, &disk, sizeof(disk), offset, &got) != 0) return -1;
        if (got != sizeof(disk)) {
            if (ftruncate(store->state_fd, offset) != 0 || fsync(store->state_fd) != 0) return -1;
            break;
        }
        if (disk.magic != HA_STATE_MAGIC || disk.version != HA_STORE_VERSION ||
            disk.bytes != sizeof(disk) || disk.crc != state_crc(&disk) ||
            disk.generation <= last_generation ||
            disk.applied_index > disk.commit_index || disk.voted_for < -1 ||
            disk.voted_for >= HA_MAX_NODES) {
            errno = EBADMSG;
            return -1;
        }
        if (last_generation != 0) {
            if (disk.term < out->term || disk.commit_index < out->commit_index ||
                disk.applied_index < out->applied_index ||
                (disk.term == out->term && out->voted_for >= 0 &&
                 disk.voted_for != out->voted_for)) {
                errno = EBADMSG;
                return -1;
            }
        }
        out->term = disk.term;
        out->voted_for = disk.voted_for;
        out->commit_index = disk.commit_index;
        out->applied_index = disk.applied_index;
        last_generation = disk.generation;
        offset += (off_t)sizeof(disk);
    }
    store->generation = last_generation;
    store->last_state = *out;
    store->has_state = last_generation != 0;
    return 0;
}

static int load_log(ha_store_t *store, ha_store_replay_cb callback, void *ctx,
                    uint64_t *last_index_out) {
    struct stat st;
    if (fstat(store->log_fd, &st) != 0) return -1;
    off_t offset = 0;
    uint64_t expected_index = 1;
    uint64_t previous_term = 0;
    while (offset < st.st_size) {
        ha_disk_log_header_t header;
        size_t got = 0;
        if (pread_full_or_eof(store->log_fd, &header, sizeof(header), offset, &got) != 0) return -1;
        if (got != sizeof(header)) {
            if (ftruncate(store->log_fd, offset) != 0 || fsync(store->log_fd) != 0) return -1;
            break;
        }
        if (header.magic != HA_LOG_MAGIC || header.version != HA_STORE_VERSION ||
            header.header_bytes != sizeof(header) ||
            header.payload_size > store->max_payload ||
            header.index != expected_index ||
            header.term < previous_term ||
            header.header_crc != log_header_crc(&header)) {
            errno = EBADMSG;
            return -1;
        }
        uint8_t *payload = NULL;
        if (header.payload_size) {
            payload = malloc(header.payload_size);
            if (!payload) return -1;
            got = 0;
            if (pread_full_or_eof(store->log_fd, payload, header.payload_size,
                                  offset + (off_t)sizeof(header), &got) != 0) {
                free(payload);
                return -1;
            }
            if (got != header.payload_size) {
                free(payload);
                if (ftruncate(store->log_fd, offset) != 0 || fsync(store->log_fd) != 0) return -1;
                break;
            }
        }
        uint32_t payload_crc = header.payload_size ? crc32c(0, payload, header.payload_size) : 0;
        if (payload_crc != header.payload_crc) {
            free(payload);
            errno = EBADMSG;
            return -1;
        }
        if (callback && callback(header.index, header.term, header.timestamp_us,
                                 header.type, payload, header.payload_size,
                                 (uint64_t)offset, ctx) != 0) {
            free(payload);
            return -1;
        }
        free(payload);
        offset += (off_t)sizeof(header) + (off_t)header.payload_size;
        previous_term = header.term;
        expected_index++;
    }
    store->last_log_index = expected_index - 1;
    store->last_log_term = previous_term;
    if (last_index_out) *last_index_out = expected_index - 1;
    if (lseek(store->log_fd, 0, SEEK_END) < 0) return -1;
    return 0;
}

int ha_store_open(ha_store_t **out, const char *node_id,
                  uint32_t max_payload, ha_stable_state_t *state_out,
                  ha_store_replay_cb replay_cb, void *replay_ctx) {
    if (!out || !state_out || max_payload == 0) return -1;
    *out = NULL;
    ha_store_t *store = calloc(1, sizeof(*store));
    if (!store) return -1;
    store->state_fd = -1;
    store->log_fd = -1;
    store->max_payload = max_payload;

    char safe_node[128];
    sanitize_node_id(node_id, safe_node, sizeof(safe_node));
    const char *data_dir = getenv("RF_HA_DATA_DIR");
    if (!data_dir || !*data_dir) data_dir = ".";
    if (choose_path(store->state_path, sizeof(store->state_path),
                    "RF_HA_STATE_PATH", data_dir, safe_node, "state") != 0 ||
        choose_path(store->log_path, sizeof(store->log_path),
                    "RF_HA_LOG_PATH", data_dir, safe_node, "log") != 0) {
        free(store);
        return -1;
    }
    if (pthread_mutex_init(&store->lock, NULL) != 0) {
        free(store);
        return -1;
    }

    store->state_fd = open_durable_file(store->state_path, NULL);
    if (store->state_fd < 0) goto fail;
    store->log_fd = open_durable_file(store->log_path, NULL);
    if (store->log_fd < 0) goto fail;
    if (load_state(store, state_out) != 0) goto fail;

    uint64_t last_index = 0;
    if (load_log(store, replay_cb, replay_ctx, &last_index) != 0) goto fail;
    if (state_out->commit_index > last_index ||
        state_out->applied_index > state_out->commit_index) {
        errno = EBADMSG;
        goto fail;
    }

    *out = store;
    return 0;

fail:
    {
        int saved = errno;
        if (store->state_fd >= 0) close(store->state_fd);
        if (store->log_fd >= 0) close(store->log_fd);
        pthread_mutex_destroy(&store->lock);
        free(store);
        errno = saved;
        return -1;
    }
}

void ha_store_close(ha_store_t *store) {
    if (!store) return;
    if (store->state_fd >= 0) close(store->state_fd);
    if (store->log_fd >= 0) close(store->log_fd);
    pthread_mutex_destroy(&store->lock);
    free(store);
}

int ha_store_persist_state(ha_store_t *store, const ha_stable_state_t *state) {
    if (!store || !state || state->applied_index > state->commit_index ||
        state->voted_for < -1 || state->voted_for >= HA_MAX_NODES) return -1;
    pthread_mutex_lock(&store->lock);
    if (store->failed) {
        pthread_mutex_unlock(&store->lock);
        errno = EIO;
        return -1;
    }
    if (store->has_state &&
        (state->term < store->last_state.term ||
         state->commit_index < store->last_state.commit_index ||
         state->applied_index < store->last_state.applied_index ||
         (state->term == store->last_state.term &&
          store->last_state.voted_for >= 0 &&
          state->voted_for != store->last_state.voted_for))) {
        pthread_mutex_unlock(&store->lock);
        errno = EINVAL;
        return -1;
    }
    ha_disk_state_t disk;
    memset(&disk, 0, sizeof(disk));
    disk.magic = HA_STATE_MAGIC;
    disk.version = HA_STORE_VERSION;
    disk.bytes = sizeof(disk);
    disk.generation = store->generation + 1;
    disk.term = state->term;
    disk.voted_for = state->voted_for;
    disk.commit_index = state->commit_index;
    disk.applied_index = state->applied_index;
    disk.crc = state_crc(&disk);
    int rc = lseek(store->state_fd, 0, SEEK_END) < 0 ? -1 :
             write_full(store->state_fd, &disk, sizeof(disk));
    if (rc == 0) rc = fdatasync(store->state_fd);
    if (rc == 0) {
        store->generation = disk.generation;
        store->last_state = *state;
        store->has_state = 1;
    } else {
        /* Never append past an uncertain term/vote record.  Recovery may
         * truncate a partial tail; accepting later writes in this process
         * could then forget a granted vote after restart. */
        store->failed = 1;
    }
    pthread_mutex_unlock(&store->lock);
    return rc;
}

int ha_store_append_log(ha_store_t *store, uint64_t index, uint64_t term,
                        uint64_t timestamp_us, uint32_t type,
                        const void *payload, uint32_t size,
                        int sync_now, uint64_t *file_offset_out) {
    if (!store || index == 0 || size > store->max_payload || (!payload && size)) return -1;
    pthread_mutex_lock(&store->lock);
    if (store->failed) {
        pthread_mutex_unlock(&store->lock);
        errno = EIO;
        return -1;
    }
    if (index != store->last_log_index + 1 || term < store->last_log_term) {
        pthread_mutex_unlock(&store->lock);
        errno = EINVAL;
        return -1;
    }
    off_t offset = lseek(store->log_fd, 0, SEEK_END);
    if (offset < 0) {
        store->failed = 1;
        pthread_mutex_unlock(&store->lock);
        return -1;
    }
    ha_disk_log_header_t header;
    memset(&header, 0, sizeof(header));
    header.magic = HA_LOG_MAGIC;
    header.version = HA_STORE_VERSION;
    header.header_bytes = sizeof(header);
    header.index = index;
    header.term = term;
    header.timestamp_us = timestamp_us;
    header.type = type;
    header.payload_size = size;
    header.payload_crc = size ? crc32c(0, payload, size) : 0;
    header.header_crc = log_header_crc(&header);
    int rc = write_full(store->log_fd, &header, sizeof(header));
    if (rc == 0 && size) rc = write_full(store->log_fd, payload, size);
    if (rc == 0 && sync_now) rc = fdatasync(store->log_fd);
    if (rc == 0) {
        store->last_log_index = index;
        store->last_log_term = term;
        if (file_offset_out) *file_offset_out = (uint64_t)offset;
    } else {
        store->failed = 1;
    }
    pthread_mutex_unlock(&store->lock);
    return rc;
}

int ha_store_sync_log(ha_store_t *store) {
    if (!store) return -1;
    pthread_mutex_lock(&store->lock);
    int rc;
    if (store->failed) {
        errno = EIO;
        rc = -1;
    } else {
        rc = fdatasync(store->log_fd);
        if (rc != 0) store->failed = 1;
    }
    pthread_mutex_unlock(&store->lock);
    return rc;
}

int ha_store_truncate_log(ha_store_t *store, uint64_t file_offset) {
    if (!store || file_offset > (uint64_t)INT64_MAX) return -1;
    pthread_mutex_lock(&store->lock);
    if (store->failed) {
        pthread_mutex_unlock(&store->lock);
        errno = EIO;
        return -1;
    }
    int rc = ftruncate(store->log_fd, (off_t)file_offset);
    if (rc == 0) rc = fsync(store->log_fd);
    if (rc == 0 && lseek(store->log_fd, 0, SEEK_END) < 0) rc = -1;
    if (rc == 0) {
        uint64_t ignored = 0;
        rc = load_log(store, NULL, NULL, &ignored);
    }
    if (rc != 0) store->failed = 1;
    pthread_mutex_unlock(&store->lock);
    return rc;
}

const char *ha_store_state_path(const ha_store_t *store) {
    return store ? store->state_path : NULL;
}

const char *ha_store_log_path(const ha_store_t *store) {
    return store ? store->log_path : NULL;
}
