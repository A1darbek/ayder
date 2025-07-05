/* aof_batch.c – append-only log with batching + CRC32C + non-blocking rewrite */
#define _POSIX_C_SOURCE 200809L
#include <pthread.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <fcntl.h>
#include <stdio.h>
#include <errno.h>
#include <stdatomic.h>
#include <stdbool.h>

#include "aof_batch.h"
#include "storage.h"
#include "crc32c.h"

/* ─── configuration ───────────────────────────── */
#define DEFAULT_RING_CAP (1 << 15)            /* 32 k entries */
#define BUFFER_POOL_SIZE 1024                 /* Pre-allocated buffers */
#define MAX_BUFFER_SIZE 8192                  /* Max size per buffer */

/* ─── Improved Buffer pool for zero-malloc AOF_append ─── */
typedef struct buffer_node {
    void *data;
    size_t capacity;
    struct buffer_node *next;
} buffer_node_t;

typedef struct {
    buffer_node_t *free_list;
    buffer_node_t *all_nodes;     /* Track ALL nodes for cleanup */
    size_t free_count;
    size_t total_allocated;
    pthread_mutex_t lock;
    atomic_bool shutdown_flag;    /* Atomic shutdown coordination */
} buffer_pool_t;

static buffer_pool_t g_buffer_pool = {0};

/* ─── types / globals ─────────────────────────── */
typedef struct {
    int id;
    uint32_t sz;
    void *data;
    int from_pool;  /* 1 if from pool, 0 if malloc'd */
} aof_cmd_t;

typedef struct {
    char *source_path;
    char *output_path;
    Storage *storage;
} rewrite_task_t;

static aof_cmd_t     *ring;
static size_t         cap, mask, head, tail;
static pthread_mutex_t lock;
static pthread_cond_t  cond;

static int            fd = -1;
static char          *g_path = NULL;          /* remember for rewrite */
static unsigned       flush_ms = 10;
static int            mode_always = 0;
static pthread_t      writer;
static atomic_bool    running = ATOMIC_VAR_INIT(false);

/* ─── Non-blocking rewrite state ─────────────── */
static atomic_bool    rewrite_active = ATOMIC_VAR_INIT(false);
static pthread_t      rewrite_thread;
static aof_cmd_t     *rewrite_buffer;
static size_t         rewrite_buf_cap, rewrite_buf_mask;
static size_t         rewrite_buf_head, rewrite_buf_tail;
static pthread_mutex_t rewrite_lock;

static pthread_t segment_rewrite_thread;
static atomic_bool segment_rewrite_active = ATOMIC_VAR_INIT(false);

static void dump_record_cb(int id, const void *data, size_t sz, void *ud)
{
    int fd = (int)(intptr_t)ud;
    aof_write_record(fd, id, data, (uint32_t)sz);
}


static void *segment_rewrite_thread_func(void *arg)
{
    rewrite_task_t *task = (rewrite_task_t*)arg;

    printf("🔄 Starting segment rewrite: %s\n", task->source_path);

    // Create temporary output file
    char tmp_path[1024];
    snprintf(tmp_path, sizeof(tmp_path), "%s.compact", task->output_path);

    int fd_out = open(tmp_path, O_CREAT | O_TRUNC | O_WRONLY | O_CLOEXEC, 0600);
    if (fd_out < 0) {
        perror("segment_rewrite: open output");
        goto cleanup;
    }

    // Load the segment into a temporary storage
    Storage temp_storage;
    storage_init(&temp_storage);

    // Read and parse the source AOF file
    int fd_in = open(task->source_path, O_RDONLY | O_CLOEXEC);
    if (fd_in < 0) {
        perror("segment_rewrite: open input");
        close(fd_out);
        unlink(tmp_path);
        storage_destroy(&temp_storage);
        goto cleanup;
    }

    // Parse the AOF file and load into temp storage
    int id;
    uint32_t size, crc_file;
    while (read(fd_in, &id, 4) == 4) {
        if (read(fd_in, &size, 4) != 4) {
            fprintf(stderr, "segment_rewrite: corrupt size field\n");
            break;
        }

        void *buf = malloc(size);
        if (!buf) {
            fprintf(stderr, "segment_rewrite: malloc failed\n");
            break;
        }

        if (read(fd_in, buf, size) != (ssize_t)size) {
            fprintf(stderr, "segment_rewrite: corrupt data field\n");
            free(buf);
            break;
        }

        if (read(fd_in, &crc_file, 4) != 4) {
            fprintf(stderr, "segment_rewrite: corrupt crc field\n");
            free(buf);
            break;
        }

        // Verify CRC
        uint32_t crc = crc32c(0, &id, 4);
        crc = crc32c(crc, &size, 4);
        crc = crc32c(crc, buf, size);

        if (crc != crc_file) {
            fprintf(stderr, "segment_rewrite: CRC mismatch\n");
            free(buf);
            break;
        }

        // Store in temp storage (this will deduplicate)
        storage_save(&temp_storage, id, buf, size);
        free(buf);
    }

    close(fd_in);

    // Write compacted version
    storage_iterate(&temp_storage, dump_record_cb, (void*)(intptr_t)fd_out);

    fsync(fd_out);
    close(fd_out);

    // Atomically replace the original file
    if (rename(tmp_path, task->output_path) != 0) {
        perror("segment_rewrite: rename failed");
        unlink(tmp_path);
    } else {
        printf("✅ Segment rewrite complete: %s\n", task->output_path);
    }

    storage_destroy(&temp_storage);

    cleanup:
    free(task->source_path);
    free(task->output_path);
    free(task);
    atomic_store(&segment_rewrite_active, false);
    return NULL;
}

/* Asynchronous rewrite: returns 0 if launch OK, -1 otherwise */
int AOF_begin_rewrite(const char *source_path)
{
    if (atomic_load(&segment_rewrite_active)) {
        printf("⚠️  Segment rewrite already in progress\n");
        return -1;
    }

    // Create task structure
    rewrite_task_t *task = malloc(sizeof(rewrite_task_t));
    if (!task) {
        return -1;
    }

    task->source_path = strdup(source_path);
    task->output_path = strdup(source_path); // Rewrite in place
    task->storage = NULL; // Not needed for this operation

    if (!task->source_path || !task->output_path) {
        free(task->source_path);
        free(task->output_path);
        free(task);
        return -1;
    }

    atomic_store(&segment_rewrite_active, true);

    if (pthread_create(&segment_rewrite_thread, NULL, segment_rewrite_thread_func, task)) {
        perror("pthread_create segment rewrite");
        atomic_store(&segment_rewrite_active, false);
        free(task->source_path);
        free(task->output_path);
        free(task);
        return -1;
    }

    pthread_detach(segment_rewrite_thread);
    return 0;
}

/* Check if segment rewrite is in progress */
int AOF_segment_rewrite_in_progress(void)
{
    return atomic_load(&segment_rewrite_active);
}

/* ─── Robust Buffer pool management ─────────────── */
static int init_buffer_pool(void)
{
    memset(&g_buffer_pool, 0, sizeof(g_buffer_pool));

    if (pthread_mutex_init(&g_buffer_pool.lock, NULL) != 0) {
        return -1;
    }

    atomic_store(&g_buffer_pool.shutdown_flag, false);

    pthread_mutex_lock(&g_buffer_pool.lock);

    /* Pre-allocate all nodes in one go */
    buffer_node_t *nodes = calloc(BUFFER_POOL_SIZE, sizeof(buffer_node_t));
    if (!nodes) {
        pthread_mutex_unlock(&g_buffer_pool.lock);
        pthread_mutex_destroy(&g_buffer_pool.lock);
        return -1;
    }

    /* Link all nodes and allocate their buffers */
    buffer_node_t *prev = NULL;
    size_t successful = 0;

    for (size_t i = 0; i < BUFFER_POOL_SIZE; i++) {
        nodes[i].data = malloc(MAX_BUFFER_SIZE);
        if (!nodes[i].data) {
            /* Allocation failed, but continue with what we have */
            break;
        }

        nodes[i].capacity = MAX_BUFFER_SIZE;
        nodes[i].next = prev;
        prev = &nodes[i];
        successful++;
    }

    g_buffer_pool.free_list = prev;
    g_buffer_pool.all_nodes = nodes;  /* Keep reference for cleanup */
    g_buffer_pool.free_count = successful;
    g_buffer_pool.total_allocated = successful;

    pthread_mutex_unlock(&g_buffer_pool.lock);

    printf("📦 Initialized buffer pool with %zu/%d buffers\n",
           successful, BUFFER_POOL_SIZE);
    return 0;
}

static void *get_buffer_from_pool(size_t needed_size)
{
    if (needed_size > MAX_BUFFER_SIZE) {
        return malloc(needed_size);  /* Fallback for large buffers */
    }

    if (atomic_load(&g_buffer_pool.shutdown_flag)) {
        return malloc(needed_size);  /* Pool shutting down, use malloc */
    }

    pthread_mutex_lock(&g_buffer_pool.lock);

    if (g_buffer_pool.free_list && g_buffer_pool.free_count > 0) {
        buffer_node_t *node = g_buffer_pool.free_list;
        g_buffer_pool.free_list = node->next;
        g_buffer_pool.free_count--;

        pthread_mutex_unlock(&g_buffer_pool.lock);
        return node->data;  /* Return the data buffer */
    }

    pthread_mutex_unlock(&g_buffer_pool.lock);
    return malloc(needed_size);  /* Pool empty, fallback */
}

static void return_buffer_to_pool(void *data, size_t size)
{
    if (size > MAX_BUFFER_SIZE || !data) {
        free(data);  /* Was allocated with malloc or invalid */
        return;
    }

    if (atomic_load(&g_buffer_pool.shutdown_flag)) {
        free(data);  /* Pool shutting down, just free it */
        return;
    }

    pthread_mutex_lock(&g_buffer_pool.lock);

    /* Find the node that owns this data buffer */
    buffer_node_t *found = NULL;
    for (size_t i = 0; i < g_buffer_pool.total_allocated; i++) {
        if (g_buffer_pool.all_nodes[i].data == data) {
            found = &g_buffer_pool.all_nodes[i];
            break;
        }
    }

    if (found && g_buffer_pool.free_count < g_buffer_pool.total_allocated) {
        /* Add back to free list */
        found->next = g_buffer_pool.free_list;
        g_buffer_pool.free_list = found;
        g_buffer_pool.free_count++;
        pthread_mutex_unlock(&g_buffer_pool.lock);
        return;
    }

    pthread_mutex_unlock(&g_buffer_pool.lock);

    /* If we get here, it wasn't from our pool or pool is full */
    free(data);
}

static void destroy_buffer_pool(void)
{
    printf("🗑️  Destroying buffer pool...\n");

    /* Signal shutdown to prevent new operations */
    atomic_store(&g_buffer_pool.shutdown_flag, true);

    pthread_mutex_lock(&g_buffer_pool.lock);

    if (g_buffer_pool.all_nodes) {
        /* Free all data buffers */
        for (size_t i = 0; i < g_buffer_pool.total_allocated; i++) {
            free(g_buffer_pool.all_nodes[i].data);
            g_buffer_pool.all_nodes[i].data = NULL;
        }

        /* Free the nodes array */
        free(g_buffer_pool.all_nodes);
        g_buffer_pool.all_nodes = NULL;
    }

    g_buffer_pool.free_list = NULL;
    g_buffer_pool.free_count = 0;
    g_buffer_pool.total_allocated = 0;

    pthread_mutex_unlock(&g_buffer_pool.lock);
    pthread_mutex_destroy(&g_buffer_pool.lock);

    printf("✅ Buffer pool destroyed\n");
}

/* ─── CRC helper ──────────────────────────────── */
static int safe_write(int fd, const void *buf, size_t len)
{
    ssize_t n = write(fd, buf, len);
    return (n == (ssize_t)len) ? 0 : -1;
}

int aof_write_record(int fd, int id, const void *data, uint32_t size)
{
    if (safe_write(fd,&id,4)         ||
        safe_write(fd,&size,4)       ||
        safe_write(fd,data,size))
        return -1;

    uint32_t crc = crc32c(0,&id,4);
    crc = crc32c(crc,&size,4);
    crc = crc32c(crc,data,size);
    return safe_write(fd,&crc,4);
}

/* ─── Clean up ring buffer entry ──────────────── */
static void cleanup_ring_entry(aof_cmd_t *c)
{
    if (c->data) {
        if (c->from_pool) {
            return_buffer_to_pool(c->data, c->sz);
        } else {
            free(c->data);
        }
        c->data = NULL;
    }
    c->from_pool = 0;
}

/* ─── background writer (batch mode) ─────────── */
static void *writer_thread(void *arg)
{
    (void)arg;

    while (atomic_load(&running)) {
        pthread_mutex_lock(&lock);

        while (head == tail && atomic_load(&running)) {
            pthread_cond_wait(&cond, &lock);
        }

        while (head != tail && fd != -1) {
            aof_cmd_t *c = &ring[tail];

            if (aof_write_record(fd, c->id, c->data, c->sz) == -1) {
                perror("writer_thread: write failed");
            }

            /* If rewrite is active, buffer this operation */
            if (atomic_load(&rewrite_active)) {
                pthread_mutex_lock(&rewrite_lock);
                size_t nxt = (rewrite_buf_head + 1) & rewrite_buf_mask;
                if (nxt != rewrite_buf_tail) {
                    void *copy = malloc(c->sz);
                    if (copy) {
                        memcpy(copy, c->data, c->sz);
                        rewrite_buffer[rewrite_buf_head] = (aof_cmd_t){
                                c->id, c->sz, copy, 0
                        };
                        rewrite_buf_head = nxt;
                    }
                }
                pthread_mutex_unlock(&rewrite_lock);
            }

            cleanup_ring_entry(c);
            tail = (tail + 1) & mask;
        }

        if (fd != -1) {
            fsync(fd);
        }

        pthread_cond_signal(&cond);
        pthread_mutex_unlock(&lock);

        struct timespec ts;
        clock_gettime(CLOCK_REALTIME, &ts);
        ts.tv_nsec += flush_ms * 1000000ULL;
        if (ts.tv_nsec >= 1000000000) {
            ts.tv_sec++;
            ts.tv_nsec -= 1000000000;
        }

        pthread_mutex_lock(&lock);
        pthread_cond_timedwait(&cond, &lock, &ts);
        pthread_mutex_unlock(&lock);
    }

    printf("📝 Writer thread exiting\n");
    return NULL;
}

/* ─── Non-blocking rewrite thread ─────────────── */

static void *rewrite_thread_func(void *arg)
{
    Storage *st = (Storage*)arg;

    printf("🔄 Starting non-blocking AOF rewrite...\n");

    char tmp[512];
    snprintf(tmp, sizeof tmp, "%s.rewrite", g_path);
    int fd_tmp = open(tmp, O_CREAT | O_TRUNC | O_WRONLY | O_CLOEXEC, 0600);
    if (fd_tmp < 0) {
        perror("rewrite: open tmp");
        atomic_store(&rewrite_active, false);
        return NULL;
    }

    storage_iterate(st, dump_record_cb, (void*)(intptr_t)fd_tmp);

    pthread_mutex_lock(&rewrite_lock);
    printf("📝 Applying %zu buffered operations...\n",
           (rewrite_buf_head - rewrite_buf_tail) & rewrite_buf_mask);

    while (rewrite_buf_tail != rewrite_buf_head) {
        aof_cmd_t *c = &rewrite_buffer[rewrite_buf_tail];
        aof_write_record(fd_tmp, c->id, c->data, c->sz);
        free(c->data);
        c->data = NULL;
        rewrite_buf_tail = (rewrite_buf_tail + 1) & rewrite_buf_mask;
    }
    pthread_mutex_unlock(&rewrite_lock);

    fsync(fd_tmp);
    close(fd_tmp);

    pthread_mutex_lock(&lock);

    while (head != tail) {
        aof_cmd_t *c = &ring[tail];
        aof_write_record(fd, c->id, c->data, c->sz);
        cleanup_ring_entry(c);
        tail = (tail + 1) & mask;
    }
    fsync(fd);

    int old_fd = fd;
    fd = -1;
    close(old_fd);

    if (rename(tmp, g_path) != 0) {
        perror("rewrite: rename failed");
        fd = open(g_path, O_APPEND | O_WRONLY | O_CLOEXEC, 0600);
        if (fd < 0) {
            perror("rewrite: failed to restore fd");
            pthread_mutex_unlock(&lock);
            exit(1);
        }
        atomic_store(&rewrite_active, false);
        pthread_mutex_unlock(&lock);
        return NULL;
    }

    fd = open(g_path, O_APPEND | O_WRONLY | O_CLOEXEC, 0600);
    if (fd < 0) {
        perror("rewrite: reopen failed");
        pthread_mutex_unlock(&lock);
        exit(1);
    }

    pthread_mutex_unlock(&lock);

    atomic_store(&rewrite_active, false);
    printf("✅ Non-blocking AOF rewrite complete!\n");
    return NULL;
}

/* ─── public API ─────────────────────────────── */
void AOF_init(const char *path, size_t ring_capacity, unsigned interval_ms)
{
    mode_always = (interval_ms == 0);
    flush_ms    = mode_always ? 1000 : interval_ms;
    g_path      = strdup(path);

    if (init_buffer_pool() != 0) {
        fprintf(stderr, "Failed to initialize buffer pool\n");
        exit(1);
    }

    if (!mode_always) {
        cap = 1; while (cap < ring_capacity) cap <<= 1;
        mask = cap - 1;
        ring = calloc(cap, sizeof *ring);
        pthread_mutex_init(&lock, NULL);
        pthread_cond_init(&cond, NULL);

        rewrite_buf_cap = cap / 4;
        if (rewrite_buf_cap < 1024) rewrite_buf_cap = 1024;
        rewrite_buf_mask = rewrite_buf_cap - 1;
        rewrite_buffer = calloc(rewrite_buf_cap, sizeof *rewrite_buffer);
        pthread_mutex_init(&rewrite_lock, NULL);
    }

    fd = open(path, O_CREAT | O_APPEND | O_WRONLY | O_CLOEXEC, 0600);
    if (fd < 0) { perror("AOF_init/open"); exit(1); }

    if (!mode_always) {
        atomic_store(&running, true);
        if (pthread_create(&writer, NULL, writer_thread, NULL)) {
            perror("pthread_create"); exit(1);
        }
    }
}

int AOF_append(int id, const void *data, size_t size) {
    if (mode_always) {
        if (aof_write_record(fd, id, data, (uint32_t) size) == -1)
            return -1;
        fsync(fd);
        return 0;
    }

    void *copy = get_buffer_from_pool(size);
    if (!copy) return -1;

    memcpy(copy, data, size);
    int from_pool = (size <= MAX_BUFFER_SIZE) ? 1 : 0;

    pthread_mutex_lock(&lock);
    size_t nxt = (head + 1) & mask;
    while (nxt == tail) {
        pthread_cond_wait(&cond, &lock);
        nxt = (head + 1) & mask;
    }
    ring[head] = (aof_cmd_t) {id, (uint32_t) size, copy, from_pool};
    head = nxt;
    pthread_cond_signal(&cond);
    pthread_mutex_unlock(&lock);
    return 0;
}

void AOF_load(Storage *st)
{
    if (!g_path) return;

    int read_fd = open(g_path, O_RDONLY | O_CLOEXEC);
    if (read_fd < 0) {
        if (errno == ENOENT) return;
        perror("AOF_load/open");
        return;
    }

    int id; uint32_t size, crc_file;
    while (read(read_fd, &id, 4) == 4) {
        if (read(read_fd, &size, 4) != 4) goto corrupt;

        void *buf = malloc(size);
        if (read(read_fd, buf, size) != (ssize_t)size) goto corrupt;

        if (read(read_fd, &crc_file, 4) != 4) goto corrupt;

        uint32_t crc = crc32c(0, &id, 4);
        crc = crc32c(crc, &size, 4);
        crc = crc32c(crc, buf, size);
        if (crc != crc_file) goto corrupt;

        storage_save(st, id, buf, size);
        free(buf);
    }
    close(read_fd);
    return;

    corrupt:
    fprintf(stderr, "❌ AOF corrupt at offset %#lx – aborting\n",
            (unsigned long)lseek(read_fd, 0, SEEK_CUR));
    close(read_fd);
    exit(2);
}

int AOF_rewrite_nonblocking(Storage *st)
{
    if (atomic_load(&rewrite_active)) {
        printf("⚠️  Rewrite already in progress\n");
        return -1;
    }

    if (mode_always) {
        printf("⚠️  Non-blocking rewrite not supported in always-sync mode\n");
        return -1;
    }

    pthread_mutex_lock(&rewrite_lock);
    rewrite_buf_head = rewrite_buf_tail = 0;
    atomic_store(&rewrite_active, true);
    pthread_mutex_unlock(&rewrite_lock);

    if (pthread_create(&rewrite_thread, NULL, rewrite_thread_func, st)) {
        perror("pthread_create rewrite");
        atomic_store(&rewrite_active, false);
        return -1;
    }

    pthread_detach(rewrite_thread);
    return 0;
}

int AOF_rewrite_in_progress(void)
{
    return atomic_load(&rewrite_active);
}

void AOF_rewrite(Storage *st)
{
    printf("🔄 Compacting AOF (blocking)…\n");

    char tmp[512];
    snprintf(tmp, sizeof tmp, "%s.tmp", g_path);
    int fd_tmp = open(tmp, O_CREAT | O_TRUNC | O_WRONLY | O_CLOEXEC, 0600);
    if (fd_tmp < 0) { perror("open tmp"); return; }

    if (mode_always) {
        Storage temp_st;
        storage_init(&temp_st);
        AOF_load(&temp_st);
        storage_iterate(&temp_st, dump_record_cb, (void*)(intptr_t)fd_tmp);
        storage_destroy(&temp_st);
    } else {
        storage_iterate(st, dump_record_cb, (void*)(intptr_t)fd_tmp);
    }

    fsync(fd_tmp);
    close(fd_tmp);

    if (!mode_always) {
        pthread_mutex_lock(&lock);
        while (head != tail) {
            aof_cmd_t *c = &ring[tail];
            aof_write_record(fd, c->id, c->data, c->sz);
            cleanup_ring_entry(c);
            tail = (tail + 1) & mask;
        }
        fsync(fd);
    }
    close(fd);

    if (rename(tmp, g_path) != 0) perror("rename");

    fd = open(g_path, O_APPEND | O_WRONLY | O_CLOEXEC, 0600);
    if (fd < 0) { perror("re-open AOF"); exit(1); }

    if (!mode_always) {
        pthread_mutex_unlock(&lock);
    }
    printf("✓ AOF rewrite complete\n");
}

void AOF_shutdown(void)
{
    printf("🛑 AOF shutdown starting...\n");

    if (mode_always) {
        if (fd != -1) {
            close(fd);
            fd = -1;
        }
        free(g_path);
        g_path = NULL;
        destroy_buffer_pool();
        printf("✅ AOF shutdown complete (always mode)\n");
        return;
    }

    /* 1. Stop accepting new operations */
    atomic_store(&running, false);

    /* 2. Wake up and wait for writer thread */
    pthread_cond_signal(&cond);
    pthread_join(writer, NULL);
    printf("📝 Writer thread joined\n");

    /* 3. Wait for any active segment rewrite to complete */
    while (atomic_load(&segment_rewrite_active)) {
        printf("⏳ Waiting for segment rewrite to complete...\n");
        struct timespec ts;
        ts.tv_sec = 0;
        ts.tv_nsec = 100000000;  // 100 ms
        nanosleep(&ts, NULL);
    }

    /* 4. Clean up remaining ring buffer entries */
    printf("🧹 Cleaning up %zu pending operations...\n", (head - tail) & mask);
    while (head != tail) {
        cleanup_ring_entry(&ring[tail]);
        tail = (tail + 1) & mask;
    }

    /* 5. Clean up rewrite buffer */
    pthread_mutex_lock(&rewrite_lock);
    while (rewrite_buf_tail != rewrite_buf_head) {
        aof_cmd_t *c = &rewrite_buffer[rewrite_buf_tail];
        if (c->data) {
            free(c->data);
            c->data = NULL;
        }
        rewrite_buf_tail = (rewrite_buf_tail + 1) & rewrite_buf_mask;
    }
    pthread_mutex_unlock(&rewrite_lock);

    /* 6. Destroy synchronization objects */
    pthread_mutex_destroy(&lock);
    pthread_cond_destroy(&cond);
    pthread_mutex_destroy(&rewrite_lock);

    /* 7. Free allocated memory */
    free(ring);
    free(rewrite_buffer);
    free(g_path);
    g_path = NULL;

    /* 8. Close file descriptor */
    if (fd != -1) {
        close(fd);
        fd = -1;
    }

    /* 9. Destroy buffer pool LAST */
    destroy_buffer_pool();

    printf("✅ AOF shutdown complete\n");
}