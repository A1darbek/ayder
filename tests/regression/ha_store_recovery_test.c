#define _GNU_SOURCE
#include "ramforge_ha_store.h"
#include "ramforge_ha_config.h"

#include <errno.h>
#include <fcntl.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <signal.h>
#include <sys/stat.h>
#include <sys/resource.h>
#include <unistd.h>

static int checks;
static int failures;

#define CHECK(expr) do { \
    checks++; \
    if (!(expr)) { \
        fprintf(stderr, "FAIL %s:%d: %s (errno=%d)\n", \
                __FILE__, __LINE__, #expr, errno); \
        failures++; \
    } \
} while (0)

typedef struct {
    int count;
    uint64_t last_index;
    char payloads[2][16];
} replay_state_t;

static int replay_cb(uint64_t index, uint64_t term, uint64_t timestamp_us,
                     uint32_t type, const void *payload, uint32_t size,
                     uint64_t file_offset, void *ctx) {
    (void)timestamp_us;
    (void)file_offset;
    replay_state_t *state = (replay_state_t *)ctx;
    if (!state || index != (uint64_t)state->count + 1 || term != 1 ||
        type != 7 || size >= sizeof(state->payloads[0])) return -1;
    memcpy(state->payloads[state->count], payload, size);
    state->payloads[state->count][size] = '\0';
    state->count++;
    state->last_index = index;
    return 0;
}

static off_t file_size(const char *path) {
    struct stat st;
    return stat(path, &st) == 0 ? st.st_size : -1;
}

int main(void) {
    char dir[] = "/tmp/ayder-ha-store-XXXXXX";
    CHECK(mkdtemp(dir) != NULL);
    char state_path[512], log_path[512];
    CHECK(snprintf(state_path, sizeof(state_path), "%s/state", dir) > 0);
    CHECK(snprintf(log_path, sizeof(log_path), "%s/log", dir) > 0);
    CHECK(setenv("RF_HA_STATE_PATH", state_path, 1) == 0);
    CHECK(setenv("RF_HA_LOG_PATH", log_path, 1) == 0);

    ha_store_t *store = NULL;
    ha_stable_state_t state;
    replay_state_t replay = {0};
    CHECK(ha_store_open(&store, "node/a", 1024, &state, replay_cb, &replay) == 0);
    CHECK(state.term == 0 && state.voted_for == -1 && state.commit_index == 0 &&
          state.applied_index == 0 && replay.count == 0);

    uint64_t first_offset = UINT64_MAX, second_offset = UINT64_MAX;
    CHECK(ha_store_append_log(store, 1, 1, 10, 7, "one", 3, 1,
                              &first_offset) == 0);
    CHECK(ha_store_append_log(store, 3, 1, 11, 7, "gap", 3, 1, NULL) != 0);
    CHECK(ha_store_append_log(store, 2, 0, 11, 7, "old", 3, 1, NULL) != 0);
    CHECK(ha_store_append_log(store, 2, 1, 11, 7, "two", 3, 1,
                              &second_offset) == 0);
    CHECK(first_offset == 0 && second_offset > first_offset);

    state = (ha_stable_state_t){.term = 1, .voted_for = 0,
                                .commit_index = 2, .applied_index = 1};
    CHECK(ha_store_persist_state(store, &state) == 0);
    ha_stable_state_t invalid = state;
    invalid.voted_for = 1;
    CHECK(ha_store_persist_state(store, &invalid) != 0);
    invalid = state;
    invalid.commit_index = 1;
    CHECK(ha_store_persist_state(store, &invalid) != 0);
    invalid = state;
    invalid.voted_for = HA_MAX_NODES;
    CHECK(ha_store_persist_state(store, &invalid) != 0);
    ha_store_close(store);
    store = NULL;

    off_t complete_log_size = file_size(log_path);
    CHECK(complete_log_size > 0);
    int fd = open(log_path, O_WRONLY | O_APPEND | O_CLOEXEC);
    CHECK(fd >= 0);
    CHECK(write(fd, "tail", 4) == 4);
    CHECK(close(fd) == 0);
    replay = (replay_state_t){0};
    CHECK(ha_store_open(&store, "node/a", 1024, &state, replay_cb, &replay) == 0);
    CHECK(file_size(log_path) == complete_log_size);
    CHECK(state.term == 1 && state.voted_for == 0 && state.commit_index == 2 &&
          state.applied_index == 1);
    CHECK(replay.count == 2 && replay.last_index == 2);
    CHECK(strcmp(replay.payloads[0], "one") == 0 &&
          strcmp(replay.payloads[1], "two") == 0);

    /* A partial stable-state append poisons the live store.  Later records
     * must not be accepted past the uncertain tail, because restart repair
     * would otherwise forget their term/vote. */
    struct rlimit old_limit, limited;
    CHECK(getrlimit(RLIMIT_FSIZE, &old_limit) == 0);
    off_t stable_state_size = file_size(state_path);
    CHECK(stable_state_size > 0);
    limited = old_limit;
    limited.rlim_cur = (rlim_t)stable_state_size + 8;
    CHECK(signal(SIGXFSZ, SIG_IGN) != SIG_ERR);
    CHECK(setrlimit(RLIMIT_FSIZE, &limited) == 0);
    ha_stable_state_t next_state = {
        .term = 2, .voted_for = -1, .commit_index = 2, .applied_index = 1
    };
    CHECK(ha_store_persist_state(store, &next_state) != 0);
    CHECK(setrlimit(RLIMIT_FSIZE, &old_limit) == 0);
    errno = 0;
    CHECK(ha_store_persist_state(store, &next_state) != 0 && errno == EIO);
    errno = 0;
    CHECK(ha_store_append_log(store, 3, 2, 12, 7, "three", 5, 1, NULL) != 0 &&
          errno == EIO);
    ha_store_close(store);
    store = NULL;

    replay = (replay_state_t){0};
    CHECK(ha_store_open(&store, "node/a", 1024, &state, replay_cb, &replay) == 0);
    CHECK(state.term == 1 && state.voted_for == 0);
    CHECK(file_size(state_path) == stable_state_size);
    CHECK(ha_store_persist_state(store, &next_state) == 0);
    ha_store_close(store);
    store = NULL;

    /* A complete, CRC-invalid record is never treated as a repairable tail. */
    fd = open(log_path, O_RDWR | O_CLOEXEC);
    CHECK(fd >= 0);
    unsigned char byte = 0;
    CHECK(pread(fd, &byte, 1, (off_t)second_offset + 48) == 1);
    byte ^= 0x80u;
    CHECK(pwrite(fd, &byte, 1, (off_t)second_offset + 48) == 1);
    CHECK(fsync(fd) == 0);
    CHECK(close(fd) == 0);
    replay = (replay_state_t){0};
    errno = 0;
    CHECK(ha_store_open(&store, "node/a", 1024, &state, replay_cb, &replay) != 0);
    CHECK(store == NULL && errno == EBADMSG);

    CHECK(unlink(state_path) == 0);
    CHECK(unlink(log_path) == 0);
    CHECK(rmdir(dir) == 0);

    if (failures) {
        fprintf(stderr, "ha store recovery: %d/%d checks failed\n", failures, checks);
        return 1;
    }
    printf("ha store recovery: %d checks passed\n", checks);
    return 0;
}
