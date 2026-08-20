#define _GNU_SOURCE

#include <errno.h>
#include <signal.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <unistd.h>

#include "../../src/aof_batch.c"

static RAMForgeMetrics test_metrics;
RAMForgeMetrics *g_metrics_ptr = &test_metrics;

static int replay_calls;
static int semantic_failure;
static uint32_t replayed_id;
static char replayed_payload[32];
static size_t replayed_size;

int RAMForge_HA_replay_record(uint32_t record_type, const void *data, size_t size) {
    (void)record_type;
    (void)data;
    (void)size;
    return 0;
}

void rf_broker_replay_aof(uint32_t rec_id, const void *data, size_t size) {
    replay_calls++;
    replayed_id = rec_id;
    replayed_size = size;
    size_t copy = size < sizeof(replayed_payload) ? size : sizeof(replayed_payload);
    memcpy(replayed_payload, data, copy);
}

int rf_broker_apply_aof_checked(uint32_t rec_id, const void *data,
                                size_t size) {
    rf_broker_replay_aof(rec_id, data, size);
    return semantic_failure ? -1 : 0;
}

static void alarm_failed(int sig) {
    (void)sig;
    static const char message[] = "FAIL sealed replay did not terminate\n";
    ssize_t written = write(STDERR_FILENO, message, sizeof(message) - 1);
    (void)written;
    _exit(124);
}

static int write_all(int fd, const void *data, size_t size) {
    const unsigned char *p = data;
    while (size != 0) {
        ssize_t n = write(fd, p, size);
        if (n < 0 && errno == EINTR) continue;
        if (n <= 0) return -1;
        p += (size_t)n;
        size -= (size_t)n;
    }
    return 0;
}

static void make_record(unsigned char record[512], uint64_t batch_id,
                        uint32_t rec_id, const void *payload, uint32_t size) {
    memset(record, 0, 512);
    const uint32_t magic = 0x5EA1ED02;
    const uint64_t timestamp = 123456789;
    const uint32_t reserved = 0;
    memcpy(record + 0, &magic, sizeof(magic));
    memcpy(record + 4, &batch_id, sizeof(batch_id));
    memcpy(record + 12, &timestamp, sizeof(timestamp));
    memcpy(record + 20, &size, sizeof(size));
    memcpy(record + 24, &rec_id, sizeof(rec_id));
    memcpy(record + 28, &reserved, sizeof(reserved));
    memcpy(record + 32, payload, size);
    uint32_t crc = crc32c(0, record, 32u + size);
    memcpy(record + 32u + size, &crc, sizeof(crc));
}

static int replace_file(const char *path, const void *first, size_t first_size,
                        const void *second, size_t second_size) {
    int fd = open(path, O_CREAT | O_TRUNC | O_WRONLY | O_CLOEXEC, 0600);
    if (fd < 0) return -1;
    int rc = write_all(fd, first, first_size);
    if (rc == 0 && second_size != 0) rc = write_all(fd, second, second_size);
    if (close(fd) != 0 && rc == 0) rc = -1;
    return rc;
}

static off_t file_size(const char *path) {
    struct stat st;
    return stat(path, &st) == 0 ? st.st_size : (off_t)-1;
}

int main(void) {
    char dir_template[] = "/tmp/ayder-sealed-replay-XXXXXX";
    char *dir = mkdtemp(dir_template);
    if (!dir) {
        perror("mkdtemp");
        return 1;
    }

    char base[512];
    char sealed[544];
    snprintf(base, sizeof(base), "%s/append.aof", dir);
    snprintf(sealed, sizeof(sealed), "%s.sealed", base);

    unsigned char first[512];
    unsigned char second[512];
    const char payload1[] = "first";
    const char payload2[] = "second";
    make_record(first, 1, 101, payload1, sizeof(payload1) - 1);
    make_record(second, 2, 102, payload2, sizeof(payload2) - 1);

    /* A complete record followed by an uncommitted short tail is repairable. */
    if (replace_file(sealed, first, sizeof(first), second, 19) != 0) {
        perror("write partial-tail fixture");
        return 1;
    }
    replay_calls = 0;
    size_t applied = AOF_sealed_replay(base);
    if (applied != 1 || replay_calls != 1 || replayed_id != 101 ||
        replayed_size != sizeof(payload1) - 1 ||
        memcmp(replayed_payload, payload1, sizeof(payload1) - 1) != 0 ||
        file_size(sealed) != 512) {
        fprintf(stderr,
                "partial tail was not repaired safely: applied=%zu calls=%d id=%u size=%zu file=%jd\n",
                applied, replay_calls, replayed_id, replayed_size,
                (intmax_t)file_size(sealed));
        return 1;
    }

    /* A correctly framed record whose state transition cannot be applied is
     * not safe to skip. Keep it intact and fail readiness. */
    if (replace_file(sealed, first, sizeof(first), NULL, 0) != 0) {
        perror("write semantic-failure fixture");
        return 1;
    }
    replay_calls = 0;
    semantic_failure = 1;
    applied = AOF_sealed_replay(base);
    semantic_failure = 0;
    if (applied != SIZE_MAX || replay_calls != 1 || file_size(sealed) != 512) {
        fprintf(stderr,
                "semantic corruption was not fail-closed: applied=%zu calls=%d file=%jd\n",
                applied, replay_calls, (intmax_t)file_size(sealed));
        return 1;
    }

    /* A full-size record with a bad CRC is committed-looking corruption, not a
     * tail to truncate. It must remain on disk and fail startup. */
    second[32 + sizeof(payload2) - 1] ^= 0x80;
    if (replace_file(sealed, first, sizeof(first), second, sizeof(second)) != 0) {
        perror("write CRC fixture");
        return 1;
    }
    replay_calls = 0;
    applied = AOF_sealed_replay(base);
    if (applied != SIZE_MAX || replay_calls != 1 || file_size(sealed) != 1024) {
        fprintf(stderr,
                "CRC corruption was not fail-closed: applied=%zu calls=%d file=%jd\n",
                applied, replay_calls, (intmax_t)file_size(sealed));
        return 1;
    }

    /* Invalid magic at aligned offset zero used to round to offset zero and
     * loop forever. Bound the call with an alarm as a behavioral regression. */
    memset(first, 0xA5, sizeof(first));
    if (replace_file(sealed, first, sizeof(first), NULL, 0) != 0) {
        perror("write invalid-magic fixture");
        return 1;
    }
    signal(SIGALRM, alarm_failed);
    alarm(2);
    replay_calls = 0;
    applied = AOF_sealed_replay(base);
    alarm(0);
    if (applied != SIZE_MAX || replay_calls != 0 || file_size(sealed) != 512) {
        fprintf(stderr,
                "invalid magic was not rejected without mutation: applied=%zu calls=%d file=%jd\n",
                applied, replay_calls, (intmax_t)file_size(sealed));
        return 1;
    }

    unlink(sealed);
    rmdir(dir);
    puts("PASS sealed replay repairs only partial tails and fails closed on corruption");
    return 0;
}
