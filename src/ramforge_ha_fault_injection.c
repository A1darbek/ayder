#define _GNU_SOURCE
#include "ramforge_ha_fault_injection.h"

#include "ramforge_ha_config.h"

#include <ctype.h>
#include <errno.h>
#include <fcntl.h>
#include <stdint.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

static int test_faults_enabled(void) {
    const char *enabled = getenv("RF_HA_TEST_FAULTS");
    return enabled && strcmp(enabled, "1") == 0;
}

static int read_cut_mask(const char *path, uint64_t *mask_out) {
    if (!path || !*path || !mask_out) return -1;

    int flags = O_RDONLY | O_CLOEXEC;
#ifdef O_NOFOLLOW
    flags |= O_NOFOLLOW;
#endif
    int fd = open(path, flags);
    if (fd < 0) return -1;

    char buf[96];
    ssize_t n;
    do {
        n = read(fd, buf, sizeof(buf) - 1);
    } while (n < 0 && errno == EINTR);

    int close_rc;
    do {
        close_rc = close(fd);
    } while (close_rc < 0 && errno == EINTR);
    if (n <= 0 || close_rc != 0) return -1;

    /* Reject overlong/truncated control data instead of parsing a prefix. */
    if ((size_t)n == sizeof(buf) - 1) return -1;
    buf[n] = '\0';

    char *p = buf;
    while (isspace((unsigned char)*p)) p++;
    static const char prefix[] = "cut_mask=";
    if (strncmp(p, prefix, sizeof(prefix) - 1) != 0) return -1;
    p += sizeof(prefix) - 1;

    errno = 0;
    char *end = NULL;
    unsigned long long parsed = strtoull(p, &end, 0);
    if (errno != 0 || end == p) return -1;
    while (isspace((unsigned char)*end)) end++;
    if (*end != '\0') return -1;

    uint64_t valid_mask = (HA_MAX_NODES >= 64)
                        ? UINT64_MAX : ((UINT64_C(1) << HA_MAX_NODES) - 1);
    if (((uint64_t)parsed & ~valid_mask) != 0) return -1;
    *mask_out = (uint64_t)parsed;
    return 0;
}

int HA_test_link_is_partitioned(int local_index, int peer_index) {
    if (!test_faults_enabled()) return 0;
    if (local_index < 0 || local_index >= HA_MAX_NODES ||
        peer_index < 0 || peer_index >= HA_MAX_NODES ||
        local_index == peer_index) {
        return 0;
    }

    const char *path = getenv("RF_HA_TEST_PARTITION_FILE");
    uint64_t cut_mask = 0;
    if (read_cut_mask(path, &cut_mask) != 0) return 0;

    int local_side = (cut_mask & HA_NODE_MASK(local_index)) != 0;
    int peer_side = (cut_mask & HA_NODE_MASK(peer_index)) != 0;
    return local_side != peer_side;
}
