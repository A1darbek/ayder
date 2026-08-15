#define _GNU_SOURCE
#include "ramforge_ha_fault_injection.h"

#include <fcntl.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

static int checks;
static int failures;

#define CHECK(expr) do { \
    checks++; \
    if (!(expr)) { \
        fprintf(stderr, "FAIL %s:%d: %s\n", __FILE__, __LINE__, #expr); \
        failures++; \
    } \
} while (0)

static int replace_contents(const char *path, const char *contents) {
    char tmp[512];
    int n = snprintf(tmp, sizeof(tmp), "%s.tmp", path);
    if (n < 0 || (size_t)n >= sizeof(tmp)) return -1;
    int fd = open(tmp, O_WRONLY | O_CREAT | O_TRUNC | O_CLOEXEC, 0600);
    if (fd < 0) return -1;
    size_t len = strlen(contents);
    ssize_t wr = write(fd, contents, len);
    int rc = (wr == (ssize_t)len && fsync(fd) == 0 && close(fd) == 0 &&
              rename(tmp, path) == 0) ? 0 : -1;
    if (rc != 0) {
        (void)close(fd);
        (void)unlink(tmp);
    }
    return rc;
}

int main(void) {
    char dir[] = "/tmp/ayder-ha-fault-test.XXXXXX";
    CHECK(mkdtemp(dir) != NULL);
    char path[512];
    CHECK(snprintf(path, sizeof(path), "%s/control", dir) > 0);
    CHECK(setenv("RF_HA_TEST_PARTITION_FILE", path, 1) == 0);

    unsetenv("RF_HA_TEST_FAULTS");
    CHECK(replace_contents(path, "cut_mask=0x4\n") == 0);
    CHECK(!HA_test_link_is_partitioned(0, 2));

    CHECK(setenv("RF_HA_TEST_FAULTS", "1", 1) == 0);
    CHECK(HA_test_link_is_partitioned(0, 2));
    CHECK(HA_test_link_is_partitioned(2, 6));
    CHECK(!HA_test_link_is_partitioned(0, 1));
    CHECK(!HA_test_link_is_partitioned(2, 2));

    CHECK(replace_contents(path, "cut_mask=0x0\n") == 0);
    CHECK(!HA_test_link_is_partitioned(0, 2));
    CHECK(replace_contents(path, "cut_mask=not-a-mask\n") == 0);
    CHECK(!HA_test_link_is_partitioned(0, 2));
    CHECK(replace_contents(path, "cut_mask=0x80\n") == 0);
    CHECK(!HA_test_link_is_partitioned(0, 2));

    CHECK(unlink(path) == 0);
    CHECK(!HA_test_link_is_partitioned(0, 2));
    CHECK(rmdir(dir) == 0);

    if (failures) {
        fprintf(stderr, "ha fault injection: %d/%d checks failed\n", failures, checks);
        return 1;
    }
    printf("ha fault injection: %d checks passed\n", checks);
    return 0;
}
