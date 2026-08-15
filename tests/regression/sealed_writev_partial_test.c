#define _GNU_SOURCE

#include <errno.h>
#include <stddef.h>
#include <stdint.h>
#include <stdio.h>
#include <string.h>
#include <sys/types.h>
#include <sys/uio.h>

static ssize_t partial_writev(int fd, const struct iovec *iov, int iovcnt);

/* Compile the production helper into this translation unit so a deterministic
 * short-write shim can exercise its otherwise kernel-dependent retry path. */
#define writev partial_writev
#include "../../src/aof_batch.c"
#undef writev

static unsigned char captured[64];
static size_t captured_len;
static int write_calls;

static ssize_t partial_writev(int fd, const struct iovec *iov, int iovcnt) {
    (void)fd;
    write_calls++;

    if (write_calls == 1) {
        errno = EINTR;
        return -1;
    }

    size_t budget = 3;
    size_t copied = 0;
    for (int i = 0; i < iovcnt && budget != 0; i++) {
        size_t take = iov[i].iov_len < budget ? iov[i].iov_len : budget;
        memcpy(captured + captured_len, iov[i].iov_base, take);
        captured_len += take;
        copied += take;
        budget -= take;
    }
    return (ssize_t)copied;
}

int main(void) {
    char first[] = "abc";
    char second[] = "defg";
    char third[] = "hijkl";
    struct iovec iov[] = {
        {.iov_base = first, .iov_len = sizeof(first) - 1},
        {.iov_base = second, .iov_len = sizeof(second) - 1},
        {.iov_base = third, .iov_len = sizeof(third) - 1},
    };
    const char expected[] = "abcdefghijkl";

    if (sealed_writev_full(123, iov, 3, sizeof(expected) - 1) != 0) {
        perror("sealed_writev_full");
        return 1;
    }
    if (captured_len != sizeof(expected) - 1 ||
        memcmp(captured, expected, sizeof(expected) - 1) != 0) {
        fprintf(stderr, "partial writev reconstruction mismatch\n");
        return 1;
    }
    if (write_calls < 5) {
        fprintf(stderr, "partial writev path was not exercised\n");
        return 1;
    }

    puts("PASS sealed_writev_full retries EINTR and accounts for every partial byte");
    return 0;
}
