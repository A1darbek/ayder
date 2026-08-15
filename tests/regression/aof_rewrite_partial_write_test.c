#define _GNU_SOURCE

#include <errno.h>
#include <stddef.h>
#include <stdint.h>
#include <stdio.h>
#include <string.h>
#include <sys/types.h>

static ssize_t partial_write(int fd, const void *buffer, size_t size);

/* Compile the production rewrite writer into this translation unit so short
 * writes and EINTR are deterministic rather than filesystem-dependent. */
#define write partial_write
#include "../../src/aof_batch.c"
#undef write

static unsigned char captured[64];
static size_t captured_len;
static int write_calls;
static int return_zero;

static ssize_t partial_write(int fd, const void *buffer, size_t size) {
    (void)fd;
    write_calls++;
    if (return_zero) return 0;
    if (write_calls == 1) {
        errno = EINTR;
        return -1;
    }
    size_t take = size < 3 ? size : 3;
    memcpy(captured + captured_len, buffer, take);
    captured_len += take;
    return (ssize_t)take;
}

int main(void) {
    static const unsigned char expected[] = "rewrite-record-with-short-writes";
    if (aof_write_full(123, expected, sizeof(expected) - 1) != 0) {
        perror("aof_write_full");
        return 1;
    }
    if (captured_len != sizeof(expected) - 1 ||
        memcmp(captured, expected, sizeof(expected) - 1) != 0 ||
        write_calls < 3) {
        fputs("rewrite partial-write reconstruction mismatch\n", stderr);
        return 1;
    }

    return_zero = 1;
    errno = 0;
    if (aof_write_full(123, expected, 1) == 0 || errno != EIO) {
        fputs("rewrite zero-byte write was not rejected as EIO\n", stderr);
        return 1;
    }

    puts("PASS AOF rewrite retries EINTR, accounts for partial writes, and rejects zero progress");
    return 0;
}
