#define _GNU_SOURCE

#include <limits.h>
#include <stdio.h>

#include "../../src/app_routes.c"

static int check(const char *query, int expected_rc, int expected_timeout) {
    Request req = {0};
    req.query_string = (char *)query;
    int timeout = -1;
    int rc = requested_sync_timeout_ms(&req, &timeout);
    if (rc != expected_rc || timeout != expected_timeout) {
        fprintf(stderr,
                "timeout parse mismatch query=%s rc=%d/%d timeout=%d/%d\n",
                query ? query : "<absent>", rc, expected_rc,
                timeout, expected_timeout);
        return 1;
    }
    return 0;
}

int main(void) {
    if (check(NULL, 0, 0) ||
        check("partition=0", 0, 0) ||
        check("timeout_ms=1", 1, 1) ||
        check("x=1&timeout_ms=2147483647", 1, INT_MAX) ||
        check("timeout_ms=0", -1, 0) ||
        check("timeout_ms=", -1, 0) ||
        check("timeout_ms=abc", -1, 0) ||
        check("timeout_ms=5x", -1, 0) ||
        check("timeout_ms=2147483648", -1, 0) ||
        check("timeout_ms=18446744073709551616", -1, 0) ||
        check("x_timeout_ms=5", 0, 0)) {
        return 1;
    }

    puts("PASS explicit sync timeout is strictly typed and bounded");
    return 0;
}
