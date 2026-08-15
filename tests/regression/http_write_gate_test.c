#define _GNU_SOURCE

#include <stdio.h>

#include "../../src/http_server.c"

int main(void) {
    if (!is_write_path("POST", "/broker/commit") ||
        !is_write_path("POST", "/broker/commit?timeout_ms=3000") ||
        !is_write_path("POST", "/broker/topics/orders/produce") ||
        !is_write_path("POST", "/broker/topics/orders/produce?timeout_ms=3000") ||
        !is_write_path("POST", "/broker/topics/orders/produce-ndjson") ||
        !is_write_path("POST", "/kv/ns/key") ||
        !is_write_path("POST", "/kv/ns/key?timeout_ms=3000") ||
        !is_write_path("DELETE", "/kv/ns/key") ||
        is_write_path("GET", "/broker/commit") ||
        is_write_path("POST", "/broker/topics/orders/producer") ||
        is_write_path("POST", "/broker/query") ||
        is_write_path("POST", "/unrelated") ||
        !is_linearizable_read_path("GET", "/kv/ns/key") ||
        !is_linearizable_read_path("GET", "/kv/ns/key/meta?timeout_ms=3") ||
        is_linearizable_read_path("POST", "/kv/ns/key") ||
        is_linearizable_read_path("GET", "/health")) {
        fprintf(stderr, "HTTP write classification regression\n");
        return 1;
    }

    puts("PASS query-bearing mutations are write-gated and KV reads are leader-gated");
    return 0;
}
