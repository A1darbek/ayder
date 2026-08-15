#include "ramforge_ha_protocol.h"

#include <stdio.h>

static int checks;
static int failures;

#define CHECK(expr) do { \
    checks++; \
    if (!(expr)) { \
        fprintf(stderr, "FAIL %s:%d: %s\n", __FILE__, __LINE__, #expr); \
        failures++; \
    } \
} while (0)

int main(void) {
    CHECK(ha_candidate_log_is_up_to_date(4, 1, 3, 100));
    CHECK(!ha_candidate_log_is_up_to_date(2, 100, 3, 1));
    CHECK(ha_candidate_log_is_up_to_date(3, 10, 3, 10));
    CHECK(ha_candidate_log_is_up_to_date(3, 11, 3, 10));
    CHECK(!ha_candidate_log_is_up_to_date(3, 9, 3, 10));

    CHECK(ha_prev_log_matches(0, 0, 0, 0));
    CHECK(!ha_prev_log_matches(0, 1, 10, 0));
    CHECK(ha_prev_log_matches(8, 4, 10, 4));
    CHECK(!ha_prev_log_matches(11, 4, 10, 4));
    CHECK(!ha_prev_log_matches(8, 5, 10, 4));

    CHECK(ha_reject_hint(10, 4) == 4);
    CHECK(ha_reject_hint(10, 12) == 9);
    CHECK(ha_next_index_after_reject(11, 4) == 5);
    CHECK(ha_next_index_after_reject(11, 99) == 10);
    CHECK(ha_next_index_after_reject(1, 0) == 1);

    /* A matched prefix must not commit an unmatched divergent local suffix. */
    CHECK(ha_follower_commit_target(4000, 2048) == 2048);
    CHECK(ha_follower_commit_target(1024, 2048) == 1024);
    CHECK(ha_follower_commit_target(0, 2048) == 0);

    uint64_t seen = 0;
    CHECK(ha_vote_mark_once(&seen, 1));
    CHECK(!ha_vote_mark_once(&seen, 1));
    CHECK(ha_vote_mark_once(&seen, 6));
    CHECK(!ha_vote_mark_once(&seen, -1));
    CHECK(!ha_vote_mark_once(&seen, HA_MAX_NODES));
    CHECK(seen == (HA_NODE_MASK(1) | HA_NODE_MASK(6)));

    if (failures) {
        fprintf(stderr, "ha protocol: %d/%d checks failed\n", failures, checks);
        return 1;
    }
    printf("ha protocol: %d checks passed\n", checks);
    return 0;
}
