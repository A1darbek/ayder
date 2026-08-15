#include "ramforge_ha_protocol.h"

#include "ramforge_ha_config.h"

int ha_candidate_log_is_up_to_date(uint64_t candidate_last_term,
                                   uint64_t candidate_last_index,
                                   uint64_t local_last_term,
                                   uint64_t local_last_index) {
    if (candidate_last_term != local_last_term)
        return candidate_last_term > local_last_term;
    return candidate_last_index >= local_last_index;
}

int ha_prev_log_matches(uint64_t prev_log_index, uint64_t prev_log_term,
                        uint64_t local_last_index, uint64_t local_prev_term) {
    if (prev_log_index == 0) return prev_log_term == 0;
    if (prev_log_index > local_last_index) return 0;
    return local_prev_term == prev_log_term;
}

uint64_t ha_reject_hint(uint64_t prev_log_index, uint64_t local_last_index) {
    if (prev_log_index == 0) return 0;
    if (local_last_index < prev_log_index) return local_last_index;
    return prev_log_index - 1;
}

uint64_t ha_next_index_after_reject(uint64_t current_next, uint64_t reject_hint) {
    if (current_next <= 1) return 1;
    uint64_t hinted = reject_hint == UINT64_MAX ? UINT64_MAX : reject_hint + 1;
    if (hinted >= current_next) return current_next - 1;
    return hinted > 0 ? hinted : 1;
}

uint64_t ha_follower_commit_target(uint64_t leader_commit_index,
                                   uint64_t matched_index) {
    /* A successful AppendEntries proves only the prefix through the request's
     * last matched entry.  A follower may have an uncommitted divergent suffix;
     * using its local last index here would incorrectly commit that suffix
     * before the leader has replaced it. */
    return leader_commit_index < matched_index
         ? leader_commit_index : matched_index;
}

int ha_vote_mark_once(uint64_t *seen_mask, int peer_index) {
    if (!seen_mask || peer_index < 0 || peer_index >= HA_MAX_NODES) return 0;
    uint64_t bit = HA_NODE_MASK(peer_index);
    if (*seen_mask & bit) return 0;
    *seen_mask |= bit;
    return 1;
}
