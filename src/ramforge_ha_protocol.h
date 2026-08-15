#ifndef AYDER_RAMFORGE_HA_PROTOCOL_H
#define AYDER_RAMFORGE_HA_PROTOCOL_H

#include "ramforge_ha_config.h"
#include <stdint.h>

int ha_candidate_log_is_up_to_date(uint64_t candidate_last_term,
                                   uint64_t candidate_last_index,
                                   uint64_t local_last_term,
                                   uint64_t local_last_index);
int ha_prev_log_matches(uint64_t prev_log_index, uint64_t prev_log_term,
                        uint64_t local_last_index, uint64_t local_prev_term);
uint64_t ha_reject_hint(uint64_t prev_log_index, uint64_t local_last_index);
uint64_t ha_next_index_after_reject(uint64_t current_next, uint64_t reject_hint);
uint64_t ha_follower_commit_target(uint64_t leader_commit_index,
                                   uint64_t matched_index);
int ha_vote_mark_once(uint64_t *seen_mask, int peer_index);

#endif
