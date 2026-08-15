#ifndef AYDER_RAMFORGE_HA_STORE_H
#define AYDER_RAMFORGE_HA_STORE_H

#include <stddef.h>
#include <stdint.h>

typedef struct {
    uint64_t term;
    int32_t voted_for;
    uint64_t commit_index;
    uint64_t applied_index;
} ha_stable_state_t;

typedef int (*ha_store_replay_cb)(uint64_t index, uint64_t term,
                                  uint64_t timestamp_us, uint32_t type,
                                  const void *payload, uint32_t size,
                                  uint64_t file_offset, void *ctx);

typedef struct ha_store ha_store_t;

int ha_store_open(ha_store_t **out, const char *node_id,
                  uint32_t max_payload, ha_stable_state_t *state_out,
                  ha_store_replay_cb replay_cb, void *replay_ctx);
void ha_store_close(ha_store_t *store);

int ha_store_persist_state(ha_store_t *store, const ha_stable_state_t *state);
int ha_store_append_log(ha_store_t *store, uint64_t index, uint64_t term,
                        uint64_t timestamp_us, uint32_t type,
                        const void *payload, uint32_t size,
                        int sync_now, uint64_t *file_offset_out);
int ha_store_sync_log(ha_store_t *store);
int ha_store_truncate_log(ha_store_t *store, uint64_t file_offset);

const char *ha_store_state_path(const ha_store_t *store);
const char *ha_store_log_path(const ha_store_t *store);

#endif
