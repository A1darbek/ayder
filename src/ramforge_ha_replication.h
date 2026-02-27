// ramforge_ha_replication.h - Zero-Copy Replication Engine
#ifndef RAMFORGE_HA_REPLICATION_H
#define RAMFORGE_HA_REPLICATION_H

#include "ramforge_ha_config.h"
#include <stdint.h>
#include <stddef.h>

typedef struct {
    uint64_t index;
    uint64_t term;
    uint64_t timestamp_us;
    uint32_t type;
    uint32_t size;
    uint32_t crc;
    uint32_t reserved;
    uint8_t data[];
} __attribute__((packed)) ha_log_entry_t;

typedef enum {
    HA_MSG_HEARTBEAT = 1,
    HA_MSG_APPEND_ENTRIES = 2,
    HA_MSG_APPEND_RESPONSE = 3,
    HA_MSG_VOTE_REQUEST = 4,
    HA_MSG_VOTE_RESPONSE = 5,
    HA_MSG_INSTALL_SNAPSHOT = 6
} ha_msg_type_t;

typedef struct {
    uint32_t magic;
    uint32_t type;
    uint64_t term;
    int32_t from_node;
    int32_t to_node;
    uint32_t payload_size;
    uint32_t crc;
} __attribute__((packed)) ha_msg_header_t;

typedef struct {
    uint64_t commit_index;
    uint64_t prev_log_index;
    uint64_t prev_log_term;
    uint32_t entry_count;
    uint32_t reserved;
} __attribute__((packed)) ha_append_entries_t;

typedef struct {
    uint64_t match_index;
    int32_t success;
    uint32_t reserved;
} __attribute__((packed)) ha_append_response_t;

typedef struct {
    uint64_t last_log_index;
    uint64_t last_log_term;
    int32_t priority;
    uint32_t reserved;
} __attribute__((packed)) ha_vote_request_t;

typedef struct {
    int32_t vote_granted;
    uint32_t reserved;
} __attribute__((packed)) ha_vote_response_t;

#define HA_SNAPSHOT_FLAG_START 0x1u
#define HA_SNAPSHOT_FLAG_END   0x2u

typedef struct {
    uint64_t snapshot_id;
    uint64_t last_included_index;
    uint64_t last_included_term;
    uint64_t leader_commit;
    uint32_t flags;          // HA_SNAPSHOT_FLAG_*
    uint32_t record_count;   // [rec_id:u32][size:u32][payload]
    uint32_t payload_bytes;  // bytes after this header
    uint32_t reserved;
} __attribute__((packed)) ha_install_snapshot_t;

int HA_replication_init(ha_config_t *config, ha_runtime_t *runtime);
void HA_replication_bind(ha_config_t *config, ha_runtime_t *runtime);
void HA_replication_shutdown(void);
void HA_request_catchup(void);
uint64_t HA_append_local_entry(uint32_t type, const void *data, uint32_t size);

int HA_replicate_to_followers(void);
int HA_start_election(void);
int send_message(int peer_index, ha_msg_header_t *header,
                 const void *payload, size_t payload_size);
int HA_leader_on_ack(int peer_index, uint64_t match_index);
int HA_wait_for_replication(uint64_t index, uint32_t timeout_ms);
int HA_forward_append_and_replicate(uint32_t rec_type, const void *data, size_t size);
uint64_t HA_repl_kick_read(void);
int HA_apply_committed_entries(void);
void HA_leader_on_nack(int peer_index, uint64_t follower_index);

void HA_get_log_stats(uint64_t *last_index, uint64_t *committed, uint64_t *applied);
int HA_has_fresh_quorum(void);

/* Dynamic membership helpers */
int HA_get_membership_view(ha_membership_view_t *out);
int HA_apply_cfg_change_entry(const void *data, uint32_t size);
int HA_apply_membership_snapshot(const ha_membership_record_t *rec);
int HA_is_peer_active(int peer_index);
int HA_local_is_voter(void);

#endif // RAMFORGE_HA_REPLICATION_H


