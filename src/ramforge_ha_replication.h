// ramforge_ha_replication.h - Zero-Copy Replication Engine
#ifndef RAMFORGE_HA_REPLICATION_H
#define RAMFORGE_HA_REPLICATION_H

#include "ramforge_ha_config.h"
#include <stdint.h>
#include <stddef.h>

// ═══════════════════════════════════════════════════════════════════════════════
// Replication Log Entry
// ═══════════════════════════════════════════════════════════════════════════════

typedef struct {
    uint64_t index;                 // Log index
    uint64_t term;                  // Election term when entry was created
    uint64_t timestamp_us;          // Creation timestamp
    uint32_t type;                  // Entry type (sealed record type)
    uint32_t size;                  // Payload size
    uint32_t crc;                   // CRC32C checksum
    uint32_t reserved;
    uint8_t data[];                 // Variable-length payload
} __attribute__((packed)) ha_log_entry_t;

// ═══════════════════════════════════════════════════════════════════════════════
// Replication Messages (Zero-Copy Protocol)
// ═══════════════════════════════════════════════════════════════════════════════

typedef enum {
    HA_MSG_HEARTBEAT = 1,           // Leader heartbeat
    HA_MSG_APPEND_ENTRIES = 2,      // Replicate log entries
    HA_MSG_APPEND_RESPONSE = 3,     // Acknowledge replication
    HA_MSG_VOTE_REQUEST = 4,        // Request votes for election
    HA_MSG_VOTE_RESPONSE = 5,       // Grant or deny vote
    HA_MSG_INSTALL_SNAPSHOT = 6     // Transfer snapshot
} ha_msg_type_t;

typedef struct {
    uint32_t magic;                 // 0xHA01
    uint32_t type;                  // ha_msg_type_t
    uint64_t term;                  // Sender's current term
    int32_t from_node;              // Sender node index
    int32_t to_node;                // Recipient node index (-1 for broadcast)
    uint32_t payload_size;          // Size of message-specific payload
    uint32_t crc;                   // Header CRC
} __attribute__((packed)) ha_msg_header_t;

typedef struct {
    uint64_t commit_index;          // Leader's commit index
    uint64_t prev_log_index;        // Index of log entry immediately preceding new ones
    uint64_t prev_log_term;         // Term of prev_log_index entry
    uint32_t entry_count;           // Number of entries in this message
    uint32_t reserved;
    // Followed by entry_count ha_log_entry_t records
} __attribute__((packed)) ha_append_entries_t;

typedef struct {
    uint64_t match_index;           // Highest log index follower has
    int32_t success;                // 1 if follower contained entry matching prev_log_index/term
    uint32_t reserved;
} __attribute__((packed)) ha_append_response_t;

typedef struct {
    uint64_t last_log_index;        // Index of candidate's last log entry
    uint64_t last_log_term;         // Term of candidate's last log entry
    int32_t priority;               // Candidate's priority
    uint32_t reserved;
} __attribute__((packed)) ha_vote_request_t;

typedef struct {
    int32_t vote_granted;           // 1 if vote granted, 0 otherwise
    uint32_t reserved;
} __attribute__((packed)) ha_vote_response_t;

// ═══════════════════════════════════════════════════════════════════════════════
// Replication Engine API
// ═══════════════════════════════════════════════════════════════════════════════

// Initialize replication engine
int HA_replication_init(ha_config_t *config, ha_runtime_t *runtime);

// Shutdown replication engine
void HA_replication_shutdown(void);
void HA_request_catchup(void);
// Append entry to local log (called by leader for new writes)
uint64_t HA_append_local_entry(uint32_t type, const void *data, uint32_t size);

// Replicate entries to followers (async, batched)
int HA_replicate_to_followers(void);
int HA_start_election(void);
int send_message(int peer_index, ha_msg_header_t *header,
                 const void *payload, size_t payload_size);
int HA_leader_on_ack(int peer_index, uint64_t match_index);
// Wait for write concern to be satisfied (returns when enough replicas ack)
int HA_wait_for_replication(uint64_t index, uint32_t timeout_ms);
int HA_forward_append_and_replicate(uint32_t rec_type, const void *data, size_t size);
uint64_t HA_repl_kick_read(void);
// Apply committed entries to state machine
int HA_apply_committed_entries(void);
void HA_leader_on_nack(int peer_index, uint64_t follower_index);

// Get current log stats
void HA_get_log_stats(uint64_t *last_index, uint64_t *committed, uint64_t *applied);

#endif // RAMFORGE_HA_REPLICATION_H
