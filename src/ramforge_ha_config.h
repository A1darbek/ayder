// ramforge_ha_config.h - Production-Ready High Availability Configuration
#ifndef RAMFORGE_HA_CONFIG_H
#define RAMFORGE_HA_CONFIG_H

#include <stdint.h>
#include <stdatomic.h>
#include <pthread.h>

typedef enum {
    HA_ROLE_LEADER = 0,
    HA_ROLE_FOLLOWER = 1,
    HA_ROLE_CANDIDATE = 2,
    HA_ROLE_LEARNER = 3
} ha_role_t;

typedef enum {
    HA_STATE_INITIALIZING = 0,
    HA_STATE_READY = 1,
    HA_STATE_SYNCING = 2,
    HA_STATE_DEGRADED = 3,
    HA_STATE_FAILED = 4
} ha_state_t;

#define HA_MAX_NODES 7
#define HA_HEARTBEAT_MS 100
#define HA_ELECTION_TIMEOUT_MS 1000
#define HA_REPLICATION_BATCH 256
#define HA_MAX_LAG_MS 5000
#define HA_NODE_MASK(i) (1ULL << (uint64_t)(i))

#define HA_LOG_CFG_CHANGE 0x48414346u /* HACF */
#define AOF_REC_HA_MEMBERSHIP 0x48414D31u /* HAM1 */

typedef enum {
    HA_CFG_OP_ADD_LEARNER = 1,
    HA_CFG_OP_PROMOTE_VOTER_JOINT_BEGIN = 2,
    HA_CFG_OP_PROMOTE_VOTER_FINALIZE = 3,
    HA_CFG_OP_REMOVE_MEMBER_JOINT_BEGIN = 4,
    HA_CFG_OP_REMOVE_MEMBER_FINALIZE = 5
} ha_cfg_change_op_t;

typedef struct {
    char node_id[64];
    char advertise_addr[256];
    char http_addr[256];
    int priority;
    int is_voter;
} ha_node_config_t;

typedef struct {
    int node_count;
    ha_node_config_t nodes[HA_MAX_NODES];
    int local_node_index;

    uint32_t heartbeat_interval_ms;
    uint32_t election_timeout_ms;
    uint32_t replication_timeout_ms;
    uint32_t max_replication_lag_ms;

    size_t replication_batch_size;
    int sync_mode;
    int write_concern;

    char leader_url[256];
    int redirect_status;
    int retry_after_sec;

    int  mtls_enabled;
    int  mtls_verify_peer;
    int  mtls_require_client_cert;
    int  mtls_verify_mode;

    char mtls_ca_file[256];
    char mtls_ca_path[256];
    char mtls_cert_file[256];
    char mtls_key_file[256];
    char mtls_key_pass[128];

    uint32_t ha_max_msg_bytes;
} ha_config_t;

typedef struct {
    uint32_t op;
    uint32_t node_index;
    uint64_t expected_epoch;
    uint64_t voters_old_mask;
    uint64_t voters_new_mask;
    uint64_t learners_mask;
    ha_node_config_t member;
} __attribute__((packed)) ha_cfg_change_t;

typedef struct {
    uint64_t epoch;
    uint64_t voters_old_mask;
    uint64_t voters_new_mask;
    uint64_t learners_mask;
    uint32_t joint_active;
    uint32_t node_count;
    ha_node_config_t nodes[HA_MAX_NODES];
} __attribute__((packed)) ha_membership_record_t;

typedef ha_membership_record_t ha_membership_view_t;

typedef struct {
    _Atomic(ha_role_t) role;
    _Atomic(ha_state_t) state;
    _Atomic uint64_t term;
    _Atomic uint64_t commit_index;
    _Atomic uint64_t applied_index;

    _Atomic int current_leader;
    _Atomic uint64_t last_heartbeat_us;
    _Atomic int voted_for;

    _Atomic uint64_t voters_old_mask;
    _Atomic uint64_t voters_new_mask;
    _Atomic uint64_t learners_mask;
    _Atomic int joint_active;
    _Atomic uint64_t config_epoch;
    _Atomic int cfg_change_inflight;
    pthread_rwlock_t membership_lock;

    _Atomic uint64_t next_index[HA_MAX_NODES];
    _Atomic uint64_t match_index[HA_MAX_NODES];
    _Atomic uint64_t last_ack_us[HA_MAX_NODES];

    _Atomic uint64_t elections_started;
    _Atomic uint64_t elections_won;
    _Atomic uint64_t heartbeats_sent;
    _Atomic uint64_t heartbeats_received;
    _Atomic uint64_t writes_replicated;
    _Atomic uint64_t writes_failed;
    _Atomic uint64_t snapshots_sent;
    _Atomic uint64_t snapshots_received;
    _Atomic uint64_t snapshot_bytes_sent;
    _Atomic uint64_t snapshot_bytes_received;
    _Atomic uint64_t snapshot_failures;

    /* Quorum stability guards (startup/reconnect hysteresis). */
    _Atomic int bootstrap_gate_active;
    _Atomic int bootstrap_quorum_confirmed;
    _Atomic uint64_t bootstrap_gate_start_us;
    _Atomic uint64_t last_quorum_ok_us;
    _Atomic uint32_t quorum_miss_streak;

    pthread_rwlock_t state_lock;

    _Atomic int votes_granted;
} ha_runtime_t;

int HA_init_from_env(ha_config_t *config, ha_runtime_t *runtime);
ha_role_t HA_get_role(const ha_runtime_t *runtime);
int HA_is_leader(const ha_runtime_t *runtime);
int HA_is_ready(const ha_runtime_t *runtime);
const char* HA_get_leader_url(const ha_config_t *config);
uint64_t HA_get_replication_lag_ms(const ha_runtime_t *runtime);

#endif // RAMFORGE_HA_CONFIG_H

