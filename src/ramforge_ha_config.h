// ramforge_ha_config.h - Production-Ready High Availability Configuration
#ifndef RAMFORGE_HA_CONFIG_H
#define RAMFORGE_HA_CONFIG_H

#include <stdint.h>
#include <stdatomic.h>
#include <pthread.h>

// ═══════════════════════════════════════════════════════════════════════════════
// HA Node Roles and States
// ═══════════════════════════════════════════════════════════════════════════════

typedef enum {
    HA_ROLE_LEADER = 0,      // Active leader accepting writes
    HA_ROLE_FOLLOWER = 1,    // Follower replicating from leader
    HA_ROLE_CANDIDATE = 2,   // Candidate during election
    HA_ROLE_LEARNER = 3      // Read-only learner (not in voting quorum)
} ha_role_t;

typedef enum {
    HA_STATE_INITIALIZING = 0,
    HA_STATE_READY = 1,
    HA_STATE_SYNCING = 2,
    HA_STATE_DEGRADED = 3,
    HA_STATE_FAILED = 4
} ha_state_t;

// ═══════════════════════════════════════════════════════════════════════════════
// HA Configuration
// ═══════════════════════════════════════════════════════════════════════════════

#define HA_MAX_NODES 7              // Max cluster size (must be odd for quorum)
#define HA_HEARTBEAT_MS 100         // Fast heartbeat for sub-second detection
#define HA_ELECTION_TIMEOUT_MS 1000 // Fast election timeout
#define HA_REPLICATION_BATCH 256    // Batch size for replication
#define HA_MAX_LAG_MS 5000          // Max acceptable replication lag

typedef struct {
    char node_id[64];               // Unique node identifier
    char advertise_addr[256];       // IP:port for cluster communication
    char http_addr[256];            // IP:port for client traffic
    int priority;                   // Election priority (higher = preferred)
    int is_voter;                   // 1 if participates in quorum, 0 for learner
} ha_node_config_t;

typedef struct {
    // Cluster configuration
    int node_count;
    ha_node_config_t nodes[HA_MAX_NODES];
    int local_node_index;           // Index of this node in nodes[]

    // Timing configuration
    uint32_t heartbeat_interval_ms;
    uint32_t election_timeout_ms;
    uint32_t replication_timeout_ms;
    uint32_t max_replication_lag_ms;

    // Replication configuration
    size_t replication_batch_size;
    int sync_mode;                  // 0=async, 1=sync to majority, 2=sync to all
    int write_concern;              // Number of nodes that must ack writes

    // Leader redirection
    char leader_url[256];           // Current leader URL for redirects
    int redirect_status;            // 307 or 308
    int retry_after_sec;            // Retry-After header value

    // --- mTLS (cluster / advertise_addr only) ---
    int  mtls_enabled;                 // RF_HA_MTLS=1
    int  mtls_verify_peer;             // default 1
    int  mtls_require_client_cert;     // default 1 (server side)
    int  mtls_verify_mode;             // 0=host/ip, 1=node_id, 2=either

    char mtls_ca_file[256];            // RF_HA_MTLS_CA_FILE
    char mtls_ca_path[256];            // RF_HA_MTLS_CA_PATH (optional)
    char mtls_cert_file[256];          // RF_HA_MTLS_CERT_FILE
    char mtls_key_file[256];           // RF_HA_MTLS_KEY_FILE
    char mtls_key_pass[128];           // RF_HA_MTLS_KEY_PASS (optional)

    uint32_t ha_max_msg_bytes;         // RF_HA_MAX_MSG_BYTES (default 2MB)
} ha_config_t;

// ═══════════════════════════════════════════════════════════════════════════════
// HA Runtime State
// ═══════════════════════════════════════════════════════════════════════════════

typedef struct {
    // Role and state
    _Atomic(ha_role_t) role;
    _Atomic(ha_state_t) state;
    _Atomic uint64_t term;          // Current election term
    _Atomic uint64_t commit_index;  // Highest committed log index
    _Atomic uint64_t applied_index; // Highest applied log index

    // Leader tracking
    _Atomic int current_leader;     // Index of current leader (-1 if unknown)
    _Atomic uint64_t last_heartbeat_us; // Last heartbeat from leader
    _Atomic int voted_for;          // Node we voted for in current term (-1 if none)

    // Replication tracking (for leader)
    _Atomic uint64_t next_index[HA_MAX_NODES];  // Next log entry to send to each follower
    _Atomic uint64_t match_index[HA_MAX_NODES]; // Highest known replicated entry per follower
    _Atomic uint64_t last_ack_us[HA_MAX_NODES]; // Last ack time from each node

    // Statistics
    _Atomic uint64_t elections_started;
    _Atomic uint64_t elections_won;
    _Atomic uint64_t heartbeats_sent;
    _Atomic uint64_t heartbeats_received;
    _Atomic uint64_t writes_replicated;
    _Atomic uint64_t writes_failed;

    // Thread safety
    pthread_rwlock_t state_lock;

    _Atomic int votes_granted;
} ha_runtime_t;

// ═══════════════════════════════════════════════════════════════════════════════
// Public API
// ═══════════════════════════════════════════════════════════════════════════════

// Initialize HA system from environment variables
int HA_init_from_env(ha_config_t *config, ha_runtime_t *runtime);

// Get current role
ha_role_t HA_get_role(const ha_runtime_t *runtime);

// Check if node is leader
int HA_is_leader(const ha_runtime_t *runtime);

// Check if node is ready to serve requests
int HA_is_ready(const ha_runtime_t *runtime);

// Get leader URL for redirection
const char* HA_get_leader_url(const ha_config_t *config);

// Get current replication lag in milliseconds
uint64_t HA_get_replication_lag_ms(const ha_runtime_t *runtime);



#endif // RAMFORGE_HA_CONFIG_H
