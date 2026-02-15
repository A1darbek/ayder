// ramforge_ha_config.c - Configuration Loading and Management
#include "ramforge_ha_config.h"
#include <string.h>
#include <stdlib.h>
#include <stdio.h>
#include <time.h>
#include <unistd.h>

static inline uint64_t now_us(void) {
    struct timespec ts;
    clock_gettime(CLOCK_MONOTONIC, &ts);
    return (uint64_t)ts.tv_sec * 1000000ULL + ts.tv_nsec / 1000;
}

static void env_copy(char *dst, size_t cap, const char *name) {
    const char *e = getenv(name);
    if (!e || !*e) return;
    strncpy(dst, e, cap - 1);
    dst[cap - 1] = 0;
}

static int env_is_on(const char *name) {
    const char *e = getenv(name);
    return (e && *e && strcmp(e, "0") != 0);
}

// Parse node list from RF_HA_NODES environment variable
// Format: "node1:host1:port1:priority,node2:host2:port2:priority,..."
static int parse_node_list(ha_config_t *config) {
    const char *env = getenv("RF_HA_NODES");
    if (!env || !*env) {
        fprintf(stderr, "❌ RF_HA_NODES not set - HA disabled\n");
        return -1;
    }

    config->node_count = 0;
    const char *p = env;

    while (*p && config->node_count < HA_MAX_NODES) {
        ha_node_config_t *node = &config->nodes[config->node_count];

        // Parse node_id
        const char *start = p;
        while (*p && *p != ':' && *p != ',') p++;
        size_t len = p - start;
        if (len >= sizeof(node->node_id)) len = sizeof(node->node_id) - 1;
        memcpy(node->node_id, start, len);
        node->node_id[len] = 0;

        if (*p != ':') break;
        p++;

        // Parse host:port for cluster communication
        // Parse HOST for cluster communication
        start = p;
        while (*p && *p != ':' && *p != ',') p++;
        size_t host_len = (size_t) (p - start);
        if (host_len >= sizeof(node->advertise_addr)) host_len = sizeof(node->advertise_addr) - 1;
        memcpy(node->advertise_addr, start, host_len);
        node->advertise_addr[host_len] = 0;

        if (*p != ':') break;
        p++;

        // Parse cluster port
        char port_buf[16];
        start = p;
        int port_len = 0;
        while (*p && *p != ':' && *p != ',' && port_len < 15) {
            port_buf[port_len++] = *p++;
        }
        port_buf[port_len] = 0;

        // Complete advertise_addr = "HOST:PORT"
        strncat(node->advertise_addr, ":", sizeof(node->advertise_addr)-strlen(node->advertise_addr)-1);
        strncat(node->advertise_addr, port_buf, sizeof(node->advertise_addr)-strlen(node->advertise_addr)-1);

        // Parse priority (optional)
        node->priority = 10; // Default priority
        if (*p == ':') {
            p++;
            node->priority = atoi(p);
            while (*p && *p != ',') p++;
        }

        // All nodes are voters by default
        node->is_voter = 1;

        // FIX: Generate HTTP address using cluster_port + 1 (data plane port)
        // Cluster port is 7000, 8000, 9000
        // Data plane HTTP is 7001, 8001, 9001
        int cluster_port = atoi(port_buf);

        char host_only[256];
        size_t cut = strcspn(node->advertise_addr, ":");
        if (cut >= sizeof(host_only)) cut = sizeof(host_only) - 1;
        memcpy(host_only, node->advertise_addr, cut);
        host_only[cut] = 0;

        // FIX: HTTP runs on cluster_port + 1 (the actual data plane port)
        snprintf(node->http_addr, sizeof(node->http_addr),
                 "http://%s:%d", host_only, cluster_port + 1);

        printf("   Node[%d] advertise_addr=%s http_addr=%s priority=%d voter=%d\n",
               (int)config->node_count,
               node->advertise_addr, node->http_addr, node->priority, node->is_voter);

        if (*p == ',') p++;
        config->node_count++;
    }

    printf("✅ Parsed %d HA nodes\n", config->node_count);
    return 0;
}
// Find local node index based on RF_HA_NODE_ID
static int find_local_node(ha_config_t *config) {
    char local_buf[256];
    const char *local_id = getenv("RF_HA_NODE_ID");
    if (!local_id || !*local_id) {
        if (gethostname(local_buf, sizeof(local_buf)) != 0) {
            fprintf(stderr, "❌ RF_HA_NODE_ID not set and hostname unavailable\n");
            return -1;
        }
        local_buf[sizeof(local_buf) - 1] = 0;
        local_id = local_buf;
    }

    for (int i = 0; i < config->node_count; i++) {
        if (strcmp(config->nodes[i].node_id, local_id) == 0) {
            config->local_node_index = i;
            printf("✅ Local node: %s (index %d)\n", local_id, i);
            return 0;
        }
    }

    fprintf(stderr, "❌ Local node ID '%s' not found in cluster\n", local_id);
    return -1;
}

int HA_init_from_env(ha_config_t *config, ha_runtime_t *runtime) {
    memset(config, 0, sizeof(*config));
    memset(runtime, 0, sizeof(*runtime));

    // Parse node list
    if (parse_node_list(config) != 0) return -1;

    // Validate cluster size
    if (config->node_count < 1) {
        fprintf(stderr, "❌ Need at least 1 node\n");
        return -1;
    }

    if (config->node_count > 1 && config->node_count % 2 == 0) {
        fprintf(stderr, "⚠️  Even number of nodes (%d) - consider adding one for proper quorum\n",
                config->node_count);
    }

    // Find local node
    if (find_local_node(config) != 0) return -1;

    // Load timing configuration
    const char *env;


    config->mtls_enabled = env_is_on("RF_HA_MTLS");
    config->mtls_verify_peer = config->mtls_enabled ? 1 : 0;
    config->mtls_require_client_cert = config->mtls_enabled ? 1 : 0;
    config->mtls_verify_mode = 2; // either host/ip OR node_id by default

    if ((env = getenv("RF_HA_MTLS_VERIFY_PEER"))) config->mtls_verify_peer = atoi(env);
    if ((env = getenv("RF_HA_MTLS_REQUIRE_CLIENT_CERT"))) config->mtls_require_client_cert = atoi(env);
    if ((env = getenv("RF_HA_MTLS_VERIFY_MODE"))) config->mtls_verify_mode = atoi(env);

    env_copy(config->mtls_ca_file,   sizeof(config->mtls_ca_file),   "RF_HA_MTLS_CA_FILE");
    env_copy(config->mtls_ca_path,   sizeof(config->mtls_ca_path),   "RF_HA_MTLS_CA_PATH");
    env_copy(config->mtls_cert_file, sizeof(config->mtls_cert_file), "RF_HA_MTLS_CERT_FILE");
    env_copy(config->mtls_key_file,  sizeof(config->mtls_key_file),  "RF_HA_MTLS_KEY_FILE");
    env_copy(config->mtls_key_pass,  sizeof(config->mtls_key_pass),  "RF_HA_MTLS_KEY_PASS");

    config->ha_max_msg_bytes = 2u * 1024u * 1024u;
    if ((env = getenv("RF_HA_MAX_MSG_BYTES"))) config->ha_max_msg_bytes = (uint32_t)strtoul(env, NULL, 10);
    if (config->ha_max_msg_bytes < 64 * 1024) config->ha_max_msg_bytes = 64 * 1024;
    config->heartbeat_interval_ms = HA_HEARTBEAT_MS;
    if ((env = getenv("RF_HA_HEARTBEAT_MS"))) {
        config->heartbeat_interval_ms = atoi(env);
    }

    config->election_timeout_ms = HA_ELECTION_TIMEOUT_MS;
    if ((env = getenv("RF_HA_ELECTION_TIMEOUT_MS"))) {
        config->election_timeout_ms = atoi(env);
    }

    config->replication_timeout_ms = 1000;
    if ((env = getenv("RF_HA_REPLICATION_TIMEOUT_MS"))) {
        config->replication_timeout_ms = atoi(env);
    }

    config->max_replication_lag_ms = HA_MAX_LAG_MS;
    if ((env = getenv("RF_HA_MAX_LAG_MS"))) {
        config->max_replication_lag_ms = atoi(env);
    }

    // Load replication configuration
    config->replication_batch_size = HA_REPLICATION_BATCH;
    if ((env = getenv("RF_HA_REPLICATION_BATCH"))) {
        config->replication_batch_size = atoi(env);
    }

    config->sync_mode = 0; // async by default
    if ((env = getenv("RF_HA_SYNC_MODE"))) {
        config->sync_mode = atoi(env);
    }

    config->write_concern = (config->node_count / 2) + 1; // majority
    if ((env = getenv("RF_HA_WRITE_CONCERN"))) {
        config->write_concern = atoi(env);
    }

    // Load redirection configuration
    config->redirect_status = 307;
    if ((env = getenv("RF_HA_REDIRECT_STATUS"))) {
        config->redirect_status = atoi(env);
    }

    config->retry_after_sec = 1;
    if ((env = getenv("RF_HA_RETRY_AFTER"))) {
        config->retry_after_sec = atoi(env);
    }



    // Initialize runtime state
    atomic_store(&runtime->role, HA_ROLE_FOLLOWER);
    atomic_store(&runtime->state, HA_STATE_INITIALIZING);
    atomic_store(&runtime->term, 0);
    atomic_store(&runtime->commit_index, 0);
    atomic_store(&runtime->applied_index, 0);
    atomic_store(&runtime->current_leader, -1);
    atomic_store(&runtime->last_heartbeat_us, now_us());
    atomic_store(&runtime->voted_for, -1);

    for (int i = 0; i < HA_MAX_NODES; i++) {
        atomic_store(&runtime->next_index[i], 1);
        atomic_store(&runtime->match_index[i], 0);
        atomic_store(&runtime->last_ack_us[i], 0);
    }

    if (pthread_rwlock_init(&runtime->state_lock, NULL) != 0) {
        fprintf(stderr, "❌ Failed to initialize state lock\n");
        return -1;
    }

    printf("✅ HA configuration loaded:\n");
    printf("   Node count: %d\n", config->node_count);
    printf("   Local node: %s\n", config->nodes[config->local_node_index].node_id);
    printf("   Heartbeat: %u ms\n", config->heartbeat_interval_ms);
    printf("   Election timeout: %u ms\n", config->election_timeout_ms);
    printf("   Write concern: %d/%d nodes\n", config->write_concern, config->node_count);
    printf("   Sync mode: %s\n",
           config->sync_mode == 0 ? "async" :
           config->sync_mode == 1 ? "sync-majority" : "sync-all");

    return 0;
}

ha_role_t HA_get_role(const ha_runtime_t *runtime) {
    return atomic_load(&runtime->role);
}

int HA_is_leader(const ha_runtime_t *runtime) {
    return atomic_load(&runtime->role) == HA_ROLE_LEADER;
}

int HA_is_ready(const ha_runtime_t *runtime) {
    ha_state_t state = atomic_load(&runtime->state);
    return state == HA_STATE_READY || state == HA_STATE_DEGRADED;
}

const char* HA_get_leader_url(const ha_config_t *config) {
    return config->leader_url;
}

uint64_t HA_get_replication_lag_ms(const ha_runtime_t *runtime) {
    uint64_t commit = atomic_load(&runtime->commit_index);
    uint64_t applied = atomic_load(&runtime->applied_index);

    if (commit <= applied) return 0;

    // Rough estimate: assume 1ms per entry lag
    return (commit - applied);
}
