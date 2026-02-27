// ramforge_ha_config.c - Configuration Loading and Management
#include "ramforge_ha_config.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <unistd.h>

#ifndef LOGI
#define LOGI(fmt, ...) fprintf(stderr, "[HA-CONFIG] " fmt "\n", ##__VA_ARGS__)
#define LOGW(fmt, ...) fprintf(stderr, "[HA-CONFIG][WARN] " fmt "\n", ##__VA_ARGS__)
#define LOGE(fmt, ...) fprintf(stderr, "[HA-CONFIG][ERR] " fmt "\n", ##__VA_ARGS__)
#endif

static inline uint64_t now_us(void) {
  struct timespec ts;
  clock_gettime(CLOCK_MONOTONIC, &ts);
  return (uint64_t)ts.tv_sec * 1000000ULL + ts.tv_nsec / 1000;
}

static void env_copy(char *dst, size_t cap, const char *name) {
  const char *e = getenv(name);
  if (!e || !*e)
    return;
  strncpy(dst, e, cap - 1);
  dst[cap - 1] = 0;
}

static int env_is_on(const char *name) {
  const char *e = getenv(name);
  return (e && *e && strcmp(e, "0") != 0);
}

static int parse_node_list(ha_config_t *config) {
  const char *env = getenv("RF_HA_NODES");
  if (!env || !*env) {
    LOGW("RF_HA_NODES not set - HA disabled");
    return -1;
  }

  config->node_count = 0;
  const char *p      = env;

  while (*p && config->node_count < HA_MAX_NODES) {
    ha_node_config_t *node = &config->nodes[config->node_count];

    const char *start = p;
    while (*p && *p != ':' && *p != ',')
      p++;
    size_t len = p - start;
    if (len >= sizeof(node->node_id))
      len = sizeof(node->node_id) - 1;
    memcpy(node->node_id, start, len);
    node->node_id[len] = 0;

    if (*p != ':')
      break;
    p++;

    start = p;
    while (*p && *p != ':' && *p != ',')
      p++;
    size_t host_len = (size_t)(p - start);
    if (host_len >= sizeof(node->advertise_addr))
      host_len = sizeof(node->advertise_addr) - 1;
    memcpy(node->advertise_addr, start, host_len);
    node->advertise_addr[host_len] = 0;

    if (*p != ':')
      break;
    p++;

    char port_buf[16];
    start        = p;
    int port_len = 0;
    while (*p && *p != ':' && *p != ',' && port_len < 15) {
      port_buf[port_len++] = *p++;
    }
    port_buf[port_len] = 0;

    strncat(node->advertise_addr, ":", sizeof(node->advertise_addr) - strlen(node->advertise_addr) - 1);
    strncat(node->advertise_addr, port_buf, sizeof(node->advertise_addr) - strlen(node->advertise_addr) - 1);

    node->priority = 10;
    if (*p == ':') {
      p++;
      node->priority = atoi(p);
      while (*p && *p != ',')
        p++;
    }

    node->is_voter = 1;

    int cluster_port = atoi(port_buf);

    char host_only[256];
    size_t cut = strcspn(node->advertise_addr, ":");
    if (cut >= sizeof(host_only))
      cut = sizeof(host_only) - 1;
    memcpy(host_only, node->advertise_addr, cut);
    host_only[cut] = 0;

    snprintf(node->http_addr, sizeof(node->http_addr), "http://%s:%d", host_only, cluster_port + 1);

    LOGI("Node[%d] advertise_addr=%s http_addr=%s priority=%d voter=%d", (int)config->node_count, node->advertise_addr,
         node->http_addr, node->priority, node->is_voter);

    if (*p == ',')
      p++;
    config->node_count++;
  }

  LOGI("Parsed %d HA nodes", config->node_count);
  return 0;
}

static int find_local_node(ha_config_t *config) {
  char local_buf[256];
  const char *local_id = getenv("RF_HA_NODE_ID");
  if (!local_id || !*local_id) {
    if (gethostname(local_buf, sizeof(local_buf)) != 0) {
      LOGE("RF_HA_NODE_ID not set and hostname unavailable");
      return -1;
    }
    local_buf[sizeof(local_buf) - 1] = 0;
    local_id                         = local_buf;
  }

  for (int i = 0; i < config->node_count; i++) {
    if (strcmp(config->nodes[i].node_id, local_id) == 0) {
      config->local_node_index = i;
      LOGI("Local node: %s (index %d)", local_id, i);
      return 0;
    }
  }

  LOGE("Local node ID '%s' not found in cluster", local_id);
  return -1;
}

int HA_init_from_env(ha_config_t *config, ha_runtime_t *runtime) {
  memset(config, 0, sizeof(*config));
  memset(runtime, 0, sizeof(*runtime));

  if (parse_node_list(config) != 0)
    return -1;

  if (config->node_count < 1) {
    LOGE("Need at least 1 node");
    return -1;
  }

  if (config->node_count > 1 && config->node_count % 2 == 0) {
    LOGW("Even number of nodes (%d) - consider adding one for proper quorum", config->node_count);
  }

  if (find_local_node(config) != 0)
    return -1;

  const char *env;

  config->mtls_enabled             = env_is_on("RF_HA_MTLS");
  config->mtls_verify_peer         = config->mtls_enabled ? 1 : 0;
  config->mtls_require_client_cert = config->mtls_enabled ? 1 : 0;
  config->mtls_verify_mode         = 2;

  if ((env = getenv("RF_HA_MTLS_VERIFY_PEER")))
    config->mtls_verify_peer = atoi(env);
  if ((env = getenv("RF_HA_MTLS_REQUIRE_CLIENT_CERT")))
    config->mtls_require_client_cert = atoi(env);
  if ((env = getenv("RF_HA_MTLS_VERIFY_MODE")))
    config->mtls_verify_mode = atoi(env);

  env_copy(config->mtls_ca_file, sizeof(config->mtls_ca_file), "RF_HA_MTLS_CA_FILE");
  env_copy(config->mtls_ca_path, sizeof(config->mtls_ca_path), "RF_HA_MTLS_CA_PATH");
  env_copy(config->mtls_cert_file, sizeof(config->mtls_cert_file), "RF_HA_MTLS_CERT_FILE");
  env_copy(config->mtls_key_file, sizeof(config->mtls_key_file), "RF_HA_MTLS_KEY_FILE");
  env_copy(config->mtls_key_pass, sizeof(config->mtls_key_pass), "RF_HA_MTLS_KEY_PASS");

  config->ha_max_msg_bytes = 2u * 1024u * 1024u;
  if ((env = getenv("RF_HA_MAX_MSG_BYTES")))
    config->ha_max_msg_bytes = (uint32_t)strtoul(env, NULL, 10);
  if (config->ha_max_msg_bytes < 64 * 1024)
    config->ha_max_msg_bytes = 64 * 1024;

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

  config->replication_batch_size = HA_REPLICATION_BATCH;
  if ((env = getenv("RF_HA_REPLICATION_BATCH"))) {
    config->replication_batch_size = atoi(env);
  }

  config->sync_mode = 0;
  if ((env = getenv("RF_HA_SYNC_MODE"))) {
    config->sync_mode = atoi(env);
  }

  config->write_concern = (config->node_count / 2) + 1;
  if ((env = getenv("RF_HA_WRITE_CONCERN"))) {
    config->write_concern = atoi(env);
  }

  config->redirect_status = 307;
  if ((env = getenv("RF_HA_REDIRECT_STATUS"))) {
    config->redirect_status = atoi(env);
  }

  config->retry_after_sec = 1;
  if ((env = getenv("RF_HA_RETRY_AFTER"))) {
    config->retry_after_sec = atoi(env);
  }

  atomic_store(&runtime->role, HA_ROLE_FOLLOWER);
  atomic_store(&runtime->state, HA_STATE_INITIALIZING);
  atomic_store(&runtime->term, 0);
  atomic_store(&runtime->commit_index, 0);
  atomic_store(&runtime->applied_index, 0);
  atomic_store(&runtime->current_leader, -1);
  atomic_store(&runtime->last_heartbeat_us, now_us());
  atomic_store(&runtime->voted_for, -1);

  uint64_t voters_mask = 0;
  uint64_t learners_mask = 0;

  for (int i = 0; i < HA_MAX_NODES; i++) {
    atomic_store(&runtime->next_index[i], 1);
    atomic_store(&runtime->match_index[i], 0);
    atomic_store(&runtime->last_ack_us[i], 0);
    if (i < config->node_count && config->nodes[i].node_id[0]) {
      if (config->nodes[i].is_voter) voters_mask |= HA_NODE_MASK(i);
      else learners_mask |= HA_NODE_MASK(i);
    }
  }

  atomic_store(&runtime->voters_old_mask, voters_mask);
  atomic_store(&runtime->voters_new_mask, voters_mask);
  atomic_store(&runtime->learners_mask, learners_mask);
  atomic_store(&runtime->joint_active, 0);
  atomic_store(&runtime->config_epoch, 1);
  atomic_store(&runtime->cfg_change_inflight, 0);

  atomic_store(&runtime->bootstrap_gate_active, 0);
  atomic_store(&runtime->bootstrap_quorum_confirmed, 0);
  atomic_store(&runtime->bootstrap_gate_start_us, 0);
  atomic_store(&runtime->last_quorum_ok_us, now_us());
  atomic_store(&runtime->quorum_miss_streak, 0);

  if (pthread_rwlock_init(&runtime->state_lock, NULL) != 0) {
    LOGE("Failed to initialize state lock");
    return -1;
  }
  if (pthread_rwlock_init(&runtime->membership_lock, NULL) != 0) {
    LOGE("Failed to initialize membership lock");
    pthread_rwlock_destroy(&runtime->state_lock);
    return -1;
  }

  LOGI("HA configuration loaded:");
  LOGI("Node count: %d", config->node_count);
  LOGI("Local node: %s", config->nodes[config->local_node_index].node_id);
  LOGI("Heartbeat: %u ms", config->heartbeat_interval_ms);
  LOGI("Election timeout: %u ms", config->election_timeout_ms);
  LOGI("Write concern: %d/%d nodes", config->write_concern, config->node_count);
  LOGI("Sync mode: %s", config->sync_mode == 0 ? "async" : config->sync_mode == 1 ? "sync-majority" : "sync-all");

  return 0;
}

ha_role_t HA_get_role(const ha_runtime_t *runtime) { return atomic_load(&runtime->role); }

int HA_is_leader(const ha_runtime_t *runtime) { return atomic_load(&runtime->role) == HA_ROLE_LEADER; }

int HA_is_ready(const ha_runtime_t *runtime) {
  ha_state_t state = atomic_load(&runtime->state);
  return state == HA_STATE_READY || state == HA_STATE_DEGRADED;
}

const char *HA_get_leader_url(const ha_config_t *config) { return config->leader_url; }

uint64_t HA_get_replication_lag_ms(const ha_runtime_t *runtime) {
  uint64_t commit  = atomic_load(&runtime->commit_index);
  uint64_t applied = atomic_load(&runtime->applied_index);

  if (commit <= applied)
    return 0;

  return (commit - applied);
}



