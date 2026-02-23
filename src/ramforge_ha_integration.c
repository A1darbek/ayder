// ramforge_ha_integration.c - Seamless HA Integration for HTTP Server
#include "ramforge_ha_integration.h"
#include "aof_batch.h"
#include "log.h"
#include "ramforge_ha_config.h"
#include "ramforge_ha_net.h"
#include "ramforge_ha_replication.h"
#include "ramforge_ha_tls.h"
#include "shared_storage.h" // ADD
#include "stdlib.h"
#include <pthread.h>
#include <stdio.h>
#include <string.h>
#include <unistd.h>
static _Atomic int g_ha_agent_only = 0;
extern SharedStorage *g_shared_storage;
static ha_config_t g_ha_config;
static ha_runtime_t g_ha_runtime;
static pthread_t g_ha_thread;
static _Atomic int g_ha_running = 0;
static _Atomic int g_ha_enabled = 0;

#define HA_SNAPSHOT_KEY ((int)0x7F004001) /* < INT_MAX */
#define SHMEM_HA_KEY ((int)0x0A001337)

// any nonzero int is fine & stable
// ═══════════════════════════════════════════════════════════════════════════════
// Background HA Worker Thread
// ═══════════════════════════════════════════════════════════════════════════════

/* one small blob we publish to shared_storage so all workers can gate correctly */
typedef struct {
  int role;       /* 0=leader,1=follower,... */
  int state;      /* ready/syncing/... */
  int leader_idx; /* node index in config */
  uint64_t term;
  uint64_t last_heartbeat_us;

  uint64_t commit_index;
  uint64_t applied_index;
  uint64_t elections_started;
  uint64_t heartbeats_sent;
  uint64_t heartbeats_received;
  uint64_t writes_replicated;
  uint64_t writes_failed;
} ha_shmem_state_t;

/* simple fixed key for shmem */

static void ha_publish_to_shmem(void) {
  if (!g_shared_storage)
    return;

  ha_shmem_state_t s  = {0};
  s.role              = atomic_load(&g_ha_runtime.role);
  s.state             = atomic_load(&g_ha_runtime.state);
  s.leader_idx        = atomic_load(&g_ha_runtime.current_leader);
  s.term              = atomic_load(&g_ha_runtime.term);
  s.last_heartbeat_us = atomic_load(&g_ha_runtime.last_heartbeat_us);

  // NEW: copy metrics from the leader runtime into shmem
  s.commit_index        = atomic_load(&g_ha_runtime.commit_index);
  s.applied_index       = atomic_load(&g_ha_runtime.applied_index);
  s.elections_started   = atomic_load(&g_ha_runtime.elections_started);
  s.heartbeats_sent     = atomic_load(&g_ha_runtime.heartbeats_sent);
  s.heartbeats_received = atomic_load(&g_ha_runtime.heartbeats_received);
  s.writes_replicated   = atomic_load(&g_ha_runtime.writes_replicated);
  s.writes_failed       = atomic_load(&g_ha_runtime.writes_failed);

  shared_storage_set(g_shared_storage, SHMEM_HA_KEY, &s, sizeof s);
}

static int ha_load_from_shmem(ha_shmem_state_t *out) {
  if (!g_shared_storage)
    return 0;
  ha_shmem_state_t s = {0};
  if (!shared_storage_get_fast(g_shared_storage, SHMEM_HA_KEY, &s, sizeof s))
    return 0;
  *out = s;
  return 1;
}

static void HA_publish_snapshot_tick(uint64_t now_us) {
  if (!g_shared_storage)
    return;

  ha_shared_snapshot_t snap = {0};
  snap.enabled              = atomic_load(&g_ha_enabled);
  snap.role                 = atomic_load(&g_ha_runtime.role);
  snap.state                = atomic_load(&g_ha_runtime.state);
  snap.term                 = atomic_load(&g_ha_runtime.term);
  snap.leader_index         = atomic_load(&g_ha_runtime.current_leader);
  snap.redirect_status      = g_ha_config.redirect_status;
  snap.retry_after          = g_ha_config.retry_after_sec;
  snap.last_heartbeat_us    = atomic_load(&g_ha_runtime.last_heartbeat_us);

  // Build leader URL from config (data-plane http_addr)
  if (snap.leader_index >= 0 && snap.leader_index < g_ha_config.node_count) {
    const char *lu = g_ha_config.nodes[snap.leader_index].http_addr;
    if (lu) {
      size_t n = strlen(lu);
      if (n >= sizeof(snap.leader_url))
        n = sizeof(snap.leader_url) - 1;
      memcpy(snap.leader_url, lu, n);
      snap.leader_url[n] = 0;
    }
  }

  // Publish every ~25ms (cheap), dedup via static cache to avoid churn
  static ha_shared_snapshot_t prev = {0};
  static uint64_t last_push        = 0;
  if (memcmp(&snap, &prev, sizeof(snap)) != 0 || (now_us - last_push) > 250000) {
    shared_storage_set(g_shared_storage, HA_SNAPSHOT_KEY, &snap, sizeof(snap));
    prev      = snap;
    last_push = now_us;
  }
}

int RAMForge_HA_read_snapshot(ha_shared_snapshot_t *out) {
  if (!g_shared_storage || !out)
    return 0;
  ha_shared_snapshot_t tmp;
  if (!shared_storage_get_fast(g_shared_storage, HA_SNAPSHOT_KEY, &tmp, sizeof(tmp)))
    return 0;
  *out = tmp;
  return 1;
}

static inline uint64_t now_us(void) {
  struct timespec ts;
  clock_gettime(CLOCK_MONOTONIC, &ts);
  return (uint64_t)ts.tv_sec * 1000000ULL + ts.tv_nsec / 1000;
}

static void *ha_worker_thread(void *arg) {
  (void)arg;

  uint64_t last_heartbeat        = 0;
  uint64_t last_election_check   = 0;
  uint64_t heartbeat_interval_us = g_ha_config.heartbeat_interval_ms * 1000;
  uint64_t election_timeout_us   = g_ha_config.election_timeout_ms * 1000;
  uint64_t last_kick             = HA_repl_kick_read();

  LOGI("HA worker thread started");

  // Mark as ready after initialization
  atomic_store(&g_ha_runtime.state, HA_STATE_READY);

  while (atomic_load(&g_ha_running)) {
    uint64_t now = now_us();

    HA_publish_snapshot_tick(now);

    // Leader: send heartbeats
    if (HA_is_leader(&g_ha_runtime)) {
      uint64_t kick = HA_repl_kick_read();
      if ((now - last_heartbeat >= heartbeat_interval_us) || (kick != last_kick)) {
        HA_replicate_to_followers();
        last_heartbeat = now;
        last_kick      = kick;
      }

      // Apply committed entries
      HA_apply_committed_entries();
    }
    // Follower: check for election timeout
    else if (atomic_load(&g_ha_runtime.role) == HA_ROLE_FOLLOWER) {
      uint64_t last_hb = atomic_load(&g_ha_runtime.last_heartbeat_us);

      if (now - last_election_check >= election_timeout_us) {
        if (now - last_hb >= election_timeout_us) {
          LOGW("Election timeout - no heartbeat from leader");
          // randomized backoff to avoid dueling candidates
          uint64_t jitter_us = (uint64_t)(rand() % 250) * 1000ULL; // 0–250ms
          usleep(jitter_us);
          HA_start_election();
          // start_election() would go here
        }
        last_election_check = now;
      }
    }

    ha_publish_to_shmem();
    // Sleep briefly
    usleep(10000); // 10ms
  }

  LOGI("HA worker thread stopped");
  return NULL;
}

// ═══════════════════════════════════════════════════════════════════════════════
// Public Integration API
// ═══════════════════════════════════════════════════════════════════════════════

int RAMForge_HA_init(void) {
  srand((unsigned)(time(NULL) ^ getpid()));
  // Check if HA is enabled via environment
  const char *enabled = getenv("RF_HA_ENABLED");
  if (!enabled || strcmp(enabled, "1") != 0) {
    LOGW("HA disabled (set RF_HA_ENABLED=1 to enable)");
    atomic_store(&g_ha_enabled, 0);
    return 0;
  }

  /* agent-only? (workers >0) */
  const char *agent = getenv("RF_HA_AGENT_ONLY");
  if (agent && agent[0] != '0') {
    atomic_store(&g_ha_agent_only, 1);
  }

  // Load configuration
  if (HA_init_from_env(&g_ha_config, &g_ha_runtime) != 0) {
    LOGE("Failed to initialize HA configuration");
    return -1;
  }

  // TLS (mTLS) for HA sockets (7000/8000/9000...): idempotent init
  if (HA_tls_init_from_env() != 0) {
    LOGE("Failed to initialize HA mTLS (RF_HA_TLS=1)");
    return -1;
  }

  // Initialize replication engine
  if (!atomic_load(&g_ha_agent_only)) {
    // FULL runtime (worker 0)
    if (HA_replication_init(&g_ha_config, &g_ha_runtime) != 0) {
      LOGE("Failed to initialize HA replication");
      return -1;
    }
    if (HA_net_start_server(&g_ha_config, &g_ha_runtime) != 0) {
      LOGE("Failed to start HA inbound server");
      return -1;
    }
  } else {
    LOGI("HA agent-only mode: will read HA state from shared memory");
  }

  // Single-node cluster: self-elect leader immediately
  if (g_ha_config.node_count == 1) {
    atomic_store(&g_ha_runtime.role, HA_ROLE_LEADER);
    atomic_store(&g_ha_runtime.current_leader, g_ha_config.local_node_index);
    atomic_store(&g_ha_runtime.state, HA_STATE_READY);
    LOGI("Single-node cluster: self-electing LEADER");
    // No worker thread needed but harmless to run
  }

  // Optional bootstrap: force this node to be initial leader (multi-node bring-up)
  if (!atomic_load(&g_ha_agent_only) && g_ha_config.node_count > 1) {
    const char *boot = getenv("RF_HA_BOOTSTRAP_LEADER");
    if (boot && strcmp(boot, "1") == 0) {
      atomic_store(&g_ha_runtime.role, HA_ROLE_LEADER);
      atomic_store(&g_ha_runtime.current_leader, g_ha_config.local_node_index);
      atomic_store(&g_ha_runtime.state, HA_STATE_READY);
      LOGI("Bootstrap: assuming LEADER (initial bring-up)");
    }
  }

  atomic_store(&g_ha_enabled, 1);
  if (!atomic_load(&g_ha_agent_only)) {
    // Start HA worker thread only for full runtime
    atomic_store(&g_ha_running, 1);
    if (pthread_create(&g_ha_thread, NULL, ha_worker_thread, NULL) != 0) {
      LOGE("Failed to create HA worker thread");
      HA_replication_shutdown();
      return -1;
    }
  }

  LOGI("HA initialized - Role: %s, Node: %s", atomic_load(&g_ha_runtime.role) == HA_ROLE_LEADER ? "LEADER" : "FOLLOWER",
       g_ha_config.nodes[g_ha_config.local_node_index].node_id);

  return 0;
}

void RAMForge_HA_shutdown(void) {
  if (!atomic_load(&g_ha_enabled))
    return;

  LOGI("Shutting down HA system...");

  if (!atomic_load(&g_ha_agent_only)) {
    HA_net_stop_server();
    atomic_store(&g_ha_running, 0);
    pthread_join(g_ha_thread, NULL);
    HA_replication_shutdown();
  }

  HA_tls_shutdown();

  LOGI("HA shutdown complete");
}

int RAMForge_HA_is_enabled(void) { return atomic_load(&g_ha_enabled); }

int RAMForge_HA_is_leader(void) {
  if (!atomic_load(&g_ha_enabled))
    return 1; // legacy default
  if (!atomic_load(&g_ha_agent_only))
    return HA_is_leader(&g_ha_runtime);
  ha_shmem_state_t s;
  if (!ha_load_from_shmem(&s))
    return 0; /* safe default: not leader */
  return (s.leader_idx == g_ha_config.local_node_index);
}

int RAMForge_HA_is_follower(void) {
  if (!atomic_load(&g_ha_enabled))
    return 0;
  if (!atomic_load(&g_ha_agent_only))
    return atomic_load(&g_ha_runtime.role) == HA_ROLE_FOLLOWER;
  ha_shmem_state_t s;
  if (!ha_load_from_shmem(&s))
    return 1; /* safe default: follow */
  return s.role == HA_ROLE_FOLLOWER;
}

const char *RAMForge_HA_get_local_node_id(void) {
  if (!atomic_load(&g_ha_enabled))
    return NULL;
  if (g_ha_config.local_node_index < 0 || g_ha_config.local_node_index >= g_ha_config.node_count) {
    return NULL;
  }
  return g_ha_config.nodes[g_ha_config.local_node_index].node_id;
}

const char *RAMForge_HA_get_leader_node_id(void) {
  if (!atomic_load(&g_ha_enabled))
    return NULL;
  int leader_idx = atomic_load(&g_ha_runtime.current_leader);
  if (leader_idx < 0 || leader_idx >= g_ha_config.node_count) {
    return NULL;
  }
  return g_ha_config.nodes[leader_idx].node_id;
}

int RAMForge_HA_get_node_count(void) {
  if (!atomic_load(&g_ha_enabled))
    return 0;
  return g_ha_config.node_count;
}

int RAMForge_HA_is_ready(void) {
  if (!atomic_load(&g_ha_enabled))
    return 1;
  return HA_is_ready(&g_ha_runtime);
}

const char *RAMForge_HA_get_leader_url(void) {
  if (!atomic_load(&g_ha_enabled))
    return NULL;
  int leader_idx;
  if (!atomic_load(&g_ha_agent_only)) {
    leader_idx = atomic_load(&g_ha_runtime.current_leader);
  } else {
    ha_shmem_state_t s;
    if (!ha_load_from_shmem(&s))
      return NULL;
    leader_idx = s.leader_idx;
  }
  if (leader_idx < 0 || leader_idx >= g_ha_config.node_count) {
    return NULL;
  }

  return g_ha_config.nodes[leader_idx].http_addr;
}

int RAMForge_HA_get_redirect_status(void) {
  if (!atomic_load(&g_ha_enabled))
    return 0;
  return g_ha_config.redirect_status;
}

int RAMForge_HA_get_retry_after(void) {
  if (!atomic_load(&g_ha_enabled))
    return 0;
  return g_ha_config.retry_after_sec;
}

// Replicate write to cluster (called after sealed append)
int RAMForge_HA_replicate_write(uint32_t record_type, const void *data, uint32_t size) {
  if (!atomic_load(&g_ha_enabled))
    return 0; // HA disabled - succeed immediately

  if (atomic_load(&g_ha_agent_only)) {
    /* publish to rf_bus; worker 0 will append+replicate */
    ha_fwd_send(record_type, data, size);
    return 0;
  }

  if (!HA_is_leader(&g_ha_runtime)) {
    return -1; // Not leader - write should have been rejected
  }

  // Append to local log
  uint64_t index = HA_append_local_entry(record_type, data, size);
  if (index == 0) {
    return -1;
  }

  // For async mode, just trigger replication and return
  if (g_ha_config.sync_mode == 0) {
    HA_request_catchup(); // non-blocking
    return 0;
  }

  // For sync modes, wait for replication
  uint32_t timeout_ms = g_ha_config.replication_timeout_ms;
  return HA_wait_for_replication(index, timeout_ms);
}

void RAMForge_HA_export_metrics(char *buf, size_t cap) {
  if (!atomic_load(&g_ha_enabled)) {
    snprintf(buf, cap, "# HA disabled\n");
    return;
  }

  // First preference: the rich snapshot (with metrics) from worker 0
  ha_shared_snapshot_t snap;
  if (RAMForge_HA_read_snapshot(&snap)) {
    char *p   = buf;
    char *end = buf + cap;

#define APP(...)                                                                                                       \
  do {                                                                                                                 \
    int n = snprintf(p, (size_t)(end - p), __VA_ARGS__);                                                               \
    if (n < 0)                                                                                                         \
      n = 0;                                                                                                           \
    if ((size_t)n >= (size_t)(end - p)) {                                                                              \
      p = end;                                                                                                         \
    } else {                                                                                                           \
      p += n;                                                                                                          \
    }                                                                                                                  \
  } while (0)

    ha_role_t role   = (ha_role_t)snap.role;
    ha_state_t state = (ha_state_t)snap.state;

    APP("# HELP ramforge_ha_role Current HA role (0=leader, 1=follower, 2=candidate, 3=learner)\n");
    APP("# TYPE ramforge_ha_role gauge\n");
    APP("ramforge_ha_role %d\n", role);

    APP("# HELP ramforge_ha_state Current HA state (0=init, 1=ready, 2=syncing, 3=degraded, 4=failed)\n");
    APP("# TYPE ramforge_ha_state gauge\n");
    APP("ramforge_ha_state %d\n", state);

    APP("# HELP ramforge_ha_term Current election term\n");
    APP("# TYPE ramforge_ha_term counter\n");
    APP("ramforge_ha_term %lu\n", (unsigned long)snap.term);

#undef APP
    return;
  }
  // Fallback AND ROBUST WORKING: use the older HA shmem state (SHMEM_HA_KEY).
  // This is already proven to work because agent workers use it
  ha_shmem_state_t s;
  if (ha_load_from_shmem(&s)) {
    char *p   = buf;
    char *end = buf + cap;

#define APP(...)                                                                                                       \
  do {                                                                                                                 \
    int n = snprintf(p, (size_t)(end - p), __VA_ARGS__);                                                               \
    if (n < 0)                                                                                                         \
      n = 0;                                                                                                           \
    if ((size_t)n >= (size_t)(end - p)) {                                                                              \
      p = end;                                                                                                         \
    } else {                                                                                                           \
      p += n;                                                                                                          \
    }                                                                                                                  \
  } while (0)

    // We at least expose correct role/state/term; other metrics fall back to 0.
    APP("# HELP ramforge_ha_role Current HA role (0=leader, 1=follower, 2=candidate, 3=learner)\n");
    APP("# TYPE ramforge_ha_role gauge\n");
    APP("ramforge_ha_role %d\n", s.role);

    APP("# HELP ramforge_ha_state Current HA state (0=init, 1=ready, 2=syncing, 3=degraded, 4=failed)\n");
    APP("# TYPE ramforge_ha_state gauge\n");
    APP("ramforge_ha_state %d\n", s.state);

    APP("# HELP ramforge_ha_term Current election term\n");
    APP("# TYPE ramforge_ha_term counter\n");
    APP("ramforge_ha_term %lu\n", (unsigned long)s.term);

    APP("# HELP ramforge_ha_commit_index Highest committed log index\n");
    APP("# TYPE ramforge_ha_commit_index counter\n");
    APP("ramforge_ha_commit_index %lu\n", (unsigned long)s.commit_index);

    APP("# HELP ramforge_ha_applied_index Highest applied log index\n");
    APP("# TYPE ramforge_ha_applied_index counter\n");
    APP("ramforge_ha_applied_index %lu\n", (unsigned long)s.applied_index);

    uint64_t lag_ms = 0;
    if (s.commit_index > s.applied_index)
      lag_ms = s.commit_index - s.applied_index;

    APP("# HELP ramforge_ha_replication_lag_ms Replication lag in milliseconds\n");
    APP("# TYPE ramforge_ha_replication_lag_ms gauge\n");
    APP("ramforge_ha_replication_lag_ms %lu\n", (unsigned long)lag_ms);

    APP("# HELP ramforge_ha_elections_total Total elections started\n");
    APP("# TYPE ramforge_ha_elections_total counter\n");
    APP("ramforge_ha_elections_total %lu\n", (unsigned long)s.elections_started);

    APP("# HELP ramforge_ha_heartbeats_sent_total Heartbeats sent\n");
    APP("# TYPE ramforge_ha_heartbeats_sent_total counter\n");
    APP("ramforge_ha_heartbeats_sent_total %lu\n", (unsigned long)s.heartbeats_sent);

    APP("# HELP ramforge_ha_heartbeats_received_total Heartbeats received\n");
    APP("# TYPE ramforge_ha_heartbeats_received_total counter\n");
    APP("ramforge_ha_heartbeats_received_total %lu\n", (unsigned long)s.heartbeats_received);

    APP("# HELP ramforge_ha_writes_replicated_total Writes successfully replicated\n");
    APP("# TYPE ramforge_ha_writes_replicated_total counter\n");
    APP("ramforge_ha_writes_replicated_total %lu\n", (unsigned long)s.writes_replicated);

    APP("# HELP ramforge_ha_writes_failed_total Writes that failed replication\n");
    APP("# TYPE ramforge_ha_writes_failed_total counter\n");
    APP("ramforge_ha_writes_failed_total %lu\n", (unsigned long)s.writes_failed);

    // --- per-node metrics (same shape as snapshot branch) ---
    int node_count = g_ha_config.node_count;
    int local_idx  = g_ha_config.local_node_index;
    int leader_idx = s.leader_idx;

    APP("# HELP ramforge_ha_node_info HA node static info (1 row per configured node)\n");
    APP("# TYPE ramforge_ha_node_info gauge\n");

    for (int i = 0; i < node_count; i++) {
      ha_node_config_t *node = &g_ha_config.nodes[i];
      int is_local           = (i == local_idx);
      int is_leader          = (i == leader_idx);
      int is_voter           = node->is_voter;

      APP("ramforge_ha_node_info"
          "{node_id=\"%s\",advertise_addr=\"%s\",http_addr=\"%s\","
          "priority=\"%d\",voter=\"%d\",local=\"%d\",leader=\"%d\"} 1\n",
          node->node_id, node->advertise_addr, node->http_addr, node->priority, is_voter, is_local, is_leader);
    }

    APP("# HELP ramforge_ha_node_role HA role per node (0=leader,1=follower from this node's view)\n");
    APP("# TYPE ramforge_ha_node_role gauge\n");

    for (int i = 0; i < node_count; i++) {
      ha_node_config_t *node = &g_ha_config.nodes[i];
      int is_leader          = (i == leader_idx);
      int role_val           = is_leader ? HA_ROLE_LEADER : HA_ROLE_FOLLOWER;

      APP("ramforge_ha_node_role{node_id=\"%s\"} %d\n", node->node_id, role_val);
    }

#undef APP
    return;
  }

  // Only in the very first moments of startup, before any shmem state exists.
  snprintf(buf, cap, "# HA snapshot not yet available\n");
}

// Get node statistics
void RAMForge_HA_get_stats(uint64_t *log_index, uint64_t *commit_index, uint64_t *applied_index, uint64_t *term,
                           int *role, int *state) {
  if (!atomic_load(&g_ha_enabled)) {
    if (log_index)
      *log_index = 0;
    if (commit_index)
      *commit_index = 0;
    if (applied_index)
      *applied_index = 0;
    if (term)
      *term = 0;
    if (role)
      *role = HA_ROLE_LEADER;
    if (state)
      *state = HA_STATE_READY;
    return;
  }

  HA_get_log_stats(log_index, commit_index, applied_index);

  if (term)
    *term = atomic_load(&g_ha_runtime.term);
  if (role)
    *role = atomic_load(&g_ha_runtime.role);
  if (state)
    *state = atomic_load(&g_ha_runtime.state);
}
