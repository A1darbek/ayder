#include "ramforge_ha_config.h"
#include "ramforge_ha_replication.h"
#include "ramforge_ha_integration.h"
#include "ramforge_ha_net.h"
#include "ramforge_ha_tls.h"
#include "aof_batch.h"
#include "shared_storage.h"
#include "rf_broker.h"

#include <pthread.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <time.h>
#include <inttypes.h>

extern SharedStorage *g_shared_storage;

static ha_config_t g_ha_config;
static ha_runtime_t g_ha_runtime;
static pthread_t g_ha_thread;
static _Atomic int g_ha_running = 0;
static _Atomic int g_ha_enabled = 0;
static _Atomic int g_ha_agent_only = 0;

static _Atomic uint64_t g_mem_fwd_seq = 1;
static _Atomic int g_pending_membership_valid = 0;
static ha_membership_record_t g_pending_membership;

#define HA_SNAPSHOT_KEY 0x00040001
#define SHMEM_HA_KEY 0xA001337
#define SHMEM_HA_MEM_FWD_BASE 0x0A60F000

typedef struct {
    int role;
    int state;
    int leader_idx;
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

static inline uint64_t now_us(void) {
    struct timespec ts;
    clock_gettime(CLOCK_MONOTONIC, &ts);
    return (uint64_t)ts.tv_sec * 1000000ULL + ts.tv_nsec / 1000ULL;
}

static int ha_mem_debug_enabled(void) {
    const char *e = getenv("RF_HA_MEM_DEBUG");
    return (e && *e && strcmp(e, "0") != 0) ? 1 : 0;
}

static int ha_mem_rsp_key(uint64_t req_id) {
    return (int)(SHMEM_HA_MEM_FWD_BASE ^ (uint32_t)(req_id & 0x7FFFFFFFu));
}

static uint64_t ha_next_req_id(void) {
    uint64_t base = ((uint64_t)getpid()) << 32;
    return base ^ atomic_fetch_add(&g_mem_fwd_seq, 1);
}

static void ha_store_pending_membership(const ha_membership_record_t *rec) {
    if (!rec) return;
    g_pending_membership = *rec;
    atomic_store(&g_pending_membership_valid, 1);
}

static void ha_apply_pending_membership_if_any(void) {
    if (!atomic_load(&g_pending_membership_valid)) return;
    if (HA_apply_membership_snapshot(&g_pending_membership) == 0) {
        atomic_store(&g_pending_membership_valid, 0);
    }
}

static void ha_publish_to_shmem(void) {
    if (!g_shared_storage) return;

    ha_shmem_state_t s;
    memset(&s, 0, sizeof(s));
    s.role = atomic_load(&g_ha_runtime.role);
    s.state = atomic_load(&g_ha_runtime.state);
    s.leader_idx = atomic_load(&g_ha_runtime.current_leader);
    s.term = atomic_load(&g_ha_runtime.term);
    s.last_heartbeat_us = atomic_load(&g_ha_runtime.last_heartbeat_us);
    s.commit_index = atomic_load(&g_ha_runtime.commit_index);
    s.applied_index = atomic_load(&g_ha_runtime.applied_index);
    s.elections_started = atomic_load(&g_ha_runtime.elections_started);
    s.heartbeats_sent = atomic_load(&g_ha_runtime.heartbeats_sent);
    s.heartbeats_received = atomic_load(&g_ha_runtime.heartbeats_received);
    s.writes_replicated = atomic_load(&g_ha_runtime.writes_replicated);
    s.writes_failed = atomic_load(&g_ha_runtime.writes_failed);

    (void)shared_storage_set(g_shared_storage, SHMEM_HA_KEY, &s, sizeof(s));
}

static int ha_load_from_shmem(ha_shmem_state_t *out) {
    if (!g_shared_storage || !out) return 0;
    ha_shmem_state_t s;
    if (!shared_storage_get_fast(g_shared_storage, SHMEM_HA_KEY, &s, sizeof(s))) return 0;
    *out = s;
    return 1;
}

static void HA_publish_snapshot_tick(uint64_t now) {
    if (!g_shared_storage) return;

    ha_shared_snapshot_t snap;
    memset(&snap, 0, sizeof(snap));
    snap.enabled = atomic_load(&g_ha_enabled);
    snap.role = atomic_load(&g_ha_runtime.role);
    snap.state = atomic_load(&g_ha_runtime.state);
    snap.term = atomic_load(&g_ha_runtime.term);
    snap.leader_index = atomic_load(&g_ha_runtime.current_leader);
    snap.redirect_status = g_ha_config.redirect_status;
    snap.retry_after = g_ha_config.retry_after_sec;
    snap.last_heartbeat_us = atomic_load(&g_ha_runtime.last_heartbeat_us);
    snap.config_epoch = atomic_load(&g_ha_runtime.config_epoch);
    snap.joint_active = atomic_load(&g_ha_runtime.joint_active);
    snap.has_fresh_quorum = HA_has_fresh_quorum();

    if (snap.leader_index >= 0 && snap.leader_index < g_ha_config.node_count) {
        const char *u = g_ha_config.nodes[snap.leader_index].http_addr;
        if (u) {
            size_t n = strlen(u);
            if (n >= sizeof(snap.leader_url)) n = sizeof(snap.leader_url) - 1;
            memcpy(snap.leader_url, u, n);
            snap.leader_url[n] = 0;
        }
    }

    static ha_shared_snapshot_t prev;
    static uint64_t last_push = 0;
    if (memcmp(&snap, &prev, sizeof(snap)) != 0 || (now - last_push) > 250000ULL) {
        (void)shared_storage_set(g_shared_storage, HA_SNAPSHOT_KEY, &snap, sizeof(snap));
        prev = snap;
        last_push = now;
    }
}

int RAMForge_HA_read_snapshot(ha_shared_snapshot_t *out) {
    if (!g_shared_storage || !out) return 0;
    ha_shared_snapshot_t tmp;
    if (!shared_storage_get_fast(g_shared_storage, HA_SNAPSHOT_KEY, &tmp, sizeof(tmp))) return 0;
    *out = tmp;
    return 1;
}

static int ha_find_node_index_by_id(const char *node_id) {
    if (!node_id || !*node_id) return -1;
    for (int i = 0; i < HA_MAX_NODES; i++) {
        if (!g_ha_config.nodes[i].node_id[0]) continue;
        if (strcmp(g_ha_config.nodes[i].node_id, node_id) == 0) return i;
    }
    return -1;
}

static int ha_find_free_slot(void) {
    for (int i = 0; i < HA_MAX_NODES; i++) {
        if (!g_ha_config.nodes[i].node_id[0]) return i;
    }
    return -1;
}

static int ha_wait_applied(uint64_t index, uint64_t min_epoch, uint32_t timeout_ms) {
    uint64_t start = now_us();
    uint64_t timeout_us = (uint64_t)timeout_ms * 1000ULL;
    while ((now_us() - start) < timeout_us) {
        if (atomic_load(&g_ha_runtime.applied_index) >= index &&
            atomic_load(&g_ha_runtime.config_epoch) >= min_epoch) {
            return 0;
        }
        usleep(500);
    }
    return -1;
}

static int ha_submit_cfg_change(const ha_cfg_change_t *chg, uint64_t expected_epoch_after, uint32_t timeout_ms) {
    if (!chg) return -1;
    if (!HA_is_leader(&g_ha_runtime)) return -4;
    if (!HA_has_fresh_quorum()) return -3;

    uint64_t idx = HA_append_local_entry(HA_LOG_CFG_CHANGE, chg, (uint32_t)sizeof(*chg));
    if (idx == 0) return -1;

    HA_request_catchup();

    uint64_t start_us = now_us();
    if (HA_wait_for_replication(idx, timeout_ms) != 0) {
        uint64_t elapsed_ms = (now_us() - start_us) / 1000ULL;
        uint32_t remain_ms = (elapsed_ms >= timeout_ms) ? 1u : (uint32_t)(timeout_ms - elapsed_ms);
        /* Avoid false timeout: commit may already be visible via applied/epoch. */
        if (ha_wait_applied(idx, expected_epoch_after, remain_ms) == 0) return 0;
        if (ha_mem_debug_enabled()) {
            fprintf(stderr, "[HA-MEM][TIMEOUT] op=%u idx=%lu phase=replicate->applied epoch=%lu expect=%lu commit=%lu applied=%lu role=%d\n",
                    (unsigned)chg->op, (unsigned long)idx,
                    (unsigned long)atomic_load(&g_ha_runtime.config_epoch),
                    (unsigned long)expected_epoch_after,
                    (unsigned long)atomic_load(&g_ha_runtime.commit_index),
                    (unsigned long)atomic_load(&g_ha_runtime.applied_index),
                    atomic_load(&g_ha_runtime.role));
        }
        return -5;
    }

    {
        uint64_t elapsed_ms = (now_us() - start_us) / 1000ULL;
        uint32_t remain_ms = (elapsed_ms >= timeout_ms) ? 1u : (uint32_t)(timeout_ms - elapsed_ms);
        if (ha_wait_applied(idx, expected_epoch_after, remain_ms) != 0) {
            if (ha_mem_debug_enabled()) {
                fprintf(stderr, "[HA-MEM][TIMEOUT] op=%u idx=%lu phase=applied epoch=%lu expect=%lu commit=%lu applied=%lu role=%d\n",
                        (unsigned)chg->op, (unsigned long)idx,
                        (unsigned long)atomic_load(&g_ha_runtime.config_epoch),
                        (unsigned long)expected_epoch_after,
                        (unsigned long)atomic_load(&g_ha_runtime.commit_index),
                        (unsigned long)atomic_load(&g_ha_runtime.applied_index),
                        atomic_load(&g_ha_runtime.role));
            }
            return -5;
        }
    }

    return 0;
}

static void ha_replicate_membership_snapshot_async(void) {
    if (atomic_load(&g_ha_agent_only)) return;
    if (!HA_is_leader(&g_ha_runtime)) return;

    ha_membership_view_t mv;
    if (HA_get_membership_view(&mv) != 0) return;
    (void)HA_forward_append_and_replicate(AOF_REC_HA_MEMBERSHIP, &mv, sizeof(mv));
}

static int ha_wait_forward_response(uint64_t req_id, int *out_rc, int *out_index, uint32_t timeout_ms) {
    if (!g_shared_storage) return -1;
    int key = ha_mem_rsp_key(req_id);
    uint64_t deadline = now_us() + ((uint64_t)timeout_ms * 1000ULL);

    while (now_us() < deadline) {
        ha_mem_fwd_rsp_t rsp;
        if (shared_storage_get_fast(g_shared_storage, key, &rsp, sizeof(rsp)) && rsp.req_id == req_id) {
            if (out_rc) *out_rc = rsp.rc;
            if (out_index) *out_index = rsp.out_index;
            return 0;
        }
        usleep(1000);
    }

    return -1;
}

static int ha_forward_membership_req(const ha_mem_fwd_req_t *req, int *out_index) {
    if (!req) return -1;
    if (rf_bus_publish(AOF_REC_HA_MEMBERSHIP_FORWARD, req, sizeof(*req)) != 0) return -1;

    int rc = -5;
    int idx = -1;
    uint32_t timeout_ms = req->timeout_ms ? req->timeout_ms : 3000;
    uint32_t factor = (req->op == HA_MEM_FWD_OP_PROMOTE || req->op == HA_MEM_FWD_OP_REMOVE) ? 5u : 3u;
    uint64_t wait_ms64 = (uint64_t)timeout_ms * (uint64_t)factor + 2000ULL;
    uint32_t wait_ms = (wait_ms64 > 0xFFFFFFFFULL) ? 0xFFFFFFFFu : (uint32_t)wait_ms64;
    if (ha_wait_forward_response(req->req_id, &rc, &idx, wait_ms) != 0) {
        if (ha_mem_debug_enabled()) {
            fprintf(stderr, "[HA-MEM][FWD-TIMEOUT] req_id=%lu op=%u wait_ms=%u timeout_ms=%u\n",
                    (unsigned long)req->req_id, (unsigned)req->op, (unsigned)wait_ms, (unsigned)timeout_ms);
        }
        return -5;
    }

    if (out_index) *out_index = idx;
    return rc;
}

static void ha_store_forward_response(uint64_t req_id, int rc, int out_index) {
    if (!g_shared_storage) return;
    ha_mem_fwd_rsp_t rsp;
    rsp.req_id = req_id;
    rsp.rc = rc;
    rsp.out_index = out_index;
    (void)shared_storage_set(g_shared_storage, ha_mem_rsp_key(req_id), &rsp, sizeof(rsp));
}

static int ha_membership_change_begin(void) {
    int expect = 0;
    if (!atomic_compare_exchange_strong(&g_ha_runtime.cfg_change_inflight, &expect, 1)) {
        return -2;
    }
    return 0;
}

static void ha_membership_change_end(void) {
    atomic_store(&g_ha_runtime.cfg_change_inflight, 0);
}

static int ha_add_member_local(const ha_node_config_t *node, int *out_index, uint32_t timeout_ms) {
    if (!node || !node->node_id[0] || !node->advertise_addr[0]) return -1;
    if (!HA_is_leader(&g_ha_runtime)) return -4;
    if (!HA_has_fresh_quorum()) return -3;

    int existing = ha_find_node_index_by_id(node->node_id);
    if (existing >= 0) {
        if (out_index) *out_index = existing;
        return 0;
    }

    int idx = ha_find_free_slot();
    if (idx < 0) return -1;

    uint64_t epoch = atomic_load(&g_ha_runtime.config_epoch);
    ha_cfg_change_t chg;
    memset(&chg, 0, sizeof(chg));
    chg.op = HA_CFG_OP_ADD_LEARNER;
    chg.node_index = (uint32_t)idx;
    chg.expected_epoch = epoch;
    chg.voters_old_mask = atomic_load(&g_ha_runtime.voters_old_mask);
    chg.voters_new_mask = atomic_load(&g_ha_runtime.voters_new_mask);
    chg.learners_mask = atomic_load(&g_ha_runtime.learners_mask);
    chg.member = *node;
    chg.member.is_voter = 0;

    int rc = ha_submit_cfg_change(&chg, epoch + 1, timeout_ms);
    if (rc == 0) {
        if (out_index) *out_index = idx;
        ha_replicate_membership_snapshot_async();
    }
    return rc;
}

static int ha_promote_member_local(const char *node_id, uint32_t timeout_ms) {
    if (!node_id || !*node_id) return -1;
    if (!HA_is_leader(&g_ha_runtime)) return -4;
    if (!HA_has_fresh_quorum()) return -3;

    int idx = ha_find_node_index_by_id(node_id);
    if (idx < 0) return -1;

    uint64_t learners = atomic_load(&g_ha_runtime.learners_mask);
    uint64_t voters_old_cur = atomic_load(&g_ha_runtime.voters_old_mask);
    uint64_t voters_new_cur = atomic_load(&g_ha_runtime.voters_new_mask);
    int joint_cur = atomic_load(&g_ha_runtime.joint_active);
    uint64_t bit = HA_NODE_MASK(idx);

    if (!(learners & bit)) {
        /* Recovery/idempotency path: promotion already began (joint), finalize it. */
        if (joint_cur && (voters_new_cur & bit) && !(voters_old_cur & bit)) {
            uint64_t epoch_now = atomic_load(&g_ha_runtime.config_epoch);
            ha_cfg_change_t fin_resume;
            memset(&fin_resume, 0, sizeof(fin_resume));
            fin_resume.op = HA_CFG_OP_PROMOTE_VOTER_FINALIZE;
            fin_resume.node_index = (uint32_t)idx;
            fin_resume.expected_epoch = epoch_now;
            fin_resume.voters_old_mask = voters_old_cur;
            fin_resume.voters_new_mask = voters_new_cur;
            fin_resume.learners_mask = learners;

            int frc = ha_submit_cfg_change(&fin_resume, epoch_now + 1, timeout_ms);
            if (frc == 0) ha_replicate_membership_snapshot_async();
            return frc;
        }
        return 0;
    }

    uint64_t epoch = atomic_load(&g_ha_runtime.config_epoch);

    ha_cfg_change_t begin;
    memset(&begin, 0, sizeof(begin));
    begin.op = HA_CFG_OP_PROMOTE_VOTER_JOINT_BEGIN;
    begin.node_index = (uint32_t)idx;
    begin.expected_epoch = epoch;
    begin.voters_old_mask = atomic_load(&g_ha_runtime.voters_old_mask);
    begin.voters_new_mask = atomic_load(&g_ha_runtime.voters_new_mask);
    begin.learners_mask = learners;
    begin.member = g_ha_config.nodes[idx];

    int rc = ha_submit_cfg_change(&begin, epoch + 1, timeout_ms);
    if (rc != 0) return rc;

    ha_cfg_change_t fin;
    memset(&fin, 0, sizeof(fin));
    fin.op = HA_CFG_OP_PROMOTE_VOTER_FINALIZE;
    fin.node_index = (uint32_t)idx;
    fin.expected_epoch = epoch + 1;
    fin.voters_old_mask = atomic_load(&g_ha_runtime.voters_old_mask);
    fin.voters_new_mask = atomic_load(&g_ha_runtime.voters_new_mask);
    fin.learners_mask = atomic_load(&g_ha_runtime.learners_mask);

    rc = ha_submit_cfg_change(&fin, epoch + 2, timeout_ms);
    if (rc == 0) ha_replicate_membership_snapshot_async();
    return rc;
}

static int ha_remove_member_local(const char *node_id, uint32_t timeout_ms) {
    if (!node_id || !*node_id) return -1;
    if (!HA_is_leader(&g_ha_runtime)) return -4;
    if (!HA_has_fresh_quorum()) return -3;

    int idx = ha_find_node_index_by_id(node_id);
    if (idx < 0) return -1;

    int leader_idx = atomic_load(&g_ha_runtime.current_leader);
    if (leader_idx >= 0 && leader_idx < HA_MAX_NODES) {
        if (strcmp(node_id, g_ha_config.nodes[leader_idx].node_id) == 0) {
            return -1;
        }
    }

    uint64_t epoch = atomic_load(&g_ha_runtime.config_epoch);
    uint64_t voters = atomic_load(&g_ha_runtime.voters_new_mask);
    uint64_t bit = HA_NODE_MASK(idx);

    if (voters & bit) {
        ha_cfg_change_t begin;
        memset(&begin, 0, sizeof(begin));
        begin.op = HA_CFG_OP_REMOVE_MEMBER_JOINT_BEGIN;
        begin.node_index = (uint32_t)idx;
        begin.expected_epoch = epoch;
        begin.voters_old_mask = atomic_load(&g_ha_runtime.voters_old_mask);
        begin.voters_new_mask = voters;
        begin.learners_mask = atomic_load(&g_ha_runtime.learners_mask);

        int rc = ha_submit_cfg_change(&begin, epoch + 1, timeout_ms);
        if (rc != 0) return rc;

        ha_cfg_change_t fin;
        memset(&fin, 0, sizeof(fin));
        fin.op = HA_CFG_OP_REMOVE_MEMBER_FINALIZE;
        fin.node_index = (uint32_t)idx;
        fin.expected_epoch = epoch + 1;
        fin.voters_old_mask = atomic_load(&g_ha_runtime.voters_old_mask);
        fin.voters_new_mask = atomic_load(&g_ha_runtime.voters_new_mask);
        fin.learners_mask = atomic_load(&g_ha_runtime.learners_mask);

        rc = ha_submit_cfg_change(&fin, epoch + 2, timeout_ms);
        if (rc == 0) ha_replicate_membership_snapshot_async();
        return rc;
    }

    ha_cfg_change_t fin;
    memset(&fin, 0, sizeof(fin));
    fin.op = HA_CFG_OP_REMOVE_MEMBER_FINALIZE;
    fin.node_index = (uint32_t)idx;
    fin.expected_epoch = epoch;
    fin.voters_old_mask = atomic_load(&g_ha_runtime.voters_old_mask);
    fin.voters_new_mask = voters;
    fin.learners_mask = atomic_load(&g_ha_runtime.learners_mask);

    int rc = ha_submit_cfg_change(&fin, epoch + 1, timeout_ms);
    if (rc == 0) ha_replicate_membership_snapshot_async();
    return rc;
}

static int ha_membership_timeout_ms(void) {
    uint32_t t = g_ha_config.replication_timeout_ms;
    if (t == 0) t = 3000;
    return (int)t;
}

static void* ha_worker_thread(void *arg) {
    (void)arg;

    uint64_t last_heartbeat = 0;
    uint64_t last_election_check = 0;
    uint64_t heartbeat_interval_us = g_ha_config.heartbeat_interval_ms * 1000ULL;
    uint64_t election_timeout_us = g_ha_config.election_timeout_ms * 1000ULL;
    uint64_t last_kick = HA_repl_kick_read();

    atomic_store(&g_ha_runtime.state, HA_STATE_READY);
    printf("HA worker thread started\n");

    while (atomic_load(&g_ha_running)) {
        uint64_t now = now_us();
        HA_publish_snapshot_tick(now);

        if (HA_is_leader(&g_ha_runtime)) {
            uint64_t kick = HA_repl_kick_read();
            if ((now - last_heartbeat >= heartbeat_interval_us) || (kick != last_kick)) {
                HA_replicate_to_followers();
                last_heartbeat = now;
                last_kick = kick;
            }

            HA_apply_committed_entries();

            if (!HA_has_fresh_quorum() && !atomic_load(&g_ha_runtime.bootstrap_gate_active)) {
                static uint64_t s_last_quorum_log = 0;
                if (now - s_last_quorum_log > 1000000ULL) {
                    printf("HA leader lost fresh quorum; stepping down\n");
                    s_last_quorum_log = now;
                }
                atomic_store(&g_ha_runtime.role, HA_ROLE_FOLLOWER);
                atomic_store(&g_ha_runtime.current_leader, -1);
            }
        } else if (atomic_load(&g_ha_runtime.role) == HA_ROLE_FOLLOWER) {
            uint64_t last_hb = atomic_load(&g_ha_runtime.last_heartbeat_us);
            if (now - last_election_check >= election_timeout_us) {
                if (now - last_hb >= election_timeout_us) {
                    if (HA_local_is_voter()) {
                        uint64_t jitter_us = (uint64_t)(rand() % 250) * 1000ULL;
                        usleep(jitter_us);
                        HA_start_election();
                    }
                }
                last_election_check = now;
            }
        }

        ha_publish_to_shmem();
        usleep(10000);
    }

    printf("HA worker thread stopped\n");
    return NULL;
}

int RAMForge_HA_init(void) {
    srand((unsigned)(time(NULL) ^ getpid()));

    const char *enabled = getenv("RF_HA_ENABLED");
    if (!enabled || strcmp(enabled, "1") != 0) {
        atomic_store(&g_ha_enabled, 0);
        return 0;
    }

    const char *agent = getenv("RF_HA_AGENT_ONLY");
    if (agent && agent[0] != '0') {
        atomic_store(&g_ha_agent_only, 1);
    }

    if (HA_init_from_env(&g_ha_config, &g_ha_runtime) != 0) {
        fprintf(stderr, "Failed to initialize HA configuration\n");
        return -1;
    }

    HA_replication_bind(&g_ha_config, &g_ha_runtime);
    ha_apply_pending_membership_if_any();

    if (HA_tls_init_from_env() != 0) {
        fprintf(stderr, "Failed to initialize HA mTLS\n");
        return -1;
    }

    if (!atomic_load(&g_ha_agent_only)) {
        if (HA_replication_init(&g_ha_config, &g_ha_runtime) != 0) {
            fprintf(stderr, "Failed to initialize HA replication\n");
            return -1;
        }
        if (HA_net_start_server(&g_ha_config, &g_ha_runtime) != 0) {
            fprintf(stderr, "Failed to start HA net server\n");
            return -1;
        }
    } else {
        printf("HA agent-only mode: reading HA state from shared memory\n");
    }

    if (g_ha_config.node_count == 1) {
        atomic_store(&g_ha_runtime.role, HA_ROLE_LEADER);
        atomic_store(&g_ha_runtime.current_leader, g_ha_config.local_node_index);
        atomic_store(&g_ha_runtime.state, HA_STATE_READY);
        atomic_store(&g_ha_runtime.bootstrap_gate_active, 0);
        atomic_store(&g_ha_runtime.bootstrap_quorum_confirmed, 1);
    }

    if (!atomic_load(&g_ha_agent_only) && g_ha_config.node_count > 1) {
        const char *boot = getenv("RF_HA_BOOTSTRAP_LEADER");
        if (boot && strcmp(boot, "1") == 0) {
            atomic_store(&g_ha_runtime.role, HA_ROLE_LEADER);
            atomic_store(&g_ha_runtime.current_leader, g_ha_config.local_node_index);
            atomic_store(&g_ha_runtime.state, HA_STATE_READY);
            atomic_store(&g_ha_runtime.bootstrap_gate_active, 1);
            atomic_store(&g_ha_runtime.bootstrap_quorum_confirmed, 0);
            atomic_store(&g_ha_runtime.bootstrap_gate_start_us, now_us());
            printf("Bootstrap: assuming LEADER (startup quorum gate active)\n");
        }
    }

    atomic_store(&g_ha_enabled, 1);

    if (!atomic_load(&g_ha_agent_only)) {
        atomic_store(&g_ha_running, 1);
        if (pthread_create(&g_ha_thread, NULL, ha_worker_thread, NULL) != 0) {
            fprintf(stderr, "Failed to create HA worker thread\n");
            HA_replication_shutdown();
            return -1;
        }
    }

    printf("HA initialized - role=%s node=%s\n",
           atomic_load(&g_ha_runtime.role) == HA_ROLE_LEADER ? "LEADER" : "FOLLOWER",
           g_ha_config.nodes[g_ha_config.local_node_index].node_id);
    return 0;
}

void RAMForge_HA_shutdown(void) {
    if (!atomic_load(&g_ha_enabled)) return;

    if (!atomic_load(&g_ha_agent_only)) {
        HA_net_stop_server();
        atomic_store(&g_ha_running, 0);
        pthread_join(g_ha_thread, NULL);
        HA_replication_shutdown();
    }

    HA_tls_shutdown();
}

int RAMForge_HA_is_enabled(void) {
    return atomic_load(&g_ha_enabled);
}

int RAMForge_HA_has_fresh_quorum(void) {
    if (!atomic_load(&g_ha_enabled)) return 1;
    if (!atomic_load(&g_ha_agent_only)) return HA_has_fresh_quorum();

    ha_shared_snapshot_t s;
    if (RAMForge_HA_read_snapshot(&s) && s.enabled) return s.has_fresh_quorum ? 1 : 0;
    return 0;
}

int RAMForge_HA_is_leader(void) {
    if (!atomic_load(&g_ha_enabled)) return 1;
    if (!atomic_load(&g_ha_agent_only)) return HA_is_leader(&g_ha_runtime);

    ha_shmem_state_t s;
    if (!ha_load_from_shmem(&s)) return 0;
    return (s.leader_idx == g_ha_config.local_node_index);
}

int RAMForge_HA_is_follower(void) {
    if (!atomic_load(&g_ha_enabled)) return 0;
    if (!atomic_load(&g_ha_agent_only)) return atomic_load(&g_ha_runtime.role) == HA_ROLE_FOLLOWER;

    ha_shmem_state_t s;
    if (!ha_load_from_shmem(&s)) return 1;
    return s.role == HA_ROLE_FOLLOWER;
}

const char* RAMForge_HA_get_leader_url(void) {
    if (!atomic_load(&g_ha_enabled)) return NULL;

    int leader_idx = -1;
    if (!atomic_load(&g_ha_agent_only)) {
        leader_idx = atomic_load(&g_ha_runtime.current_leader);
    } else {
        ha_shmem_state_t s;
        if (!ha_load_from_shmem(&s)) return NULL;
        leader_idx = s.leader_idx;
    }

    if (leader_idx < 0 || leader_idx >= g_ha_config.node_count) return NULL;
    return g_ha_config.nodes[leader_idx].http_addr;
}

int RAMForge_HA_get_redirect_status(void) {
    if (!atomic_load(&g_ha_enabled)) return 0;
    return g_ha_config.redirect_status;
}

int RAMForge_HA_get_retry_after(void) {
    if (!atomic_load(&g_ha_enabled)) return 0;
    return g_ha_config.retry_after_sec;
}

int RAMForge_HA_replicate_write(uint32_t record_type, const void *data, uint32_t size) {
    if (!atomic_load(&g_ha_enabled)) return 0;

    if (atomic_load(&g_ha_agent_only)) {
        return ha_fwd_send(record_type, data, size);
    }

    if (!HA_is_leader(&g_ha_runtime)) return -1;

    uint64_t idx = HA_append_local_entry(record_type, data, size);
    if (idx == 0) return -1;

    if (g_ha_config.sync_mode == 0) {
        HA_request_catchup();
        return 0;
    }

    return HA_wait_for_replication(idx, g_ha_config.replication_timeout_ms);
}

const char* RAMForge_HA_get_local_node_id(void) {
    if (!atomic_load(&g_ha_enabled)) return NULL;
    if (g_ha_config.local_node_index < 0 || g_ha_config.local_node_index >= g_ha_config.node_count) return NULL;
    return g_ha_config.nodes[g_ha_config.local_node_index].node_id;
}

const char* RAMForge_HA_get_leader_node_id(void) {
    if (!atomic_load(&g_ha_enabled)) return NULL;
    int leader_idx = atomic_load(&g_ha_runtime.current_leader);
    if (leader_idx < 0 || leader_idx >= g_ha_config.node_count) return NULL;
    return g_ha_config.nodes[leader_idx].node_id;
}

int RAMForge_HA_get_membership(ha_membership_view_t *out) {
    if (!atomic_load(&g_ha_enabled) || !out) return -1;
    return HA_get_membership_view(out);
}

int RAMForge_HA_add_member(const ha_node_config_t *node, int *out_index) {
    if (!atomic_load(&g_ha_enabled)) return -1;

    uint32_t timeout_ms = (uint32_t)ha_membership_timeout_ms();

    if (atomic_load(&g_ha_agent_only)) {
        ha_mem_fwd_req_t req;
        memset(&req, 0, sizeof(req));
        req.req_id = ha_next_req_id();
        req.op = HA_MEM_FWD_OP_ADD;
        req.timeout_ms = timeout_ms;
        req.priority = node ? node->priority : 0;
        if (node) {
            snprintf(req.node_id, sizeof(req.node_id), "%s", node->node_id);
            snprintf(req.advertise_addr, sizeof(req.advertise_addr), "%s", node->advertise_addr);
            snprintf(req.http_addr, sizeof(req.http_addr), "%s", node->http_addr);
        }
        return ha_forward_membership_req(&req, out_index);
    }

    int rc = ha_membership_change_begin();
    if (rc != 0) return rc;

    rc = ha_add_member_local(node, out_index, timeout_ms);
    ha_membership_change_end();
    return rc;
}

int RAMForge_HA_promote_member(const char *node_id) {
    if (!atomic_load(&g_ha_enabled)) return -1;

    uint32_t timeout_ms = (uint32_t)ha_membership_timeout_ms();

    if (atomic_load(&g_ha_agent_only)) {
        ha_mem_fwd_req_t req;
        memset(&req, 0, sizeof(req));
        req.req_id = ha_next_req_id();
        req.op = HA_MEM_FWD_OP_PROMOTE;
        req.timeout_ms = timeout_ms;
        snprintf(req.node_id, sizeof(req.node_id), "%s", node_id ? node_id : "");
        return ha_forward_membership_req(&req, NULL);
    }

    int rc = ha_membership_change_begin();
    if (rc != 0) return rc;

    rc = ha_promote_member_local(node_id, timeout_ms);
    ha_membership_change_end();
    return rc;
}

int RAMForge_HA_remove_member(const char *node_id) {
    if (!atomic_load(&g_ha_enabled)) return -1;

    uint32_t timeout_ms = (uint32_t)ha_membership_timeout_ms();

    if (atomic_load(&g_ha_agent_only)) {
        ha_mem_fwd_req_t req;
        memset(&req, 0, sizeof(req));
        req.req_id = ha_next_req_id();
        req.op = HA_MEM_FWD_OP_REMOVE;
        req.timeout_ms = timeout_ms;
        snprintf(req.node_id, sizeof(req.node_id), "%s", node_id ? node_id : "");
        return ha_forward_membership_req(&req, NULL);
    }

    int rc = ha_membership_change_begin();
    if (rc != 0) return rc;

    rc = ha_remove_member_local(node_id, timeout_ms);
    ha_membership_change_end();
    return rc;
}

int RAMForge_HA_handle_membership_forward(const void *payload, uint32_t size) {
    if (!payload || size < sizeof(ha_mem_fwd_req_t)) return -1;

    const ha_mem_fwd_req_t *req = (const ha_mem_fwd_req_t *)payload;
    int rc = -1;
    int idx = -1;

    if (!atomic_load(&g_ha_enabled)) {
        rc = -1;
    } else if (atomic_load(&g_ha_agent_only)) {
        rc = -4;
    } else {
        int inflight = ha_membership_change_begin();
        if (inflight != 0) {
            rc = inflight;
        } else {
            switch (req->op) {
                case HA_MEM_FWD_OP_ADD: {
                    ha_node_config_t node;
                    memset(&node, 0, sizeof(node));
                    snprintf(node.node_id, sizeof(node.node_id), "%s", req->node_id);
                    snprintf(node.advertise_addr, sizeof(node.advertise_addr), "%s", req->advertise_addr);
                    snprintf(node.http_addr, sizeof(node.http_addr), "%s", req->http_addr);
                    node.priority = req->priority;
                    node.is_voter = 0;
                    rc = ha_add_member_local(&node, &idx, req->timeout_ms ? req->timeout_ms : (uint32_t)ha_membership_timeout_ms());
                } break;
                case HA_MEM_FWD_OP_PROMOTE:
                    rc = ha_promote_member_local(req->node_id, req->timeout_ms ? req->timeout_ms : (uint32_t)ha_membership_timeout_ms());
                    break;
                case HA_MEM_FWD_OP_REMOVE:
                    rc = ha_remove_member_local(req->node_id, req->timeout_ms ? req->timeout_ms : (uint32_t)ha_membership_timeout_ms());
                    break;
                default:
                    rc = -1;
                    break;
            }
            ha_membership_change_end();
        }
    }

    if (ha_mem_debug_enabled()) {
        fprintf(stderr, "[HA-MEM][FWD-RSP] req_id=%lu op=%u rc=%d idx=%d epoch=%lu joint=%d old=%lu new=%lu learners=%lu\n",
                (unsigned long)req->req_id, (unsigned)req->op, rc, idx,
                (unsigned long)atomic_load(&g_ha_runtime.config_epoch),
                atomic_load(&g_ha_runtime.joint_active),
                (unsigned long)atomic_load(&g_ha_runtime.voters_old_mask),
                (unsigned long)atomic_load(&g_ha_runtime.voters_new_mask),
                (unsigned long)atomic_load(&g_ha_runtime.learners_mask));
    }
    ha_store_forward_response(req->req_id, rc, idx);
    return rc;
}

int RAMForge_HA_replay_record(uint32_t record_type, const void *data, size_t size) {
    if (!data) return 0;

    if (record_type == HA_LOG_CFG_CHANGE) {
        if (size < sizeof(ha_cfg_change_t)) return 1;
        if (!atomic_load(&g_ha_enabled)) return 1;
        (void)HA_apply_cfg_change_entry(data, (uint32_t)size);
        return 1;
    }

    if (record_type == AOF_REC_HA_MEMBERSHIP) {
        if (size < sizeof(ha_membership_record_t)) return 1;
        const ha_membership_record_t *rec = (const ha_membership_record_t *)data;
        if (!atomic_load(&g_ha_enabled)) {
            ha_store_pending_membership(rec);
            return 1;
        }
        (void)HA_apply_membership_snapshot(rec);
        return 1;
    }

    return 0;
}

void RAMForge_HA_export_metrics(char *buf, size_t cap) {
    if (!buf || cap == 0) return;

    if (!atomic_load(&g_ha_enabled)) {
        snprintf(buf, cap, "# HA disabled\n");
        return;
    }

    ha_shared_snapshot_t snap;
    if (!RAMForge_HA_read_snapshot(&snap)) {
        snprintf(buf, cap, "# HA snapshot unavailable\n");
        return;
    }

    snprintf(buf, cap,
             "ramforge_ha_role %d\n"
             "ramforge_ha_state %d\n"
             "ramforge_ha_term %lu\n"
             "ramforge_ha_epoch %lu\n"
             "ramforge_ha_joint_active %d\n"
             "ramforge_ha_fresh_quorum %d\n",
             snap.role,
             snap.state,
             (unsigned long)snap.term,
             (unsigned long)snap.config_epoch,
             snap.joint_active,
             snap.has_fresh_quorum);
}





