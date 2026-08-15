// ramforge_ha_replication.c - PATCHED v2: Fixed stack corruption on failed sends
#include "ramforge_ha_replication.h"
#include "ramforge_ha_config.h"
#include "crc32c.h"
#include "slab_alloc.h"
#include "rf_broker.h"
#include "aof_batch.h"
#include "ramforge_ha_integration.h"
#include <stdlib.h>
#include <string.h>
#include <pthread.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <arpa/inet.h>
#include <unistd.h>
#include <fcntl.h>
#include <errno.h>
#include <time.h>
#include <stdio.h>
#include <poll.h>
#include "ramforge_ha_tls.h"
#include "ramforge_ha_protocol.h"
#include "ramforge_ha_store.h"
#include "ramforge_ha_fault_injection.h"

// In-Memory Log Ring Buffer (Zero-Copy, Lock-Free)

#define LOG_RING_SIZE (1 << 22)     // 4M entries
#define LOG_RING_MASK (LOG_RING_SIZE - 1)
extern int rf_bus_publish(int rec_id, const void *payload, uint32_t size);

static _Atomic uint64_t g_repl_kick = 0;

void HA_request_catchup(void) {
    // count kicks; multiple producers can kick without losing signal
    atomic_fetch_add(&g_repl_kick, 1);
}


typedef struct {
    ha_log_entry_t *entry;          // NULL if slot empty
    _Atomic int refs;               // Reference count for GC
    uint64_t file_offset;           // durable-log byte offset
    _Atomic int apply_status;       // 0=pending, 1=applied, -1=failed
    uint64_t apply_batch_id;        // local business-AOF proof
} log_slot_t;

static struct {
    log_slot_t slots[LOG_RING_SIZE];
    _Atomic uint64_t head;          // Next index to write
    _Atomic uint64_t tail;          // Oldest retained index
    pthread_mutex_t append_lock;    // Only for coordinating appends
    pthread_mutex_t apply_lock;     // Serial committed state-machine apply
} g_log_ring;


static ha_config_t *g_config;
static ha_runtime_t *g_runtime;
static ha_store_t *g_store;
static pthread_mutex_t g_stable_transition_lock = PTHREAD_MUTEX_INITIALIZER;

static inline uint64_t now_us(void);

static inline int ha_popcount_u64(uint64_t x) {
#if defined(__GNUC__) || defined(__clang__)
    return __builtin_popcountll(x);
#else
    int n = 0;
    while (x) { x &= (x - 1); n++; }
    return n;
#endif
}

static inline int ha_majority_needed(uint64_t mask) {
    int n = ha_popcount_u64(mask);
    return n > 0 ? (n / 2) + 1 : 0;
}

static inline uint64_t ha_voters_old_mask(void) {
    return atomic_load(&g_runtime->voters_old_mask);
}

static inline uint64_t ha_voters_new_mask(void) {
    return atomic_load(&g_runtime->voters_new_mask);
}

static inline uint64_t ha_learners_mask(void) {
    return atomic_load(&g_runtime->learners_mask);
}

static inline int ha_joint_active(void) {
    return atomic_load(&g_runtime->joint_active);
}

static inline uint64_t ha_active_mask(void) {
    return ha_voters_old_mask() | ha_voters_new_mask() | ha_learners_mask();
}

int HA_is_peer_active(int peer_index) {
    if (!g_config || !g_runtime) return 0;
    if (peer_index < 0 || peer_index >= HA_MAX_NODES) return 0;
    if (!g_config->nodes[peer_index].node_id[0]) return 0;
    return (ha_active_mask() & HA_NODE_MASK(peer_index)) ? 1 : 0;
}

int HA_local_is_voter(void) {
    if (!g_config || !g_runtime) return 0;
    int local = g_config->local_node_index;
    if (local < 0 || local >= HA_MAX_NODES) return 0;
    return (ha_voters_new_mask() & HA_NODE_MASK(local)) ? 1 : 0;
}

static int ha_count_match_in_mask(uint64_t mask, uint64_t index) {
    if (!g_config || !g_runtime) return 0;
    int c = 0;
    int local = g_config->local_node_index;
    for (int i = 0; i < HA_MAX_NODES; i++) {
        uint64_t bit = HA_NODE_MASK(i);
        if (!(mask & bit)) continue;
        if (!HA_is_peer_active(i) && i != local) continue;
        if (i == local) {
            c++;
        } else if (atomic_load(&g_runtime->match_index[i]) >= index) {
            c++;
        }
    }
    return c;
}

static int ha_has_commit_quorum_for(uint64_t index) {
    uint64_t voters_new = ha_voters_new_mask();
    int need_new = ha_majority_needed(voters_new);
    if (need_new <= 0) return 0;

    int have_new = ha_count_match_in_mask(voters_new, index);
    if (have_new < need_new) return 0;

    if (!ha_joint_active()) return 1;

    uint64_t voters_old = ha_voters_old_mask();
    int need_old = ha_majority_needed(voters_old);
    if (need_old <= 0) return 0;

    int have_old = ha_count_match_in_mask(voters_old, index);
    return have_old >= need_old;
}

static uint64_t ha_fresh_window_us(void) {
    const char *e = getenv("RF_HA_FRESH_QUORUM_MS");
    uint64_t ms = 0;
    if (e && *e) {
        uint64_t v = strtoull(e, NULL, 10);
        if (v > 0) ms = v;
    }
    if (ms == 0) {
        uint64_t hb = g_config ? g_config->heartbeat_interval_ms : 100;
        uint64_t et = g_config ? g_config->election_timeout_ms : 1000;
        uint64_t a = hb * 6;
        uint64_t b = et / 2;
        ms = (a > b) ? a : b;
    }
    return ms * 1000ULL;
}

static int ha_count_fresh_in_mask(uint64_t mask, uint64_t now) {
    if (!g_config || !g_runtime) return 0;
    int local = g_config->local_node_index;
    int c = 0;
    uint64_t win = ha_fresh_window_us();

    for (int i = 0; i < HA_MAX_NODES; i++) {
        uint64_t bit = HA_NODE_MASK(i);
        if (!(mask & bit)) continue;
        if (!HA_is_peer_active(i) && i != local) continue;
        if (i == local) {
            c++;
            continue;
        }
        uint64_t ack = atomic_load(&g_runtime->last_ack_us[i]);
        if (ack && now >= ack && (now - ack) <= win) c++;
    }
    return c;
}

static int ha_has_fresh_quorum_now(uint64_t now) {
    uint64_t new_mask = ha_voters_new_mask();
    int need_new = ha_majority_needed(new_mask);
    if (need_new <= 0) return 0;

    int have_new = ha_count_fresh_in_mask(new_mask, now);
    if (have_new < need_new) return 0;

    if (!ha_joint_active()) return 1;

    uint64_t old_mask = ha_voters_old_mask();
    int need_old = ha_majority_needed(old_mask);
    if (need_old <= 0) return 0;

    int have_old = ha_count_fresh_in_mask(old_mask, now);
    return have_old >= need_old;
}

int HA_has_fresh_quorum(void) {
    if (!g_config || !g_runtime) return 0;
    if (!HA_is_leader(g_runtime)) return 0;
    if (!HA_local_is_voter()) return 0;

    uint64_t now = now_us();
    int raw = ha_has_fresh_quorum_now(now);

    if (atomic_load(&g_runtime->bootstrap_gate_active)) {
        if (raw) {
            atomic_store(&g_runtime->bootstrap_gate_active, 0);
            atomic_store(&g_runtime->bootstrap_quorum_confirmed, 1);
            atomic_store(&g_runtime->last_quorum_ok_us, now);
            atomic_store(&g_runtime->quorum_miss_streak, 0);
            return 1;
        }
        return 0;
    }

    if (raw) {
        atomic_store(&g_runtime->last_quorum_ok_us, now);
        atomic_store(&g_runtime->quorum_miss_streak, 0);
        return 1;
    }

    atomic_fetch_add(&g_runtime->quorum_miss_streak, 1);

    uint64_t grace_ms = g_config->election_timeout_ms;
    const char *g = getenv("RF_HA_QUORUM_GRACE_MS");
    if (g && *g) {
        uint64_t v = strtoull(g, NULL, 10);
        if (v > 0) grace_ms = v;
    }

    uint32_t miss_limit = 3;
    const char *m = getenv("RF_HA_QUORUM_MISS_STREAK");
    if (m && *m) {
        uint32_t v = (uint32_t)strtoul(m, NULL, 10);
        if (v > 0) miss_limit = v;
    }

    uint64_t last_ok = atomic_load(&g_runtime->last_quorum_ok_us);
    uint32_t miss = atomic_load(&g_runtime->quorum_miss_streak);

    if (last_ok && now >= last_ok && (now - last_ok) <= (grace_ms * 1000ULL) && miss <= miss_limit) {
        return 1;
    }

    return 0;
}

void HA_replication_bind(ha_config_t *config, ha_runtime_t *runtime) {
    g_config = config;
    g_runtime = runtime;
}
// Pre-allocated send buffer per thread to avoid stack issues
static __thread uint8_t *t_send_buf = NULL;
static __thread size_t t_send_buf_size = 0;

#define SEND_BUF_SIZE (1 << 20)  // 1MB

static uint8_t* get_send_buffer(void) {
    if (!t_send_buf) {
        t_send_buf = slab_alloc(SEND_BUF_SIZE);
        if (t_send_buf) {
            t_send_buf_size = SEND_BUF_SIZE;
        }
    }
    return t_send_buf;
}

static inline uint64_t now_us(void) {
    struct timespec ts;
    clock_gettime(CLOCK_MONOTONIC, &ts);
    return (uint64_t)ts.tv_sec * 1000000ULL + ts.tv_nsec / 1000;
}

static uint64_t term_at_unlocked(uint64_t idx) {
    if (idx == 0) return 0;
    ha_log_entry_t *e = g_log_ring.slots[idx & LOG_RING_MASK].entry;
    return (e && e->index == idx) ? e->term : 0;
}

static uint64_t term_at(uint64_t idx) {
    pthread_mutex_lock(&g_log_ring.append_lock);
    uint64_t term = term_at_unlocked(idx);
    pthread_mutex_unlock(&g_log_ring.append_lock);
    return term;
}

static int ha_has_election_quorum(uint64_t granted_mask) {
    uint64_t voters_new = ha_voters_new_mask();
    int need_new = ha_majority_needed(voters_new);
    if (need_new <= 0 || ha_popcount_u64(granted_mask & voters_new) < need_new)
        return 0;
    if (!ha_joint_active()) return 1;
    uint64_t voters_old = ha_voters_old_mask();
    int need_old = ha_majority_needed(voters_old);
    return need_old > 0 &&
           ha_popcount_u64(granted_mask & voters_old) >= need_old;
}

static int persist_state_values(uint64_t term, int voted_for,
                                uint64_t commit_index, uint64_t applied_index) {
    if (!g_store || applied_index > commit_index) return -1;
    ha_stable_state_t state = {
        .term = term,
        .voted_for = voted_for,
        .commit_index = commit_index,
        .applied_index = applied_index
    };
    if (ha_store_persist_state(g_store, &state) != 0) {
        if (g_runtime) atomic_store(&g_runtime->state, HA_STATE_FAILED);
        return -1;
    }
    return 0;
}

int HA_step_down_to_term(uint64_t term, int leader_index) {
    if (!g_runtime || !g_store) return -1;
    pthread_mutex_lock(&g_stable_transition_lock);
    uint64_t current = atomic_load(&g_runtime->term);
    if (term < current) {
        pthread_mutex_unlock(&g_stable_transition_lock);
        return -1;
    }
    int voted_for = (term > current) ? -1 : atomic_load(&g_runtime->voted_for);
    if (term > current && persist_state_values(term, voted_for,
                                               atomic_load(&g_runtime->commit_index),
                                               atomic_load(&g_runtime->applied_index)) != 0) {
        pthread_mutex_unlock(&g_stable_transition_lock);
        return -1;
    }
    atomic_store(&g_runtime->term, term);
    atomic_store(&g_runtime->voted_for, voted_for);
    atomic_store(&g_runtime->role, HA_ROLE_FOLLOWER);
    atomic_store(&g_runtime->current_leader, leader_index);
    atomic_store(&g_runtime->votes_granted, 0);
    atomic_store(&g_runtime->votes_seen_mask, 0);
    atomic_store(&g_runtime->votes_granted_mask, 0);
    pthread_mutex_unlock(&g_stable_transition_lock);
    return 0;
}

int HA_grant_vote_durable(uint64_t term, int candidate_index) {
    if (!g_runtime || !g_store || candidate_index < 0 || candidate_index >= HA_MAX_NODES) return -1;
    pthread_mutex_lock(&g_stable_transition_lock);
    uint64_t current = atomic_load(&g_runtime->term);
    int current_vote = atomic_load(&g_runtime->voted_for);
    if (term < current || (term == current && current_vote != -1 && current_vote != candidate_index)) {
        pthread_mutex_unlock(&g_stable_transition_lock);
        return -1;
    }
    int vote = candidate_index;
    if (term != current || current_vote != vote) {
        if (persist_state_values(term, vote,
                                 atomic_load(&g_runtime->commit_index),
                                 atomic_load(&g_runtime->applied_index)) != 0) {
            pthread_mutex_unlock(&g_stable_transition_lock);
            return -1;
        }
    }
    atomic_store(&g_runtime->term, term);
    atomic_store(&g_runtime->voted_for, vote);
    atomic_store(&g_runtime->role, HA_ROLE_FOLLOWER);
    atomic_store(&g_runtime->current_leader, -1);
    pthread_mutex_unlock(&g_stable_transition_lock);
    return 0;
}

int HA_begin_election_durable(int local_index, uint64_t *new_term) {
    if (!g_runtime || !g_store || local_index < 0 || local_index >= HA_MAX_NODES) return -1;
    pthread_mutex_lock(&g_stable_transition_lock);
    uint64_t term = atomic_load(&g_runtime->term) + 1;
    if (persist_state_values(term, local_index,
                             atomic_load(&g_runtime->commit_index),
                             atomic_load(&g_runtime->applied_index)) != 0) {
        pthread_mutex_unlock(&g_stable_transition_lock);
        return -1;
    }
    atomic_store(&g_runtime->term, term);
    atomic_store(&g_runtime->voted_for, local_index);
    atomic_store(&g_runtime->role, HA_ROLE_CANDIDATE);
    atomic_store(&g_runtime->current_leader, -1);
    atomic_store(&g_runtime->votes_granted, 1);
    atomic_store(&g_runtime->votes_seen_mask, HA_NODE_MASK(local_index));
    atomic_store(&g_runtime->votes_granted_mask, HA_NODE_MASK(local_index));
    pthread_mutex_unlock(&g_stable_transition_lock);
    if (new_term) *new_term = term;
    return 0;
}

int HA_update_commit_index(uint64_t commit_index) {
    if (!g_runtime || !g_store) return -1;
    pthread_mutex_lock(&g_stable_transition_lock);
    uint64_t current = atomic_load(&g_runtime->commit_index);
    uint64_t last = atomic_load(&g_log_ring.head) - 1;
    if (commit_index <= current) {
        pthread_mutex_unlock(&g_stable_transition_lock);
        return 0;
    }
    if (commit_index > last ||
        persist_state_values(atomic_load(&g_runtime->term),
                             atomic_load(&g_runtime->voted_for),
                             commit_index,
                             atomic_load(&g_runtime->applied_index)) != 0) {
        pthread_mutex_unlock(&g_stable_transition_lock);
        return -1;
    }
    atomic_store(&g_runtime->commit_index, commit_index);
    pthread_mutex_unlock(&g_stable_transition_lock);
    return 0;
}

void HA_get_last_log_info(uint64_t *last_index, uint64_t *last_term) {
    uint64_t last = atomic_load(&g_log_ring.head) - 1;
    if (last_index) *last_index = last;
    if (last_term) *last_term = term_at(last);
}

int HA_log_matches(uint64_t prev_log_index, uint64_t prev_log_term) {
    uint64_t last = atomic_load(&g_log_ring.head) - 1;
    return ha_prev_log_matches(prev_log_index, prev_log_term, last,
                               term_at(prev_log_index));
}

/* recompute commit index by majority rule */
static void leader_maybe_advance_commit(void) {
    uint64_t last_local = atomic_load(&g_log_ring.head) - 1;
    if (last_local == 0) return;

    uint64_t cur_commit = atomic_load(&g_runtime->commit_index);
    for (uint64_t cand = last_local; cand > cur_commit; cand--) {
        if (term_at(cand) != atomic_load(&g_runtime->term)) continue;
        if (ha_has_commit_quorum_for(cand)) {
            (void)HA_update_commit_index(cand);
            return;
        }
    }
}

int HA_leader_on_ack(int peer_index, uint64_t match_index) {
    if (!HA_is_leader(g_runtime)) return -1;

    /* monotonic */
    uint64_t cur = atomic_load(&g_runtime->match_index[peer_index]);
    while (match_index > cur &&
           !atomic_compare_exchange_weak(&g_runtime->match_index[peer_index], &cur, match_index)) {}

    atomic_store(&g_runtime->next_index[peer_index], match_index + 1);
    leader_maybe_advance_commit();
    return 0;
}

// Log Entry Management

static ha_log_entry_t* create_log_entry_with_term(uint64_t index, uint64_t term,
                                                  uint64_t timestamp_us,
                                                  uint32_t type,
                                                  const void *data, uint32_t size) {
    size_t total_size = sizeof(ha_log_entry_t) + size;
    ha_log_entry_t *entry = slab_alloc(total_size);
    if (!entry) return NULL;

    entry->index = index;
    entry->term = term;
    entry->timestamp_us = timestamp_us;
    entry->type = type;
    entry->size = size;
    entry->reserved = 0;

    if (data && size) {
        memcpy(entry->data, data, size);
    }

    // Calculate CRC over entry (excluding CRC field itself)
    entry->crc = crc32c(0, entry, offsetof(ha_log_entry_t, crc));
    if (size) {
        entry->crc = crc32c(entry->crc, entry->data, size);
    }

    return entry;
}

static ha_log_entry_t* create_log_entry(uint64_t index, uint32_t type,
                                        const void *data, uint32_t size) {
    return create_log_entry_with_term(index, atomic_load(&g_runtime->term),
                                      now_us(), type, data, size);
}

static void release_log_entry(ha_log_entry_t *entry) {
    if (entry) {
        slab_free(entry);
    }
}

// Network Layer (PATCHED v2: Robust Connection Handling)

typedef struct {
    int fd;
    SSL *ssl;
    struct sockaddr_in addr;
    _Atomic uint64_t last_contact_us;
    _Atomic uint64_t last_connect_attempt_us;
    _Atomic int connected;
    pthread_mutex_t lock;
} peer_conn_t;

static peer_conn_t g_peer_conns[HA_MAX_NODES];

// Better connection validation
static int is_socket_alive(int fd) {
    if (fd < 0) return 0;

    int error = 0;
    socklen_t len = sizeof(error);

    // Check socket error state
    if (getsockopt(fd, SOL_SOCKET, SO_ERROR, &error, &len) < 0) {
        return 0;
    }
    if (error != 0) {
        return 0;
    }

    // Quick poll to check for hangup
    struct pollfd pfd = { .fd = fd, .events = POLLOUT, .revents = 0 };
    int pr = poll(&pfd, 1, 0);
    if (pr < 0) return 0;
    if (pfd.revents & (POLLERR | POLLHUP | POLLNVAL)) {
        return 0;
    }

    return 1;
}

// Clean up connection gracefully - MUST be called with lock NOT held
static void cleanup_peer_connection_locked(peer_conn_t *conn) {
    // Already holding lock when called
    if (conn->ssl) {
        HA_tls_graceful_shutdown(conn->ssl);
        SSL_free(conn->ssl);
        conn->ssl = NULL;
    }
    if (conn->fd >= 0) {
        close(conn->fd);
        conn->fd = -1;
    }
    atomic_store(&conn->connected, 0);
}

// Proper connection establishment with timeout
static int connect_to_peer(int peer_index) {
    if (!g_config) return -1;
    if (peer_index == g_config->local_node_index) return -1;
    if (peer_index < 0 || peer_index >= HA_MAX_NODES) return -1;
    if (!HA_is_peer_active(peer_index)) return -1;

    peer_conn_t *conn = &g_peer_conns[peer_index];

    pthread_mutex_lock(&conn->lock);

    // Check if already connected and valid
    if (atomic_load(&conn->connected) && conn->fd >= 0) {
        if (is_socket_alive(conn->fd)) {
            int fd = conn->fd;
            pthread_mutex_unlock(&conn->lock);
            return fd;
        }
        // Socket is dead, clean up
        cleanup_peer_connection_locked(conn);
    }

    // Throttle reconnection attempts (max once per 100ms)
    uint64_t now = now_us();
    uint64_t last_attempt = atomic_load(&conn->last_connect_attempt_us);
    if (now - last_attempt < 100000) {
        pthread_mutex_unlock(&conn->lock);
        return -1;
    }
    atomic_store(&conn->last_connect_attempt_us, now);

    // Parse address
    const char *addr_str = g_config->nodes[peer_index].advertise_addr;
    if (!addr_str) {
        pthread_mutex_unlock(&conn->lock);
        return -1;
    }

    char host[256] = {0};
    int port = 0;

    if (sscanf(addr_str, "%255[^:]:%d", host, &port) != 2) {
        pthread_mutex_unlock(&conn->lock);
        return -1;
    }

    // Create socket
    int fd = socket(AF_INET, SOCK_STREAM, 0);
    if (fd < 0) {
        pthread_mutex_unlock(&conn->lock);
        return -1;
    }

    // Set socket options for low latency
    int flag = 1;
    setsockopt(fd, IPPROTO_TCP, TCP_NODELAY, &flag, sizeof(flag));
    setsockopt(fd, SOL_SOCKET, SO_KEEPALIVE, &flag, sizeof(flag));

    // Set send/receive buffers
    int bufsize = 256 * 1024;
    setsockopt(fd, SOL_SOCKET, SO_SNDBUF, &bufsize, sizeof(bufsize));
    setsockopt(fd, SOL_SOCKET, SO_RCVBUF, &bufsize, sizeof(bufsize));

    // Set connection timeout (1 second)
    struct timeval timeout;
    timeout.tv_sec = 1;
    timeout.tv_usec = 0;
    setsockopt(fd, SOL_SOCKET, SO_RCVTIMEO, &timeout, sizeof(timeout));
    setsockopt(fd, SOL_SOCKET, SO_SNDTIMEO, &timeout, sizeof(timeout));

    // Setup address
    memset(&conn->addr, 0, sizeof(conn->addr));
    conn->addr.sin_family = AF_INET;
    conn->addr.sin_port = htons(port);

    if (inet_pton(AF_INET, host, &conn->addr.sin_addr) <= 0) {
        close(fd);
        pthread_mutex_unlock(&conn->lock);
        return -1;
    }

    // BLOCKING connect with timeout
    int ret = connect(fd, (struct sockaddr*)&conn->addr, sizeof(conn->addr));
    if (ret < 0) {
        close(fd);
        pthread_mutex_unlock(&conn->lock);
        return -1;
    }

    // Make socket non-blocking for I/O
    int flags = fcntl(fd, F_GETFL, 0);
    if (flags >= 0) {
        fcntl(fd, F_SETFL, flags | O_NONBLOCK);
    }

    // TLS wrap (mTLS)
    conn->ssl = NULL;
    if (HA_tls_enabled()) {
        char sni[256] = {0};
        const char *last_colon = strrchr(addr_str, ':');
        size_t hn = (last_colon && last_colon > addr_str) ? (size_t)(last_colon - addr_str) : 0;
        if (hn >= sizeof(sni)) hn = sizeof(sni) - 1;
        if (hn) memcpy(sni, addr_str, hn);
        sni[hn] = 0;

        SSL *ssl = HA_tls_new_client(fd, sni[0] ? sni : NULL);
        if (!ssl) {
            close(fd);
            pthread_mutex_unlock(&conn->lock);
            return -1;
        }

        int tmo = 1000;
        const char *t = getenv("RF_HA_TLS_TIMEOUT_MS");
        if (t && *t) {
            int v = atoi(t);
            if (v > 0) tmo = v;
        }

        if (HA_tls_handshake(ssl, fd, 0, tmo) != 0) {
            SSL_free(ssl);
            close(fd);
            pthread_mutex_unlock(&conn->lock);
            return -1;
        }

        if (!HA_tls_verify_peer_identity(ssl, g_config, peer_index)) {
            SSL_free(ssl);
            close(fd);
            pthread_mutex_unlock(&conn->lock);
            return -1;
        }
        conn->ssl = ssl;
    }

    conn->fd = fd;
    atomic_store(&conn->connected, 1);
    atomic_store(&conn->last_contact_us, now);

    pthread_mutex_unlock(&conn->lock);

    printf("[HA-CONNECT] Connected to peer %d (%s:%d) fd=%d\n",
           peer_index, host, port, fd);

    return fd;
}

// Clean up connection - called on send failure
static __attribute__((unused)) void cleanup_peer_connection(int peer_index, int expected_fd) {
    if (peer_index < 0 || peer_index >= HA_MAX_NODES) return;

    peer_conn_t *conn = &g_peer_conns[peer_index];

    pthread_mutex_lock(&conn->lock);
    if (expected_fd < 0 || conn->fd == expected_fd) {
        cleanup_peer_connection_locked(conn);
    }
    pthread_mutex_unlock(&conn->lock);
}

int send_message(int peer_index, ha_msg_header_t *header,
                 const void *payload, size_t payload_size) {
    if (!g_config) return -1;
    if (peer_index < 0 || peer_index >= HA_MAX_NODES) return -1;
    if (!HA_is_peer_active(peer_index)) return -1;
    if (HA_test_link_is_partitioned(g_config->local_node_index, peer_index))
        return -1;

    int fd = connect_to_peer(peer_index);
    if (fd < 0) return -1;

    // Prepare header
    header->magic = 0xA01;
    header->from_node = g_config->local_node_index;
    header->to_node = peer_index;
    header->payload_size = payload_size;
    header->crc = crc32c(0, header, offsetof(ha_msg_header_t, crc));

    peer_conn_t *pc = &g_peer_conns[peer_index];

    // Lock for the duration of the send to prevent races
    pthread_mutex_lock(&pc->lock);

    // Re-check fd is still valid
    if (pc->fd != fd || !atomic_load(&pc->connected)) {
        pthread_mutex_unlock(&pc->lock);
        return -1;
    }

    int result = 0;

    if (HA_tls_enabled() && pc->ssl) {
        int tmo = 1000;
        const char *t = getenv("RF_HA_TLS_TIMEOUT_MS");
        if (t && *t) {
            int v = atoi(t);
            if (v > 0) tmo = v;
        }

        ssize_t wr = HA_tls_writen(pc->ssl, fd, header, sizeof(*header), tmo);
        if (wr != (ssize_t)sizeof(*header)) {
            result = -1;
        } else if (payload && payload_size) {
            wr = HA_tls_writen(pc->ssl, fd, payload, payload_size, tmo);
            if (wr != (ssize_t)payload_size) {
                result = -1;
            }
        }
    } else {
        // Non-TLS path
        size_t sent = 0;
        int retries = 0;

        while (sent < sizeof(*header) && result == 0) {
            ssize_t n = send(fd, (char*)header + sent, sizeof(*header) - sent, MSG_NOSIGNAL);
            if (n > 0) {
                sent += n;
                retries = 0;
            } else if (n < 0) {
                if (errno == EAGAIN || errno == EWOULDBLOCK) {
                    if (++retries > 100) { result = -1; break; }
                    struct pollfd pfd = {fd, POLLOUT, 0};
                    int pr = poll(&pfd, 1, 100);
                    if (pr <= 0 || (pfd.revents & (POLLERR | POLLHUP))) {
                        result = -1;
                        break;
                    }
                    continue;
                }
                result = -1;
            }
        }

        if (result == 0 && payload && payload_size > 0) {
            sent = 0;
            retries = 0;
            while (sent < payload_size && result == 0) {
                ssize_t n = send(fd, (char*)payload + sent, payload_size - sent, MSG_NOSIGNAL);
                if (n > 0) {
                    sent += n;
                    retries = 0;
                } else if (n < 0) {
                    if (errno == EAGAIN || errno == EWOULDBLOCK) {
                        if (++retries > 100) { result = -1; break; }
                        struct pollfd pfd = {fd, POLLOUT, 0};
                        int pr = poll(&pfd, 1, 100);
                        if (pr <= 0 || (pfd.revents & (POLLERR | POLLHUP))) {
                            result = -1;
                            break;
                        }
                        continue;
                    }
                    result = -1;
                }
            }
        }
    }

    if (result == 0) {
        atomic_store(&pc->last_contact_us, now_us());
        pthread_mutex_unlock(&pc->lock);
        return 0;
    }

    // Failed - cleanup while still holding lock
    cleanup_peer_connection_locked(pc);
    pthread_mutex_unlock(&pc->lock);
    return -1;
}

// Leader Election & Heartbeats

/* helper: copy up to N entries starting at index 'from' into a flat buffer */
static size_t pack_entries(uint64_t from, uint32_t max,
                           uint8_t *dst, size_t cap, uint64_t *last_idx_out) {
    size_t used = 0;
    uint64_t i = from;
    pthread_mutex_lock(&g_log_ring.append_lock);
    for (uint32_t n = 0; n < max; n++, i++) {
        uint64_t slot = i & LOG_RING_MASK;
        log_slot_t *s = &g_log_ring.slots[slot];
        ha_log_entry_t *e = s->entry;
        if (!e || e->index != i) break;
        size_t need = sizeof(*e) + e->size;
        if (used + need > cap) break;
        memcpy(dst + used, e, need);
        used += need;
    }
    pthread_mutex_unlock(&g_log_ring.append_lock);
    if (last_idx_out) *last_idx_out = (i > from) ? (i - 1) : 0;
    return used;
}

// Send heartbeat with actual log entries when available
// FIXED: Use heap allocation instead of alloca to prevent stack overflow
static void send_heartbeat_with_entries(void) {
    if (!g_runtime || !g_config) return;
    if (!HA_is_leader(g_runtime)) return;

    uint8_t *buf = get_send_buffer();
    if (!buf) return;

    size_t buf_max = SEND_BUF_SIZE;
    if (g_config->ha_max_msg_bytes && g_config->ha_max_msg_bytes < buf_max)
        buf_max = g_config->ha_max_msg_bytes;

    // CHANGE 1: Much higher budget during catchup
    uint64_t start = now_us();
    uint64_t budget_us = 50000; // 50ms default, was 5ms
    const char *b = getenv("RF_HA_REPL_BUDGET_US");
    if (b && *b) {
        uint64_t v = strtoull(b, NULL, 10);
        if (v > 0) budget_us = v;
    }

    // CHANGE 2: Larger batch size for catchup
    uint32_t batch_size = g_config->replication_batch_size ?: 256;

    int local = g_config->local_node_index;
    uint64_t last_local = atomic_load(&g_log_ring.head) - 1;

    for (int peer = 0; peer < HA_MAX_NODES; peer++) {
        if (peer == local) continue;
        if (!HA_is_peer_active(peer)) continue;

        uint64_t next = atomic_load(&g_runtime->next_index[peer]);
        if (next == 0) next = 1;

        // CHANGE 4: Aggressive catchup - send multiple batches per peer if behind
        uint64_t gap = (last_local >= next) ? (last_local - next + 1) : 0;
        int batches_this_peer = (gap > batch_size * 4) ? 8 : 1;  // Up to 8 batches if way behind

        for (int batch = 0; batch < batches_this_peer; batch++) {
            if (budget_us && (now_us() - start) > budget_us) break;

            if (next > last_local) break;  // Caught up

            ha_append_entries_t *ae = (ha_append_entries_t *)buf;
            uint8_t *payload = buf + sizeof(*ae);
            ae->commit_index = atomic_load(&g_runtime->commit_index);
            ae->prev_log_index = (next > 1) ? (next - 1) : 0;
            ae->prev_log_term = term_at(ae->prev_log_index);
            ae->entry_count = 0;
            ae->reserved = 0;

            uint64_t last_sent = 0;
            size_t payload_sz = pack_entries(next, batch_size, payload,
                                             buf_max - sizeof(*ae), &last_sent);

            if (payload_sz) {
                ae->entry_count = (uint32_t)(last_sent - next + 1);
                ha_msg_header_t h = {
                        .type = HA_MSG_APPEND_ENTRIES,
                        .term = atomic_load(&g_runtime->term)
                };
                if (send_message(peer, &h, buf, sizeof(*ae) + payload_sz) == 0 && last_sent) {
                    /* Advance only local send cursor; global next_index moves on follower ACK. */
                    next = last_sent + 1;
                } else {
                    break;  // Send failed, retry from same next index on next tick
                }
            } else {
                break;  // No more entries to pack
            }
        }

        // Still send empty heartbeat if nothing to replicate
        if (atomic_load(&g_runtime->next_index[peer]) > last_local) {
            ha_append_entries_t ae = {
                .commit_index = atomic_load(&g_runtime->commit_index),
                .prev_log_index = last_local,
                .prev_log_term = term_at(last_local),
                .entry_count = 0
            };
            ha_msg_header_t h = {.type = HA_MSG_HEARTBEAT, .term = atomic_load(&g_runtime->term)};
            (void)send_message(peer, &h, &ae, sizeof(ae));
        }
    }

    atomic_fetch_add(&g_runtime->heartbeats_sent, 1);
}

static void start_election(void) {
    if (!g_runtime || !g_config) return;
    if (!HA_local_is_voter()) return;

    uint64_t new_term = 0;
    if (HA_begin_election_durable(g_config->local_node_index, &new_term) != 0) {
        atomic_store(&g_runtime->state, HA_STATE_FAILED);
        return;
    }
    atomic_fetch_add(&g_runtime->elections_started, 1);

    printf("Starting election for term %lu\n", new_term);

    uint64_t voter_mask = ha_voters_new_mask();
    if (ha_majority_needed(voter_mask) <= 0) {
        (void)HA_step_down_to_term(new_term, -1);
        return;
    }

    uint64_t last_index = 0;
    uint64_t last_term = 0;
    HA_get_last_log_info(&last_index, &last_term);

    for (int i = 0; i < HA_MAX_NODES; i++) {
        if (i == g_config->local_node_index) continue;
        if (!(voter_mask & HA_NODE_MASK(i))) continue;
        if (!HA_is_peer_active(i)) continue;

        ha_msg_header_t header = {
                .type = HA_MSG_VOTE_REQUEST,
                .term = new_term
        };

        ha_vote_request_t request = {
                .last_log_index = last_index,
                .last_log_term = last_term,
                .priority = g_config->nodes[g_config->local_node_index].priority
        };

        // Ignore send failures
        send_message(i, &header, &request, sizeof(request));
    }

    uint64_t start = now_us();
    uint64_t timeout_us = g_config->election_timeout_ms * 1000;

    while ((now_us() - start) < timeout_us) {
        uint64_t granted_mask = atomic_load(&g_runtime->votes_granted_mask);
        if (ha_has_election_quorum(granted_mask)) {
            pthread_rwlock_wrlock(&g_runtime->state_lock);

            if (atomic_load(&g_runtime->term) == new_term &&
                atomic_load(&g_runtime->role) == HA_ROLE_CANDIDATE) {
                atomic_store(&g_runtime->role, HA_ROLE_LEADER);
                atomic_store(&g_runtime->current_leader, g_config->local_node_index);
                atomic_store(&g_runtime->state, HA_STATE_READY);
                atomic_fetch_add(&g_runtime->elections_won, 1);

                uint64_t next_idx = atomic_load(&g_log_ring.head);
                for (int i = 0; i < HA_MAX_NODES; i++) {
                    atomic_store(&g_runtime->next_index[i], next_idx);
                    atomic_store(&g_runtime->match_index[i], 0);
                }
                atomic_store(&g_runtime->match_index[g_config->local_node_index],
                             next_idx - 1);

                printf("âœ… Won election, now LEADER for term %lu\n", new_term);
            }

            int won = atomic_load(&g_runtime->role) == HA_ROLE_LEADER &&
                      atomic_load(&g_runtime->term) == new_term;
            pthread_rwlock_unlock(&g_runtime->state_lock);
            if (!won) return;
            if (HA_append_local_entry(HA_LOG_NOOP, NULL, 0) == 0) {
                atomic_store(&g_runtime->state, HA_STATE_FAILED);
                return;
            }
            send_heartbeat_with_entries();
            return;
        }

        usleep(10000);
    }

    if (atomic_load(&g_runtime->term) == new_term &&
        atomic_load(&g_runtime->role) == HA_ROLE_CANDIDATE)
        (void)HA_step_down_to_term(new_term, -1);
    printf("Election timeout for term %lu\n", new_term);
}

int HA_start_election(void) {
    start_election();
    return 0;
}

// Public API Implementation

static int restore_log_entry(uint64_t index, uint64_t term,
                             uint64_t timestamp_us, uint32_t type,
                             const void *payload, uint32_t size,
                             uint64_t file_offset, void *ctx) {
    (void)ctx;
    if (index == 0 || index >= LOG_RING_SIZE) {
        errno = EOVERFLOW;
        return -1;
    }
    ha_log_entry_t *entry = create_log_entry_with_term(index, term, timestamp_us,
                                                       type, payload, size);
    if (!entry) return -1;
    log_slot_t *slot = &g_log_ring.slots[index & LOG_RING_MASK];
    if (slot->entry) {
        release_log_entry(entry);
        errno = EBADMSG;
        return -1;
    }
    slot->entry = entry;
    slot->file_offset = file_offset;
    slot->apply_batch_id = 0;
    atomic_store(&slot->refs, 1);
    atomic_store(&slot->apply_status, 0);
    atomic_store(&g_log_ring.head, index + 1);
    return 0;
}

int HA_replication_init(ha_config_t *config, ha_runtime_t *runtime) {
    HA_replication_bind(config, runtime);

    memset(&g_log_ring, 0, sizeof(g_log_ring));
    atomic_store(&g_log_ring.head, 1);
    atomic_store(&g_log_ring.tail, 1);

    if (pthread_mutex_init(&g_log_ring.append_lock, NULL) != 0) {
        return -1;
    }
    if (pthread_mutex_init(&g_log_ring.apply_lock, NULL) != 0) {
        pthread_mutex_destroy(&g_log_ring.append_lock);
        return -1;
    }

    ha_stable_state_t recovered;
    const char *node_id = config->nodes[config->local_node_index].node_id;
    if (ha_store_open(&g_store, node_id, config->ha_max_msg_bytes,
                      &recovered, restore_log_entry, NULL) != 0) {
        fprintf(stderr, "HA replication: durable store recovery failed: %s\n", strerror(errno));
        pthread_mutex_destroy(&g_log_ring.apply_lock);
        pthread_mutex_destroy(&g_log_ring.append_lock);
        return -1;
    }
    uint64_t last_index = atomic_load(&g_log_ring.head) - 1;
    uint64_t last_term = term_at(last_index);
    if (recovered.term < last_term || recovered.commit_index > last_index ||
        recovered.applied_index > recovered.commit_index) {
        fprintf(stderr, "HA replication: stable state/log relationship is invalid\n");
        ha_store_close(g_store);
        g_store = NULL;
        pthread_mutex_destroy(&g_log_ring.apply_lock);
        pthread_mutex_destroy(&g_log_ring.append_lock);
        return -1;
    }
    atomic_store(&runtime->term, recovered.term);
    atomic_store(&runtime->voted_for, recovered.voted_for);
    atomic_store(&runtime->commit_index, recovered.commit_index);
    atomic_store(&runtime->applied_index, recovered.applied_index);
    for (uint64_t idx = 1; idx <= last_index; idx++) {
        log_slot_t *slot = &g_log_ring.slots[idx & LOG_RING_MASK];
        if (!slot->entry || slot->entry->index != idx ||
            rf_idemp_restore_raft_state(slot->entry->type, slot->entry->data,
                                        slot->entry->size, idx,
                                        idx <= recovered.commit_index,
                                        idx <= recovered.applied_index) < 0) {
            fprintf(stderr, "HA replication: failed to restore idempotency state at index %lu\n",
                    (unsigned long)idx);
            ha_store_close(g_store);
            g_store = NULL;
            pthread_mutex_destroy(&g_log_ring.apply_lock);
            pthread_mutex_destroy(&g_log_ring.append_lock);
            return -1;
        }
    }
    for (uint64_t idx = 1; idx <= recovered.applied_index; idx++) {
        log_slot_t *slot = &g_log_ring.slots[idx & LOG_RING_MASK];
        if (!slot->entry || slot->entry->index != idx) {
            fprintf(stderr, "HA replication: applied index references a missing log entry\n");
            ha_store_close(g_store);
            g_store = NULL;
            pthread_mutex_destroy(&g_log_ring.apply_lock);
            pthread_mutex_destroy(&g_log_ring.append_lock);
            return -1;
        }
        atomic_store(&slot->apply_status, 1);
    }
    if (persist_state_values(recovered.term, recovered.voted_for,
                             recovered.commit_index, recovered.applied_index) != 0) {
        ha_store_close(g_store);
        g_store = NULL;
        pthread_mutex_destroy(&g_log_ring.apply_lock);
        pthread_mutex_destroy(&g_log_ring.append_lock);
        return -1;
    }

    // Initialize peer connections
    for (int i = 0; i < HA_MAX_NODES; i++) {
        g_peer_conns[i].fd = -1;
        g_peer_conns[i].ssl = NULL;
        atomic_store(&g_peer_conns[i].connected, 0);
        atomic_store(&g_peer_conns[i].last_contact_us, 0);
        atomic_store(&g_peer_conns[i].last_connect_attempt_us, 0);
        pthread_mutex_init(&g_peer_conns[i].lock, NULL);
    }

    printf("HA replication engine initialized (term=%lu last=%lu commit=%lu applied=%lu state=%s log=%s)\n",
           (unsigned long)recovered.term, (unsigned long)last_index,
           (unsigned long)recovered.commit_index, (unsigned long)recovered.applied_index,
           ha_store_state_path(g_store), ha_store_log_path(g_store));
    return 0;
}

void HA_replication_shutdown(void) {
    // Graceful shutdown of all connections
    for (int i = 0; i < HA_MAX_NODES; i++) {
        pthread_mutex_lock(&g_peer_conns[i].lock);
        cleanup_peer_connection_locked(&g_peer_conns[i]);
        pthread_mutex_unlock(&g_peer_conns[i].lock);
        pthread_mutex_destroy(&g_peer_conns[i].lock);
    }

    // Free log entries
    for (uint64_t i = 0; i < LOG_RING_SIZE; i++) {
        if (g_log_ring.slots[i].entry) {
            release_log_entry(g_log_ring.slots[i].entry);
            g_log_ring.slots[i].entry = NULL;
        }
    }

    pthread_mutex_destroy(&g_log_ring.append_lock);
    pthread_mutex_destroy(&g_log_ring.apply_lock);

    ha_store_close(g_store);
    g_store = NULL;

    // Free thread-local buffer if allocated
    if (t_send_buf) {
        slab_free(t_send_buf);
        t_send_buf = NULL;
        t_send_buf_size = 0;
    }

    g_config = NULL;
    g_runtime = NULL;

    printf("HA replication engine shutdown complete\n");
}

uint64_t HA_append_local_entry(uint32_t type, const void *data, uint32_t size) {
    if (!g_store || !g_runtime || size > g_config->ha_max_msg_bytes) return 0;
    pthread_mutex_lock(&g_log_ring.append_lock);

    uint64_t index = atomic_load(&g_log_ring.head);
    uint64_t tail = atomic_load(&g_log_ring.tail);
    if (index - tail >= LOG_RING_SIZE) {
        pthread_mutex_unlock(&g_log_ring.append_lock);
        errno = ENOSPC;
        return 0;
    }
    uint64_t slot_idx = index & LOG_RING_MASK;

    ha_log_entry_t *entry = create_log_entry(index, type, data, size);
    if (!entry) {
        pthread_mutex_unlock(&g_log_ring.append_lock);
        return 0;
    }

    uint64_t file_offset = 0;
    if (ha_store_append_log(g_store, entry->index, entry->term,
                            entry->timestamp_us, entry->type,
                            entry->data, entry->size, 1, &file_offset) != 0) {
        release_log_entry(entry);
        atomic_store(&g_runtime->state, HA_STATE_FAILED);
        pthread_mutex_unlock(&g_log_ring.append_lock);
        return 0;
    }

    log_slot_t *slot = &g_log_ring.slots[slot_idx];
    if (slot->entry) {
        release_log_entry(slot->entry);
    }

    slot->entry = entry;
    slot->file_offset = file_offset;
    slot->apply_batch_id = 0;
    atomic_store(&slot->refs, 1);
    atomic_store(&slot->apply_status, 0);
    atomic_store(&g_log_ring.head, index + 1);

    pthread_mutex_unlock(&g_log_ring.append_lock);

    leader_maybe_advance_commit();

    return index;
}

static int ha_entry_crc_valid(const ha_log_entry_t *entry) {
    uint32_t crc = crc32c(0, entry, offsetof(ha_log_entry_t, crc));
    if (entry->size) crc = crc32c(crc, entry->data, entry->size);
    return crc == entry->crc;
}

int HA_follower_store_entries(uint64_t leader_term,
                              const ha_append_entries_t *append,
                              const void *entries, size_t entries_size,
                              uint64_t *match_index) {
    if (!g_store || !g_runtime || !append || (!entries && entries_size)) return -1;
    if (leader_term < atomic_load(&g_runtime->term)) return -1;
    if (!HA_log_matches(append->prev_log_index, append->prev_log_term)) {
        uint64_t local_last = atomic_load(&g_log_ring.head) - 1;
        if (match_index)
            *match_index = ha_reject_hint(append->prev_log_index, local_last);
        return -1;
    }

    const uint8_t *begin = (const uint8_t *)entries;
    const uint8_t *p = begin;
    const uint8_t *end = begin + entries_size;
    uint64_t expected_index = append->prev_log_index + 1;
    uint64_t previous_term = append->prev_log_term;
    uint32_t count = 0;
    while (p < end) {
        if ((size_t)(end - p) < sizeof(ha_log_entry_t)) return -1;
        const ha_log_entry_t *entry = (const ha_log_entry_t *)p;
        size_t need = sizeof(*entry) + (size_t)entry->size;
        if (need < sizeof(*entry) || need > (size_t)(end - p) ||
            entry->size > g_config->ha_max_msg_bytes ||
            entry->index != expected_index || entry->term > leader_term ||
            entry->term < previous_term ||
            !ha_entry_crc_valid(entry)) return -1;
        p += need;
        previous_term = entry->term;
        expected_index++;
        count++;
    }
    if (p != end || count != append->entry_count) return -1;

    pthread_mutex_lock(&g_log_ring.append_lock);
    p = begin;
    uint64_t last_match = append->prev_log_index;
    int appended = 0;
    while (p < end) {
        const ha_log_entry_t *incoming = (const ha_log_entry_t *)p;
        size_t need = sizeof(*incoming) + (size_t)incoming->size;
        uint64_t last_local = atomic_load(&g_log_ring.head) - 1;

        if (incoming->index <= last_local) {
            log_slot_t *existing_slot = &g_log_ring.slots[incoming->index & LOG_RING_MASK];
            ha_log_entry_t *existing = existing_slot->entry;
            int identical = existing && existing->index == incoming->index &&
                            existing->term == incoming->term &&
                            existing->type == incoming->type &&
                            existing->size == incoming->size &&
                            existing->crc == incoming->crc;
            if (identical) {
                last_match = incoming->index;
                p += need;
                continue;
            }
            if (incoming->index <= atomic_load(&g_runtime->commit_index) ||
                !existing || existing->index != incoming->index ||
                ha_store_truncate_log(g_store, existing_slot->file_offset) != 0) {
                atomic_store(&g_runtime->state, HA_STATE_FAILED);
                pthread_mutex_unlock(&g_log_ring.append_lock);
                return -1;
            }
            for (uint64_t idx = incoming->index; idx <= last_local; idx++) {
                log_slot_t *slot = &g_log_ring.slots[idx & LOG_RING_MASK];
                if (slot->entry && slot->entry->index == idx) {
                    rf_idemp_abort_raft_state(slot->entry->type, slot->entry->data,
                                              slot->entry->size, idx);
                    release_log_entry(slot->entry);
                }
                slot->entry = NULL;
                slot->file_offset = 0;
                slot->apply_batch_id = 0;
                atomic_store(&slot->refs, 0);
                atomic_store(&slot->apply_status, 0);
            }
            atomic_store(&g_log_ring.head, incoming->index);
            last_local = incoming->index - 1;
        }

        if (incoming->index != last_local + 1 ||
            incoming->index - atomic_load(&g_log_ring.tail) >= LOG_RING_SIZE) {
            pthread_mutex_unlock(&g_log_ring.append_lock);
            return -1;
        }

        ha_log_entry_t *copy = create_log_entry_with_term(
            incoming->index, incoming->term, incoming->timestamp_us,
            incoming->type, incoming->data, incoming->size);
        if (!copy) {
            pthread_mutex_unlock(&g_log_ring.append_lock);
            return -1;
        }
        uint64_t file_offset = 0;
        if (ha_store_append_log(g_store, copy->index, copy->term,
                                copy->timestamp_us, copy->type,
                                copy->data, copy->size, 0, &file_offset) != 0) {
            release_log_entry(copy);
            atomic_store(&g_runtime->state, HA_STATE_FAILED);
            pthread_mutex_unlock(&g_log_ring.append_lock);
            return -1;
        }
        log_slot_t *slot = &g_log_ring.slots[copy->index & LOG_RING_MASK];
        slot->entry = copy;
        slot->file_offset = file_offset;
        slot->apply_batch_id = 0;
        atomic_store(&slot->refs, 1);
        atomic_store(&slot->apply_status, 0);
        atomic_store(&g_log_ring.head, copy->index + 1);
        if (rf_idemp_restore_raft_state(copy->type, copy->data, copy->size,
                                        copy->index, 0, 0) < 0) {
            atomic_store(&g_runtime->state, HA_STATE_FAILED);
            pthread_mutex_unlock(&g_log_ring.append_lock);
            return -1;
        }
        last_match = copy->index;
        appended = 1;
        p += need;
    }
    if (appended && ha_store_sync_log(g_store) != 0) {
        atomic_store(&g_runtime->state, HA_STATE_FAILED);
        pthread_mutex_unlock(&g_log_ring.append_lock);
        return -1;
    }
    pthread_mutex_unlock(&g_log_ring.append_lock);
    if (match_index) *match_index = last_match;
    return 0;
}

int HA_record_vote_response_once(int peer_index, uint64_t term, int granted) {
    if (!g_runtime || peer_index < 0 || peer_index >= HA_MAX_NODES ||
        atomic_load(&g_runtime->role) != HA_ROLE_CANDIDATE ||
        term != atomic_load(&g_runtime->term)) return 0;
    uint64_t bit = HA_NODE_MASK(peer_index);
    uint64_t seen = atomic_load(&g_runtime->votes_seen_mask);
    for (;;) {
        if (seen & bit) return 0;
        if (atomic_compare_exchange_weak(&g_runtime->votes_seen_mask, &seen, seen | bit)) break;
    }
    if (granted) {
        atomic_fetch_or(&g_runtime->votes_granted_mask, bit);
        atomic_fetch_add(&g_runtime->votes_granted, 1);
    }
    return 1;
}

uint64_t HA_repl_kick_read(void) {
    return atomic_load(&g_repl_kick);
}

int HA_forward_append_and_replicate(uint32_t rec_type, const void *data, size_t size) {
    if (!g_config || !g_runtime) return -1;
    if (!HA_is_leader(g_runtime)) return -1;
    uint64_t idx = HA_append_local_entry(rec_type, data, (uint32_t)size);
    if (idx == 0) return -1;
    HA_request_catchup();
    return 0;
}

int HA_replicate_to_followers(void) {
    if (!HA_is_leader(g_runtime)) return -1;
    send_heartbeat_with_entries();
    return 0;
}

int HA_wait_for_replication(uint64_t index, uint32_t timeout_ms) {
    if (!HA_is_leader(g_runtime)) return -1;

    uint64_t expected_term = atomic_load(&g_runtime->term);
    uint64_t start = now_us();
    uint64_t timeout_us = timeout_ms * 1000ULL;

    while ((now_us() - start) < timeout_us) {
        if (!HA_is_leader(g_runtime) ||
            atomic_load(&g_runtime->term) != expected_term ||
            atomic_load(&g_runtime->state) == HA_STATE_FAILED) {
            atomic_fetch_add(&g_runtime->writes_failed, 1);
            return -1;
        }
        /* Commit index is the authoritative completion signal. */
        if (atomic_load(&g_runtime->commit_index) >= index) {
            atomic_fetch_add(&g_runtime->writes_replicated, 1);
            return 0;
        }
        if (ha_has_commit_quorum_for(index)) {
            if (HA_update_commit_index(index) == 0) {
                atomic_fetch_add(&g_runtime->writes_replicated, 1);
                return 0;
            }
            atomic_fetch_add(&g_runtime->writes_failed, 1);
            return -1;
        }
        usleep(200);
    }

    atomic_fetch_add(&g_runtime->writes_failed, 1);
    return -1;
}

static int HA_update_applied_index(uint64_t applied_index) {
    pthread_mutex_lock(&g_stable_transition_lock);
    uint64_t current = atomic_load(&g_runtime->applied_index);
    uint64_t commit = atomic_load(&g_runtime->commit_index);
    if (applied_index <= current) {
        pthread_mutex_unlock(&g_stable_transition_lock);
        return 0;
    }
    if (applied_index > commit ||
        persist_state_values(atomic_load(&g_runtime->term),
                             atomic_load(&g_runtime->voted_for),
                             commit, applied_index) != 0) {
        pthread_mutex_unlock(&g_stable_transition_lock);
        return -1;
    }
    atomic_store(&g_runtime->applied_index, applied_index);
    pthread_mutex_unlock(&g_stable_transition_lock);
    return 0;
}

int HA_apply_committed_entries(void) {
    if (!g_runtime || !g_store) return -1;
    pthread_mutex_lock(&g_log_ring.apply_lock);
    uint64_t commit_idx = atomic_load(&g_runtime->commit_index);
    uint64_t applied_idx = atomic_load(&g_runtime->applied_index);

    if (applied_idx >= commit_idx) {
        pthread_mutex_unlock(&g_log_ring.apply_lock);
        return 0;
    }

    for (uint64_t idx = applied_idx + 1; idx <= commit_idx; idx++) {
        uint64_t slot_idx = idx & LOG_RING_MASK;
        log_slot_t *slot = &g_log_ring.slots[slot_idx];

        if (!slot->entry || slot->entry->index != idx) {
            atomic_store(&g_runtime->state, HA_STATE_FAILED);
            pthread_mutex_unlock(&g_log_ring.apply_lock);
            return -1;
        }

        ha_log_entry_t *entry = slot->entry;
        uint64_t batch_id = 0;
        if (entry->type != HA_LOG_NOOP) {
            batch_id = AOF_append_sealed((int)entry->type, entry->data, entry->size);
            if (batch_id == 0 || AOF_sealed_wait(batch_id) != 0) {
                slot->apply_batch_id = batch_id;
                atomic_store(&slot->apply_status, -1);
                atomic_store(&g_runtime->state, HA_STATE_FAILED);
                pthread_mutex_unlock(&g_log_ring.apply_lock);
                return -1;
            }
            int handled = RAMForge_HA_replay_record(entry->type, entry->data,
                                                    entry->size);
            if (handled < 0 ||
                (handled == 0 &&
                 rf_broker_apply_aof_checked(entry->type, entry->data,
                                             entry->size) != 0)) {
                slot->apply_batch_id = batch_id;
                atomic_store(&slot->apply_status, -1);
                atomic_store(&g_runtime->state, HA_STATE_FAILED);
                pthread_mutex_unlock(&g_log_ring.apply_lock);
                return -1;
            }
            if (rf_bus_publish_wait_except((int)entry->type, entry->data, entry->size,
                                           g_config->replication_timeout_ms, 0,
                                           NULL) != 0) {
                slot->apply_batch_id = batch_id;
                atomic_store(&slot->apply_status, -1);
                atomic_store(&g_runtime->state, HA_STATE_FAILED);
                pthread_mutex_unlock(&g_log_ring.apply_lock);
                return -1;
            }
        }
        if (HA_update_applied_index(idx) != 0) {
            slot->apply_batch_id = batch_id;
            atomic_store(&slot->apply_status, -1);
            pthread_mutex_unlock(&g_log_ring.apply_lock);
            return -1;
        }
        slot->apply_batch_id = batch_id;
        atomic_store(&slot->apply_status, 1);
    }

    pthread_mutex_unlock(&g_log_ring.apply_lock);
    return 0;
}

int HA_wait_for_local_apply(uint64_t index, uint32_t timeout_ms,
                            uint64_t *aof_batch_id) {
    if (!g_runtime || index == 0) return -1;
    uint64_t start = now_us();
    uint64_t timeout_us = (uint64_t)timeout_ms * 1000ULL;
    while ((now_us() - start) < timeout_us) {
        if (atomic_load(&g_runtime->commit_index) >= index)
            (void)HA_apply_committed_entries();
        if (atomic_load(&g_runtime->applied_index) >= index) {
            log_slot_t *slot = &g_log_ring.slots[index & LOG_RING_MASK];
            if (!slot->entry || slot->entry->index != index ||
                atomic_load(&slot->apply_status) != 1) return -1;
            if (aof_batch_id) *aof_batch_id = slot->apply_batch_id;
            return 0;
        }
        if (atomic_load(&g_runtime->state) == HA_STATE_FAILED) return -1;
        usleep(200);
    }
    return -1;
}

void HA_get_log_stats(uint64_t *last_index, uint64_t *committed, uint64_t *applied) {
    if (last_index) *last_index = atomic_load(&g_log_ring.head) - 1;
    if (committed) *committed = atomic_load(&g_runtime->commit_index);
    if (applied) *applied = atomic_load(&g_runtime->applied_index);
}









void HA_leader_on_nack(int peer_index, uint64_t follower_index) {
    if (!g_runtime || !g_config) return;
    if (!HA_is_leader(g_runtime)) return;
    if (peer_index < 0 || peer_index >= HA_MAX_NODES) return;

    uint64_t want = follower_index + 1;
    uint64_t cur = atomic_load(&g_runtime->next_index[peer_index]);
    if (want < cur) {
        atomic_store(&g_runtime->next_index[peer_index], want);
    }
}

static int ha_recalc_node_count_locked(void) {
    int highest = -1;
    uint64_t active = ha_active_mask();
    for (int i = 0; i < HA_MAX_NODES; i++) {
        if ((active & HA_NODE_MASK(i)) && g_config->nodes[i].node_id[0]) highest = i;
    }
    g_config->node_count = (highest >= 0) ? (highest + 1) : 0;
    if (g_config->node_count < 1 && g_config->local_node_index >= 0) {
        g_config->node_count = g_config->local_node_index + 1;
    }
    return g_config->node_count;
}

int HA_get_membership_view(ha_membership_view_t *out) {
    if (!out || !g_config || !g_runtime) return -1;
    memset(out, 0, sizeof(*out));

    pthread_rwlock_rdlock(&g_runtime->membership_lock);
    out->epoch = atomic_load(&g_runtime->config_epoch);
    out->voters_old_mask = atomic_load(&g_runtime->voters_old_mask);
    out->voters_new_mask = atomic_load(&g_runtime->voters_new_mask);
    out->learners_mask = atomic_load(&g_runtime->learners_mask);
    out->joint_active = (uint32_t)atomic_load(&g_runtime->joint_active);
    out->node_count = (uint32_t)g_config->node_count;
    memcpy(out->nodes, g_config->nodes, sizeof(out->nodes));
    pthread_rwlock_unlock(&g_runtime->membership_lock);
    return 0;
}

int HA_apply_membership_snapshot(const ha_membership_record_t *rec) {
    if (!rec || !g_config || !g_runtime) return -1;

    uint64_t valid_mask = (UINT64_C(1) << HA_MAX_NODES) - 1;
    uint64_t voters = rec->voters_old_mask | rec->voters_new_mask;
    uint64_t active = voters | rec->learners_mask;
    if (rec->epoch == 0 || rec->node_count > HA_MAX_NODES ||
        rec->joint_active > 1 || rec->voters_new_mask == 0 ||
        (active & ~valid_mask) != 0 ||
        (voters & rec->learners_mask) != 0 ||
        (!rec->joint_active && rec->voters_old_mask != rec->voters_new_mask))
        return -1;
    for (int i = 0; i < HA_MAX_NODES; i++) {
        if (!(active & HA_NODE_MASK(i))) continue;
        if (!memchr(rec->nodes[i].node_id, '\0', sizeof(rec->nodes[i].node_id)) ||
            !memchr(rec->nodes[i].advertise_addr, '\0',
                    sizeof(rec->nodes[i].advertise_addr)) ||
            !memchr(rec->nodes[i].http_addr, '\0',
                    sizeof(rec->nodes[i].http_addr)) ||
            rec->nodes[i].node_id[0] == '\0') return -1;
    }

    pthread_rwlock_wrlock(&g_runtime->membership_lock);

    memcpy(g_config->nodes, rec->nodes, sizeof(g_config->nodes));
    g_config->node_count = (int)((rec->node_count <= HA_MAX_NODES) ? rec->node_count : HA_MAX_NODES);

    uint64_t voters_old = rec->voters_old_mask;
    uint64_t voters_new = rec->voters_new_mask;
    uint64_t learners = rec->learners_mask;

    atomic_store(&g_runtime->voters_old_mask, voters_old);
    atomic_store(&g_runtime->voters_new_mask, voters_new);
    atomic_store(&g_runtime->learners_mask, learners);
    atomic_store(&g_runtime->joint_active, rec->joint_active ? 1 : 0);
    atomic_store(&g_runtime->config_epoch, rec->epoch);

    for (int i = 0; i < HA_MAX_NODES; i++) {
        if (!g_config->nodes[i].node_id[0]) continue;
        g_config->nodes[i].is_voter = (voters_new & HA_NODE_MASK(i)) ? 1 : 0;
    }

    ha_recalc_node_count_locked();
    pthread_rwlock_unlock(&g_runtime->membership_lock);
    return 0;
}

int HA_apply_cfg_change_entry(const void *data, uint32_t size) {
    if (!data || size != sizeof(ha_cfg_change_t) || !g_config || !g_runtime) return -1;
    const ha_cfg_change_t *chg = (const ha_cfg_change_t *)data;

    if (chg->node_index >= HA_MAX_NODES) return -1;

    pthread_rwlock_wrlock(&g_runtime->membership_lock);

    uint64_t epoch = atomic_load(&g_runtime->config_epoch);
    if (chg->expected_epoch && chg->expected_epoch < epoch) {
        pthread_rwlock_unlock(&g_runtime->membership_lock);
        return 0;
    }
    if (chg->expected_epoch && chg->expected_epoch > epoch) {
        pthread_rwlock_unlock(&g_runtime->membership_lock);
        return -1;
    }

    uint64_t voters_old = atomic_load(&g_runtime->voters_old_mask);
    uint64_t voters_new = atomic_load(&g_runtime->voters_new_mask);
    uint64_t learners = atomic_load(&g_runtime->learners_mask);
    int joint = atomic_load(&g_runtime->joint_active);

    int idx = (int)chg->node_index;
    uint64_t bit = HA_NODE_MASK(idx);
    int rc = 0;

    switch (chg->op) {
        case HA_CFG_OP_ADD_LEARNER:
            if (!g_config->nodes[idx].node_id[0]) {
                g_config->nodes[idx] = chg->member;
            }
            g_config->nodes[idx].is_voter = 0;
            learners |= bit;
            voters_old &= ~bit;
            voters_new &= ~bit;
            joint = 0;
            break;

        case HA_CFG_OP_PROMOTE_VOTER_JOINT_BEGIN:
            if (!g_config->nodes[idx].node_id[0] && chg->member.node_id[0]) {
                g_config->nodes[idx] = chg->member;
            }
            learners &= ~bit;
            voters_old = voters_new;
            voters_new |= bit;
            joint = 1;
            g_config->nodes[idx].is_voter = 1;
            break;

        case HA_CFG_OP_PROMOTE_VOTER_FINALIZE:
            learners &= ~bit;
            voters_new |= bit;
            voters_old = voters_new;
            joint = 0;
            g_config->nodes[idx].is_voter = 1;
            break;

        case HA_CFG_OP_REMOVE_MEMBER_JOINT_BEGIN:
            voters_old = voters_new;
            voters_new &= ~bit;
            learners &= ~bit;
            joint = 1;
            break;

        case HA_CFG_OP_REMOVE_MEMBER_FINALIZE:
            voters_new &= ~bit;
            voters_old = voters_new;
            learners &= ~bit;
            joint = 0;
            memset(&g_config->nodes[idx], 0, sizeof(g_config->nodes[idx]));
            break;

        default:
            rc = -1;
            break;
    }

    if (rc == 0) {
        atomic_store(&g_runtime->voters_old_mask, voters_old);
        atomic_store(&g_runtime->voters_new_mask, voters_new);
        atomic_store(&g_runtime->learners_mask, learners);
        atomic_store(&g_runtime->joint_active, joint ? 1 : 0);
        atomic_store(&g_runtime->config_epoch, epoch + 1);
        ha_recalc_node_count_locked();
    }

    pthread_rwlock_unlock(&g_runtime->membership_lock);
    return rc;
}







