// ramforge_ha_replication.c - PATCHED v2: Fixed stack corruption on failed sends
#include "ramforge_ha_replication.h"
#include "ramforge_ha_config.h"
#include "crc32c.h"
#include "slab_alloc.h"
#include "rf_broker.h"
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

// ═══════════════════════════════════════════════════════════════════════════════
// In-Memory Log Ring Buffer (Zero-Copy, Lock-Free)
// ═══════════════════════════════════════════════════════════════════════════════

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
} log_slot_t;

static struct {
    log_slot_t slots[LOG_RING_SIZE];
    _Atomic uint64_t head;          // Next index to write
    _Atomic uint64_t tail;          // Oldest retained index
    pthread_mutex_t append_lock;    // Only for coordinating appends
} g_log_ring;


static ha_config_t *g_config;
static ha_runtime_t *g_runtime;

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

static uint64_t term_at(uint64_t idx) {
    if (idx == 0) return 0;
    ha_log_entry_t *e = g_log_ring.slots[idx & LOG_RING_MASK].entry;
    return (e && e->index == idx) ? e->term : 0;
}

/* recompute commit index by majority rule */
static void leader_maybe_advance_commit(void) {
    uint64_t last_local = atomic_load(&g_log_ring.head) - 1;
    if (last_local == 0) return;
    int n = g_config->node_count;
    uint64_t arr[HA_MAX_NODES];
    int m = 0;

    /* leader counts as having replicated everything it has locally */
    arr[m++] = last_local;
    for (int i = 0; i < n; i++) {
        if (i == g_config->local_node_index) continue;
        arr[m++] = atomic_load(&g_runtime->match_index[i]);
    }

    /* nth_element-ish: small n, so O(n log n) sort is fine */
    for (int i = 0; i < m; i++)
        for (int j = i + 1; j < m; j++)
            if (arr[j] < arr[i]) { uint64_t t = arr[i]; arr[i] = arr[j]; arr[j] = t; }

    int majority = (n / 2) + 1;
    uint64_t cand = arr[m - majority]; /* k-th largest */

    /* Raft safety: only commit entries from current term */
    if (cand > atomic_load(&g_runtime->commit_index) &&
        term_at(cand) == atomic_load(&g_runtime->term)) {
        atomic_store(&g_runtime->commit_index, cand);
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

// ═══════════════════════════════════════════════════════════════════════════════
// Log Entry Management
// ═══════════════════════════════════════════════════════════════════════════════

static ha_log_entry_t* create_log_entry(uint64_t index, uint32_t type,
                                        const void *data, uint32_t size) {
    size_t total_size = sizeof(ha_log_entry_t) + size;
    ha_log_entry_t *entry = slab_alloc(total_size);
    if (!entry) return NULL;

    entry->index = index;
    entry->term = atomic_load(&g_runtime->term);
    entry->timestamp_us = now_us();
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

static void release_log_entry(ha_log_entry_t *entry) {
    if (entry) {
        slab_free(entry);
    }
}

// ═══════════════════════════════════════════════════════════════════════════════
// Network Layer (PATCHED v2: Robust Connection Handling)
// ═══════════════════════════════════════════════════════════════════════════════

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
    if (peer_index < 0 || peer_index >= g_config->node_count) return -1;

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


int send_message(int peer_index, ha_msg_header_t *header,
                 const void *payload, size_t payload_size) {
    if (!g_config) return -1;
    if (peer_index < 0 || peer_index >= g_config->node_count) return -1;

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

// ═══════════════════════════════════════════════════════════════════════════════
// Leader Election & Heartbeats
// ═══════════════════════════════════════════════════════════════════════════════

/* helper: copy up to N entries starting at index 'from' into a flat buffer */
static size_t pack_entries(uint64_t from, uint32_t max,
                           uint8_t *dst, size_t cap, uint64_t *last_idx_out) {
    size_t used = 0;
    uint64_t i = from;
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

    int n = g_config->node_count;
    int local = g_config->local_node_index;
    uint64_t last_local = atomic_load(&g_log_ring.head) - 1;

    for (int peer = 0; peer < n; peer++) {
        if (peer == local) continue;

        uint64_t match = atomic_load(&g_runtime->match_index[peer]);
        uint64_t next = atomic_load(&g_runtime->next_index[peer]);

        // CHANGE 3: Correct next_index if it drifted ahead of match
        if (next > match + 1) {
            next = match + 1;
            atomic_store(&g_runtime->next_index[peer], next);
        }
        if (next == 0) next = 1;

        // CHANGE 4: Aggressive catchup - send multiple batches per peer if behind
        uint64_t gap = (last_local >= next) ? (last_local - next + 1) : 0;
        int batches_this_peer = (gap > batch_size * 4) ? 8 : 1;  // Up to 8 batches if way behind

        for (int batch = 0; batch < batches_this_peer; batch++) {
            if (budget_us && (now_us() - start) > budget_us) break;

            next = atomic_load(&g_runtime->next_index[peer]);
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
                ha_msg_header_t h = {
                        .type = HA_MSG_APPEND_ENTRIES,
                        .term = atomic_load(&g_runtime->term)
                };
                if (send_message(peer, &h, buf, sizeof(*ae) + payload_sz) == 0 && last_sent) {
                    atomic_store(&g_runtime->next_index[peer], last_sent + 1);
                }
            } else {
                break;  // No more entries to pack
            }
        }

        // Still send empty heartbeat if nothing to replicate
        if (atomic_load(&g_runtime->next_index[peer]) > last_local) {
            ha_append_entries_t ae = {.commit_index = atomic_load(&g_runtime->commit_index)};
            ha_msg_header_t h = {.type = HA_MSG_HEARTBEAT, .term = atomic_load(&g_runtime->term)};
            (void)send_message(peer, &h, &ae, sizeof(ae));
        }
    }

    atomic_fetch_add(&g_runtime->heartbeats_sent, 1);
}

static void start_election(void) {
    if (!g_runtime || !g_config) return;

    pthread_rwlock_wrlock(&g_runtime->state_lock);
    uint64_t new_term = atomic_fetch_add(&g_runtime->term, 1) + 1;
    atomic_store(&g_runtime->role, HA_ROLE_CANDIDATE);
    atomic_store(&g_runtime->current_leader, -1);
    atomic_store(&g_runtime->voted_for, g_config->local_node_index);
    atomic_store(&g_runtime->votes_granted, 1);
    atomic_fetch_add(&g_runtime->elections_started, 1);
    pthread_rwlock_unlock(&g_runtime->state_lock);

    printf("🗳️  Starting election for term %lu\n", new_term);

    int votes_needed = (g_config->node_count / 2) + 1;

    uint64_t last_index = atomic_load(&g_log_ring.head);
    uint64_t last_term = 0;

    if (last_index > 0) {
        uint64_t slot_idx = (last_index - 1) & LOG_RING_MASK;
        ha_log_entry_t *entry = g_log_ring.slots[slot_idx].entry;
        if (entry) last_term = entry->term;
    }

    for (int i = 0; i < g_config->node_count; i++) {
        if (i == g_config->local_node_index) continue;
        if (!g_config->nodes[i].is_voter) continue;

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
        if (atomic_load(&g_runtime->votes_granted) >= votes_needed) {
            pthread_rwlock_wrlock(&g_runtime->state_lock);

            if (atomic_load(&g_runtime->term) == new_term) {
                atomic_store(&g_runtime->role, HA_ROLE_LEADER);
                atomic_store(&g_runtime->current_leader, g_config->local_node_index);
                atomic_store(&g_runtime->state, HA_STATE_READY);
                atomic_fetch_add(&g_runtime->elections_won, 1);

                uint64_t next_idx = atomic_load(&g_log_ring.head);
                for (int i = 0; i < HA_MAX_NODES; i++) {
                    atomic_store(&g_runtime->next_index[i], next_idx);
                    atomic_store(&g_runtime->match_index[i], 0);
                }

                printf("✅ Won election, now LEADER for term %lu\n", new_term);
            }

            pthread_rwlock_unlock(&g_runtime->state_lock);
            send_heartbeat_with_entries();
            return;
        }

        usleep(10000);
    }

    atomic_store(&g_runtime->role, HA_ROLE_FOLLOWER);
    printf("⏱️  Election timeout for term %lu\n", new_term);
}

int HA_start_election(void) {
    start_election();
    return 0;
}

// ═══════════════════════════════════════════════════════════════════════════════
// Public API Implementation
// ═══════════════════════════════════════════════════════════════════════════════

int HA_replication_init(ha_config_t *config, ha_runtime_t *runtime) {
    g_config = config;
    g_runtime = runtime;

    memset(&g_log_ring, 0, sizeof(g_log_ring));
    atomic_store(&g_log_ring.head, 1);
    atomic_store(&g_log_ring.tail, 1);

    if (pthread_mutex_init(&g_log_ring.append_lock, NULL) != 0) {
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

    printf("✅ HA replication engine initialized\n");
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

    // Free thread-local buffer if allocated
    if (t_send_buf) {
        slab_free(t_send_buf);
        t_send_buf = NULL;
        t_send_buf_size = 0;
    }

    g_config = NULL;
    g_runtime = NULL;

    printf("✅ HA replication engine shutdown complete\n");
}

uint64_t HA_append_local_entry(uint32_t type, const void *data, uint32_t size) {
    pthread_mutex_lock(&g_log_ring.append_lock);

    uint64_t index = atomic_fetch_add(&g_log_ring.head, 1);
    uint64_t slot_idx = index & LOG_RING_MASK;

    ha_log_entry_t *entry = create_log_entry(index, type, data, size);
    if (!entry) {
        pthread_mutex_unlock(&g_log_ring.append_lock);
        return 0;
    }

    log_slot_t *slot = &g_log_ring.slots[slot_idx];
    if (slot->entry) {
        release_log_entry(slot->entry);
    }

    slot->entry = entry;
    atomic_store(&slot->refs, 1);

    pthread_mutex_unlock(&g_log_ring.append_lock);

    return index;
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

    uint64_t start = now_us();
    uint64_t timeout_us = timeout_ms * 1000;

    int required_acks = g_config->write_concern;

    while ((now_us() - start) < timeout_us) {
        int acks = 1;

        for (int i = 0; i < g_config->node_count; i++) {
            if (i == g_config->local_node_index) continue;

            uint64_t match = atomic_load(&g_runtime->match_index[i]);
            if (match >= index) {
                acks++;
            }
        }

        if (acks >= required_acks) {
            atomic_fetch_add(&g_runtime->writes_replicated, 1);
            return 0;
        }

        usleep(100);
    }

    atomic_fetch_add(&g_runtime->writes_failed, 1);
    return -1;
}

int HA_apply_committed_entries(void) {
    uint64_t commit_idx = atomic_load(&g_runtime->commit_index);
    uint64_t applied_idx = atomic_load(&g_runtime->applied_index);

    if (applied_idx >= commit_idx) return 0;

    for (uint64_t idx = applied_idx + 1; idx <= commit_idx; idx++) {
        uint64_t slot_idx = idx & LOG_RING_MASK;
        log_slot_t *slot = &g_log_ring.slots[slot_idx];

        if (!slot->entry || slot->entry->index != idx) {
            break;
        }

        ha_log_entry_t *entry = slot->entry;
        (void)rf_bus_publish((int)entry->type, entry->data, entry->size);
        atomic_store(&g_runtime->applied_index, idx);
    }

    return 0;
}

void HA_get_log_stats(uint64_t *last_index, uint64_t *committed, uint64_t *applied) {
    if (last_index) *last_index = atomic_load(&g_log_ring.head) - 1;
    if (committed) *committed = atomic_load(&g_runtime->commit_index);
    if (applied) *applied = atomic_load(&g_runtime->applied_index);
}
