// ramforge_ha_net.c - PATCHED: Graceful connection handling on peer disconnect
#define _GNU_SOURCE
#include "ramforge_ha_net.h"
#include "ramforge_ha_replication.h"   // for ha_msg_header_t etc.
#include <pthread.h>
#include <string.h>
#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <errno.h>
#include <arpa/inet.h>
#include <netinet/in.h>
#include <fcntl.h>
#include <sys/socket.h>
#include <time.h>
#include <stdatomic.h>
#include "rf_broker.h"
#include "aof_batch.h"
#include "crc32c.h"
#include "ramforge_ha_tls.h"
#include "slab_alloc.h"
#include "ramforge_ha_integration.h"
#define TCP_NODELAY           1

#define HA_MAX_PAYLOAD (16u * 1024u * 1024u) // 16MB safety cap
static ha_config_t  *g_cfg  = NULL;
static ha_runtime_t *g_rt   = NULL;
static pthread_t     g_thr  = 0;
static _Atomic int   g_run  = 0;
static _Atomic int   g_listen_fd = -1;
extern int rf_bus_publish(int rec_id, const void *payload, uint32_t size);
typedef struct {
    uint64_t snapshot_id;
    uint64_t last_included_index;
    uint64_t leader_commit;
    int active;
} ha_snapshot_rx_state_t;

static ha_snapshot_rx_state_t g_snapshot_rx = {0};

static int apply_replicated_record(uint32_t rec_id, const void *payload, uint32_t size, int via_snapshot) {
    if (rec_id == HA_LOG_CFG_CHANGE) {
        return HA_apply_cfg_change_entry(payload, size);
    }

    if (RAMForge_HA_replay_record(rec_id, payload, size)) {
        return 0;
    }

    if (via_snapshot) {
        rf_broker_replay_aof(rec_id, payload, size);
    } else {
        (void)rf_bus_publish((int)rec_id, payload, size);
    }

    (void)AOF_append_sealed((int)rec_id, payload, size);
    return 0;
}

static inline uint64_t now_us(void){
    struct timespec ts; clock_gettime(CLOCK_MONOTONIC, &ts);
    return (uint64_t)ts.tv_sec*1000000ULL + ts.tv_nsec/1000;
}

static inline uint64_t ha_cfg_apply_log_throttle_us(void) {
    const char *e = getenv("RF_HA_CFG_APPLY_LOG_THROTTLE_MS");
    uint64_t ms = 1000; /* default: 1s */
    if (e && *e) {
        uint64_t v = strtoull(e, NULL, 10);
        if (v > 0) ms = v;
    }
    return ms * 1000ULL;
}

static void ha_log_cfg_apply_event(uint64_t idx, uint32_t size, int rc, int is_duplicate) {
    static _Atomic uint64_t s_last_idx = 0;
    static _Atomic uint64_t s_last_log_us = 0;
    static _Atomic uint64_t s_suppressed = 0;

    uint64_t now = now_us();
    uint64_t last_idx = atomic_load(&s_last_idx);
    uint64_t last_us = atomic_load(&s_last_log_us);

    if (is_duplicate || (last_idx == idx && (now - last_us) < ha_cfg_apply_log_throttle_us())) {
        atomic_fetch_add(&s_suppressed, 1);
        return;
    }

    uint64_t suppressed = atomic_exchange(&s_suppressed, 0);
    if (suppressed) {
        fprintf(stderr, "[HA-CFG-APPLY] suppressed=%lu duplicate/throttled logs\n", (unsigned long)suppressed);
    }

    fprintf(stderr, "[HA-CFG-APPLY] idx=%lu size=%u rc=%d\n",
            (unsigned long)idx, (unsigned)size, rc);
    atomic_store(&s_last_idx, idx);
    atomic_store(&s_last_log_us, now);
}

static ssize_t readn(int fd, void *buf, size_t n){
    size_t off=0;
    while(off<n){
        ssize_t r = recv(fd, (char*)buf+off, n-off, 0);
        if (r == 0) return off;              // peer closed
        if (r < 0) {
            if (errno==EINTR) continue;
            if (errno==EAGAIN || errno==EWOULDBLOCK) {
                usleep(1000);  // Brief sleep before retry
                continue;
            }
            return -1;
        }
        off += (size_t)r;
    }
    return (ssize_t)off;
}

static ssize_t readn_any(int fd, SSL *ssl, void *buf, size_t n, const _Atomic int *run_flag) {
    if (HA_tls_enabled() && ssl) {
        int tmo = 1000;
        const char *t = getenv("RF_HA_TLS_TIMEOUT_MS");
        if (t && *t) {
            int v = atoi(t);
            if (v > 0) tmo = v;
        }
        return HA_tls_readn(ssl, fd, buf, n, tmo, run_flag);
    }
    (void) run_flag;
    return readn(fd, buf, n);
}

static void close_conn(int fd, SSL *ssl) {
    if (ssl) {
        // Graceful TLS shutdown
        HA_tls_graceful_shutdown(ssl);
        SSL_free(ssl);
    }
    if (fd >= 0) close(fd);
}

/* Only HB/Append refresh heartbeat; votes do NOT. */
static void handle_heartbeat_only(const ha_msg_header_t *h){
    uint64_t my_term = atomic_load(&g_rt->term);
    if (h->term > my_term){
        atomic_store(&g_rt->term, h->term);
        atomic_store(&g_rt->voted_for, -1);
        atomic_store(&g_rt->role, HA_ROLE_FOLLOWER);
    }
    atomic_store(&g_rt->current_leader, h->from_node);
    atomic_store(&g_rt->state, HA_STATE_READY);
    atomic_store(&g_rt->last_heartbeat_us, now_us());
    atomic_fetch_add(&g_rt->heartbeats_received, 1);

    /* Heartbeat-only traffic must still refresh leader lease on the sender. */
    if (h->from_node >= 0) {
        ha_msg_header_t hd = {.type = HA_MSG_APPEND_RESPONSE, .term = atomic_load(&g_rt->term)};
        ha_append_response_t resp = {
            .match_index = atomic_load(&g_rt->applied_index),
            .success = 1
        };
        (void) send_message(h->from_node, &hd, &resp, sizeof(resp));
    }
}


static void handle_append_entries(const ha_msg_header_t *h, const void *pl, size_t psz) {
    if (!pl || psz < sizeof(ha_append_entries_t)) {
        handle_heartbeat_only(h);
        return;
    }

    const ha_append_entries_t *ae = (const ha_append_entries_t *) pl;
    /* prev-log check against what we have actually applied */
    uint64_t have = atomic_load(&g_rt->applied_index);
    if (ae->prev_log_index && ae->prev_log_index != have) {
        ha_msg_header_t hd = {.type = HA_MSG_APPEND_RESPONSE, .term = atomic_load(&g_rt->term)};
        ha_append_response_t resp = {.match_index = have, .success = 0};
        (void) send_message(h->from_node, &hd, &resp, sizeof(resp));
        return;
    }
    const uint8_t *p = (const uint8_t *) (ae + 1);
    const uint8_t *end = (const uint8_t *) pl + psz;

    /* become/refresh follower & leader tracking */
    uint64_t my_term = atomic_load(&g_rt->term);
    if (h->term > my_term) {
        atomic_store(&g_rt->term, h->term);
        atomic_store(&g_rt->voted_for, -1);
        atomic_store(&g_rt->role, HA_ROLE_FOLLOWER);
    }
    atomic_store(&g_rt->current_leader, h->from_node);
    atomic_store(&g_rt->state, HA_STATE_READY);
    atomic_store(&g_rt->last_heartbeat_us, now_us());

        /* parse packed ha_log_entry_t blobs and apply to local broker */
    uint64_t last_idx = 0;
    uint64_t applied_before = atomic_load(&g_rt->applied_index);
    while (p + sizeof(ha_log_entry_t) <= end) {
        const ha_log_entry_t *e = (const ha_log_entry_t *) p;
        size_t need = sizeof(*e) + e->size;
        if (p + need > end) break; /* truncated */

        int rc = apply_replicated_record(e->type, e->data, e->size, 0);
        if (rc == 0) {
            last_idx = e->index;
        } else {
            atomic_fetch_add(&g_rt->writes_failed, 1);
        }

        if (e->type == HA_LOG_CFG_CHANGE) {
            int is_duplicate = (e->index <= applied_before);
            ha_log_cfg_apply_event(e->index, e->size, rc, is_duplicate);
        }

        p += need;
    }

    /* advance only to what we actually applied; commit cannot exceed applied */
    if (last_idx) {
        atomic_store(&g_rt->applied_index, last_idx);
    }
    if (ae->commit_index) {
        uint64_t applied2 = atomic_load(&g_rt->applied_index);
        uint64_t new_commit = (ae->commit_index > applied2) ? applied2 : ae->commit_index;
        uint64_t old_commit = atomic_load(&g_rt->commit_index);
        if (new_commit > old_commit) {
            atomic_store(&g_rt->commit_index, new_commit);
        }
    }

    /* send acks back (match_index); for empty-append, return prev_log_index */
    if (h->from_node >= 0) {
        ha_msg_header_t hd = {.type = HA_MSG_APPEND_RESPONSE, .term = atomic_load(&g_rt->term)};
        uint64_t ack_idx = last_idx ? last_idx : ae->prev_log_index;
        ha_append_response_t resp = {.match_index = ack_idx, .success = 1};
        (void) send_message(h->from_node, &hd, &resp, sizeof(resp));
    }
}

static void handle_install_snapshot(const ha_msg_header_t *h, const void *pl, size_t psz) {
    if (!pl || psz < sizeof(ha_install_snapshot_t)) return;

    const ha_install_snapshot_t *snap = (const ha_install_snapshot_t *)pl;
    if (snap->payload_bytes > psz - sizeof(*snap)) return;

    uint64_t my_term = atomic_load(&g_rt->term);
    if (h->term > my_term) {
        atomic_store(&g_rt->term, h->term);
        atomic_store(&g_rt->voted_for, -1);
        atomic_store(&g_rt->role, HA_ROLE_FOLLOWER);
    }
    atomic_store(&g_rt->current_leader, h->from_node);
    atomic_store(&g_rt->last_heartbeat_us, now_us());

    if (snap->flags & HA_SNAPSHOT_FLAG_START) {
        g_snapshot_rx.active = 1;
        g_snapshot_rx.snapshot_id = snap->snapshot_id;
        g_snapshot_rx.last_included_index = snap->last_included_index;
        g_snapshot_rx.leader_commit = snap->leader_commit;
        atomic_store(&g_rt->state, HA_STATE_SYNCING);
    }

    if (!g_snapshot_rx.active || g_snapshot_rx.snapshot_id != snap->snapshot_id) {
        return;
    }

    const uint8_t *p = (const uint8_t *)(snap + 1);
    const uint8_t *end = p + snap->payload_bytes;
    for (uint32_t i = 0; i < snap->record_count; i++) {
        if (p + 8 > end) break;
        uint32_t rec_id = 0, rec_sz = 0;
        memcpy(&rec_id, p, 4);
        memcpy(&rec_sz, p + 4, 4);
        p += 8;
        if (p + rec_sz > end) break;

        (void)apply_replicated_record(rec_id, p, rec_sz, 1);
        p += rec_sz;
    }

    atomic_fetch_add(&g_rt->snapshot_bytes_received, snap->payload_bytes + sizeof(*snap));

    if (snap->flags & HA_SNAPSHOT_FLAG_END) {
        atomic_fetch_add(&g_rt->snapshots_received, 1);
        atomic_store(&g_rt->applied_index, snap->last_included_index);
        uint64_t c = (snap->leader_commit < snap->last_included_index)
                   ? snap->leader_commit : snap->last_included_index;
        if (c > atomic_load(&g_rt->commit_index)) {
            atomic_store(&g_rt->commit_index, c);
        }
        atomic_store(&g_rt->state, HA_STATE_READY);
        g_snapshot_rx.active = 0;

        ha_msg_header_t hd = {.type = HA_MSG_APPEND_RESPONSE, .term = atomic_load(&g_rt->term)};
        ha_append_response_t resp = {.match_index = snap->last_included_index, .success = 1};
        (void)send_message(h->from_node, &hd, &resp, sizeof(resp));
    }
}
/* Minimal recency rule: grant if same/newer term, haven't voted */
static void handle_vote_request(const ha_msg_header_t *h, const ha_vote_request_t *req){
    (void)req;
    ha_vote_response_t resp = { .vote_granted = 0 };

    uint64_t my_term = atomic_load(&g_rt->term);
    if (h->term < my_term){
        // deny, stale term
    } else {
        if (h->term > my_term){
            atomic_store(&g_rt->term, h->term);
            atomic_store(&g_rt->voted_for, -1);
            atomic_store(&g_rt->role, HA_ROLE_FOLLOWER);
        }
        int voted_for = atomic_load(&g_rt->voted_for);
        if (voted_for == -1 || voted_for == h->from_node){
            atomic_store(&g_rt->voted_for, h->from_node);
            resp.vote_granted = 1;
        }
    }

    ha_msg_header_t hd = {
            .type = HA_MSG_VOTE_RESPONSE,
            .term = atomic_load(&g_rt->term),
    };
    (void) send_message(h->from_node, &hd, &resp, sizeof(resp));
}

static void handle_vote_response(const ha_msg_header_t *h, const ha_vote_response_t *resp){
    if (atomic_load(&g_rt->role) != HA_ROLE_CANDIDATE) return;
    if (h->term != atomic_load(&g_rt->term)) return;

    /* Vote responses provide fresh peer liveness (helps bootstrap leader lease
       right after election). */
    if (h->from_node >= 0 && h->from_node < HA_MAX_NODES) {
        atomic_store(&g_rt->last_ack_us[h->from_node], now_us());
    }

    if (resp->vote_granted){
        atomic_fetch_add(&g_rt->votes_granted, 1);
    }
}

static void handle_append_response(const ha_msg_header_t *h, const ha_append_response_t *resp) {

    uint64_t my_term = atomic_load(&g_rt->term);
    if (h->term > my_term) {
        /* Stale leader/candidate -> immediate stepdown on higher term */
        atomic_store(&g_rt->term, h->term);
        atomic_store(&g_rt->voted_for, -1);
        atomic_store(&g_rt->role, HA_ROLE_FOLLOWER);
        atomic_store(&g_rt->current_leader, -1);
        return;
    }

    if (atomic_load(&g_rt->role) != HA_ROLE_LEADER) return;
    if (h->term != my_term) return;
    if (h->from_node < 0) return;

    /* Any in-term append response (success/failure) proves fresh peer contact */
    if (h->from_node < HA_MAX_NODES) {
        atomic_store(&g_rt->last_ack_us[h->from_node], now_us());
    }
    if (resp->success) {
        /* follower caught up through resp->match_index */
        HA_leader_on_ack(h->from_node, resp->match_index);
    } else {
        /* fast rewind: jump to follower's known-good match_index + 1 */
        uint64_t want = resp->match_index + 1;
        uint64_t cur = atomic_load(&g_rt->next_index[h->from_node]);
        if (want < cur) {
            atomic_store(&g_rt->next_index[h->from_node], want);
        }
        uint64_t mcur = atomic_load(&g_rt->match_index[h->from_node]);
        if (resp->match_index < mcur) {
            atomic_store(&g_rt->match_index[h->from_node], resp->match_index);
        }
    }
}

typedef struct {
    int fd;
} conn_arg_t;

static void *conn_thread(void *argp) {
    conn_arg_t *arg = (conn_arg_t *) argp;
    int cfd = arg->fd;
    slab_free(arg);

    // Basic socket knobs (keep it fast + avoid permanent blocking)
    int one = 1;
    setsockopt(cfd, IPPROTO_TCP, TCP_NODELAY, &one, sizeof(one));
    setsockopt(cfd, SOL_SOCKET, SO_KEEPALIVE, &one, sizeof(one));

    struct timeval tv;
    tv.tv_sec = 0;
    tv.tv_usec = 250000; // 250ms recv timeout (lets threads exit on stop)
    setsockopt(cfd, SOL_SOCKET, SO_RCVTIMEO, &tv, sizeof(tv));
    tv.tv_sec = 0;
    tv.tv_usec = 250000;
    setsockopt(cfd, SOL_SOCKET, SO_SNDTIMEO, &tv, sizeof(tv));

    SSL *ssl = NULL;
    if (HA_tls_enabled()) {
        ssl = HA_tls_new_server(cfd);
        if (!ssl) {
            close(cfd);
            return NULL;
        }
        int tmo = 1000;
        const char *t = getenv("RF_HA_TLS_TIMEOUT_MS");
        if (t && *t) {
            int v = atoi(t);
            if (v > 0) tmo = v;
        }
        if (HA_tls_handshake(ssl, cfd, 1, tmo) != 0) {
            // Handshake failed - could be peer disconnect or cert issue
            // Don't log excessively for normal disconnects
            close_conn(cfd, ssl);
            return NULL;
        }
    }

    int verified_peer = 0;

    while (atomic_load(&g_run)) {
        ha_msg_header_t h;
        ssize_t r = readn_any(cfd, ssl, &h, sizeof(h), &g_run);

        // PATCHED: Handle connection errors gracefully
        if (r <= 0) {
            if (r == 0) {
                // Peer closed connection cleanly - normal during shutdown
                break;
            }
            // Error cases
            int e = errno;
            if (e == EAGAIN || e == EWOULDBLOCK || e == ETIMEDOUT) {
                // Timeout - check if we should still run
                continue;
            }
            if (e == ECANCELED) {
                // Shutdown requested via run_flag
                break;
            }
            // Connection reset, broken pipe, etc - peer died
            // This is normal during Ctrl+C, no need to log
            break;
        }

        if (r != (ssize_t) sizeof(h)) {
            // Partial read - connection is bad
            break;
        }

        // Header sanity + CRC check
        if (h.magic != 0xA01) break;
        if (h.to_node != g_cfg->local_node_index) break;
        if (h.payload_size > HA_MAX_PAYLOAD) break;
        if (h.from_node < 0 || h.from_node >= HA_MAX_NODES) break;
        if (!g_cfg->nodes[h.from_node].node_id[0]) break;

        uint32_t want_crc = crc32c(0, &h, offsetof(ha_msg_header_t, crc));
        if (h.crc != want_crc) {
            fprintf(stderr, "??? HA_net: bad header CRC from_node=%d\n", h.from_node);
            break;
        }

        // Bind TLS identity to claimed from_node (optional strict policy)
        if (HA_tls_enabled() && ssl && !verified_peer) {
            if (!HA_tls_verify_peer_identity(ssl, g_cfg, h.from_node)) {
                fprintf(stderr, "??? HA_net: TLS identity does not match from_node=%d\n", h.from_node);
                break;
            }
            verified_peer = 1;
        }

        void *payload = NULL;
        if (h.payload_size) {
            payload = slab_alloc(h.payload_size);
            if (!payload) break;
            ssize_t pr = readn_any(cfd, ssl, payload, h.payload_size, &g_run);
            if (pr != (ssize_t) h.payload_size) {
                slab_free(payload);
                // Partial payload read - connection died mid-message
                break;
            }
        }

        switch (h.type) {
            case HA_MSG_HEARTBEAT:
                handle_heartbeat_only(&h);
                break;
            case HA_MSG_APPEND_ENTRIES:
                handle_append_entries(&h, payload, h.payload_size);
                break;
            case HA_MSG_APPEND_RESPONSE:
                if (payload && h.payload_size >= sizeof(ha_append_response_t))
                    handle_append_response(&h, (const ha_append_response_t *) payload);
                break;
            case HA_MSG_VOTE_REQUEST:
                if (payload && h.payload_size >= sizeof(ha_vote_request_t))
                    handle_vote_request(&h, (const ha_vote_request_t *) payload);
                break;
            case HA_MSG_VOTE_RESPONSE:
                if (payload && h.payload_size >= sizeof(ha_vote_response_t))
                    handle_vote_response(&h, (const ha_vote_response_t *) payload);
                break;
            case HA_MSG_INSTALL_SNAPSHOT:
                handle_install_snapshot(&h, payload, h.payload_size);
                break;
            default:
                break;
        }

        if (payload) slab_free(payload);
    }

    // PATCHED: Graceful cleanup
    close_conn(cfd, ssl);
    return NULL;
}

static void *server_thread(void *arg){
    (void)arg;
    // Parse "host:port" from this node's advertise_addr
    const char *addr = g_cfg->nodes[g_cfg->local_node_index].advertise_addr;
    char host[256] = {0};
    int port = 0;
    /* Robust split: host = before last ':', port = after last ':' */
    const char *last_colon = addr ? strrchr(addr, ':') : NULL;
    if (!addr || !last_colon || last_colon == addr) {
        fprintf(stderr, "??? HA_net: bad advertise_addr '%s'\n", addr ? addr : "(null)");
        return NULL;
    }
    size_t hostlen = (size_t) (last_colon - addr);
    if (hostlen >= sizeof(host)) hostlen = sizeof(host) - 1;
    memcpy(host, addr, hostlen);
    host[hostlen] = 0;
    port = atoi(last_colon + 1);

    fprintf(stderr, "???? HA_net: binding to advertise_addr='%s' => host='%s' port=%d\n",
            addr, host, port);

    int fd = socket(AF_INET, SOCK_STREAM, 0);
    if (fd < 0) {
        perror("socket");
        return NULL;
    }

    int one=1; setsockopt(fd, SOL_SOCKET, SO_REUSEADDR, &one, sizeof(one));
#ifdef SO_REUSEPORT
    setsockopt(fd, SOL_SOCKET, SO_REUSEPORT, &one, sizeof(one));
#endif

    struct sockaddr_in sa; memset(&sa,0,sizeof sa);
    sa.sin_family = AF_INET; sa.sin_port = htons((uint16_t)port);
    if (inet_pton(AF_INET, host, &sa.sin_addr) != 1) {
        fprintf(stderr, "??? HA_net: inet_pton failed for '%s' (addr='%s') ??? falling back to 0.0.0.0\n",
                host, addr);
        sa.sin_addr.s_addr = htonl(INADDR_ANY);
    }
    if (bind(fd, (struct sockaddr*)&sa, sizeof sa) < 0){
        perror("bind"); close(fd); return NULL;
    }
    if (listen(fd, 256) < 0){
        perror("listen"); close(fd); return NULL;
    }
    printf("???? HA net server listening on %s:%d\n", host, port);
    atomic_store(&g_listen_fd, fd);

    while (atomic_load(&g_run)){
        struct sockaddr_in ca; socklen_t calen=sizeof ca;
        int cfd = accept(fd, (struct sockaddr*)&ca, &calen);
        if (cfd < 0) {
            if (errno == EINTR) continue;
            if (!atomic_load(&g_run)) break;
            usleep(10000);
            continue;
        }

        conn_arg_t *ca0 = calloc(1, sizeof(*ca0));
        if (!ca0) {
            close(cfd);
            continue;
        }
        ca0->fd = cfd;

        pthread_t t;
        if (pthread_create(&t, NULL, conn_thread, ca0) == 0) {
            pthread_detach(t);
        } else {
            slab_free(ca0);
            close(cfd);
        }
    }
    close(fd);
    atomic_store(&g_listen_fd, -1);
    return NULL;
}

int HA_net_start_server(ha_config_t *config, ha_runtime_t *runtime){
    g_cfg = config; g_rt = runtime;
    atomic_store(&g_run, 1);
    if (pthread_create(&g_thr, NULL, server_thread, NULL) != 0){
        perror("pthread_create"); atomic_store(&g_run, 0); return -1;
    }
    return 0;
}

void HA_net_stop_server(void){
    if (!atomic_load(&g_run)) return;
    atomic_store(&g_run, 0);

    int lf = atomic_load(&g_listen_fd);
    if (lf >= 0) {
        shutdown(lf, SHUT_RDWR);
    }
    pthread_join(g_thr, NULL);
}








