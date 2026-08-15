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
#include "crc32c.h"
#include "ramforge_ha_tls.h"
#include "ramforge_ha_protocol.h"
#include "slab_alloc.h"
#define TCP_NODELAY           1

#define HA_MAX_PAYLOAD (16u * 1024u * 1024u) // 16MB safety cap
static ha_config_t  *g_cfg  = NULL;
static ha_runtime_t *g_rt   = NULL;
static pthread_t     g_thr  = 0;
static _Atomic int   g_run  = 0;
static _Atomic int   g_listen_fd = -1;

static inline uint64_t now_us(void){
    struct timespec ts; clock_gettime(CLOCK_MONOTONIC, &ts);
    return (uint64_t)ts.tv_sec*1000000ULL + ts.tv_nsec/1000;
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

static void send_append_response_to(const ha_msg_header_t *request,
                                    uint64_t match_index, int success) {
    if (!request || request->from_node < 0 || request->from_node >= HA_MAX_NODES)
        return;
    ha_msg_header_t header = {
        .type = HA_MSG_APPEND_RESPONSE,
        .term = atomic_load(&g_rt->term)
    };
    ha_append_response_t response = {
        .match_index = match_index,
        .success = success ? 1 : 0
    };
    (void)send_message(request->from_node, &header, &response, sizeof(response));
}

/* AppendEntries, including empty heartbeats, is accepted only after the
 * sender term and prev-log tuple have been validated.  A success response
 * proves that the indexed Raft log is on stable storage; it does not claim
 * that the state machine has already applied the record. */
static void handle_append_entries(const ha_msg_header_t *h, const void *pl, size_t psz) {
    uint64_t local_last = 0;
    HA_get_last_log_info(&local_last, NULL);
    uint64_t my_term = atomic_load(&g_rt->term);
    if (h->term < my_term) {
        send_append_response_to(h, local_last, 0);
        return;
    }
    if (!pl || psz < sizeof(ha_append_entries_t) ||
        HA_step_down_to_term(h->term, h->from_node) != 0) {
        send_append_response_to(h, local_last, 0);
        return;
    }

    const ha_append_entries_t *append = (const ha_append_entries_t *)pl;
    const uint8_t *entries = (const uint8_t *)(append + 1);
    size_t entries_size = psz - sizeof(*append);
    uint64_t match_index = local_last;
    if (HA_follower_store_entries(h->term, append, entries, entries_size,
                                  &match_index) != 0) {
        HA_get_last_log_info(&local_last, NULL);
        send_append_response_to(h, local_last, 0);
        return;
    }

    atomic_store(&g_rt->last_heartbeat_us, now_us());
    atomic_fetch_add(&g_rt->heartbeats_received, 1);

    uint64_t new_commit = ha_follower_commit_target(append->commit_index,
                                                    match_index);
    if (new_commit > atomic_load(&g_rt->commit_index) &&
        HA_update_commit_index(new_commit) != 0) {
        atomic_fetch_add(&g_rt->writes_failed, 1);
        send_append_response_to(h, match_index, 0);
        return;
    }
    if (HA_apply_committed_entries() != 0) {
        atomic_fetch_add(&g_rt->writes_failed, 1);
        send_append_response_to(h, match_index, 0);
        return;
    }
    if (atomic_load(&g_rt->state) != HA_STATE_FAILED)
        atomic_store(&g_rt->state, HA_STATE_READY);
    send_append_response_to(h, match_index, 1);
}

/* Snapshot installation previously mutated the state machine without a
 * durable snapshot manifest/log boundary.  Reject it until that protocol is
 * implemented; indefinite indexed-log catch-up remains the safe path. */
static void handle_install_snapshot(const ha_msg_header_t *h, const void *pl, size_t psz) {
    (void)pl;
    (void)psz;
    uint64_t local_last = 0;
    HA_get_last_log_info(&local_last, NULL);
    if (h->term >= atomic_load(&g_rt->term))
        (void)HA_step_down_to_term(h->term, h->from_node);
    atomic_fetch_add(&g_rt->snapshot_failures, 1);
    send_append_response_to(h, local_last, 0);
}
static void handle_vote_request(const ha_msg_header_t *h, const ha_vote_request_t *req){
    ha_vote_response_t resp = { .vote_granted = 0 };

    uint64_t my_term = atomic_load(&g_rt->term);
    if (h->term > my_term && HA_step_down_to_term(h->term, -1) != 0)
        goto respond;
    if (h->term < atomic_load(&g_rt->term)) goto respond;

    uint64_t candidate_bit = HA_NODE_MASK(h->from_node);
    uint64_t voter_union = atomic_load(&g_rt->voters_old_mask) |
                           atomic_load(&g_rt->voters_new_mask);
    if (!(voter_union & candidate_bit)) goto respond;

    uint64_t local_last_index = 0, local_last_term = 0;
    HA_get_last_log_info(&local_last_index, &local_last_term);
    if (!ha_candidate_log_is_up_to_date(req->last_log_term, req->last_log_index,
                                        local_last_term, local_last_index))
        goto respond;

    int voted_for = atomic_load(&g_rt->voted_for);
    if ((voted_for == -1 || voted_for == h->from_node) &&
        HA_grant_vote_durable(h->term, h->from_node) == 0) {
        resp.vote_granted = 1;
    }

respond:
    ha_msg_header_t hd = {
            .type = HA_MSG_VOTE_RESPONSE,
            .term = atomic_load(&g_rt->term),
    };
    (void) send_message(h->from_node, &hd, &resp, sizeof(resp));
}

static void handle_vote_response(const ha_msg_header_t *h, const ha_vote_response_t *resp){
    uint64_t my_term = atomic_load(&g_rt->term);
    if (h->term > my_term) {
        (void)HA_step_down_to_term(h->term, -1);
        return;
    }
    if (atomic_load(&g_rt->role) != HA_ROLE_CANDIDATE) return;
    if (h->term != my_term) return;

    (void)HA_record_vote_response_once(h->from_node, h->term,
                                       resp->vote_granted != 0);
}

static void handle_append_response(const ha_msg_header_t *h, const ha_append_response_t *resp) {

    uint64_t my_term = atomic_load(&g_rt->term);
    if (h->term > my_term) {
        /* Stale leader/candidate -> immediate stepdown on higher term */
        (void)HA_step_down_to_term(h->term, -1);
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
        /* A rejection carries a rewind hint, not proof of a replicated
         * match.  Never promote or regress match_index from a NACK. */
        uint64_t cur = atomic_load(&g_rt->next_index[h->from_node]);
        uint64_t want = ha_next_index_after_reject(cur, resp->match_index);
        atomic_store(&g_rt->next_index[h->from_node], want);
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

    int verified_peer_index = -1;

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

        // Bind every message on this TLS connection to the authenticated node.
        if (HA_tls_enabled() && ssl) {
            if (verified_peer_index < 0) {
                if (!HA_tls_verify_peer_identity(ssl, g_cfg, h.from_node)) {
                    fprintf(stderr, "??? HA_net: TLS identity does not match from_node=%d\n", h.from_node);
                    break;
                }
                verified_peer_index = h.from_node;
            } else if (h.from_node != verified_peer_index) {
                fprintf(stderr,
                        "??? HA_net: from_node changed on authenticated connection (%d -> %d)\n",
                        verified_peer_index, h.from_node);
                break;
            }
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
                handle_append_entries(&h, payload, h.payload_size);
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








