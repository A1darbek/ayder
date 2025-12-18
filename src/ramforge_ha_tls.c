// ramforge_ha_tls.c - PATCHED: Graceful connection handling

#define _GNU_SOURCE

#include "ramforge_ha_tls.h"

#include <errno.h>
#include <fcntl.h>
#include <poll.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

#include <openssl/err.h>
#include <openssl/x509v3.h>

static _Atomic int g_tls_enabled = 0;
static _Atomic int g_tls_inited  = 0;
static _Atomic int g_tls_strict_id = 1;
static _Atomic int g_tls_timeout_ms = 1000;

static SSL_CTX *g_server_ctx = NULL;
static SSL_CTX *g_client_ctx = NULL;

static void tls_log_errors(const char *where) {
    unsigned long e;
    fprintf(stderr, "❌ TLS error at %s:\n", where ? where : "(unknown)");
    while ((e = ERR_get_error()) != 0) {
        char buf[256];
        ERR_error_string_n(e, buf, sizeof(buf));
        fprintf(stderr, "   - %s\n", buf);
    }
}

static int set_nonblock(int fd, int on) {
    int fl = fcntl(fd, F_GETFL, 0);
    if (fl < 0) return -1;
    if (on) fl |= O_NONBLOCK;
    else    fl &= ~O_NONBLOCK;
    return fcntl(fd, F_SETFL, fl);
}

static int poll_wait(int fd, short events, int timeout_ms) {
    struct pollfd pfd = { .fd = fd, .events = events, .revents = 0 };
    for (;;) {
        int r = poll(&pfd, 1, timeout_ms);
        if (r > 0) {
            // Check for error conditions
            if (pfd.revents & (POLLERR | POLLHUP | POLLNVAL)) {
                errno = ECONNRESET;
                return -1;
            }
            return 0;
        }
        if (r == 0) { errno = ETIMEDOUT; return -1; }
        if (errno == EINTR) continue;
        return -1;
    }
}

int HA_tls_enabled(void) {
    return atomic_load(&g_tls_enabled);
}

int HA_tls_init_from_env(void) {
    int expect = 0;
    if (!atomic_compare_exchange_strong(&g_tls_inited, &expect, 1)) {
        return 0; // already initialized
    }

    const char *en = getenv("RF_HA_TLS");
    if (!en || strcmp(en, "1") != 0) {
        atomic_store(&g_tls_enabled, 0);
        return 0;
    }

    const char *ca   = getenv("RF_HA_TLS_CA");
    const char *cert = getenv("RF_HA_TLS_CERT");
    const char *key  = getenv("RF_HA_TLS_KEY");
    if (!ca || !*ca || !cert || !*cert || !key || !*key) {
        fprintf(stderr, "❌ RF_HA_TLS=1 but RF_HA_TLS_CA/CERT/KEY not fully set\n");
        return -1;
    }

    const char *strict = getenv("RF_HA_TLS_STRICT_ID");
    if (strict && strict[0] == '0') atomic_store(&g_tls_strict_id, 0);

    const char *tmo = getenv("RF_HA_TLS_TIMEOUT_MS");
    if (tmo && *tmo) {
        int v = atoi(tmo);
        if (v > 0) atomic_store(&g_tls_timeout_ms, v);
    }

    OPENSSL_init_ssl(0, NULL);
    SSL_load_error_strings();
    OpenSSL_add_ssl_algorithms();

    g_server_ctx = SSL_CTX_new(TLS_server_method());
    g_client_ctx = SSL_CTX_new(TLS_client_method());
    if (!g_server_ctx || !g_client_ctx) {
        tls_log_errors("SSL_CTX_new");
        return -1;
    }

    // Min TLS 1.2 (TLS 1.3 preferred automatically)
    SSL_CTX_set_min_proto_version(g_server_ctx, TLS1_2_VERSION);
    SSL_CTX_set_min_proto_version(g_client_ctx, TLS1_2_VERSION);

    // Reasonable secure defaults
    SSL_CTX_set_options(g_server_ctx, SSL_OP_NO_COMPRESSION);
    SSL_CTX_set_options(g_client_ctx, SSL_OP_NO_COMPRESSION);

    SSL_CTX_set_mode(g_server_ctx, SSL_MODE_ENABLE_PARTIAL_WRITE | SSL_MODE_ACCEPT_MOVING_WRITE_BUFFER | SSL_MODE_RELEASE_BUFFERS);
    SSL_CTX_set_mode(g_client_ctx, SSL_MODE_ENABLE_PARTIAL_WRITE | SSL_MODE_ACCEPT_MOVING_WRITE_BUFFER | SSL_MODE_RELEASE_BUFFERS);

    // Cipher suites: TLS1.3 (OpenSSL ignores TLS1.2 list for TLS1.3)
    (void)SSL_CTX_set_ciphersuites(g_server_ctx,
                                   "TLS_AES_256_GCM_SHA384:TLS_CHACHA20_POLY1305_SHA256:TLS_AES_128_GCM_SHA256");
    (void)SSL_CTX_set_ciphersuites(g_client_ctx,
                                   "TLS_AES_256_GCM_SHA384:TLS_CHACHA20_POLY1305_SHA256:TLS_AES_128_GCM_SHA256");

    // TLS1.2 fallback cipher list (ECDHE only)
    if (SSL_CTX_set_cipher_list(g_server_ctx,
                                "ECDHE-ECDSA-AES256-GCM-SHA384:ECDHE-RSA-AES256-GCM-SHA384:"
                                "ECDHE-ECDSA-CHACHA20-POLY1305:ECDHE-RSA-CHACHA20-POLY1305:"
                                "ECDHE-ECDSA-AES128-GCM-SHA256:ECDHE-RSA-AES128-GCM-SHA256") != 1) {
        tls_log_errors("SSL_CTX_set_cipher_list(server)");
        return -1;
    }
    if (SSL_CTX_set_cipher_list(g_client_ctx,
                                "ECDHE-ECDSA-AES256-GCM-SHA384:ECDHE-RSA-AES256-GCM-SHA384:"
                                "ECDHE-ECDSA-CHACHA20-POLY1305:ECDHE-RSA-CHACHA20-POLY1305:"
                                "ECDHE-ECDSA-AES128-GCM-SHA256:ECDHE-RSA-AES128-GCM-SHA256") != 1) {
        tls_log_errors("SSL_CTX_set_cipher_list(client)");
        return -1;
    }

    // CA trust
    if (SSL_CTX_load_verify_locations(g_server_ctx, ca, NULL) != 1 ||
        SSL_CTX_load_verify_locations(g_client_ctx, ca, NULL) != 1) {
        tls_log_errors("SSL_CTX_load_verify_locations");
        return -1;
    }

    // Node cert+key (used both as server identity and client identity)
    if (SSL_CTX_use_certificate_chain_file(g_server_ctx, cert) != 1 ||
        SSL_CTX_use_PrivateKey_file(g_server_ctx, key, SSL_FILETYPE_PEM) != 1 ||
        SSL_CTX_check_private_key(g_server_ctx) != 1) {
        tls_log_errors("server cert/key");
        return -1;
    }
    if (SSL_CTX_use_certificate_chain_file(g_client_ctx, cert) != 1 ||
        SSL_CTX_use_PrivateKey_file(g_client_ctx, key, SSL_FILETYPE_PEM) != 1 ||
        SSL_CTX_check_private_key(g_client_ctx) != 1) {
        tls_log_errors("client cert/key");
        return -1;
    }

    // Verify peer certs.
    SSL_CTX_set_verify(g_server_ctx, SSL_VERIFY_PEER | SSL_VERIFY_FAIL_IF_NO_PEER_CERT, NULL);
    SSL_CTX_set_verify(g_client_ctx, SSL_VERIFY_PEER, NULL);

    // Session caching helps a lot if connections flap.
    SSL_CTX_set_session_cache_mode(g_server_ctx, SSL_SESS_CACHE_SERVER);
    SSL_CTX_set_session_cache_mode(g_client_ctx, SSL_SESS_CACHE_CLIENT);

    atomic_store(&g_tls_enabled, 1);
    fprintf(stderr, "🔐 HA mTLS enabled (strict_id=%d timeout_ms=%d)\n",
            atomic_load(&g_tls_strict_id), atomic_load(&g_tls_timeout_ms));
    return 0;
}

void HA_tls_shutdown(void) {
    if (g_server_ctx) { SSL_CTX_free(g_server_ctx); g_server_ctx = NULL; }
    if (g_client_ctx) { SSL_CTX_free(g_client_ctx); g_client_ctx = NULL; }
    atomic_store(&g_tls_enabled, 0);
}

SSL* HA_tls_new_client(int fd, const char *sni_host) {
    if (!HA_tls_enabled() || !g_client_ctx) return NULL;
    SSL *ssl = SSL_new(g_client_ctx);
    if (!ssl) return NULL;
    SSL_set_fd(ssl, fd);
    SSL_set_connect_state(ssl);
    if (sni_host && *sni_host) {
        (void)SSL_set_tlsext_host_name(ssl, sni_host);
    }
    return ssl;
}

SSL* HA_tls_new_server(int fd) {
    if (!HA_tls_enabled() || !g_server_ctx) return NULL;
    SSL *ssl = SSL_new(g_server_ctx);
    if (!ssl) return NULL;
    SSL_set_fd(ssl, fd);
    SSL_set_accept_state(ssl);
    return ssl;
}

int HA_tls_handshake(SSL *ssl, int fd, int is_server, int timeout_ms) {
    if (!ssl) return -1;
    (void)set_nonblock(fd, 1);

    int attempts = 0;
    int max_attempts = (timeout_ms / 50) + 10;  // Prevent infinite loop

    for (;;) {
        if (++attempts > max_attempts) {
            fprintf(stderr, "⚠️ TLS handshake timeout after %d attempts\n", attempts);
            return -1;
        }

        int r = is_server ? SSL_accept(ssl) : SSL_connect(ssl);
        if (r == 1) {
            long vr = SSL_get_verify_result(ssl);
            if (vr != X509_V_OK) {
                fprintf(stderr, "❌ TLS verify_result=%ld\n", vr);
                return -1;
            }
            return 0;
        }

        int err = SSL_get_error(ssl, r);

        if (err == SSL_ERROR_WANT_READ) {
            if (poll_wait(fd, POLLIN, timeout_ms) != 0) {
                if (errno != ETIMEDOUT) return -1;
                continue;  // Retry on timeout
            }
            continue;
        }
        if (err == SSL_ERROR_WANT_WRITE) {
            if (poll_wait(fd, POLLOUT, timeout_ms) != 0) {
                if (errno != ETIMEDOUT) return -1;
                continue;
            }
            continue;
        }

        // SSL_ERROR_SYSCALL - check for connection reset
        if (err == SSL_ERROR_SYSCALL) {
            int e = errno;
            if (e == ECONNRESET || e == EPIPE || e == ECONNREFUSED) {
                // Peer disconnected - not a TLS error, just connection closed
                return -1;
            }
            if (e == 0) {
                // EOF - peer closed connection
                return -1;
            }
        }

        // Only log actual TLS errors, not connection resets
        if (err != SSL_ERROR_SYSCALL) {
            tls_log_errors(is_server ? "SSL_accept" : "SSL_connect");
        }
        return -1;
    }
}

ssize_t HA_tls_readn(SSL *ssl, int fd, void *buf, size_t n, int timeout_ms, const _Atomic int *run_flag) {
    if (!ssl) return -1;

    size_t off = 0;
    int retry_count = 0;
    int max_retries = (timeout_ms / 50) + 20;  // Prevent infinite loop

    while (off < n) {
        // Check shutdown flag
        if (run_flag && !atomic_load(run_flag)) {
            errno = ECANCELED;
            return -1;
        }

        // Prevent infinite loop
        if (++retry_count > max_retries && off == 0) {
            errno = ETIMEDOUT;
            return -1;
        }

        ERR_clear_error();  // Clear any pending errors before read
        int r = SSL_read(ssl, (char*)buf + off, (int)(n - off));

        if (r > 0) {
            off += (size_t)r;
            retry_count = 0;  // Reset on successful read
            continue;
        }

        int err = SSL_get_error(ssl, r);

        // Clean close by peer
        if (err == SSL_ERROR_ZERO_RETURN) {
            return (ssize_t)off;
        }

        // System-level error (connection reset, broken pipe, etc.)
        if (err == SSL_ERROR_SYSCALL) {
            int e = errno;
            // Connection closed by peer - this is normal during shutdown
            if (e == ECONNRESET || e == EPIPE || e == ENOTCONN || e == 0) {
                // Return what we have, or -1 if nothing
                return off > 0 ? (ssize_t)off : -1;
            }
            // Other syscall error
            return -1;
        }

        // Need more data from network
        if (err == SSL_ERROR_WANT_READ) {
            int pr = poll_wait(fd, POLLIN, timeout_ms);
            if (pr != 0) {
                if (errno == ETIMEDOUT) continue;  // Retry on timeout
                if (errno == ECONNRESET) return off > 0 ? (ssize_t)off : -1;
                return -1;
            }
            continue;
        }

        // Need to write (renegotiation)
        if (err == SSL_ERROR_WANT_WRITE) {
            int pr = poll_wait(fd, POLLOUT, timeout_ms);
            if (pr != 0) {
                if (errno == ETIMEDOUT) continue;
                return -1;
            }
            continue;
        }

        // Other SSL error - connection is dead
        // Don't log for every normal disconnect
        return -1;
    }
    return (ssize_t)off;
}

ssize_t HA_tls_writen(SSL *ssl, int fd, const void *buf, size_t n, int timeout_ms) {
    if (!ssl) return -1;

    size_t off = 0;
    int retry_count = 0;
    int max_retries = (timeout_ms / 50) + 20;

    while (off < n) {
        if (++retry_count > max_retries && off == 0) {
            errno = ETIMEDOUT;
            return -1;
        }

        ERR_clear_error();
        int r = SSL_write(ssl, (const char*)buf + off, (int)(n - off));

        if (r > 0) {
            off += (size_t)r;
            retry_count = 0;
            continue;
        }

        int err = SSL_get_error(ssl, r);

        // System error
        if (err == SSL_ERROR_SYSCALL) {
            int e = errno;
            if (e == ECONNRESET || e == EPIPE || e == ENOTCONN || e == 0) {
                return off > 0 ? (ssize_t)off : -1;
            }
            return -1;
        }

        if (err == SSL_ERROR_WANT_READ) {
            int pr = poll_wait(fd, POLLIN, timeout_ms);
            if (pr != 0) {
                if (errno == ETIMEDOUT) continue;
                return -1;
            }
            continue;
        }

        if (err == SSL_ERROR_WANT_WRITE) {
            int pr = poll_wait(fd, POLLOUT, timeout_ms);
            if (pr != 0) {
                if (errno == ETIMEDOUT) continue;
                return -1;
            }
            continue;
        }

        // Other error
        return -1;
    }
    return (ssize_t)off;
}

// Graceful SSL shutdown (best effort, non-blocking)
void HA_tls_graceful_shutdown(SSL *ssl) {
    if (!ssl) return;

    // Set both shutdown flags to avoid waiting for peer
    SSL_set_shutdown(ssl, SSL_SENT_SHUTDOWN | SSL_RECEIVED_SHUTDOWN);

    // Try one non-blocking shutdown call
    SSL_shutdown(ssl);

    // Don't wait for response - we're closing anyway
}

static void split_host_from_advertise(const char *addr, char *host, size_t cap) {
    if (!addr || !*addr || cap == 0) { if (cap) host[0]=0; return; }
    const char *last_colon = strrchr(addr, ':');
    if (!last_colon || last_colon == addr) {
        strncpy(host, addr, cap-1);
        host[cap-1] = 0;
        return;
    }
    size_t n = (size_t)(last_colon - addr);
    if (n >= cap) n = cap - 1;
    memcpy(host, addr, n);
    host[n] = 0;
}

static int cert_matches_expectations(X509 *cert, const char *node_id, const char *adv_host) {
    if (!cert) return 0;
    int ok = 0;

    // 1) node_id as DNS/CN (covers common deployments)
    if (node_id && *node_id) {
        if (X509_check_host(cert, node_id, 0, 0, NULL) == 1) ok = 1;
    }

    // 2) advertise host (DNS or IP SAN)
    if (!ok && adv_host && *adv_host) {
        // If it's an IP literal, prefer IP SAN match.
        int is_ip = 0;
        for (const char *p = adv_host; *p; p++) {
            if ((*p >= '0' && *p <= '9') || *p == '.' || *p == ':') { is_ip = 1; continue; }
            is_ip = 0; break;
        }
        if (is_ip) {
            if (X509_check_ip_asc(cert, adv_host, 0) == 1) ok = 1;
        } else {
            if (X509_check_host(cert, adv_host, 0, 0, NULL) == 1) ok = 1;
        }
    }

    return ok;
}

int HA_tls_verify_peer_identity(SSL *ssl, const ha_config_t *cfg, int peer_index) {
    if (!HA_tls_enabled()) return 1;
    if (!ssl || !cfg || peer_index < 0 || peer_index >= cfg->node_count) return 0;
    if (atomic_load(&g_tls_strict_id) == 0) return 1;

    X509 *cert = SSL_get_peer_certificate(ssl);
    if (!cert) return 0;

    char host[256]; host[0]=0;
    split_host_from_advertise(cfg->nodes[peer_index].advertise_addr, host, sizeof(host));

    const char *node_id = cfg->nodes[peer_index].node_id;
    int ok = cert_matches_expectations(cert, node_id, host);
    X509_free(cert);

    if (!ok) {
        fprintf(stderr, "❌ TLS peer identity mismatch for peer_index=%d expect(node_id='%s' host='%s')\n",
                peer_index, node_id ? node_id : "(null)", host);
    }
    return ok;
}