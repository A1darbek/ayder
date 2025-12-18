
#pragma once

#include <stddef.h>
#include <stdint.h>
#include <stdatomic.h>

#include <openssl/ssl.h>

#include "ramforge_ha_config.h"

// Initialize TLS contexts from env (idempotent).
// If RF_HA_TLS!=1, this is a no-op and returns 0.
int  HA_tls_init_from_env(void);
void HA_tls_shutdown(void);
int  HA_tls_enabled(void);

void HA_tls_graceful_shutdown(SSL *ssl);

// Create SSL objects bound to an existing fd.
SSL* HA_tls_new_client(int fd, const char *sni_host);
SSL* HA_tls_new_server(int fd);

// Nonblocking handshake helper (poll-based). Returns 0 on success, -1 on failure.
int  HA_tls_handshake(SSL *ssl, int fd, int is_server, int timeout_ms);

// Exact read/write helpers for stream framing (poll-based).
// Return bytes transferred, or -1 on failure.
ssize_t HA_tls_readn(SSL *ssl, int fd, void *buf, size_t n, int timeout_ms, const _Atomic int *run_flag);
ssize_t HA_tls_writen(SSL *ssl, int fd, const void *buf, size_t n, int timeout_ms);

// Optional identity binding: require peer cert to match node_id and/or advertise host/IP.
// If RF_HA_TLS_STRICT_ID=0, this returns 1 (skip check) as long as chain verification passed.
int HA_tls_verify_peer_identity(SSL *ssl, const ha_config_t *cfg, int peer_index);
