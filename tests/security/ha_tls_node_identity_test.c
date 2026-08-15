#define _GNU_SOURCE

#include <stdio.h>

#include <openssl/x509.h>
#include <openssl/x509v3.h>

/* Include the production translation unit so the test exercises the exact
 * strict-identity predicate without exposing it as public API. */
#include "../../src/ramforge_ha_tls.c"

int g_log_level = LOG_DEBUG;
int g_log_timestamp = 0;
__thread char g_log_worker_id[8] = {0};

static X509 *make_peer_certificate(void) {
    X509 *cert = X509_new();
    if (!cert) return NULL;

    if (X509_set_version(cert, 2) != 1 ||
        ASN1_INTEGER_set(X509_get_serialNumber(cert), 1) != 1) {
        X509_free(cert);
        return NULL;
    }

    X509_NAME *subject = X509_get_subject_name(cert);
    if (!subject ||
        X509_NAME_add_entry_by_txt(subject, "CN", MBSTRING_ASC,
                                   (const unsigned char *)"node2", -1, -1, 0) != 1 ||
        X509_set_issuer_name(cert, subject) != 1) {
        X509_free(cert);
        return NULL;
    }

    X509V3_CTX ctx;
    X509V3_set_ctx_nodb(&ctx);
    X509V3_set_ctx(&ctx, cert, cert, NULL, NULL, 0);
    X509_EXTENSION *san = X509V3_EXT_conf_nid(
        NULL, &ctx, NID_subject_alt_name,
        "DNS:node2,DNS:localhost,IP:127.0.0.1");
    if (!san || X509_add_ext(cert, san, -1) != 1) {
        X509_EXTENSION_free(san);
        X509_free(cert);
        return NULL;
    }
    X509_EXTENSION_free(san);
    return cert;
}

int main(void) {
    X509 *cert = make_peer_certificate();
    if (!cert) {
        fprintf(stderr, "failed to construct test certificate\n");
        return 1;
    }

    if (!cert_matches_node_id(cert, "node2")) {
        fprintf(stderr, "certificate did not authorize its own node ID\n");
        X509_free(cert);
        return 1;
    }
    if (cert_matches_node_id(cert, "node1")) {
        fprintf(stderr, "shared advertise-host SAN authorized the wrong node ID\n");
        X509_free(cert);
        return 1;
    }

    X509_free(cert);
    puts("PASS strict TLS identity requires the claimed node ID");
    return 0;
}
