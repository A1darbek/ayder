// src/http_server.c - ULTRA-FAST HTTP Server
// Zero-copy, SIMD-optimized, async I/O beast mode activated! 🚀
// FIXED: Proper SO_REUSEPORT for multi-worker clustering
#include "http_server.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <uv.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <unistd.h>
#include <fcntl.h>
#include <errno.h>
#include <stdarg.h>
#include "picohttpparser.h"
#include "http_framing.h"
#include "object_pool.h"
#include "router.h"
#include "slab_alloc.h"
#include "app.h"
#include "common.h"
#include <stdatomic.h>
#include <inttypes.h>
#include "shared_storage.h"
#include "aof_batch.h"
#include "ramforge_rotation_metrics.h"
#include "metrics_shared.h"
#include <ctype.h>
#include "persistence_zp.h"
#include "ramforge_ha_integration.h"
#include "http_timing.h"
#include "cluster.h"

extern SharedStorage *g_shared_storage;

RF_TLS uint64_t rf_http_recv_parse_us = 0;
RF_TLS uint64_t rf_http_queue_us      = 0;

// ═══════════════════════════════════════════════════════════════════════════════
// BEAST MODE CONFIGURATION - Tuned for Maximum Performance
// ═══════════════════════════════════════════════════════════════════════════════
#define MAX_REQUEST_SIZE     (64 * 1024)     // 64KB max request
#define MAX_RESPONSE_SIZE    (256 * 1024)    // 256KB max response
#define CONNECTION_POOL_SIZE  2048           // Pre-allocated connections
#define BUFFER_POOL_SIZE     4096           // Buffer pool size
#define TCP_NODELAY           1             // Disable Nagle's algorithm
#define TCP_KEEPALIVE         1             // Enable TCP keepalive
#define MAX_HEADERS          32             // Maximum number of headers to parse
#define PICO_PARTIAL_SIZE    4096           // Buffer for partial requests

static const char ERROR_400[] =
        "HTTP/1.1 400 Bad Request\r\n"
        "Content-Type: application/json\r\n"
        "Content-Length: 23\r\n"
        "Connection: close\r\n\r\n"
        "{\"error\":\"Bad Request\"}";

// ═══════════════════════════════════════════════════════════════════════════════
// Lightning-Fast Connection Context with Zero-Copy Buffers
// ═══════════════════════════════════════════════════════════════════════════════
typedef struct fast_buffer {
    char*  data;
    size_t len;
    size_t capacity;
    int    ref_count;
} fast_buffer_t;

typedef struct connection_ctx connection_ctx_t;

typedef struct {
    uv_write_t req;
    fast_buffer_t* buffer;
    int keep_alive;
    uint64_t request_id;
} write_req_t;


struct connection_ctx {
    // picohttpparser structures - ZERO allocation!
    struct phr_header headers[MAX_HEADERS];
    size_t num_headers;
    const char* method;
    size_t method_len;
    const char* path;
    size_t path_len;
    int minor_version;

    // Request buffer for picohttpparser (zero-copy parsing!)
    char* request_buf;
    size_t request_len;
    size_t request_capacity;

    // Parsed data (string views into request_buf - NO COPYING!)
    char method_str[16];        // Only for router compatibility
    char url[512];              // Only for router compatibility
    const char* body;           // Points directly into request_buf!
    size_t body_len;

    // Connection state
    uv_tcp_t* client;
    int msg_complete;
    int keep_alive;
    uint64_t request_id;
    uint64_t start_time_ns;

    // Partial request handling
    int parse_state;           // 0=header, 1=body, 2=complete
    size_t content_length;
    size_t body_received;

    // Pre-allocated response buffer
    fast_buffer_t* response_buf;

    uint32_t rl_limit;
    uint32_t rl_remaining;
    uint64_t rl_reset_epoch_s;
    int rl_have;
    int     is_chunked;
    size_t  header_len;
    size_t  cursor;          // absolute start of newly received chunk bytes
    http_chunk_decoder_t chunk_decoder;

    char   *chunk_buf;       // assembled body
    size_t  chunk_len;
    size_t  chunk_cap;

    uint64_t first_byte_us;     // when first byte of THIS request arrived
    uint64_t msg_complete_us;   // when THIS request became complete
    int connection_counted;
    uint64_t last_activity_us;
    connection_ctx_t *timeout_prev;
    connection_ctx_t *timeout_next;

};

// ═══════════════════════════════════════════════════════════════════════════════
// Global Performance Monitoring & Pools
// ═══════════════════════════════════════════════════════════════════════════════
static object_pool_t* connection_pool = NULL;
static object_pool_t *write_pool = NULL;
static object_pool_t* buffer_pool = NULL;
static uv_loop_t* main_loop = NULL;
static uv_tcp_t *server_handle = NULL;
static connection_ctx_t *connection_head = NULL;
static int http_shutdown_started = 0;

// Performance counters (lock-free atomic)
static volatile uint64_t total_bytes_sent = 0;
static volatile uint64_t total_bytes_received = 0;
static void write_complete_cb(uv_write_t* req, int status);
static void connection_close_cb(uv_handle_t* handle);
static void send_response(connection_ctx_t* ctx, const char* json_data,
                          size_t json_len, int status_code);


// expose to exporter
uint64_t RAMForge_http_2xx(void){ return atomic_load(&g_metrics.http_responses_2xx); }
uint64_t RAMForge_http_4xx(void){ return atomic_load(&g_metrics.http_responses_4xx); }
uint64_t RAMForge_http_3xx(void){ return atomic_load(&g_metrics.http_responses_3xx); }
uint64_t RAMForge_http_401(void){ return atomic_load(&g_metrics.http_responses_401); }
uint64_t RAMForge_http_429(void){ return atomic_load(&g_metrics.http_responses_429); }
uint64_t RAMForge_http_5xx(void){ return atomic_load(&g_metrics.http_responses_5xx); }
uint64_t RAMForge_http_503(void){ return atomic_load(&g_metrics.http_responses_503); }
uint64_t RAMForge_http_total(void){ return atomic_load(&g_metrics.http_requests_total); }

static void http_status_record(int status_code) {
    if (status_code >= 200 && status_code < 300) {
        atomic_fetch_add(&g_metrics.http_responses_2xx, 1);
    } else if (status_code >= 300 && status_code < 400) {
        atomic_fetch_add(&g_metrics.http_responses_3xx, 1);
    } else if (status_code >= 400 && status_code < 500) {
        atomic_fetch_add(&g_metrics.http_responses_4xx, 1);
        if (status_code == 401) atomic_fetch_add(&g_metrics.http_responses_401, 1);
        if (status_code == 429) atomic_fetch_add(&g_metrics.http_responses_429, 1);
    } else if (status_code >= 500 && status_code < 600) {
        atomic_fetch_add(&g_metrics.http_responses_5xx, 1);
        if (status_code == 503) atomic_fetch_add(&g_metrics.http_responses_503, 1);
    }
}

static void http_duration_record(uint64_t elapsed_us) {
    static const uint64_t upper_us[AYDER_METRIC_HIST_BUCKETS] = {
        500, 1000, 2500, 5000, 10000, 25000, 50000, 100000, 250000, 1000000
    };
    for (size_t i = 0; i < AYDER_METRIC_HIST_BUCKETS; i++) {
        if (elapsed_us <= upper_us[i]) {
            atomic_fetch_add(&g_metrics.http_duration_hist[i], 1);
            break;
        }
    }
    atomic_fetch_add(&g_metrics.http_duration_count, 1);
    atomic_fetch_add(&g_metrics.http_duration_sum_us, elapsed_us);
}

// --- Tunables (env overrides) ---
static int    RL_DEFAULT_RPS        = 50;     // tokens per second
static int    RL_DEFAULT_BURST      = 100;    // bucket capacity
static uint64_t RL_DEFAULT_DAILY    = 100000; // daily requests
static int    RL_ENABLED            = 1;      // set RF_HTTP_DISABLE_RL=1 to disable

static size_t BP_MAX_SEALED_Q       = 4096;   // backpressure if sealed queue > this
static size_t BP_MAX_ACTIVE_CONNS   = 32768;  // backpressure if active conns > this
static int    BP_RETRY_AFTER_SEC    = 1;      // Retry-After for 503
static int    METRICS_PUBLIC        = 0;      // explicit opt-in only
static int    TEST_HOOKS_ENABLED    = 0;
static char   CORS_ALLOWED_ORIGIN[256] = {0};
static uint64_t HEADER_TIMEOUT_MS  = 10000;
static uint64_t BODY_TIMEOUT_MS    = 30000;
static uint64_t IDLE_TIMEOUT_MS    = 60000;
static uint64_t REQUEST_TIMEOUT_MS = 30000;

int   HA_HTTP_IS_FOLLOWER   = 0;          // 1 => follower/replica/read-only
int   HA_READ_ONLY       = 0;          // force read-only regardless of role
/* Redirect target/behavior */
char  HA_LEADER_URL[1024] = {0};
static int   HA_REDIRECT_STATUS  = 307;       // 307 (default) or 308
static int   HA_RETRY_AFTER      = 1;         // seconds for Retry-After on redirect

/* Auto-configuration state */
static int   HA_AUTO_CONFIGURED  = 0;         // 1 if we successfully auto-configured from HA

/* Pull live HA state into HTTP gate on each request */
static inline void ha_refresh_http_flags(void){
    if (!RAMForge_HA_is_enabled()) return;
    /* live role */
    HA_HTTP_IS_FOLLOWER = RAMForge_HA_is_follower() ? 1 : 0;

    /* live leader url - only if we have HA enabled and configured */
    const char *lu = RAMForge_HA_get_leader_url();
    if (lu && *lu) {
        size_t n = strlen(lu);
        if (n >= sizeof(HA_LEADER_URL)) n = sizeof(HA_LEADER_URL) - 1;
        memcpy(HA_LEADER_URL, lu, n);
        HA_LEADER_URL[n] = 0;
    }
    /* live redirect tunables */
    int st = RAMForge_HA_get_redirect_status();
    if (st == 307 || st == 308) HA_REDIRECT_STATUS = st;
    int ra = RAMForge_HA_get_retry_after();
    if (ra >= 0) HA_RETRY_AFTER = ra;
    ha_shared_snapshot_t s;
    if (RAMForge_HA_read_snapshot(&s) && s.enabled) {
        HA_HTTP_IS_FOLLOWER = (s.role == HA_ROLE_FOLLOWER) ? 1 : 0;

        if (s.leader_url[0]) {
            size_t n = strlen(s.leader_url);
            if (n >= sizeof(HA_LEADER_URL)) n = sizeof(HA_LEADER_URL) - 1;
            memcpy(HA_LEADER_URL, s.leader_url, n);
            HA_LEADER_URL[n] = 0;
        } else {
            HA_LEADER_URL[0] = 0;
        }

        if (s.redirect_status == 307 || s.redirect_status == 308)
            HA_REDIRECT_STATUS = s.redirect_status;
        if (s.retry_after >= 0)
            HA_RETRY_AFTER = s.retry_after;
    }
}

// --- Token config parsed from env RF_BEARER_TOKENS ---
// Format: "tokenA@rps:burst:daily,tokenB@20:40:5000,tokenC"
typedef struct {
    char token[128];
    uint32_t rps;
    uint32_t burst;
    uint64_t daily;
} api_key_cfg_t;

static api_key_cfg_t g_keys[128];
static int g_key_count = 0;

// --- Per-key shared RL state stored in shared_storage ---
typedef struct {
    uint64_t last_us;        // last refill timestamp (monotonic us)
    uint32_t tokens;         // current bucket tokens
    uint64_t day_epoch;      // days since epoch (UTC)
    uint64_t daily_used;     // requests used in that day
} rl_state_t;


static void ha_auto_configure_http(void) {
    if (!RAMForge_HA_is_enabled()) return;

    /* Check if user explicitly set follower mode */
    const char *force_follower_env = getenv("RF_HTTP_FORCE_FOLLOWER");
    if (force_follower_env && force_follower_env[0] != '0') {
        /* User explicitly wants follower mode - respect it */
        HA_HTTP_IS_FOLLOWER = 1;
        fprintf(stderr, "[HA-HTTP] Explicit follower mode via RF_HTTP_FORCE_FOLLOWER\n");
    }

    /* Check if user explicitly set leader URL */
    const char *leader_url_env = getenv("RF_HTTP_LEADER_URL");
    if (leader_url_env && *leader_url_env) {
        /* User explicitly set leader URL - respect it */
        snprintf(HA_LEADER_URL, sizeof(HA_LEADER_URL), "%s", leader_url_env);
        fprintf(stderr, "[HA-HTTP] Explicit leader URL: %s\n", HA_LEADER_URL);
    }
    const char *local_id = RAMForge_HA_get_local_node_id();
    if (local_id) {
        fprintf(stderr, "[HA-HTTP] Auto-configuration enabled for node '%s'\n", local_id);
        fprintf(stderr, "[HA-HTTP] Redirect behavior will follow live HA role changes\n");
        HA_AUTO_CONFIGURED = 1;
    }
}

// small fnv1a for integer keys into shared_storage
static inline uint32_t fnv1a32(const void *p, size_t n){
    const uint8_t *s=(const uint8_t*)p; uint32_t h=2166136261u;
    for(size_t i=0;i<n;i++){ h^=s[i]; h*=16777619u; }
    return h ? h : 1; // avoid 0
}
static inline int rl_key_for_token(const char *tok, size_t len){
    // namespace 0xA1 << 24 to avoid collision with other maps
    return ((int)fnv1a32(tok, len)) | 0xA1000000;
}

static int ensure_chunk_cap(connection_ctx_t *ctx, size_t need){
    if (ctx->chunk_cap >= need) return 1;
    if (need > MAX_REQUEST_SIZE) return 0;
    size_t nc = ctx->chunk_cap ? ctx->chunk_cap : 4096;
    while (nc < need) {
        if (nc > MAX_REQUEST_SIZE / 2) {
            nc = MAX_REQUEST_SIZE;
            break;
        }
        nc *= 2;
    }
    char *nb = slab_alloc(nc);
    if (!nb) return 0;
    if (ctx->chunk_buf && ctx->chunk_len) memcpy(nb, ctx->chunk_buf, ctx->chunk_len);
    if (ctx->chunk_buf) slab_free(ctx->chunk_buf);
    ctx->chunk_buf = nb; ctx->chunk_cap = nc; return 1;
}

static inline uint64_t monotonic_us(void){
    struct timespec ts; clock_gettime(CLOCK_MONOTONIC, &ts);
    return (uint64_t)ts.tv_sec*1000000ULL + ts.tv_nsec/1000;
}
static inline uint64_t epoch_day_utc(void){
    time_t t=time(NULL); return (uint64_t)(t/86400);
}

static void parse_bearer_tokens_from_env(void){
    const char *env = getenv("RF_BEARER_TOKENS");
    if (!env || !*env) return;
    const char *p = env;
    while (*p && g_key_count < (int)(sizeof(g_keys)/sizeof(g_keys[0]))) {
        // token [@rps:burst:daily] , ...
        while (*p == ' ' || *p == ',') p++;
        if (!*p) break;
        api_key_cfg_t cfg = {0};
        // token
        const char *start = p;
        while (*p && *p!='@' && *p!=',') p++;
        size_t toklen = (size_t)(p - start);
        while (toklen && (start[toklen - 1] == ' ' || start[toklen - 1] == '\t')) toklen--;
        while (toklen && (*start == ' ' || *start == '\t')) { start++; toklen--; }
        if (toklen == 0 || toklen >= sizeof(cfg.token)) {
            fprintf(stderr, "[AUTH] Ignoring empty or overlong bearer-token entry\n");
            while (*p && *p != ',') p++;
            if (*p == ',') p++;
            continue;
        }
        memcpy(cfg.token, start, toklen); cfg.token[toklen]=0;

        cfg.rps   = RL_DEFAULT_RPS;
        cfg.burst = RL_DEFAULT_BURST;
        cfg.daily = RL_DEFAULT_DAILY;

        if (*p=='@'){ // @rps:burst:daily
            p++;
            cfg.rps = (uint32_t)strtoul(p, (char**)&p, 10);
            if (*p==':'){ p++; cfg.burst = (uint32_t)strtoul(p,(char**)&p,10); }
            if (*p==':'){ p++; cfg.daily = (uint64_t)strtoull(p,(char**)&p,10); }
        }
        // skip to next ','
        while (*p && *p!=',') p++;
        if (*p==',') p++;
        if (cfg.rps==0)  cfg.rps   = RL_DEFAULT_RPS;
        if (cfg.burst==0)cfg.burst = RL_DEFAULT_BURST;
        if (cfg.daily==0)cfg.daily = RL_DEFAULT_DAILY;

        g_keys[g_key_count++] = cfg;
    }
    fprintf(stderr,"[AUTH] Loaded %d bearer token(s) from RF_BEARER_TOKENS\n", g_key_count);
}

static void load_timeout_ms(const char *name, uint64_t *target) {
    const char *value = getenv(name);
    if (!value || !*value) return;
    char *end = NULL;
    errno = 0;
    unsigned long long parsed = strtoull(value, &end, 10);
    if (errno != 0 || end == value || *end != '\0' || parsed == 0 ||
        parsed > 86400000ULL) {
        fprintf(stderr, "[HTTP] Invalid %s; expected milliseconds in 1..86400000\n", name);
        exit(1);
    }
    *target = (uint64_t)parsed;
}

static void load_auth_env_overrides(void){
    const char *e;
    if ((e=getenv("RL_DEFAULT_RPS")))     RL_DEFAULT_RPS     = atoi(e)>0?atoi(e):RL_DEFAULT_RPS;
    if ((e=getenv("RL_DEFAULT_BURST")))   RL_DEFAULT_BURST   = atoi(e)>0?atoi(e):RL_DEFAULT_BURST;
    if ((e=getenv("RL_DEFAULT_DAILY")))   RL_DEFAULT_DAILY   = strtoull(e,NULL,10)?:RL_DEFAULT_DAILY;
    if ((e=getenv("BP_MAX_SEALED_Q")))    BP_MAX_SEALED_Q    = (size_t)strtoull(e,NULL,10)?:BP_MAX_SEALED_Q;
    if ((e=getenv("BP_MAX_ACTIVE_CONNS")))BP_MAX_ACTIVE_CONNS= (size_t)strtoull(e,NULL,10)?:BP_MAX_ACTIVE_CONNS;
    if ((e=getenv("BP_RETRY_AFTER_SEC"))) BP_RETRY_AFTER_SEC = atoi(e)>0?atoi(e):BP_RETRY_AFTER_SEC;
    if ((e=getenv("RF_HTTP_DISABLE_RL"))) RL_ENABLED = (e[0] == '0') ? 1 : 0;
    if ((e=getenv("RF_METRICS_PUBLIC"))) METRICS_PUBLIC = (e[0] == '1');
    if ((e=getenv("RF_ENABLE_TEST_HOOKS"))) TEST_HOOKS_ENABLED = (e[0] == '1');
    if ((e=getenv("RF_CORS_ALLOWED_ORIGIN")) && *e) {
        size_t n = strlen(e);
        int valid = n < sizeof(CORS_ALLOWED_ORIGIN) && strcmp(e, "*") != 0;
        for (size_t i = 0; valid && i < n; i++) {
            unsigned char c = (unsigned char)e[i];
            if (c <= 0x20 || c == 0x7f || c == ',') valid = 0;
        }
        if (!valid) {
            fprintf(stderr,
                    "[HTTP] RF_CORS_ALLOWED_ORIGIN must be one explicit origin without controls or commas\n");
            exit(1);
        }
        memcpy(CORS_ALLOWED_ORIGIN, e, n + 1);
    }

    if (!RL_ENABLED) {
        fprintf(stderr, "[AUTH] Rate limiting disabled via RF_HTTP_DISABLE_RL\n");
    }

    load_timeout_ms("RF_HTTP_HEADER_TIMEOUT_MS", &HEADER_TIMEOUT_MS);
    load_timeout_ms("RF_HTTP_BODY_TIMEOUT_MS", &BODY_TIMEOUT_MS);
    load_timeout_ms("RF_HTTP_IDLE_TIMEOUT_MS", &IDLE_TIMEOUT_MS);
    load_timeout_ms("RF_HTTP_REQUEST_TIMEOUT_MS", &REQUEST_TIMEOUT_MS);

}

static const char *g_skip_prefixes[64];
static int g_skip_prefixes_n = 0;

static void parse_auth_skip_prefixes_from_env(void){
    const char *env = getenv("RF_AUTH_SKIP_PREFIXES");
    if (!env || !*env) return;
    const char *p = env;
    while (*p && g_skip_prefixes_n < 64){
        while (*p==',' || *p==' ' || *p==';') p++;
        if (!*p) break;
        const char *s = p;
        while (*p && *p!=',' && *p!=';' ) p++;
        size_t n = (size_t)(p - s);
        char *copy = slab_alloc(n+1);
        memcpy(copy, s, n); copy[n] = 0;
        g_skip_prefixes[g_skip_prefixes_n++] = copy;
        if (*p) p++;
    }
}

static int has_skip_prefix(const char *path){
    for (int i=0;i<g_skip_prefixes_n;i++){
        const char *pre = g_skip_prefixes[i];
        size_t n = strlen(pre);
        if (n && strncmp(path, pre, n)==0) return 1;
    }
    return 0;
}

// fast header fetch (case-insensitive name)
static const struct phr_header* hdr_find(const connection_ctx_t *ctx, const char *name){
    size_t nlen = strlen(name);
    for (size_t i=0;i<ctx->num_headers;i++){
        if (ctx->headers[i].name_len == nlen &&
            strncasecmp(ctx->headers[i].name, name, nlen)==0){
            return &ctx->headers[i];
        }
    }
    return NULL;
}

static const char* get_bearer_from_headers(const connection_ctx_t *ctx, size_t *out_len){
    const struct phr_header *h = NULL;
    for (size_t i = 0; i < ctx->num_headers; i++) {
        if (ctx->headers[i].name_len == 13 &&
            strncasecmp(ctx->headers[i].name, "Authorization", 13) == 0) {
            if (h) return NULL; /* ambiguous credentials are rejected */
            h = &ctx->headers[i];
        }
    }
    if (!h) return NULL;

    const char *v = h->value;
    size_t vlen = h->value_len;

    // trim left spaces
    while (vlen && isspace((unsigned char)*v)) { v++; vlen--; }

    // must start with Bearer (case-insensitive)
    if (vlen < 6 || strncasecmp(v, "Bearer", 6) != 0) return NULL;
    v += 6; vlen -= 6;

    // RFC 6750 credentials require at least one space after the scheme.
    if (!vlen || (*v != ' ' && *v != '\t')) return NULL;
    while (vlen && (*v == ' ' || *v == '\t')) { v++; vlen--; }

    // trim right spaces
    while (vlen && isspace((unsigned char)v[vlen-1])) vlen--;

    if (vlen == 0) return NULL;
    if (out_len) *out_len = vlen;
    return v;
}

static const char *cors_headers_for_request(const connection_ctx_t *ctx,
                                            char *buffer, size_t capacity) {
    if (!buffer || capacity == 0) return "";
    buffer[0] = '\0';
    if (!CORS_ALLOWED_ORIGIN[0]) return buffer;

    const struct phr_header *origin = hdr_find(ctx, "Origin");
    size_t allowed_len = strlen(CORS_ALLOWED_ORIGIN);
    if (!origin || origin->value_len != allowed_len ||
        memcmp(origin->value, CORS_ALLOWED_ORIGIN, allowed_len) != 0) return buffer;

    int n = snprintf(buffer, capacity,
                     "Access-Control-Allow-Origin: %s\r\n"
                     "Access-Control-Allow-Methods: GET, POST, PUT, DELETE, QUERY, OPTIONS\r\n"
                     "Access-Control-Allow-Headers: Content-Type, Authorization\r\n"
                     "Vary: Origin\r\n",
                     CORS_ALLOWED_ORIGIN);
    if (n < 0 || (size_t)n >= capacity) buffer[0] = '\0';
    return buffer;
}

static int path_is_broker_query(const char *path){
    if (!path) return 0;
    return strcmp(path, "/broker/query") == 0 ||
           strncmp(path, "/broker/query?", 14) == 0;
}

static int header_value_json_content_type(const struct phr_header *h){
    if (!h) return 0;
    const char *v = h->value;
    size_t n = h->value_len;
    while (n && isspace((unsigned char)*v)) { v++; n--; }
    while (n && isspace((unsigned char)v[n - 1])) n--;

    const char media[] = "application/json";
    size_t media_len = sizeof(media) - 1;
    if (n < media_len || strncasecmp(v, media, media_len) != 0) return 0;
    if (n == media_len) return 1;
    return v[media_len] == ';' || isspace((unsigned char)v[media_len]);
}

static int extract_query_fingerprint_hex(const char *json, size_t len, char hex[65]) {
    static const char marker[] = "\"query_fingerprint\":\"sha256:";
    const char *p = NULL;
    if (!json || len < sizeof(marker)) return 0;
    for (const char *cur = json; cur + sizeof(marker) - 1 <= json + len; cur++) {
        if (memcmp(cur, marker, sizeof(marker) - 1) == 0) {
            p = cur + sizeof(marker) - 1;
            break;
        }
    }
    if (!p || p + 64 > json + len) return 0;
    for (int i = 0; i < 64; i++) {
        unsigned char c = (unsigned char)p[i];
        if (!((c >= '0' && c <= '9') || (c >= 'a' && c <= 'f'))) return 0;
        hex[i] = (char)c;
    }
    hex[64] = 0;
    return 1;
}

static void query_etag_from_hex(const char hex[65], char etag[96]) {
    snprintf(etag, 96, "\"ayder-query-sha256-%s\"", hex);
}

static int if_none_match_has_etag(const connection_ctx_t *ctx, const char *etag) {
    const struct phr_header *h = hdr_find(ctx, "If-None-Match");
    if (!h || !etag) return 0;
    const char *v = h->value;
    size_t n = h->value_len;
    while (n && isspace((unsigned char)*v)) { v++; n--; }
    while (n && isspace((unsigned char)v[n - 1])) n--;
    if (n == 1 && v[0] == '*') return 1;
    size_t etag_len = strlen(etag);
    const char *end = v + n;
    while (v < end) {
        while (v < end && (isspace((unsigned char)*v) || *v == ',')) v++;
        const char *start = v;
        while (v < end && *v != ',') v++;
        const char *item_end = v;
        while (item_end > start && isspace((unsigned char)item_end[-1])) item_end--;
        if ((size_t)(item_end - start) == etag_len && memcmp(start, etag, etag_len) == 0) return 1;
    }
    return 0;
}

static int path_is_public(const char *path){
    if (has_skip_prefix(path)) return 1;  // runtime-configurable
    // allow these without auth/rate: tweak via RF_AUTH_SKIP_PATHS if you want
    if (strcmp(path,"/health")==0)  return 1;
    if (METRICS_PUBLIC && strcmp(path,"/metrics")==0) return 1;
    if (METRICS_PUBLIC && strcmp(path,"/metrics_ha")==0) return 1;
    if (strcmp(path,"/ready")==0)   return 1;
    return 0;
}

static api_key_cfg_t* find_key_cfg(const char *tok, size_t len){
    for (int i=0;i<g_key_count;i++){
        size_t configured_len = strlen(g_keys[i].token);
        size_t diff = configured_len ^ len;
        for (size_t j = 0; j < sizeof(g_keys[i].token); j++) {
            unsigned char a = j < configured_len ? (unsigned char)g_keys[i].token[j] : 0;
            unsigned char b = j < len ? (unsigned char)tok[j] : 0;
            diff |= (size_t)(a ^ b);
        }
        if (diff == 0)
            return &g_keys[i];
    }
    return NULL;
}


/* Create a pre-bound, non-blocking, CLOEXEC listener with SO_REUSEPORT.      */
static int make_reuseport_listener_ipv4(const char *ip, int port) {
    int fd = socket(AF_INET, SOCK_STREAM | SOCK_NONBLOCK | SOCK_CLOEXEC, 0);
    if (fd < 0) {
        perror("socket");
        return -1;
    }

    int one = 1;
    if (setsockopt(fd, SOL_SOCKET, SO_REUSEADDR, &one, sizeof(one)) < 0) {
        perror("setsockopt(SO_REUSEADDR)");
        close(fd);
        return -1;
    }
    if (setsockopt(fd, SOL_SOCKET, SO_REUSEPORT, &one, sizeof(one)) < 0) {
        perror("setsockopt(SO_REUSEPORT)");
        close(fd);
        return -1;
    }

    struct sockaddr_in addr;
    memset(&addr, 0, sizeof(addr));
    addr.sin_family = AF_INET;
    addr.sin_port = htons((uint16_t) port);
    if (uv_inet_pton(AF_INET, ip, &addr.sin_addr) != 0) {
        fprintf(stderr, "inet_pton failed for %s\n", ip);
        close(fd);
        return -1;
    }
    if (bind(fd, (struct sockaddr *) &addr, sizeof(addr)) < 0) {
        perror("bind");
        close(fd);
        return -1;
    }
    return fd;
}

// High-resolution timing
static inline uint64_t get_time_ns(void) {
    struct timespec ts;
    clock_gettime(CLOCK_MONOTONIC, &ts);
    return (uint64_t)ts.tv_sec * 1000000000ULL + ts.tv_nsec;
}

// ═══════════════════════════════════════════════════════════════════════════════
// Zero-Copy Buffer Management
// ═══════════════════════════════════════════════════════════════════════════════
static fast_buffer_t* buffer_create(size_t size) {
    fast_buffer_t* buf = slab_alloc(sizeof(fast_buffer_t));
    buf->data = slab_alloc(size);
    buf->len = 0;
    buf->capacity = size;
    buf->ref_count = 1;
    return buf;
}

static void buffer_release(fast_buffer_t* buf) {
    if (!buf) return;

    // Atomic decrement would be better, but simple check for now
    buf->ref_count--;
    if (buf->ref_count <= 0) {
        if (buf->data) {
            slab_free(buf->data);
            buf->data = NULL;
        }
        slab_free(buf);
    }
}

static void buffer_reset(fast_buffer_t* buf) {
    buf->len = 0;
}

static int buffer_appendf(fast_buffer_t *buf, const char *format, ...) {
    if (!buf || !buf->data || buf->len >= buf->capacity) return -1;
    va_list args;
    va_start(args, format);
    int n = vsnprintf(buf->data + buf->len, buf->capacity - buf->len,
                      format, args);
    va_end(args);
    if (n < 0 || (size_t)n >= buf->capacity - buf->len) return -1;
    buf->len += (size_t)n;
    return 0;
}

static int buffer_append_bytes(fast_buffer_t *buf, const void *data, size_t len) {
    if (!buf || (!data && len) || len > buf->capacity - buf->len) return -1;
    if (len) memcpy(buf->data + buf->len, data, len);
    buf->len += len;
    return 0;
}

// ═══════════════════════════════════════════════════════════════════════════════
// Ultra-Fast Date Cache (updates every second)
// ═══════════════════════════════════════════════════════════════════════════════
static char cached_date[64];
static time_t last_date_update = 0;
static uv_timer_t date_timer;
static uv_timer_t lat_timer;

static void connection_track(connection_ctx_t *ctx) {
    ctx->timeout_prev = NULL;
    ctx->timeout_next = connection_head;
    if (connection_head) connection_head->timeout_prev = ctx;
    connection_head = ctx;
}

static void connection_untrack(connection_ctx_t *ctx) {
    if (ctx->timeout_prev) ctx->timeout_prev->timeout_next = ctx->timeout_next;
    else if (connection_head == ctx) connection_head = ctx->timeout_next;
    if (ctx->timeout_next) ctx->timeout_next->timeout_prev = ctx->timeout_prev;
    ctx->timeout_prev = NULL;
    ctx->timeout_next = NULL;
}

static void server_close_cb(uv_handle_t *handle) {
    if ((void *)server_handle == (void *)handle) server_handle = NULL;
    slab_free(handle);
}

static void begin_http_loop_shutdown(uv_loop_t *loop) {
    if (http_shutdown_started) return;
    http_shutdown_started = 1;

    if (server_handle && !uv_is_closing((uv_handle_t *)server_handle))
        uv_close((uv_handle_t *)server_handle, server_close_cb);

    for (connection_ctx_t *ctx = connection_head; ctx;) {
        connection_ctx_t *next = ctx->timeout_next;
        ctx->keep_alive = 0;
        if (ctx->client && !uv_is_closing((uv_handle_t *)ctx->client)) {
            uv_read_stop((uv_stream_t *)ctx->client);
            uv_close((uv_handle_t *)ctx->client, connection_close_cb);
        }
        ctx = next;
    }
    uv_timer_stop(&date_timer);
    uv_timer_stop(&lat_timer);
    uv_stop(loop);
}

static void update_date_cache(uv_timer_t* timer) {
    if (cluster_shutdown_is_requested()) {
        begin_http_loop_shutdown(timer->loop);
        return;
    }
    uint64_t mono_now = monotonic_us();
    for (connection_ctx_t *ctx = connection_head; ctx;) {
        connection_ctx_t *next = ctx->timeout_next;
        uint64_t elapsed_ms = 0;
        uint64_t limit_ms = IDLE_TIMEOUT_MS;
        uint64_t start_us = ctx->last_activity_us;

        if (ctx->first_byte_us) {
            start_us = ctx->first_byte_us;
            limit_ms = ctx->parse_state == 0 ? HEADER_TIMEOUT_MS : BODY_TIMEOUT_MS;
            if (REQUEST_TIMEOUT_MS < limit_ms) limit_ms = REQUEST_TIMEOUT_MS;
        }
        if (mono_now >= start_us) elapsed_ms = (mono_now - start_us) / 1000ULL;
        if (start_us && elapsed_ms >= limit_ms && ctx->client &&
            !uv_is_closing((uv_handle_t *)ctx->client)) {
            ctx->keep_alive = 0;
            uv_read_stop((uv_stream_t *)ctx->client);
            uv_close((uv_handle_t *)ctx->client, connection_close_cb);
        }
        ctx = next;
    }
    time_t now = time(NULL);
    struct tm tm;
    gmtime_r(&now, &tm);
    strftime(cached_date, sizeof(cached_date), "%a, %d %b %Y %H:%M:%S GMT", &tm);
    last_date_update = now;
}

static void send_unauthorized(connection_ctx_t *ctx){
    const char body[] = "{\"error\":\"unauthorized\"}";
    char cors[512];
    const char *cors_headers = cors_headers_for_request(ctx, cors, sizeof(cors));
    fast_buffer_t *buf = ctx->response_buf; buffer_reset(buf);
    int n = snprintf(buf->data, buf->capacity,
                     "HTTP/1.1 401 Unauthorized\r\n"
                     "Date: %s\r\n"
                     "Server: Ayder/3.0\r\n"
                     "Content-Type: application/json\r\n"
                     "Content-Length: %zu\r\n"
                     "WWW-Authenticate: Bearer realm=\"Ayder\", error=\"invalid_token\"\r\n"
                     "Connection: %s\r\n"
                     "%s"
                     "\r\n",
                     cached_date, sizeof(body)-1, ctx->keep_alive?"keep-alive":"close",
                     cors_headers);
    memcpy(buf->data+n, body, sizeof(body)-1);
    buf->len = n + (int)sizeof(body)-1;
    write_req_t *wr = object_pool_get(write_pool); if(!wr) wr = slab_alloc(sizeof(*wr));
    wr->buffer=buf; wr->keep_alive=ctx->keep_alive; wr->request_id=ctx->request_id; buf->ref_count++;
    uv_buf_t uvb = uv_buf_init(buf->data, buf->len);
    uv_write((uv_write_t*)wr, (uv_stream_t*)ctx->client, &uvb, 1, (uv_write_cb)write_complete_cb);

    http_status_record(401);
}

static void send_rate_limited(connection_ctx_t *ctx, int retry_after_s,
                              uint32_t limit, uint32_t remaining, uint64_t reset_epoch_s){
    char body[256];
    char cors[512];
    const char *cors_headers = cors_headers_for_request(ctx, cors, sizeof(cors));
    int bl = snprintf(body, sizeof(body),
                      "{\"error\":\"rate_limited\",\"retry_after\":%d}", retry_after_s);
    fast_buffer_t *buf = ctx->response_buf; buffer_reset(buf);
    int n = snprintf(buf->data, buf->capacity,
                     "HTTP/1.1 429 Too Many Requests\r\n"
                     "Date: %s\r\n"
                     "Server: Ayder/3.0\r\n"
                     "Content-Type: application/json\r\n"
                     "Content-Length: %d\r\n"
                     "Retry-After: %d\r\n"
                     "X-RateLimit-Limit: %u\r\n"
                     "X-RateLimit-Remaining: %u\r\n"
                     "X-RateLimit-Reset: %" PRIu64 "\r\n"
                     "Connection: %s\r\n"
                     "%s"
                     "\r\n",
                     cached_date, bl, retry_after_s, limit, remaining, reset_epoch_s,
                     ctx->keep_alive?"keep-alive":"close", cors_headers);
    memcpy(buf->data+n, body, bl); buf->len = n + bl;
    write_req_t *wr = object_pool_get(write_pool); if(!wr) wr = slab_alloc(sizeof(*wr));
    wr->buffer=buf; wr->keep_alive=ctx->keep_alive; wr->request_id=ctx->request_id; buf->ref_count++;
    uv_buf_t uvb = uv_buf_init(buf->data, buf->len);
    uv_write((uv_write_t*)wr, (uv_stream_t*)ctx->client, &uvb, 1, (uv_write_cb)write_complete_cb);

    http_status_record(429);
}

static void send_backpressure(connection_ctx_t *ctx, int retry_after_s){
    const char body[] = "{\"error\":\"overloaded\"}";
    char cors[512];
    const char *cors_headers = cors_headers_for_request(ctx, cors, sizeof(cors));
    fast_buffer_t *buf = ctx->response_buf; buffer_reset(buf);
    int n = snprintf(buf->data, buf->capacity,
                     "HTTP/1.1 503 Service Unavailable\r\n"
                     "Date: %s\r\n"
                     "Server: Ayder/3.0\r\n"
                     "Content-Type: application/json\r\n"
                     "Content-Length: %zu\r\n"
                     "Retry-After: %d\r\n"
                     "Connection: %s\r\n"
                     "%s"
                     "\r\n",
                     cached_date, sizeof(body)-1, retry_after_s,
                     ctx->keep_alive?"keep-alive":"close", cors_headers);
    memcpy(buf->data+n, body, sizeof(body)-1); buf->len = n + (int)sizeof(body)-1;
    write_req_t *wr = object_pool_get(write_pool); if(!wr) wr = slab_alloc(sizeof(*wr));
    wr->buffer=buf; wr->keep_alive=ctx->keep_alive; wr->request_id=ctx->request_id; buf->ref_count++;
    uv_buf_t uvb = uv_buf_init(buf->data, buf->len);
    uv_write((uv_write_t*)wr, (uv_stream_t*)ctx->client, &uvb, 1, (uv_write_cb)write_complete_cb);

    http_status_record(503);
    atomic_fetch_add(&g_metrics.backpressure_events_total, 1);
}

static int rl_take_one(const api_key_cfg_t *cfg, const char *tok, size_t toklen,
                       uint32_t *out_limit, uint32_t *out_remaining, uint64_t *out_reset_epoch_s,
                       int *out_retry_after_s)
{
    if (!g_shared_storage) return 0; // if no shmem, allow all (fail-open)
    const int skey = rl_key_for_token(tok, toklen);
    rl_state_t st = {0};
    int have = shared_storage_get_fast(g_shared_storage, skey, &st, sizeof(st)) ? 1 : 0;

    const uint64_t now_mono = monotonic_us();
    const uint64_t today    = epoch_day_utc();

    if (!have) { st.tokens = cfg->burst; st.last_us = now_mono; st.day_epoch=today; st.daily_used=0; }

    // daily window rollover
    if (st.day_epoch != today) { st.day_epoch=today; st.daily_used=0; }

    // refill token bucket
    if (cfg->rps > 0){
        uint64_t elapsed = (now_mono > st.last_us) ? (now_mono - st.last_us) : 0;
        uint64_t gained  = (elapsed * (uint64_t)cfg->rps) / 1000000ULL;
        if (gained){
            uint64_t nt = (uint64_t)st.tokens + gained;
            st.tokens = (uint32_t)(nt > cfg->burst ? cfg->burst : nt);
            st.last_us = now_mono; // coarse; keeps math simple
        }
    }

    // quota first
    if (st.daily_used >= cfg->daily){
        // seconds until next UTC day
        time_t now_s = time(NULL);
        time_t next_day_s = (now_s/86400 + 1) * 86400;
        int retry_s = (int)(next_day_s - now_s);
        if (retry_s < 1) retry_s = 1;
        if (out_limit)      *out_limit      = (uint32_t)cfg->rps;
        if (out_remaining)  *out_remaining  = 0;
        if (out_reset_epoch_s) *out_reset_epoch_s = (uint64_t)next_day_s;
        if (out_retry_after_s) *out_retry_after_s = retry_s;
        return 1;
    }

    // token bucket request cost = 1
    if (st.tokens == 0){
        // time until 1 token arrives
        int retry_s = (cfg->rps>0) ? 1 : 1;
        if (out_limit)      *out_limit      = (uint32_t)cfg->rps;
        if (out_remaining)  *out_remaining  = 0;
        if (out_reset_epoch_s) *out_reset_epoch_s = (uint64_t)(time(NULL)+retry_s);
        if (out_retry_after_s) *out_retry_after_s = retry_s;
        // persist unchanged counters
        shared_storage_set(g_shared_storage, skey, &st, sizeof(st));
        return 2;
    }

    st.tokens--;
    st.daily_used++;
    shared_storage_set(g_shared_storage, skey, &st, sizeof(st));
    if (out_limit)      *out_limit      = (uint32_t)cfg->rps;
    if (out_remaining)  *out_remaining  = st.tokens;
    if (out_reset_epoch_s) *out_reset_epoch_s = (uint64_t)(time(NULL)+1); // reset tick in ~1s
    if (out_retry_after_s) *out_retry_after_s = 0;
    return 0;
}


uint64_t RAMForge_http_active_connections(void){
    return atomic_load(&g_metrics.http_active_connections);
}
static int overload_retry_after_sec(void){
    // simple two-knob policy
    if (AOF_sealed_queue_depth() > BP_MAX_SEALED_Q) return BP_RETRY_AFTER_SEC;
    if (atomic_load(&g_metrics.http_active_connections) > BP_MAX_ACTIVE_CONNS)
        return BP_RETRY_AFTER_SEC;
    return 0;
}

// ═══════════════════════════════════════════════════════════════════════════════
// Memory Allocation Callbacks (Pool-based for Speed)
// ═══════════════════════════════════════════════════════════════════════════════
static void alloc_cb(uv_handle_t* handle, size_t suggested_size, uv_buf_t* buf) {
    (void)handle;
    (void)suggested_size;

    void* mem = object_pool_get(buffer_pool);
    if (!mem) {
        mem = slab_alloc(MAX_REQUEST_SIZE);
    }

    buf->base = (char*)mem;
    buf->len = MAX_REQUEST_SIZE;
}

// ──────────────────────────────────────────────────────────────────────────────
// Generic responder (non-JSON payloads, e.g. Prometheus text format)
// ──────────────────────────────────────────────────────────────────────────────
static void send_raw(connection_ctx_t *ctx,
                     const char *content_type,
                     const char *body,
                     size_t body_len,
                     int status_code)
{
    fast_buffer_t *buf = ctx->response_buf;
    buffer_reset(buf);

    const char *status_txt =
            (status_code == 200) ? "200 OK" :
            (status_code == 204) ? "204 No Content" :
            (status_code == 404) ? "404 Not Found" :
            (status_code == 503) ? "503 Service Unavailable" :
            "500 Internal Server Error";

    if (buffer_appendf(buf,
                       "HTTP/1.1 %s\r\n"
                       "Date: %s\r\n"
                       "Server: Ayder/3.0\r\n"
                       "Content-Type: %s\r\n"
                       "Content-Length: %zu\r\n"
                       "Connection: %s\r\n\r\n",
                       status_txt, cached_date, content_type, body_len,
                       ctx->keep_alive ? "keep-alive" : "close") != 0 ||
        buffer_append_bytes(buf, body, body_len) != 0) {
        static const char error[] = "{\"ok\":false,\"error\":\"response_too_large\"}";
        send_response(ctx, error, sizeof(error) - 1, 500);
        return;
    }

    write_req_t *wr = object_pool_get(write_pool);
    if (!wr) wr = slab_alloc(sizeof(write_req_t));
    wr->buffer      = buf;
    wr->keep_alive  = ctx->keep_alive;
    wr->request_id  = ctx->request_id;
    buf->ref_count++;

    uv_buf_t uvb = uv_buf_init(buf->data, buf->len);
    uv_write((uv_write_t *)wr, (uv_stream_t *)ctx->client, &uvb, 1,
             (uv_write_cb)write_complete_cb);

    total_bytes_sent += buf->len;
    http_status_record(status_code);
}


static void send_redirect_response(connection_ctx_t *ctx,
                                   int status_code,
                                   const char *location,
                                   int retry_after_s,
                                   const char *json_body,
                                   size_t json_len)
{
    if (status_code != 307 && status_code != 308) status_code = 307;
    if (!location) location = "";

    fast_buffer_t *buf = ctx->response_buf;
    buffer_reset(buf);

    const char *status_txt = (status_code == 308) ? "308 Permanent Redirect" : "307 Temporary Redirect";
    size_t body_len = (json_body && json_len) ? json_len : 0;
    char cors[512];
    const char *cors_headers = cors_headers_for_request(ctx, cors, sizeof(cors));

    /* Ensure we have enough capacity */
    size_t max_header_size = 1024 + strlen(location);  /* Conservative estimate */
    if (max_header_size + body_len >= buf->capacity) {
        /* Truncate body if necessary */
        if (max_header_size >= buf->capacity) {
            body_len = 0;
        } else {
            body_len = buf->capacity - max_header_size - 1;
        }
    }

    int hdr_len;
    size_t remaining = buf->capacity;

    if (retry_after_s > 0) {
        hdr_len = snprintf(buf->data, remaining,
                           "HTTP/1.1 %s\r\n"
                           "Date: %s\r\n"
                           "Server: Ayder/3.0\r\n"
                           "Content-Type: application/json; charset=utf-8\r\n"
                           "Content-Length: %zu\r\n"
                           "Location: %s\r\n"
                           "Retry-After: %d\r\n"
                           "Connection: %s\r\n"
                           "Cache-Control: no-store\r\n"
                           "%s"
                           "\r\n",
                           status_txt, cached_date, body_len, location, retry_after_s,
                           ctx->keep_alive ? "keep-alive" : "close", cors_headers);
    } else {
        hdr_len = snprintf(buf->data, remaining,
                           "HTTP/1.1 %s\r\n"
                           "Date: %s\r\n"
                           "Server: Ayder/3.0\r\n"
                           "Content-Type: application/json; charset=utf-8\r\n"
                           "Content-Length: %zu\r\n"
                           "Location: %s\r\n"
                           "Connection: %s\r\n"
                           "Cache-Control: no-store\r\n"
                           "%s"
                           "\r\n",
                           status_txt, cached_date, body_len, location,
                           ctx->keep_alive ? "keep-alive" : "close", cors_headers);
    }

    /* Handle snprintf overflow/error */
    if (hdr_len < 0) {
        hdr_len = 0;
    } else if ((size_t)hdr_len >= remaining) {
        hdr_len = (int)remaining - 1;
        buf->data[hdr_len] = '\0';
    }

    /* Copy body if there's room */
    if (body_len > 0 && (size_t)hdr_len + body_len < buf->capacity) {
        memcpy(buf->data + hdr_len, json_body, body_len);
        buf->len = hdr_len + body_len;
    } else {
        buf->len = hdr_len;
    }

    write_req_t *wr = object_pool_get(write_pool);
    if (!wr) wr = slab_alloc(sizeof(*wr));
    wr->buffer = buf;
    wr->keep_alive = ctx->keep_alive;
    wr->request_id = ctx->request_id;
    buf->ref_count++;

    uv_buf_t uvb = uv_buf_init(buf->data, (unsigned int)buf->len);
    uv_write((uv_write_t*)wr, (uv_stream_t*)ctx->client, &uvb, 1, (uv_write_cb)write_complete_cb);

    http_status_record(status_code);
    total_bytes_sent += buf->len;
}


static const char *http_status_text(int status_code) {
    switch (status_code) {
        case 200: return "200 OK";
        case 204: return "204 No Content";
        case 304: return "304 Not Modified";
        case 307: return "307 Temporary Redirect";
        case 308: return "308 Permanent Redirect";
        case 400: return "400 Bad Request";
        case 401: return "401 Unauthorized";
        case 404: return "404 Not Found";
        case 405: return "405 Method Not Allowed";
        case 415: return "415 Unsupported Media Type";
        case 429: return "429 Too Many Requests";
        case 503: return "503 Service Unavailable";
        default: return "500 Internal Server Error";
    }
}

static void send_response(connection_ctx_t* ctx, const char* json_data, size_t json_len, int status_code) {
    static const char overflow_body[] =
        "{\"ok\":false,\"error\":\"response_too_large\"}";
    fast_buffer_t* buf = ctx->response_buf;
    char cors[512];
    const char *cors_headers = cors_headers_for_request(ctx, cors, sizeof(cors));
    size_t body_len = status_code == 304 ? 0 : json_len;
    buffer_reset(buf);

    if (buffer_appendf(buf,
                       "HTTP/1.1 %s\r\n"
                       "Date: %s\r\n"
                       "Server: Ayder/3.0\r\n"
                       "Content-Type: application/json; charset=utf-8\r\n"
                       "Content-Length: %zu\r\n"
                       "Connection: %s\r\n"
                       "Cache-Control: no-cache\r\n"
                       "%s",
                       http_status_text(status_code), cached_date, body_len,
                       ctx->keep_alive ? "keep-alive" : "close", cors_headers) != 0)
        goto overflow;

    if (ctx->rl_have && status_code == 200) {
        if (buffer_appendf(buf,
                           "X-RateLimit-Limit: %u\r\n"
                           "X-RateLimit-Remaining: %u\r\n"
                           "X-RateLimit-Reset: %" PRIu64 "\r\n",
                           ctx->rl_limit, ctx->rl_remaining,
                           ctx->rl_reset_epoch_s) != 0)
            goto overflow;
        ctx->rl_have = 0;
    }
    if (path_is_broker_query(ctx->url)) {
        char fp_hex[65];
        if (buffer_appendf(buf, "Accept-Query: application/json\r\n") != 0)
            goto overflow;
        if (extract_query_fingerprint_hex(json_data, json_len, fp_hex)) {
            char etag[96];
            query_etag_from_hex(fp_hex, etag);
            if (buffer_appendf(buf,
                               "ETag: %s\r\n"
                               "Content-Location: /broker/query-results/sha256:%s\r\n",
                               etag, fp_hex) != 0)
                goto overflow;
        }
        if (buffer_appendf(buf,
                           "Access-Control-Expose-Headers: Accept-Query, ETag, Content-Location\r\n") != 0)
            goto overflow;
    }
    if (buffer_appendf(buf, "\r\n") != 0 ||
        (body_len && buffer_append_bytes(buf, json_data, body_len) != 0))
        goto overflow;
    goto enqueue;

overflow:
    status_code = 500;
    ctx->keep_alive = 0;
    buffer_reset(buf);
    if (buffer_appendf(buf,
                       "HTTP/1.1 500 Internal Server Error\r\n"
                       "Content-Type: application/json\r\n"
                       "Content-Length: %zu\r\n"
                       "Connection: close\r\n\r\n",
                       sizeof(overflow_body) - 1) != 0 ||
        buffer_append_bytes(buf, overflow_body, sizeof(overflow_body) - 1) != 0) {
        if (!uv_is_closing((uv_handle_t *)ctx->client))
            uv_close((uv_handle_t *)ctx->client, connection_close_cb);
        return;
    }

enqueue:;
    write_req_t* wr = object_pool_get(write_pool);
    if (!wr) wr = slab_alloc(sizeof(write_req_t));
    if (!wr) {
        if (!uv_is_closing((uv_handle_t *)ctx->client))
            uv_close((uv_handle_t *)ctx->client, connection_close_cb);
        return;
    }
    wr->buffer = buf;
    wr->keep_alive = ctx->keep_alive;
    wr->request_id = ctx->request_id;
    buf->ref_count++;
    uv_buf_t uvb = uv_buf_init(buf->data, (unsigned int)buf->len);
    int write_rc = uv_write((uv_write_t*)wr, (uv_stream_t*)ctx->client, &uvb, 1,
                            (uv_write_cb)write_complete_cb);
    if (write_rc != 0) {
        buffer_release(buf);
        if (write_pool) object_pool_release(write_pool, wr); else slab_free(wr);
        if (!uv_is_closing((uv_handle_t *)ctx->client))
            uv_close((uv_handle_t *)ctx->client, connection_close_cb);
    }

    http_status_record(status_code);
    total_bytes_sent += buf->len;
}

static void write_complete_cb(uv_write_t* req, int status) {
    write_req_t* wr = (write_req_t*)req;

    if (status < 0) {
        fprintf(stderr, "[HTTP] Write error: %s\n", uv_strerror(status));
    }
    /* Always release buffer, even on canceled/broken pipe */
    if (wr->buffer) {
        buffer_release(wr->buffer);
        wr->buffer = NULL;
    }
    if (!wr->keep_alive && req->handle && !uv_is_closing((uv_handle_t *)req->handle)) {
        uv_close((uv_handle_t*)req->handle, connection_close_cb);
    }
    if (write_pool) object_pool_release(write_pool, wr);
    else slab_free(wr);
}

static size_t http_path_only_len(const char *url) {
    const char *query = url ? strchr(url, '?') : NULL;
    return url ? (query ? (size_t)(query - url) : strlen(url)) : 0;
}

static int http_path_equals(const char *url, const char *expected) {
    size_t path_len = http_path_only_len(url);
    size_t expected_len = expected ? strlen(expected) : 0;
    return url && expected && path_len == expected_len &&
           memcmp(url, expected, path_len) == 0;
}

static int http_path_starts_with(const char *url, const char *prefix) {
    size_t path_len = http_path_only_len(url);
    size_t prefix_len = prefix ? strlen(prefix) : 0;
    return url && prefix && path_len >= prefix_len &&
           memcmp(url, prefix, prefix_len) == 0;
}

static int http_path_ends_with(const char *url, const char *suffix) {
    size_t path_len = http_path_only_len(url);
    size_t suffix_len = suffix ? strlen(suffix) : 0;
    return url && suffix && path_len >= suffix_len &&
           memcmp(url + path_len - suffix_len, suffix, suffix_len) == 0;
}

static int is_write_path(const char *method, const char *url) {
    if (!method || !url) return 0;
    int is_post = (strcmp(method, "POST") == 0);
    int is_delete = (strcmp(method, "DELETE") == 0);
    if (!(is_post || is_delete)) return 0;
    /* Broker produces */
    if (is_post && http_path_equals(url, "/broker/commit")) return 1;
    if (is_post && http_path_starts_with(url, "/broker/topics/")) {
        if (http_path_ends_with(url, "/produce") ||
            http_path_ends_with(url, "/produce-ndjson")) return 1;
    }
    /* KV writes */
    if (http_path_starts_with(url, "/kv/")) {
        if (is_post) return 1;    /* put */
        if (is_delete) return 1;    /* delete */
    }
    /* Admin mutators */
    if (is_post && (
            http_path_equals(url, "/broker/topics") ||
            http_path_equals(url, "/broker/retention") ||
            http_path_equals(url, "/broker/delete-before") ||
            http_path_equals(url, "/admin/sealed/gc") ||
            http_path_equals(url, "/admin/compact") ||
            http_path_equals(url, "/ha/membership/add") ||
            http_path_equals(url, "/ha/membership/promote") ||
            http_path_equals(url, "/ha/membership/remove")
    ))
        return 1;
    return 0;
}

static int is_linearizable_read_path(const char *method, const char *url) {
    return method && strcmp(method, "GET") == 0 &&
           http_path_starts_with(url, "/kv/");
}



static int check_keep_alive(struct phr_header* headers, size_t num_headers, int minor_version) {
    for (size_t i = 0; i < num_headers; i++) {
        if (headers[i].name_len == 10 &&
            strncasecmp(headers[i].name, "Connection", 10) == 0) {
            if (headers[i].value_len == 5 &&
                strncasecmp(headers[i].value, "close", 5) == 0) {
                return 0; // Explicit close
            }
            if (headers[i].value_len == 10 &&
                strncasecmp(headers[i].value, "keep-alive", 10) == 0) {
                return 1; // Explicit keep-alive
            }
        }
    }

    // HTTP/1.1 defaults to keep-alive, HTTP/1.0 defaults to close
    return (minor_version >= 1);
}

static void load_ha_http_env_overrides(void){
    const char *e;
    if ((e=getenv("RF_HTTP_FORCE_FOLLOWER"))) HA_HTTP_IS_FOLLOWER = (e[0] != '0');
    if ((e=getenv("RF_HTTP_READ_ONLY")))     HA_READ_ONLY     = (e[0] != '0');
    if ((e=getenv("RF_HTTP_LEADER_URL")))    snprintf(HA_LEADER_URL, sizeof(HA_LEADER_URL), "%s", e);
    if ((e=getenv("RF_HTTP_REDIRECT_STATUS"))){ int s=atoi(e); HA_REDIRECT_STATUS = (s==308)?308:307; }
    if ((e=getenv("RF_HTTP_RETRY_AFTER")))   { int s=atoi(e); if (s>=0) HA_RETRY_AFTER=s; }
}

// ═══════════════════════════════════════════════════════════════════════════════
// Lightning-Fast Request Processing
// ═══════════════════════════════════════════════════════════════════════════════
static void process_request(connection_ctx_t* ctx) {
    uint64_t handler_start_us = monotonic_us();

    uint64_t done_us  = ctx->msg_complete_us ? ctx->msg_complete_us : handler_start_us;
    uint64_t first_us = ctx->first_byte_us   ? ctx->first_byte_us   : done_us;

    rf_http_queue_us      = handler_start_us - done_us;
    rf_http_recv_parse_us = done_us - first_us;

    // Copy method + url (you already have this)
    size_t method_copy_len = ctx->method_len < sizeof(ctx->method_str) - 1 ?
                             ctx->method_len : sizeof(ctx->method_str) - 1;
    memcpy(ctx->method_str, ctx->method, method_copy_len);
    ctx->method_str[method_copy_len] = '\0';

    size_t path_copy_len = ctx->path_len < sizeof(ctx->url) - 1 ?
                           ctx->path_len : sizeof(ctx->url) - 1;
    memcpy(ctx->url, ctx->path, path_copy_len);
    ctx->url[path_copy_len] = '\0';

    /* ── AUTH/RL/BACKPRESSURE: INSERTED HERE ───────────────────────────── */
    {
        // 0) global backpressure (keep this first)
        int rafter = overload_retry_after_sec();
        if (unlikely(rafter > 0)) { send_backpressure(ctx, rafter); goto cleanup; }

        if (!path_is_public(ctx->url)) {
            // 1) Bearer check
            size_t toklen = 0;
            const char *tok = get_bearer_from_headers(ctx, &toklen);
            if (!tok || toklen == 0) { send_unauthorized(ctx); goto cleanup; }

            api_key_cfg_t *cfg = find_key_cfg(tok, toklen);
            if (!cfg) { send_unauthorized(ctx); goto cleanup; }

            /* 1.5) >>> INSERT FORCE-503 HOOK RIGHT HERE <<< */
            if (TEST_HOOKS_ENABLED) {
                const struct phr_header *dbg = hdr_find(ctx, "X-Force-Overload");
                if (dbg && dbg->value_len >= 1 &&
                    (dbg->value[0]=='1' || dbg->value[0]=='y' || dbg->value[0]=='Y')) {
                    send_backpressure(ctx, BP_RETRY_AFTER_SEC);
                    goto cleanup;
                }
            }


            // 2) Rate-limit + daily quota


            if (RL_ENABLED) {
                uint32_t lim=0, rem=0; uint64_t reset=0; int wait_s=0;
                int rl = rl_take_one(cfg, tok, toklen, &lim, &rem, &reset, &wait_s);
                if (rl > 0) {
                    if (wait_s < 1) wait_s = 1;
                    send_rate_limited(ctx, wait_s, lim, rem, reset);
                    goto cleanup;
                } else {
                    ctx->rl_limit = lim;
                    ctx->rl_remaining = rem;
                    ctx->rl_reset_epoch_s = reset;
                    ctx->rl_have = 1;
                }
            } else {
                ctx->rl_have = 0;
            }
        }
    }
    /* ──────────────────────────────────────────────────────────────────── */

    /* Pull latest HA role/leader before gating writes */
    ha_refresh_http_flags();

    /* ────────────────────────────────────────────────────────────────────
       HA WRITE-GATE:
       - If persistence is not ready → 503 sealed_only
       - If follower/read-only → 307/308 redirect to leader (if configured) or 503
       ──────────────────────────────────────────────────────────────────── */
    int write_path = is_write_path(ctx->method_str, ctx->url);
    int linearizable_read_path =
        is_linearizable_read_path(ctx->method_str, ctx->url);
    if (write_path || linearizable_read_path) {
        /* FOLLOWER/READ-ONLY: redirect FIRST, regardless of local persistence */
        if (HA_READ_ONLY || HA_HTTP_IS_FOLLOWER) {
            if (HA_LEADER_URL[0]) {
                char loc[1600];
                int loc_len = snprintf(loc, sizeof(loc), "%s%s", HA_LEADER_URL, ctx->url);
                if (loc_len < 0 || (size_t)loc_len >= sizeof(loc)) {
                    loc[sizeof(loc) - 1] = '\0';
                    loc_len = (int)sizeof(loc) - 1;
                }
                int slashA = (HA_LEADER_URL[strlen(HA_LEADER_URL) - 1] == '/');
                int slashB = (ctx->url[0] == '/');
                if (slashA && slashB) {
                    snprintf(loc, sizeof loc, "%.*s%s", (int)(strlen(HA_LEADER_URL)-1), HA_LEADER_URL, ctx->url);
                } else if (!slashA && !slashB) {
                    snprintf(loc, sizeof loc, "%s/%s", HA_LEADER_URL, ctx->url);
                } else {
                    snprintf(loc, sizeof loc, "%s%s", HA_LEADER_URL, ctx->url);
                }

                char body[512];
                int body_len = snprintf(body, sizeof(body),
                                        "{\"ok\":false,\"error\":\"redirect_to_leader\","
                                        "\"message\":\"%s must be sent to leader. Client should follow 307 redirect.\","
                                        "\"leader_url\":\"%.256s\",\"retry_after\":%d,"
                                        "\"redirect\":{\"status\":%d,\"location\":\"%.512s\"}}",
                                        write_path ? "Writes" : "Linearizable reads",
                                        HA_LEADER_URL, HA_RETRY_AFTER, HA_REDIRECT_STATUS, loc);
                if (body_len < 0) body_len = 0;
                if ((size_t)body_len >= sizeof(body)) {
                    body[sizeof(body) - 1] = '\0';
                    body_len = (int)sizeof(body) - 1;
                }

                send_redirect_response(ctx, HA_REDIRECT_STATUS, loc, HA_RETRY_AFTER,
                                       body, (size_t)body_len);
            } else {
                const char body[] = "{\"ok\":false,\"error\":\"read_only_follower\",\"leader_url\":null}";
                send_response(ctx, body, sizeof(body) - 1, 503);
            }
            goto cleanup;
        }

        /* We’re leader here; only mutations require writable persistence. */
        if (write_path && !Persistence_is_ready()) {
            const char body[] = "{\"ok\":false,\"error\":\"sealed_only\",\"message\":\"Writes disabled until persistence is ready\"}";
            send_response(ctx, body, sizeof(body) - 1, 503);
            goto cleanup;
        }
    }
    /* ──────────────────────────────────────────────────────────────────── */

    if (strcmp(ctx->method_str, "QUERY") == 0 && path_is_broker_query(ctx->url)) {
        const struct phr_header *ct = hdr_find(ctx, "Content-Type");
        if (!ct) {
            const char body[] = "{\"ok\":false,\"error\":\"missing_content_type\",\"accept_query\":\"application/json\"}";
            send_response(ctx, body, sizeof(body) - 1, 400);
            goto cleanup;
        }
        if (!header_value_json_content_type(ct)) {
            const char body[] = "{\"ok\":false,\"error\":\"unsupported_query_media_type\",\"accept_query\":\"application/json\"}";
            send_response(ctx, body, sizeof(body) - 1, 415);
            goto cleanup;
        }
    }

    // Prepare response buffer (your existing code continues)
    char response_json[MAX_RESPONSE_SIZE];
    response_json[0] = '\0';

    int result = route_request_len(ctx->method_str, ctx->url,
                                   ctx->body, ctx->body_len, response_json);
    size_t response_len = strlen(response_json);

    if (result == 0 && strcmp(ctx->method_str, "QUERY") == 0 && path_is_broker_query(ctx->url)) {
        char fp_hex[65];
        char etag[96];
        if (extract_query_fingerprint_hex(response_json, response_len, fp_hex)) {
            query_etag_from_hex(fp_hex, etag);
            if (if_none_match_has_etag(ctx, etag)) {
                send_response(ctx, response_json, response_len, 304);
                goto cleanup;
            }
        }
    }

    if (result == 0 &&
        (strcmp(ctx->url, "/metrics") == 0 || strcmp(ctx->url, "/metrics_ha") == 0)) {
        send_raw(ctx, "text/plain; version=0.0.4", response_json, response_len, 200);
        goto cleanup;
    }

    int status_code =
            (result == 0)  ? 200 :
            (result == -1) ? 404 :
            (result == -2) ? 405 :
            (result == -3) ? 503 :
            (result == -4) ? 400 :
            500;

    send_response(ctx, response_json, response_len, status_code);

    cleanup:
    // (your existing cleanup / latency histogram block stays the same)
    uint64_t elapsed = get_time_ns() - ctx->start_time_ns;
    uint64_t completed_us = monotonic_us();
    uint64_t request_elapsed_us = ctx->first_byte_us && completed_us >= ctx->first_byte_us
                                ? completed_us - ctx->first_byte_us
                                : elapsed / 1000;
    http_duration_record(request_elapsed_us);
    if (ctx->keep_alive) {
        ctx->request_len = 0;
        ctx->first_byte_us = 0;
        ctx->msg_complete_us = 0;
        ctx->parse_state = 0;
        ctx->msg_complete = 0;
        ctx->body = NULL;
        ctx->body_len = 0;
        ctx->body_received = 0;
        ctx->content_length = 0;
        ctx->request_id++;
        ctx->start_time_ns = get_time_ns();
        ctx->last_activity_us = monotonic_us();
        if (ctx->chunk_buf) { slab_free(ctx->chunk_buf); ctx->chunk_buf = NULL; }
        ctx->chunk_len = 0;
        ctx->chunk_cap = 0;
        ctx->header_len = 0;
        ctx->cursor = 0;
        memset(&ctx->chunk_decoder, 0, sizeof(ctx->chunk_decoder));
        ctx->is_chunked = 0;

    }
}

// ═══════════════════════════════════════════════════════════════════════════════
// picohttpparser Integration - ULTRA FAST PARSING
// ═══════════════════════════════════════════════════════════════════════════════
static void relocate_request_views(connection_ctx_t *ctx,
                                   const char *old_buf,
                                   char *new_buf)
{
    size_t i;

    if (ctx->method) ctx->method = new_buf + (ctx->method - old_buf);
    if (ctx->path) ctx->path = new_buf + (ctx->path - old_buf);
    if (ctx->body) ctx->body = new_buf + (ctx->body - old_buf);
    for (i = 0; i < ctx->num_headers; ++i) {
        if (ctx->headers[i].name) {
            ctx->headers[i].name = new_buf + (ctx->headers[i].name - old_buf);
        }
        if (ctx->headers[i].value) {
            ctx->headers[i].value = new_buf + (ctx->headers[i].value - old_buf);
        }
    }
}

static int mark_request_complete(connection_ctx_t *ctx)
{
    ctx->parse_state = 2;
    ctx->msg_complete = 1;
    if (ctx->msg_complete_us == 0) ctx->msg_complete_us = monotonic_us();
    return 1;
}

static int parse_chunked_body(connection_ctx_t *ctx)
{
    size_t decoded_len;
    size_t leftover_len = 0;
    http_chunk_result_t result;

    if (ctx->cursor > ctx->request_len) return -1;
    decoded_len = ctx->request_len - ctx->cursor;
    if (decoded_len == 0) return 0;

    result = http_chunk_decoder_feed(&ctx->chunk_decoder,
                                     ctx->request_buf + ctx->cursor,
                                     &decoded_len, &leftover_len);
    if (result == HTTP_CHUNK_ERROR) return -1;

    if (decoded_len != 0) {
        if (decoded_len > MAX_REQUEST_SIZE - ctx->chunk_len ||
            !ensure_chunk_cap(ctx, ctx->chunk_len + decoded_len)) {
            return -1;
        }
        memcpy(ctx->chunk_buf + ctx->chunk_len,
               ctx->request_buf + ctx->cursor, decoded_len);
        ctx->chunk_len += decoded_len;
    }
    ctx->cursor = ctx->request_len;

    if (result == HTTP_CHUNK_INCOMPLETE) return 0;

    /* HTTP pipelining is not supported; never ignore bytes past a message. */
    if (leftover_len != 0) return -1;

    ctx->body = ctx->chunk_buf;
    ctx->body_len = ctx->chunk_len;
    return mark_request_complete(ctx);
}

static int parse_http_request(connection_ctx_t* ctx, const char* data, size_t len) {
    if (len > MAX_REQUEST_SIZE - ctx->request_len) {
        return -1; /* bad request */
    }
    // Ensure we have space in request buffer
    if (ctx->request_len + len > ctx->request_capacity) {
        size_t new_capacity = ctx->request_capacity;
        while (new_capacity < ctx->request_len + len) {
            new_capacity *= 2;
            if (new_capacity > MAX_REQUEST_SIZE) {
                return -1; /* refuse to grow beyond hard cap */
            }
        }

        char* new_buf = slab_alloc(new_capacity);
        if (!new_buf) return -1;
        if (ctx->request_buf) {
            memcpy(new_buf, ctx->request_buf, ctx->request_len);
            if (ctx->parse_state != 0) {
                relocate_request_views(ctx, ctx->request_buf, new_buf);
            }
            slab_free(ctx->request_buf);
        }
        ctx->request_buf = new_buf;
        ctx->request_capacity = new_capacity;
    }

    // Append new data
    memcpy(ctx->request_buf + ctx->request_len, data, len);
    ctx->request_len += len;
    total_bytes_received += len;

    if (ctx->parse_state == 0) {
        // Parse headers
        ctx->num_headers = MAX_HEADERS;

        int pret = phr_parse_request(ctx->request_buf, ctx->request_len,
                                     &ctx->method, &ctx->method_len,
                                     &ctx->path, &ctx->path_len,
                                     &ctx->minor_version,
                                     ctx->headers, &ctx->num_headers,
                                     0);  // last_len = 0 for complete parsing

        if (pret == -1) {
            // Parse error
            return -1;
        } else if (pret == -2) {
            // Need more data for headers
            return 0;
        }

        // Headers parsed successfully.
        size_t header_len = (size_t)pret;
        http_framing_t framing;

        if (header_len > MAX_REQUEST_SIZE ||
            http_resolve_framing(ctx->headers, ctx->num_headers,
                                 MAX_REQUEST_SIZE - header_len,
                                 &framing) != HTTP_FRAMING_OK) {
            return -1;
        }

        ctx->header_len = header_len;
        ctx->keep_alive = check_keep_alive(ctx->headers, ctx->num_headers,
                                           ctx->minor_version);

        if (framing.mode == HTTP_BODY_CHUNKED) {
            ctx->is_chunked = 1;
            ctx->cursor = header_len;
            ctx->chunk_buf = NULL;
            ctx->chunk_len = 0;
            ctx->chunk_cap = 0;
            http_chunk_decoder_init(&ctx->chunk_decoder,
                                    MAX_REQUEST_SIZE - header_len);
            ctx->parse_state = 1;
            /* Process body bytes that arrived in the same read as the headers. */
            return parse_chunked_body(ctx);
        }

        if (framing.mode == HTTP_BODY_NONE) {
            /* Extra bytes would otherwise be silently discarded as pipelining. */
            if (ctx->request_len != header_len) return -1;
            ctx->body = NULL;
            ctx->body_len = 0;
            return mark_request_complete(ctx);
        }

        ctx->content_length = framing.content_length;
        ctx->body_received = ctx->request_len - header_len;
        ctx->body = framing.content_length == 0 ? NULL :
                    ctx->request_buf + header_len;
        if (ctx->body_received > ctx->content_length) return -1;
        if (ctx->body_received == ctx->content_length) {
            ctx->body_len = ctx->content_length;
            return mark_request_complete(ctx);
        }
        ctx->parse_state = 1;
        return 0;
    } else if (ctx->parse_state == 1) {
        size_t available;

        if (ctx->is_chunked) return parse_chunked_body(ctx);
        if (ctx->header_len > ctx->request_len) return -1;

        available = ctx->request_len - ctx->header_len;
        if (available > ctx->content_length) return -1;
        ctx->body_received = available;
        ctx->body = ctx->content_length == 0 ? NULL :
                    ctx->request_buf + ctx->header_len;
        if (available == ctx->content_length) {
            ctx->body_len = ctx->content_length;
            return mark_request_complete(ctx);
        }
        return 0;
    }

    /* No data is accepted after the message boundary and before cleanup. */
    return -1;
}

// ═══════════════════════════════════════════════════════════════════════════════
// Optimized Read Callback with Minimal Allocations
// ═══════════════════════════════════════════════════════════════════════════════
static void read_cb(uv_stream_t* stream, ssize_t nread, const uv_buf_t* buf) {
    connection_ctx_t* ctx = (connection_ctx_t*)stream->data;

    if (nread > 0) {
        ctx->last_activity_us = monotonic_us();
        if (ctx->request_len == 0 && ctx->parse_state == 0 && ctx->first_byte_us == 0) {
            ctx->first_byte_us = monotonic_us();
            atomic_fetch_add(&g_metrics.http_requests_total, 1);
        }
        // Parse HTTP request using picohttpparser
        int parse_result = parse_http_request(ctx, buf->base, nread);

        if (parse_result < 0) {
            // Parse error
            fprintf(stderr, "[HTTP-pico] Parse error\n");
            /* best-effort 400 response; ignore errors */
            uv_buf_t e = uv_buf_init((char *) ERROR_400, (unsigned int) (sizeof(ERROR_400) - 1));
            (void) uv_try_write(stream, &e, 1);
            http_status_record(400);
            uv_close((uv_handle_t *) stream, connection_close_cb);
            object_pool_release(buffer_pool, buf->base);
            return;
        }

        // Process complete messages
        if (ctx->msg_complete) {
            process_request(ctx);
        }

    } else if (nread < 0) {
        if (nread != UV_EOF && nread != UV_ECONNRESET) {
            fprintf(stderr, "[HTTP-pico] Read error: %s\n", uv_strerror(nread));
        }
        uv_close((uv_handle_t*)stream, connection_close_cb);
    }

    // Return buffer to pool
    object_pool_release(buffer_pool, buf->base);
}

// ═══════════════════════════════════════════════════════════════════════════════
// Connection Management
// ═══════════════════════════════════════════════════════════════════════════════
static void connection_close_cb(uv_handle_t* handle) {
    connection_ctx_t* ctx = (connection_ctx_t*)handle->data;

    if (ctx) {
        int connection_counted = ctx->connection_counted;
        connection_untrack(ctx);
        if (ctx->request_buf) {
            slab_free(ctx->request_buf);
            ctx->request_buf = NULL;
        }
        if (ctx->response_buf) {
            buffer_release(ctx->response_buf);
            ctx->response_buf = NULL;
        }

        if (ctx->chunk_buf) { slab_free(ctx->chunk_buf); ctx->chunk_buf = NULL; }

        // Clear the context before returning to pool
        memset(ctx, 0, sizeof(connection_ctx_t));

        // Return to pool or free
        if (connection_pool) {
            object_pool_release(connection_pool, ctx);
        } else {
            slab_free(ctx);
        }

        // Decrement the process-group connection gauge exactly once.
        if (connection_counted) {
            atomic_fetch_sub(&g_metrics.http_active_connections, 1);
        }
    }

    // Always free the handle itself
    slab_free(handle);
}

static void accept_connection(uv_stream_t* server, int status) {
    if (status < 0) {
        fprintf(stderr, "[HTTP-pico] Connection error: %s\n", uv_strerror(status));
        return;
    }

    // Get connection context from pool
    connection_ctx_t* ctx = NULL;
    if (connection_pool) {
        ctx = (connection_ctx_t*)object_pool_get(connection_pool);
    }

    if (!ctx) {
        ctx = slab_alloc(sizeof(connection_ctx_t));
        if (!ctx) {
            fprintf(stderr, "[HTTP-pico] Failed to allocate connection context\n");
            return;
        }
    }

    // CRITICAL: Always zero-initialize the context
    memset(ctx, 0, sizeof(connection_ctx_t));

    // Initialize request buffer for picohttpparser
    ctx->request_capacity = PICO_PARTIAL_SIZE;
    ctx->request_buf = slab_alloc(ctx->request_capacity);
    if (!ctx->request_buf) {
        fprintf(stderr, "[HTTP-pico] Failed to allocate request buffer\n");
        slab_free(ctx);
        return;
    }

    ctx->response_buf = buffer_create(MAX_RESPONSE_SIZE);
    if (!ctx->response_buf) {
        fprintf(stderr, "[HTTP-pico] Failed to allocate response buffer\n");
        slab_free(ctx->request_buf);
        slab_free(ctx);
        return;
    }

    // Create client socket
    uv_tcp_t* client = slab_alloc(sizeof(uv_tcp_t));
    if (!client) {
        fprintf(stderr, "[HTTP-pico] Failed to allocate client socket\n");
        slab_free(ctx->request_buf);
        buffer_release(ctx->response_buf);
        slab_free(ctx);
        return;
    }

    uv_tcp_init(main_loop, client);

    // Enable TCP optimizations for sub-1ms latency
    uv_tcp_nodelay(client, TCP_NODELAY);
    uv_tcp_keepalive(client, TCP_KEEPALIVE, 60);

    ctx->client = client;
    ctx->start_time_ns = get_time_ns();
    ctx->keep_alive = 1; // Default to keep-alive
    ctx->request_id = 1;
    ctx->parse_state = 0; // Start parsing headers
    ctx->last_activity_us = monotonic_us();
    client->data = ctx;

    if (uv_accept(server, (uv_stream_t*)client) == 0) {
        uint64_t previous = atomic_fetch_add(&g_metrics.http_active_connections, 1);
        if (previous >= BP_MAX_ACTIVE_CONNS) {
            atomic_fetch_sub(&g_metrics.http_active_connections, 1);
            atomic_fetch_add(&g_metrics.backpressure_events_total, 1);
            uv_close((uv_handle_t*)client, connection_close_cb);
            return;
        }
        ctx->connection_counted = 1;
        connection_track(ctx);
        if (uv_read_start((uv_stream_t*)client, alloc_cb, read_cb) != 0) {
            uv_close((uv_handle_t*)client, connection_close_cb);
        }
    } else {
        uv_close((uv_handle_t*)client, connection_close_cb);
    }
}
// ═══════════════════════════════════════════════════════════════════════════════
// Performance Statistics Timer
// ═══════════════════════════════════════════════════════════════════════════════
static void init_latency_timer(void)
{
    uv_timer_init(main_loop, &lat_timer);
}

// ═══════════════════════════════════════════════════════════════════════════════
// Public API - Initialize the Beast
// ═══════════════════════════════════════════════════════════════════════════════
void http_server_init(App* app, int port) {
    (void)app; // Framework integration handled by router


    // Create object pools
    connection_pool = object_pool_create(CONNECTION_POOL_SIZE, NULL, NULL);
    buffer_pool = object_pool_create(BUFFER_POOL_SIZE, NULL, NULL);
    write_pool = object_pool_create(4096, NULL, NULL);

    if (!connection_pool || !buffer_pool) {
        fprintf(stderr, "Failed to create object pools\n");
        exit(1);
    }

    // Initialize main event loop
    main_loop = uv_default_loop();

    init_latency_timer();

    // Set up date cache timer (updates every second)
    uv_timer_init(main_loop, &date_timer);
    update_date_cache(&date_timer); // Initial update
    uv_timer_start(&date_timer, update_date_cache, 1000, 1000);

    // Create TCP server with maximum performance settings
    server_handle = slab_alloc(sizeof(uv_tcp_t));
    uv_tcp_init(main_loop, server_handle);

    /* Pre-bind a socket with SO_REUSEPORT, then adopt it in libuv.   */
    int fd = make_reuseport_listener_ipv4("0.0.0.0", port);
    if (fd < 0) {
        fprintf(stderr, "❌ Failed to create SO_REUSEPORT listener\n");
        exit(1);
    }

    int open_rc = uv_tcp_open(server_handle, fd);
    if (open_rc != 0) {
        fprintf(stderr, "❌ uv_tcp_open failed: %s\n", uv_strerror(open_rc));
        close(fd);
        exit(1);
    }


    // Start listening with large backlog for high-traffic scenarios
    int listen_result = uv_listen((uv_stream_t*)server_handle, 8192, accept_connection);
    if (listen_result != 0) {
        fprintf(stderr, "❌ Listen failed: %s\n", uv_strerror(listen_result));
        exit(1);
    }


    load_auth_env_overrides();
    load_ha_http_env_overrides();
    parse_bearer_tokens_from_env();

    parse_auth_skip_prefixes_from_env();

    ha_auto_configure_http();

    // Run the event loop
    uv_run(main_loop, UV_RUN_DEFAULT);

    begin_http_loop_shutdown(main_loop);
    for (int i = 0; i < 1000 && connection_head; i++)
        (void)uv_run(main_loop, UV_RUN_NOWAIT);

    // Cleanup
    if (!connection_head) {
        object_pool_destroy(connection_pool);
        object_pool_destroy(buffer_pool);
        object_pool_destroy(write_pool);
        connection_pool = NULL;
        buffer_pool = NULL;
        write_pool = NULL;
    } else {
        fprintf(stderr, "[HTTP] Deferring pool cleanup because connections are still closing\n");
    }
}


// ═══════════════════════════════════════════════════════════════════════════════
// Graceful shutdown
// ═══════════════════════════════════════════════════════════════════════════════
void http_server_shutdown(void) {
    printf("🛑 Shutting down Ayder HTTP Server...\n");
    if (main_loop) begin_http_loop_shutdown(main_loop);
}









