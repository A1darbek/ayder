// src/http_server.c - ULTRA-FAST HTTP Server
// Zero-copy, SIMD-optimized, async I/O beast mode activated!
// FIXED: Proper SO_REUSEPORT for multi-worker clustering
#include "http_server.h"
#include "aof_batch.h"
#include "app.h"
#include "common.h"
#include "http_timing.h"
#include "log.h"
#include "object_pool.h"
#include "persistence_zp.h"
#include "picohttpparser.h"
#include "ramforge_ha_integration.h"
#include "ramforge_rotation_metrics.h"
#include "router.h"
#include "shared_storage.h"
#include "slab_alloc.h"
#include <ctype.h>
#include <fcntl.h>
#include <inttypes.h>
#include <netinet/in.h>
#include <stdatomic.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/socket.h>
#include <time.h>
#include <unistd.h>
#include <uv.h>

extern SharedStorage *g_shared_storage;

RF_TLS uint64_t rf_http_recv_parse_us = 0;
RF_TLS uint64_t rf_http_queue_us      = 0;

// ═══════════════════════════════════════════════════════════════════════════════
// BEAST MODE CONFIGURATION - Tuned for Maximum Performance
// ═══════════════════════════════════════════════════════════════════════════════
#define MAX_REQUEST_SIZE (64 * 1024)   // 64KB max request
#define MAX_RESPONSE_SIZE (256 * 1024) // 256KB max response
#define CONNECTION_POOL_SIZE 2048      // Pre-allocated connections
#define BUFFER_POOL_SIZE 4096          // Buffer pool size
#define TCP_NODELAY 1                  // Disable Nagle's algorithm
#define TCP_KEEPALIVE 1                // Enable TCP keepalive
#define MAX_HEADERS 32                 // Maximum number of headers to parse
#define PICO_PARTIAL_SIZE 4096         // Buffer for partial requests

static const char ERROR_400[] = "HTTP/1.1 400 Bad Request\r\n"
                                "Content-Type: application/json\r\n"
                                "Content-Length: 27\r\n"
                                "Connection: close\r\n\r\n"
                                "{\"error\":\"Bad Request\"}";

// ═══════════════════════════════════════════════════════════════════════════════
// Lightning-Fast Connection Context with Zero-Copy Buffers
// ═══════════════════════════════════════════════════════════════════════════════
typedef struct fast_buffer {
  char *data;
  size_t len;
  size_t capacity;
  int ref_count;
} fast_buffer_t;

typedef struct {
  uv_write_t req;
  fast_buffer_t *buffer;
  int keep_alive;
  uint64_t request_id;
} write_req_t;

typedef struct {
  // picohttpparser structures - ZERO allocation!
  struct phr_header headers[MAX_HEADERS];
  size_t num_headers;
  const char *method;
  size_t method_len;
  const char *path;
  size_t path_len;
  int minor_version;

  // Request buffer for picohttpparser (zero-copy parsing!)
  char *request_buf;
  size_t request_len;
  size_t request_capacity;

  // Parsed data (string views into request_buf - NO COPYING!)
  char method_str[16]; // Only for router compatibility
  char url[512];       // Only for router compatibility
  const char *body;    // Points directly into request_buf!
  size_t body_len;

  // Connection state
  uv_tcp_t *client;
  int msg_complete;
  int keep_alive;
  uint64_t request_id;
  uint64_t start_time_ns;

  // Partial request handling
  int parse_state; // 0=header, 1=body, 2=complete
  size_t content_length;
  size_t body_received;

  // Pre-allocated response buffer
  fast_buffer_t *response_buf;

  uint32_t rl_limit;
  uint32_t rl_remaining;
  uint64_t rl_reset_epoch_s;
  int rl_have;
  int is_chunked;
  size_t header_len;
  size_t cursor;     // absolute index into request_buf while decoding
  int chunk_state;   // 0=size line, 1=data, 2=data CRLF, 3=final CRLF
  size_t chunk_need; // bytes left in current chunk

  char *chunk_buf; // assembled body
  size_t chunk_len;
  size_t chunk_cap;

  uint64_t first_byte_us;   // when first byte of THIS request arrived
  uint64_t msg_complete_us; // when THIS request became complete

} connection_ctx_t;

// ═══════════════════════════════════════════════════════════════════════════════
// Global Performance Monitoring & Pools
// ═══════════════════════════════════════════════════════════════════════════════
static object_pool_t *connection_pool = NULL;
static object_pool_t *write_pool      = NULL;
static object_pool_t *buffer_pool     = NULL;
static uv_loop_t *main_loop           = NULL;

// Performance counters (lock-free atomic)
static volatile uint64_t total_requests       = 0;
static volatile uint64_t active_connections   = 0;
static volatile uint64_t total_bytes_sent     = 0;
static volatile uint64_t total_bytes_received = 0;
static void write_complete_cb(uv_write_t *req, int status);
static void connection_close_cb(uv_handle_t *handle);

// --- HTTP status counters (prometheus-exported)
static atomic_uint_fast64_t g_http_2xx, g_http_4xx, g_http_401, g_http_429, g_http_5xx, g_http_503, g_http_3xx;

// expose to exporter
uint64_t RAMForge_http_2xx(void) { return atomic_load(&g_http_2xx); }
uint64_t RAMForge_http_4xx(void) { return atomic_load(&g_http_4xx); }
uint64_t RAMForge_http_3xx(void) { return atomic_load(&g_http_3xx); }
uint64_t RAMForge_http_401(void) { return atomic_load(&g_http_401); }
uint64_t RAMForge_http_429(void) { return atomic_load(&g_http_429); }
uint64_t RAMForge_http_5xx(void) { return atomic_load(&g_http_5xx); }
uint64_t RAMForge_http_503(void) { return atomic_load(&g_http_503); }
uint64_t RAMForge_http_total(void) { return total_requests; }

// --- Tunables (env overrides) ---
static int RL_DEFAULT_RPS        = 50;     // tokens per second
static int RL_DEFAULT_BURST      = 100;    // bucket capacity
static uint64_t RL_DEFAULT_DAILY = 100000; // daily requests

static size_t BP_MAX_SEALED_Q     = 4096;  // backpressure if sealed queue > this
static size_t BP_MAX_ACTIVE_CONNS = 32768; // backpressure if active conns > this
static int BP_RETRY_AFTER_SEC     = 1;     // Retry-After for 503

int HA_ROLE_FOLLOWER          = 0; // 1 => follower/replica/read-only
int HA_READ_ONLY              = 0; // force read-only regardless of role
/* Redirect target/behavior */
char HA_LEADER_URL[1024]      = {0};
static int HA_REDIRECT_STATUS = 307; // 307 (default) or 308
static int HA_RETRY_AFTER     = 1;   // seconds for Retry-After on redirect

/* Auto-configuration state */
static int HA_AUTO_CONFIGURED = 0; // 1 if we successfully auto-configured from HA

/* Pull live HA state into HTTP gate on each request */
static inline void ha_refresh_http_flags(void) {
  if (!RAMForge_HA_is_enabled())
    return;
  /* live role */
  HA_ROLE_FOLLOWER = RAMForge_HA_is_follower() ? 1 : 0;

  /* live leader url - only if we have HA enabled and configured */
  const char *lu = RAMForge_HA_get_leader_url();
  if (lu && *lu) {
    size_t n = strlen(lu);
    if (n >= sizeof(HA_LEADER_URL))
      n = sizeof(HA_LEADER_URL) - 1;
    memcpy(HA_LEADER_URL, lu, n);
    HA_LEADER_URL[n] = 0;
  }
  /* live redirect tunables */
  int st = RAMForge_HA_get_redirect_status();
  if (st == 307 || st == 308)
    HA_REDIRECT_STATUS = st;
  int ra = RAMForge_HA_get_retry_after();
  if (ra >= 0)
    HA_RETRY_AFTER = ra;
  ha_shared_snapshot_t s;
  if (RAMForge_HA_read_snapshot(&s) && s.enabled) {
    HA_ROLE_FOLLOWER = (s.role == HA_ROLE_FOLLOWER) ? 1 : 0;

    if (s.leader_url[0]) {
      size_t n = strlen(s.leader_url);
      if (n >= sizeof(HA_LEADER_URL))
        n = sizeof(HA_LEADER_URL) - 1;
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
  uint64_t last_us;    // last refill timestamp (monotonic us)
  uint32_t tokens;     // current bucket tokens
  uint64_t day_epoch;  // days since epoch (UTC)
  uint64_t daily_used; // requests used in that day
} rl_state_t;

static void ha_auto_configure_http(void) {
  if (!RAMForge_HA_is_enabled())
    return;

  /* Check if user explicitly set follower mode */
  const char *force_follower_env = getenv("RF_HTTP_FORCE_FOLLOWER");
  if (force_follower_env && force_follower_env[0] != '0') {
    /* User explicitly wants follower mode - respect it */
    HA_ROLE_FOLLOWER = 1;
    LOGI("[HA-HTTP] Explicit follower mode via RF_HTTP_FORCE_FOLLOWER");
  }

  /* Check if user explicitly set leader URL */
  const char *leader_url_env = getenv("RF_HTTP_LEADER_URL");
  if (leader_url_env && *leader_url_env) {
    /* User explicitly set leader URL - respect it */
    snprintf(HA_LEADER_URL, sizeof(HA_LEADER_URL), "%s", leader_url_env);
    LOGI("[HA-HTTP] Explicit leader URL: %s", HA_LEADER_URL);
  }
  const char *local_id = RAMForge_HA_get_local_node_id();
  if (local_id) {
    LOGI("[HA-HTTP] Auto-configuration enabled for node '%s'", local_id);
    LOGI("[HA-HTTP] Redirect behavior will follow live HA role changes");
    HA_AUTO_CONFIGURED = 1;
  }
}

// small fnv1a for integer keys into shared_storage
static inline uint32_t fnv1a32(const void *p, size_t n) {
  const uint8_t *s = (const uint8_t *)p;
  uint32_t h       = 2166136261u;
  for (size_t i = 0; i < n; i++) {
    h ^= s[i];
    h *= 16777619u;
  }
  return h ? h : 1; // avoid 0
}
static inline int rl_key_for_token(const char *tok, size_t len) {
  // namespace 0xA1 << 24 to avoid collision with other maps
  return ((int)fnv1a32(tok, len)) | 0xA1000000;
}

static inline int hexval(int c) {
  if (c >= '0' && c <= '9')
    return c - '0';
  if (c >= 'a' && c <= 'f')
    return 10 + (c - 'a');
  if (c >= 'A' && c <= 'F')
    return 10 + (c - 'A');
  return -1;
}
static int ensure_chunk_cap(connection_ctx_t *ctx, size_t need) {
  if (ctx->chunk_cap >= need)
    return 1;
  size_t nc = ctx->chunk_cap ? ctx->chunk_cap * 2 : 4096;
  while (nc < need)
    nc *= 2;
  if (nc > MAX_REQUEST_SIZE)
    return 0;
  char *nb = ctx->chunk_buf ? slab_alloc(nc) : slab_alloc(nc);
  if (!nb)
    return 0;
  if (ctx->chunk_buf && ctx->chunk_len)
    memcpy(nb, ctx->chunk_buf, ctx->chunk_len);
  if (ctx->chunk_buf)
    slab_free(ctx->chunk_buf);
  ctx->chunk_buf = nb;
  ctx->chunk_cap = nc;
  return 1;
}

static inline uint64_t monotonic_us(void) {
  struct timespec ts;
  clock_gettime(CLOCK_MONOTONIC, &ts);
  return (uint64_t)ts.tv_sec * 1000000ULL + ts.tv_nsec / 1000;
}
static inline uint64_t epoch_day_utc(void) {
  time_t t = time(NULL);
  return (uint64_t)(t / 86400);
}

static void parse_bearer_tokens_from_env(void) {
  const char *env = getenv("RF_BEARER_TOKENS");
  if (!env || !*env)
    return;
  const char *p = env;
  while (*p && g_key_count < (int)(sizeof(g_keys) / sizeof(g_keys[0]))) {
    // token [@rps:burst:daily] , ...
    while (*p == ' ' || *p == ',')
      p++;
    if (!*p)
      break;
    api_key_cfg_t cfg = {0};
    // token
    const char *start = p;
    while (*p && *p != '@' && *p != ',')
      p++;
    size_t toklen = (size_t)(p - start);
    if (toklen >= sizeof(cfg.token))
      toklen = sizeof(cfg.token) - 1;
    memcpy(cfg.token, start, toklen);
    cfg.token[toklen] = 0;

    cfg.rps   = RL_DEFAULT_RPS;
    cfg.burst = RL_DEFAULT_BURST;
    cfg.daily = RL_DEFAULT_DAILY;

    if (*p == '@') { // @rps:burst:daily
      p++;
      cfg.rps = (uint32_t)strtoul(p, (char **)&p, 10);
      if (*p == ':') {
        p++;
        cfg.burst = (uint32_t)strtoul(p, (char **)&p, 10);
      }
      if (*p == ':') {
        p++;
        cfg.daily = (uint64_t)strtoull(p, (char **)&p, 10);
      }
    }
    // skip to next ','
    while (*p && *p != ',')
      p++;
    if (*p == ',')
      p++;
    if (cfg.rps == 0)
      cfg.rps = RL_DEFAULT_RPS;
    if (cfg.burst == 0)
      cfg.burst = RL_DEFAULT_BURST;
    if (cfg.daily == 0)
      cfg.daily = RL_DEFAULT_DAILY;

    g_keys[g_key_count++] = cfg;
  }
  LOGI("[AUTH] Loaded %d bearer token(s) from RF_BEARER_TOKENS", g_key_count);
}

static void load_auth_env_overrides(void) {
  const char *e;
  if ((e = getenv("RL_DEFAULT_RPS")))
    RL_DEFAULT_RPS = atoi(e) > 0 ? atoi(e) : RL_DEFAULT_RPS;
  if ((e = getenv("RL_DEFAULT_BURST")))
    RL_DEFAULT_BURST = atoi(e) > 0 ? atoi(e) : RL_DEFAULT_BURST;
  if ((e = getenv("RL_DEFAULT_DAILY")))
    RL_DEFAULT_DAILY = strtoull(e, NULL, 10) ?: RL_DEFAULT_DAILY;
  if ((e = getenv("BP_MAX_SEALED_Q")))
    BP_MAX_SEALED_Q = (size_t)strtoull(e, NULL, 10) ?: BP_MAX_SEALED_Q;
  if ((e = getenv("BP_MAX_ACTIVE_CONNS")))
    BP_MAX_ACTIVE_CONNS = (size_t)strtoull(e, NULL, 10) ?: BP_MAX_ACTIVE_CONNS;
  if ((e = getenv("BP_RETRY_AFTER_SEC")))
    BP_RETRY_AFTER_SEC = atoi(e) > 0 ? atoi(e) : BP_RETRY_AFTER_SEC;
}

static const char *g_skip_prefixes[64];
static int g_skip_prefixes_n = 0;

static void parse_auth_skip_prefixes_from_env(void) {
  const char *env = getenv("RF_AUTH_SKIP_PREFIXES");
  if (!env || !*env)
    return;
  const char *p = env;
  while (*p && g_skip_prefixes_n < 64) {
    while (*p == ',' || *p == ' ' || *p == ';')
      p++;
    if (!*p)
      break;
    const char *s = p;
    while (*p && *p != ',' && *p != ';')
      p++;
    size_t n   = (size_t)(p - s);
    char *copy = slab_alloc(n + 1);
    memcpy(copy, s, n);
    copy[n]                              = 0;
    g_skip_prefixes[g_skip_prefixes_n++] = copy;
    if (*p)
      p++;
  }
}

static int has_skip_prefix(const char *path) {
  for (int i = 0; i < g_skip_prefixes_n; i++) {
    const char *pre = g_skip_prefixes[i];
    size_t n        = strlen(pre);
    if (n && strncmp(path, pre, n) == 0)
      return 1;
  }
  return 0;
}

// fast header fetch (case-insensitive name)
static const struct phr_header *hdr_find(const connection_ctx_t *ctx, const char *name) {
  size_t nlen = strlen(name);
  for (size_t i = 0; i < ctx->num_headers; i++) {
    if (ctx->headers[i].name_len == nlen && strncasecmp(ctx->headers[i].name, name, nlen) == 0) {
      return &ctx->headers[i];
    }
  }
  return NULL;
}

static const char *get_bearer_from_headers(const connection_ctx_t *ctx, size_t *out_len) {
  const struct phr_header *h = hdr_find(ctx, "Authorization");
  if (!h)
    return NULL;

  const char *v = h->value;
  size_t vlen   = h->value_len;

  // trim left spaces
  while (vlen && isspace((unsigned char)*v)) {
    v++;
    vlen--;
  }

  // must start with Bearer (case-insensitive)
  if (vlen < 6 || strncasecmp(v, "Bearer", 6) != 0)
    return NULL;
  v += 6;
  vlen -= 6;

  // optional space(s) after Bearer
  while (vlen && isspace((unsigned char)*v)) {
    v++;
    vlen--;
  }

  // trim right spaces
  while (vlen && isspace((unsigned char)v[vlen - 1]))
    vlen--;

  if (vlen == 0)
    return NULL;
  if (out_len)
    *out_len = vlen;
  return v;
}

static int path_is_public(const char *path) {
  if (has_skip_prefix(path))
    return 1; // runtime-configurable
  // allow these without auth/rate: tweak via RF_AUTH_SKIP_PATHS if you want
  if (strcmp(path, "/health") == 0)
    return 1;
  if (strcmp(path, "/metrics") == 0)
    return 1;
  if (strcmp(path, "/metrics_ha") == 0)
    return 1;
  if (strcmp(path, "/ready") == 0)
    return 1;
  return 0;
}

static api_key_cfg_t *find_key_cfg(const char *tok, size_t len) {
  for (int i = 0; i < g_key_count; i++) {
    if (strlen(g_keys[i].token) == len && memcmp(g_keys[i].token, tok, len) == 0)
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
  addr.sin_port   = htons((uint16_t)port);
  if (uv_inet_pton(AF_INET, ip, &addr.sin_addr) != 0) {
    LOGE("inet_pton failed for %s", ip);
    close(fd);
    return -1;
  }
  if (bind(fd, (struct sockaddr *)&addr, sizeof(addr)) < 0) {
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
static fast_buffer_t *buffer_create(size_t size) {
  fast_buffer_t *buf = slab_alloc(sizeof(fast_buffer_t));
  buf->data          = slab_alloc(size);
  buf->len           = 0;
  buf->capacity      = size;
  buf->ref_count     = 1;
  return buf;
}

static void buffer_release(fast_buffer_t *buf) {
  if (!buf)
    return;

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

static void buffer_reset(fast_buffer_t *buf) { buf->len = 0; }

// ═══════════════════════════════════════════════════════════════════════════════
// Ultra-Fast Date Cache (updates every second)
// ═══════════════════════════════════════════════════════════════════════════════
static char cached_date[64];
static time_t last_date_update = 0;
static uv_timer_t date_timer;

static void update_date_cache(uv_timer_t *timer) {
  (void)timer;
  time_t now = time(NULL);
  struct tm tm;
  gmtime_r(&now, &tm);
  strftime(cached_date, sizeof(cached_date), "%a, %d %b %Y %H:%M:%S GMT", &tm);
  last_date_update = now;
}

static void send_unauthorized(connection_ctx_t *ctx) {
  const char body[]  = "{\"error\":\"unauthorized\"}";
  fast_buffer_t *buf = ctx->response_buf;
  buffer_reset(buf);
  int n = snprintf(buf->data, buf->capacity,
                   "HTTP/1.1 401 Unauthorized\r\n"
                   "Date: %s\r\n"
                   "Server: Ayder/3.0\r\n"
                   "Content-Type: application/json\r\n"
                   "Content-Length: %zu\r\n"
                   "WWW-Authenticate: Bearer realm=\"Ayder\", error=\"invalid_token\"\r\n"
                   "Connection: %s\r\n"
                   "Access-Control-Allow-Origin: *\r\n"
                   "Access-Control-Allow-Headers: Content-Type, Authorization\r\n"
                   "\r\n",
                   cached_date, sizeof(body) - 1, ctx->keep_alive ? "keep-alive" : "close");
  memcpy(buf->data + n, body, sizeof(body) - 1);
  buf->len        = n + (int)sizeof(body) - 1;
  write_req_t *wr = object_pool_get(write_pool);
  if (!wr)
    wr = slab_alloc(sizeof(*wr));
  wr->buffer     = buf;
  wr->keep_alive = ctx->keep_alive;
  wr->request_id = ctx->request_id;
  buf->ref_count++;
  uv_buf_t uvb = uv_buf_init(buf->data, buf->len);
  uv_write((uv_write_t *)wr, (uv_stream_t *)ctx->client, &uvb, 1, (uv_write_cb)write_complete_cb);

  atomic_fetch_add(&g_http_401, 1);
  atomic_fetch_add(&g_http_4xx, 1);
}

static void send_rate_limited(connection_ctx_t *ctx, int retry_after_s, uint32_t limit, uint32_t remaining,
                              uint64_t reset_epoch_s) {
  char body[256];
  int bl             = snprintf(body, sizeof(body), "{\"error\":\"rate_limited\",\"retry_after\":%d}", retry_after_s);
  fast_buffer_t *buf = ctx->response_buf;
  buffer_reset(buf);
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
                   "Access-Control-Allow-Origin: *\r\n"
                   "Access-Control-Allow-Headers: Content-Type, Authorization\r\n"
                   "\r\n",
                   cached_date, bl, retry_after_s, limit, remaining, reset_epoch_s,
                   ctx->keep_alive ? "keep-alive" : "close");
  memcpy(buf->data + n, body, bl);
  buf->len        = n + bl;
  write_req_t *wr = object_pool_get(write_pool);
  if (!wr)
    wr = slab_alloc(sizeof(*wr));
  wr->buffer     = buf;
  wr->keep_alive = ctx->keep_alive;
  wr->request_id = ctx->request_id;
  buf->ref_count++;
  uv_buf_t uvb = uv_buf_init(buf->data, buf->len);
  uv_write((uv_write_t *)wr, (uv_stream_t *)ctx->client, &uvb, 1, (uv_write_cb)write_complete_cb);

  atomic_fetch_add(&g_http_429, 1);
  atomic_fetch_add(&g_http_4xx, 1);
}

static void send_backpressure(connection_ctx_t *ctx, int retry_after_s) {
  const char body[]  = "{\"error\":\"overloaded\"}";
  fast_buffer_t *buf = ctx->response_buf;
  buffer_reset(buf);
  int n = snprintf(buf->data, buf->capacity,
                   "HTTP/1.1 503 Service Unavailable\r\n"
                   "Date: %s\r\n"
                   "Server: Ayder/3.0\r\n"
                   "Content-Type: application/json\r\n"
                   "Content-Length: %zu\r\n"
                   "Retry-After: %d\r\n"
                   "Connection: %s\r\n"
                   "Access-Control-Allow-Origin: *\r\n"
                   "Access-Control-Allow-Headers: Content-Type, Authorization\r\n"
                   "\r\n",
                   cached_date, sizeof(body) - 1, retry_after_s, ctx->keep_alive ? "keep-alive" : "close");
  memcpy(buf->data + n, body, sizeof(body) - 1);
  buf->len        = n + (int)sizeof(body) - 1;
  write_req_t *wr = object_pool_get(write_pool);
  if (!wr)
    wr = slab_alloc(sizeof(*wr));
  wr->buffer     = buf;
  wr->keep_alive = ctx->keep_alive;
  wr->request_id = ctx->request_id;
  buf->ref_count++;
  uv_buf_t uvb = uv_buf_init(buf->data, buf->len);
  uv_write((uv_write_t *)wr, (uv_stream_t *)ctx->client, &uvb, 1, (uv_write_cb)write_complete_cb);

  atomic_fetch_add(&g_http_503, 1);
  atomic_fetch_add(&g_http_5xx, 1);
}

static int rl_take_one(const api_key_cfg_t *cfg, const char *tok, size_t toklen, uint32_t *out_limit,
                       uint32_t *out_remaining, uint64_t *out_reset_epoch_s, int *out_retry_after_s) {
  if (!g_shared_storage)
    return 0; // if no shmem, allow all (fail-open)
  const int skey = rl_key_for_token(tok, toklen);
  rl_state_t st  = {0};
  int have       = shared_storage_get_fast(g_shared_storage, skey, &st, sizeof(st)) ? 1 : 0;

  const uint64_t now_mono = monotonic_us();
  const uint64_t today    = epoch_day_utc();

  if (!have) {
    st.tokens     = cfg->burst;
    st.last_us    = now_mono;
    st.day_epoch  = today;
    st.daily_used = 0;
  }

  // daily window rollover
  if (st.day_epoch != today) {
    st.day_epoch  = today;
    st.daily_used = 0;
  }

  // refill token bucket
  if (cfg->rps > 0) {
    uint64_t elapsed = (now_mono > st.last_us) ? (now_mono - st.last_us) : 0;
    uint64_t gained  = (elapsed * (uint64_t)cfg->rps) / 1000000ULL;
    if (gained) {
      uint64_t nt = (uint64_t)st.tokens + gained;
      st.tokens   = (uint32_t)(nt > cfg->burst ? cfg->burst : nt);
      st.last_us  = now_mono; // coarse; keeps math simple
    }
  }

  // quota first
  if (st.daily_used >= cfg->daily) {
    // seconds until next UTC day
    time_t now_s      = time(NULL);
    time_t next_day_s = (now_s / 86400 + 1) * 86400;
    int retry_s       = (int)(next_day_s - now_s);
    if (retry_s < 1)
      retry_s = 1;
    if (out_limit)
      *out_limit = (uint32_t)cfg->rps;
    if (out_remaining)
      *out_remaining = 0;
    if (out_reset_epoch_s)
      *out_reset_epoch_s = (uint64_t)next_day_s;
    if (out_retry_after_s)
      *out_retry_after_s = retry_s;
    return 1;
  }

  // token bucket request cost = 1
  if (st.tokens == 0) {
    // time until 1 token arrives
    int retry_s = (cfg->rps > 0) ? 1 : 1;
    if (out_limit)
      *out_limit = (uint32_t)cfg->rps;
    if (out_remaining)
      *out_remaining = 0;
    if (out_reset_epoch_s)
      *out_reset_epoch_s = (uint64_t)(time(NULL) + retry_s);
    if (out_retry_after_s)
      *out_retry_after_s = retry_s;
    // persist unchanged counters
    shared_storage_set(g_shared_storage, skey, &st, sizeof(st));
    return 2;
  }

  st.tokens--;
  st.daily_used++;
  shared_storage_set(g_shared_storage, skey, &st, sizeof(st));
  if (out_limit)
    *out_limit = (uint32_t)cfg->rps;
  if (out_remaining)
    *out_remaining = st.tokens;
  if (out_reset_epoch_s)
    *out_reset_epoch_s = (uint64_t)(time(NULL) + 1); // reset tick in ~1s
  if (out_retry_after_s)
    *out_retry_after_s = 0;
  return 0;
}

uint64_t RAMForge_http_active_connections(void) { return active_connections; }
static int overload_retry_after_sec(void) {
  // simple two-knob policy
  if (AOF_sealed_queue_depth() > BP_MAX_SEALED_Q)
    return BP_RETRY_AFTER_SEC;
  if (active_connections > BP_MAX_ACTIVE_CONNS)
    return BP_RETRY_AFTER_SEC;
  return 0;
}

// ═══════════════════════════════════════════════════════════════════════════════
// Memory Allocation Callbacks (Pool-based for Speed)
// ═══════════════════════════════════════════════════════════════════════════════
static void alloc_cb(uv_handle_t *handle, size_t suggested_size, uv_buf_t *buf) {
  (void)handle;
  (void)suggested_size;

  void *mem = object_pool_get(buffer_pool);
  if (!mem) {
    mem = slab_alloc(MAX_REQUEST_SIZE);
  }

  buf->base = (char *)mem;
  buf->len  = MAX_REQUEST_SIZE;
}

// ──────────────────────────────────────────────────────────────────────────────
// Generic responder (non-JSON payloads, e.g. Prometheus text format)
// ──────────────────────────────────────────────────────────────────────────────
static void send_raw(connection_ctx_t *ctx, const char *content_type, const char *body, size_t body_len,
                     int status_code) {
  fast_buffer_t *buf = ctx->response_buf;
  buffer_reset(buf);

  const char *status_txt = (status_code == 200)   ? "200 OK"
                           : (status_code == 204) ? "204 No Content"
                           : (status_code == 404) ? "404 Not Found"
                           : (status_code == 503) ? "503 Service Unavailable"
                                                  : "500 Internal Server Error";

  int hdr_len = snprintf(buf->data, buf->capacity,
                         "HTTP/1.1 %s\r\n"
                         "Date: %s\r\n"
                         "Server: Ayder/3.0\r\n"
                         "Content-Type: %s\r\n"
                         "Content-Length: %zu\r\n"
                         "Connection: %s\r\n\r\n",
                         status_txt, cached_date, content_type, body_len, ctx->keep_alive ? "keep-alive" : "close");

  if (body && body_len) {
    memcpy(buf->data + hdr_len, body, body_len);
    buf->len = hdr_len + body_len;
  } else {
    buf->len = hdr_len;
  }

  write_req_t *wr = object_pool_get(write_pool);
  if (!wr)
    wr = slab_alloc(sizeof(write_req_t));
  wr->buffer     = buf;
  wr->keep_alive = ctx->keep_alive;
  wr->request_id = ctx->request_id;
  buf->ref_count++;

  uv_buf_t uvb = uv_buf_init(buf->data, buf->len);
  uv_write((uv_write_t *)wr, (uv_stream_t *)ctx->client, &uvb, 1, (uv_write_cb)write_complete_cb);

  total_bytes_sent += buf->len;
}

static void send_redirect_response(connection_ctx_t *ctx, int status_code, const char *location, int retry_after_s,
                                   const char *json_body, size_t json_len) {
  if (status_code != 307 && status_code != 308)
    status_code = 307;
  if (!location)
    location = "";

  fast_buffer_t *buf = ctx->response_buf;
  buffer_reset(buf);

  const char *status_txt = (status_code == 308) ? "308 Permanent Redirect" : "307 Temporary Redirect";
  size_t body_len        = (json_body && json_len) ? json_len : 0;

  /* Ensure we have enough capacity */
  size_t max_header_size = 1024 + strlen(location); /* Conservative estimate */
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
    hdr_len =
        snprintf(buf->data, remaining,
                 "HTTP/1.1 %s\r\n"
                 "Date: %s\r\n"
                 "Server: Ayder/3.0\r\n"
                 "Content-Type: application/json; charset=utf-8\r\n"
                 "Content-Length: %zu\r\n"
                 "Location: %s\r\n"
                 "Retry-After: %d\r\n"
                 "Connection: %s\r\n"
                 "Cache-Control: no-store\r\n"
                 "Access-Control-Allow-Origin: *\r\n"
                 "Access-Control-Allow-Methods: GET, POST, PUT, DELETE, OPTIONS\r\n"
                 "Access-Control-Allow-Headers: Content-Type, Authorization\r\n"
                 "\r\n",
                 status_txt, cached_date, body_len, location, retry_after_s, ctx->keep_alive ? "keep-alive" : "close");
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
                       "Access-Control-Allow-Origin: *\r\n"
                       "Access-Control-Allow-Methods: GET, POST, PUT, DELETE, OPTIONS\r\n"
                       "Access-Control-Allow-Headers: Content-Type, Authorization\r\n"
                       "\r\n",
                       status_txt, cached_date, body_len, location, ctx->keep_alive ? "keep-alive" : "close");
  }

  /* Handle snprintf overflow/error */
  if (hdr_len < 0) {
    hdr_len = 0;
  } else if ((size_t)hdr_len >= remaining) {
    hdr_len            = (int)remaining - 1;
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
  if (!wr)
    wr = slab_alloc(sizeof(*wr));
  wr->buffer     = buf;
  wr->keep_alive = ctx->keep_alive;
  wr->request_id = ctx->request_id;
  buf->ref_count++;

  uv_buf_t uvb = uv_buf_init(buf->data, (unsigned int)buf->len);
  uv_write((uv_write_t *)wr, (uv_stream_t *)ctx->client, &uvb, 1, (uv_write_cb)write_complete_cb);

  atomic_fetch_add(&g_http_3xx, 1);
  total_bytes_sent += buf->len;
}

static void send_response(connection_ctx_t *ctx, const char *json_data, size_t json_len, int status_code) {
  fast_buffer_t *buf = ctx->response_buf;
  buffer_reset(buf);

  const char *status_text = (status_code == 200)   ? "200 OK"
                            : (status_code == 404) ? "404 Not Found"
                            : (status_code == 400) ? "400 Bad Request"
                            : (status_code == 503) ? "503 Service Unavailable"
                                                   : "500 Internal Server Error";

  int header_len = snprintf(buf->data, buf->capacity,
                            "HTTP/1.1 %s\r\n"
                            "Date: %s\r\n"
                            "Server: Ayder/3.0\r\n"
                            "Content-Type: application/json; charset=utf-8\r\n"
                            "Content-Length: %zu\r\n"
                            "Connection: %s\r\n"
                            "Cache-Control: no-cache\r\n"
                            "Access-Control-Allow-Origin: *\r\n"
                            "Access-Control-Allow-Methods: GET, POST, PUT, DELETE, OPTIONS\r\n"
                            "Access-Control-Allow-Headers: Content-Type, Authorization\r\n",
                            status_text, cached_date, json_len, ctx->keep_alive ? "keep-alive" : "close");

  /* add optional headers BEFORE the blank line */
  if (ctx->rl_have && status_code == 200) {
    header_len += snprintf(buf->data + header_len, buf->capacity - header_len,
                           "X-RateLimit-Limit: %u\r\n"
                           "X-RateLimit-Remaining: %u\r\n"
                           "X-RateLimit-Reset: %" PRIu64 "\r\n",
                           ctx->rl_limit, ctx->rl_remaining, ctx->rl_reset_epoch_s);
    ctx->rl_have = 0;
  }
  /* end of headers */
  header_len += snprintf(buf->data + header_len, buf->capacity - header_len, "\r\n");
  /* body */
  if (json_data && json_len > 0) {
    memcpy(buf->data + header_len, json_data, json_len);
    buf->len = header_len + json_len;
  } else {
    buf->len = header_len;
  }
  write_req_t *wr = object_pool_get(write_pool);
  if (!wr)
    wr = slab_alloc(sizeof(write_req_t));
  wr->buffer     = buf;
  wr->keep_alive = ctx->keep_alive;
  wr->request_id = ctx->request_id;
  buf->ref_count++;
  uv_buf_t uvb = uv_buf_init(buf->data, buf->len);
  uv_write((uv_write_t *)wr, (uv_stream_t *)ctx->client, &uvb, 1, (uv_write_cb)write_complete_cb);

  if (status_code >= 200 && status_code < 300)
    atomic_fetch_add(&g_http_2xx, 1);
  else if (status_code == 401)
    atomic_fetch_add(&g_http_401, 1), atomic_fetch_add(&g_http_4xx, 1);
  else if (status_code == 429)
    atomic_fetch_add(&g_http_429, 1), atomic_fetch_add(&g_http_4xx, 1);
  else if (status_code == 307)
    atomic_fetch_add(&g_http_3xx, 1);
  else if (status_code >= 400 && status_code < 500)
    atomic_fetch_add(&g_http_4xx, 1);
  else if (status_code == 503)
    atomic_fetch_add(&g_http_503, 1), atomic_fetch_add(&g_http_5xx, 1);
  else if (status_code >= 500)
    atomic_fetch_add(&g_http_5xx, 1);

  total_bytes_sent += buf->len;
}

static void write_complete_cb(uv_write_t *req, int status) {
  write_req_t *wr = (write_req_t *)req;

  if (status < 0) {
    LOGE("[HTTP] Write error: %s", uv_strerror(status));
  }
  /* Always release buffer, even on canceled/broken pipe */
  if (wr->buffer) {
    buffer_release(wr->buffer);
    wr->buffer = NULL;
  }
  if (!wr->keep_alive && req->handle) {
    uv_close((uv_handle_t *)req->handle, connection_close_cb);
  }
  if (write_pool)
    object_pool_release(write_pool, wr);
  else
    slab_free(wr);
}

static size_t find_content_length(struct phr_header *headers, size_t num_headers) {
  for (size_t i = 0; i < num_headers; i++) {
    if (headers[i].name_len == 14 && strncasecmp(headers[i].name, "Content-Length", 14) == 0) {
      // Fast string to number conversion
      size_t len      = 0;
      const char *val = headers[i].value;
      for (size_t j = 0; j < headers[i].value_len; j++) {
        if (val[j] >= '0' && val[j] <= '9') {
          len = len * 10 + (val[j] - '0');
        } else {
          break;
        }
      }
      return len;
    }
  }
  return 0;
}

static int is_write_path(const char *method, const char *url) {
  if (!method || !url)
    return 0;
  int is_post   = (strcmp(method, "POST") == 0);
  int is_delete = (strcmp(method, "DELETE") == 0);
  if (!(is_post || is_delete))
    return 0;
  /* Broker produces */
  if (is_post && !strncmp(url, "/broker/topics/", 15)) {
    if (strstr(url, "/produce"))
      return 1; /* includes /produce-ndjson */
  }
  /* KV writes */
  if (!strncmp(url, "/kv/", 4)) {
    if (is_post)
      return 1; /* put */
    if (is_delete)
      return 1; /* delete */
  }
  /* Admin mutators */
  if (is_post &&
      (!strcmp(url, "/broker/topics") || !strcmp(url, "/broker/retention") || !strcmp(url, "/broker/delete-before") ||
       !strcmp(url, "/admin/sealed/gc") || !strcmp(url, "/admin/compact")))
    return 1;
  return 0;
}

static int check_keep_alive(struct phr_header *headers, size_t num_headers, int minor_version) {
  for (size_t i = 0; i < num_headers; i++) {
    if (headers[i].name_len == 10 && strncasecmp(headers[i].name, "Connection", 10) == 0) {
      if (headers[i].value_len == 5 && strncasecmp(headers[i].value, "close", 5) == 0) {
        return 0; // Explicit close
      }
      if (headers[i].value_len == 10 && strncasecmp(headers[i].value, "keep-alive", 10) == 0) {
        return 1; // Explicit keep-alive
      }
    }
  }

  // HTTP/1.1 defaults to keep-alive, HTTP/1.0 defaults to close
  return (minor_version >= 1);
}

static void load_ha_http_env_overrides(void) {
  const char *e;
  if ((e = getenv("RF_HTTP_FORCE_FOLLOWER")))
    HA_ROLE_FOLLOWER = (e[0] != '0');
  if ((e = getenv("RF_HTTP_READ_ONLY")))
    HA_READ_ONLY = (e[0] != '0');
  if ((e = getenv("RF_HTTP_LEADER_URL")))
    snprintf(HA_LEADER_URL, sizeof(HA_LEADER_URL), "%s", e);
  if ((e = getenv("RF_HTTP_REDIRECT_STATUS"))) {
    int s              = atoi(e);
    HA_REDIRECT_STATUS = (s == 308) ? 308 : 307;
  }
  if ((e = getenv("RF_HTTP_RETRY_AFTER"))) {
    int s = atoi(e);
    if (s >= 0)
      HA_RETRY_AFTER = s;
  }
}

// ═══════════════════════════════════════════════════════════════════════════════
// Lightning-Fast Request Processing
// ═══════════════════════════════════════════════════════════════════════════════
static void process_request(connection_ctx_t *ctx) {
  total_requests++;

  uint64_t handler_start_us = monotonic_us();

  uint64_t done_us  = ctx->msg_complete_us ? ctx->msg_complete_us : handler_start_us;
  uint64_t first_us = ctx->first_byte_us ? ctx->first_byte_us : done_us;

  rf_http_queue_us      = handler_start_us - done_us;
  rf_http_recv_parse_us = done_us - first_us;

  // Copy method + url (you already have this)
  size_t method_copy_len =
      ctx->method_len < sizeof(ctx->method_str) - 1 ? ctx->method_len : sizeof(ctx->method_str) - 1;
  memcpy(ctx->method_str, ctx->method, method_copy_len);
  ctx->method_str[method_copy_len] = '\0';

  size_t path_copy_len = ctx->path_len < sizeof(ctx->url) - 1 ? ctx->path_len : sizeof(ctx->url) - 1;
  memcpy(ctx->url, ctx->path, path_copy_len);
  ctx->url[path_copy_len] = '\0';

  /* ── AUTH/RL/BACKPRESSURE: INSERTED HERE ───────────────────────────── */
  {
    // 0) global backpressure (keep this first)
    int rafter = overload_retry_after_sec();
    if (unlikely(rafter > 0)) {
      send_backpressure(ctx, rafter);
      goto cleanup;
    }

    if (!path_is_public(ctx->url)) {
      // 1) Bearer check
      size_t toklen   = 0;
      const char *tok = get_bearer_from_headers(ctx, &toklen);
      if (!tok || toklen == 0) {
        send_unauthorized(ctx);
        goto cleanup;
      }

      api_key_cfg_t *cfg = find_key_cfg(tok, toklen);
      if (!cfg) {
        send_unauthorized(ctx);
        goto cleanup;
      }

      /* 1.5) >>> INSERT FORCE-503 HOOK RIGHT HERE <<< */
      {
        const struct phr_header *dbg = hdr_find(ctx, "X-Force-Overload");
        if (dbg && dbg->value_len >= 1 && (dbg->value[0] == '1' || dbg->value[0] == 'y' || dbg->value[0] == 'Y')) {
          send_backpressure(ctx, BP_RETRY_AFTER_SEC);
          goto cleanup;
        }
      }

      // 2) Rate-limit + daily quota

      uint32_t lim = 0, rem = 0;
      uint64_t reset = 0;
      int wait_s     = 0;
      int rl         = rl_take_one(cfg, tok, toklen, &lim, &rem, &reset, &wait_s);
      if (rl > 0) {
        if (wait_s < 1)
          wait_s = 1;
        send_rate_limited(ctx, wait_s, lim, rem, reset);
        goto cleanup;
      } else {
        ctx->rl_limit         = lim;
        ctx->rl_remaining     = rem;
        ctx->rl_reset_epoch_s = reset;
        ctx->rl_have          = 1;
      }
    }
  }
  /* ──────────────────────────────────────────────────────────────────── */

  /* Pull latest HA role/leader before gating writes */
  ha_refresh_http_flags();

  /* ────────────────────────────────────────────────────────────────────
+       HA WRITE-GATE:
+       - If persistence is not ready → 503 sealed_only
+       - If follower/read-only → 307/308 redirect to leader (if configured) or 503
+       ──────────────────────────────────────────────────────────────────── */
  if (is_write_path(ctx->method_str, ctx->url)) {
    /* FOLLOWER/READ-ONLY: redirect FIRST, regardless of local persistence */
    if (HA_READ_ONLY || HA_ROLE_FOLLOWER) {
      if (HA_LEADER_URL[0]) {
        char loc[1600];
        int loc_len = snprintf(loc, sizeof(loc), "%s%s", HA_LEADER_URL, ctx->url);
        if (loc_len < 0 || (size_t)loc_len >= sizeof(loc)) {
          loc[sizeof(loc) - 1] = '\0';
          loc_len              = (int)sizeof(loc) - 1;
        }
        int slashA = (HA_LEADER_URL[strlen(HA_LEADER_URL) - 1] == '/');
        int slashB = (ctx->url[0] == '/');
        if (slashA && slashB) {
          snprintf(loc, sizeof loc, "%.*s%s", (int)(strlen(HA_LEADER_URL) - 1), HA_LEADER_URL, ctx->url);
        } else if (!slashA && !slashB) {
          snprintf(loc, sizeof loc, "%s/%s", HA_LEADER_URL, ctx->url);
        } else {
          snprintf(loc, sizeof loc, "%s%s", HA_LEADER_URL, ctx->url);
        }

        char body[512];
        int body_len = snprintf(body, sizeof(body),
                                "{\"ok\":false,\"error\":\"redirect_to_leader\","
                                "\"message\":\"Writes must be sent to leader. Client should follow 307 redirect.\","
                                "\"leader_url\":\"%.256s\",\"retry_after\":%d,"
                                "\"redirect\":{\"status\":%d,\"location\":\"%.512s\"}}",
                                HA_LEADER_URL, HA_RETRY_AFTER, HA_REDIRECT_STATUS, loc);
        if (body_len < 0)
          body_len = 0;
        if ((size_t)body_len >= sizeof(body)) {
          body[sizeof(body) - 1] = '\0';
          body_len               = (int)sizeof(body) - 1;
        }

        send_redirect_response(ctx, HA_REDIRECT_STATUS, loc, HA_RETRY_AFTER, body, (size_t)body_len);
      } else {
        const char body[] = "{\"ok\":false,\"error\":\"read_only_follower\",\"leader_url\":null}";
        send_response(ctx, body, sizeof(body) - 1, 503);
      }
      goto cleanup;
    }

    /* We’re leader here; only now does persistence matter */
    if (!Persistence_is_ready()) {
      const char body[] =
          "{\"ok\":false,\"error\":\"sealed_only\",\"message\":\"Writes disabled until persistence is ready\"}";
      send_response(ctx, body, sizeof(body) - 1, 503);
      goto cleanup;
    }
  }
  /* ──────────────────────────────────────────────────────────────────── */

  // Prepare response buffer (your existing code continues)
  char response_json[MAX_RESPONSE_SIZE];
  response_json[0] = '\0';

  int result          = route_request_len(ctx->method_str, ctx->url, ctx->body, ctx->body_len, response_json);
  size_t response_len = strlen(response_json);

  if (result == 0 && strcmp(ctx->url, "/metrics") == 0) {
    send_raw(ctx, "text/plain; version=0.0.4", response_json, response_len, 200);
    goto cleanup;
  }

  int status_code = (result == 0) ? 200 : (result == -1) ? 404 : (result == -2) ? 405 : (result == -3) ? 503 : 500;

  send_response(ctx, response_json, response_len, status_code);

cleanup:
  if (ctx->keep_alive) {
    ctx->request_len     = 0;
    ctx->first_byte_us   = 0;
    ctx->msg_complete_us = 0;
    ctx->parse_state     = 0;
    ctx->msg_complete    = 0;
    ctx->body            = NULL;
    ctx->body_len        = 0;
    ctx->body_received   = 0;
    ctx->content_length  = 0;
    ctx->request_id++;
    ctx->start_time_ns = get_time_ns();
    if (ctx->is_chunked) {
      if (ctx->chunk_buf) {
        slab_free(ctx->chunk_buf);
        ctx->chunk_buf = NULL;
      }
      ctx->chunk_len   = 0;
      ctx->chunk_cap   = 0;
      ctx->cursor      = 0;
      ctx->chunk_state = 0;
      ctx->chunk_need  = 0;
      ctx->is_chunked  = 0;
    }
  }
}

// ═══════════════════════════════════════════════════════════════════════════════
// picohttpparser Integration - ULTRA FAST PARSING
// ═══════════════════════════════════════════════════════════════════════════════
static int parse_http_request(connection_ctx_t *ctx, const char *data, size_t len) {

  if (ctx->request_len + len > MAX_REQUEST_SIZE) {
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

    char *new_buf = slab_alloc(new_capacity);
    if (ctx->request_buf) {
      memcpy(new_buf, ctx->request_buf, ctx->request_len);
      slab_free(ctx->request_buf);
    }
    ctx->request_buf      = new_buf;
    ctx->request_capacity = new_capacity;
  }

  // Append new data
  memcpy(ctx->request_buf + ctx->request_len, data, len);
  ctx->request_len += len;
  total_bytes_received += len;

  if (ctx->parse_state == 0) {
    // Parse headers
    ctx->num_headers = MAX_HEADERS;

    int pret = phr_parse_request(ctx->request_buf, ctx->request_len, &ctx->method, &ctx->method_len, &ctx->path,
                                 &ctx->path_len, &ctx->minor_version, ctx->headers, &ctx->num_headers,
                                 0); // last_len = 0 for complete parsing

    if (pret == -1) {
      // Parse error
      return -1;
    } else if (pret == -2) {
      // Need more data for headers
      return 0;
    }

    // Headers parsed successfully!
    size_t header_len = pret;

    ctx->header_len             = header_len;
    const struct phr_header *te = hdr_find(ctx, "Transfer-Encoding");
    if (te && te->value_len >= 7 && strncasecmp(te->value, "chunked", 7) == 0) {
      // initialize chunked state
      ctx->is_chunked  = 1;
      ctx->cursor      = header_len;
      ctx->chunk_state = 0; // read size line
      ctx->chunk_need  = 0;
      ctx->chunk_buf   = NULL;
      ctx->chunk_len   = 0;
      ctx->chunk_cap   = 0;
      ctx->parse_state = 1; // wait for body via chunk decoder
      return 0;
    }

    // Check keep-alive
    ctx->keep_alive = check_keep_alive(ctx->headers, ctx->num_headers, ctx->minor_version);

    // Get content length
    ctx->content_length = find_content_length(ctx->headers, ctx->num_headers);

    if (ctx->content_length > MAX_REQUEST_SIZE) {
      return -1;
    }

    if (ctx->content_length > 0) {
      // Has body - calculate how much body we already have
      ctx->body_received = ctx->request_len - header_len;
      ctx->body          = ctx->request_buf + header_len;

      if (ctx->body_received >= ctx->content_length) {
        // Complete request
        ctx->body_len     = ctx->content_length;
        ctx->msg_complete = 1;
        if (ctx->msg_complete_us == 0)
          ctx->msg_complete_us = monotonic_us();
        return 1;
      } else {
        // Need more body data
        ctx->parse_state = 1;
        return 0;
      }
    } else {
      // No body - request is complete
      ctx->body         = NULL;
      ctx->body_len     = 0;
      ctx->msg_complete = 1;
      if (ctx->msg_complete_us == 0)
        ctx->msg_complete_us = monotonic_us();
      return 1;
    }
  } else if (ctx->parse_state == 1) {

    if (ctx->is_chunked) {
      size_t end = ctx->request_len;

      for (;;) {
        if (ctx->chunk_state == 0) {
          // read hex size until CRLF
          size_t p      = ctx->cursor;
          int have_crlf = 0;
          size_t size   = 0;
          int any       = 0;
          // scan until \r\n
          while (p + 1 < end) {
            if (ctx->request_buf[p] == '\r' && ctx->request_buf[p + 1] == '\n') {
              have_crlf = 1;
              break;
            }
            int hv = hexval((unsigned char)ctx->request_buf[p]);
            if (hv >= 0) {
              size = (size << 4) | (unsigned)hv;
              any  = 1;
            } else if (ctx->request_buf[p] == ';') { /* ignore extensions */
            } else {                                 /* tolerate spaces */
            }
            p++;
          }
          if (!have_crlf)
            return 0; // need more
          if (!any)
            return -1; // bad chunk size

          ctx->chunk_need  = size;
          ctx->cursor      = p + 2; // skip CRLF
          ctx->chunk_state = (size == 0) ? 3 : 1;
        } else if (ctx->chunk_state == 1) {
          // need chunk_need bytes
          size_t avail = (end > ctx->cursor) ? (end - ctx->cursor) : 0;
          size_t take  = (avail > ctx->chunk_need) ? ctx->chunk_need : avail;
          if (take) {
            size_t want = ctx->chunk_len + take;
            if (!ensure_chunk_cap(ctx, want))
              return -1;
            memcpy(ctx->chunk_buf + ctx->chunk_len, ctx->request_buf + ctx->cursor, take);
            ctx->chunk_len += take;
            ctx->cursor += take;
            ctx->chunk_need -= take;
          }
          if (ctx->chunk_need > 0)
            return 0;           // wait more
          ctx->chunk_state = 2; // expect CRLF after data
        } else if (ctx->chunk_state == 2) {
          if (ctx->cursor + 2 > end)
            return 0;
          if (ctx->request_buf[ctx->cursor] != '\r' || ctx->request_buf[ctx->cursor + 1] != '\n')
            return -1;
          ctx->cursor += 2;
          ctx->chunk_state = 0; // next size line
        } else if (ctx->chunk_state == 3) {
          // final CRLF after "0\r\n" + (optional trailers). We accept just one empty line.
          // If trailers come, they’ll be lines ending with CRLF, ending with an empty line.
          // We accept immediate CRLF and finish; if not present yet, wait.
          if (ctx->cursor + 2 > end)
            return 0;
          if (ctx->request_buf[ctx->cursor] == '\r' && ctx->request_buf[ctx->cursor + 1] == '\n') {
            ctx->cursor += 2;
            // complete request
            ctx->body         = ctx->chunk_buf;
            ctx->body_len     = ctx->chunk_len;
            ctx->msg_complete = 1;
            return 1;
          } else {
            // optional trailers present; skip until \r\n\r\n
            size_t p = ctx->cursor;
            while (p + 3 < end) {
              if (ctx->request_buf[p] == '\r' && ctx->request_buf[p + 1] == '\n' && ctx->request_buf[p + 2] == '\r' &&
                  ctx->request_buf[p + 3] == '\n') {
                ctx->cursor       = p + 4;
                ctx->body         = ctx->chunk_buf;
                ctx->body_len     = ctx->chunk_len;
                ctx->msg_complete = 1;
                return 1;
              }
              p++;
            }
            return 0; // wait more trailer bytes
          }
        }
      }
    }

    // Receiving body
    size_t header_len = ctx->request_len - ctx->body_received - len;
    ctx->body         = ctx->request_buf + header_len;
    ctx->body_received += len;

    if (ctx->body_received > MAX_REQUEST_SIZE) {
      return -1;
    }

    if (ctx->body_received >= ctx->content_length) {
      // Body complete
      ctx->body_len     = ctx->content_length;
      ctx->msg_complete = 1;
      if (ctx->msg_complete_us == 0)
        ctx->msg_complete_us = monotonic_us();
      return 1;
    }
    // Still need more body data
    return 0;
  }

  return 0;
}

// ═══════════════════════════════════════════════════════════════════════════════
// Optimized Read Callback with Minimal Allocations
// ═══════════════════════════════════════════════════════════════════════════════
static void read_cb(uv_stream_t *stream, ssize_t nread, const uv_buf_t *buf) {
  connection_ctx_t *ctx = (connection_ctx_t *)stream->data;

  if (nread > 0) {
    if (ctx->request_len == 0 && ctx->parse_state == 0 && ctx->first_byte_us == 0) {
      ctx->first_byte_us = monotonic_us();
    }
    // Parse HTTP request using picohttpparser
    int parse_result = parse_http_request(ctx, buf->base, nread);

    if (parse_result < 0) {
      // Parse error
      LOGE("[HTTP-pico] Parse error");
      /* best-effort 400 response; ignore errors */
      uv_buf_t e = uv_buf_init((char *)ERROR_400, (unsigned int)(sizeof(ERROR_400) - 1));
      (void)uv_try_write(stream, &e, 1);
      uv_close((uv_handle_t *)stream, connection_close_cb);
      object_pool_release(buffer_pool, buf->base);
      return;
    }

    // Process complete messages
    if (ctx->msg_complete) {
      process_request(ctx);
    }

  } else if (nread < 0) {
    if (nread != UV_EOF && nread != UV_ECONNRESET) {
      LOGE("[HTTP-pico] Read error: %s", uv_strerror(nread));
    }
    uv_close((uv_handle_t *)stream, connection_close_cb);
  }

  // Return buffer to pool
  object_pool_release(buffer_pool, buf->base);
}

// ═══════════════════════════════════════════════════════════════════════════════
// Connection Management
// ═══════════════════════════════════════════════════════════════════════════════
static void connection_close_cb(uv_handle_t *handle) {
  connection_ctx_t *ctx = (connection_ctx_t *)handle->data;

  if (ctx) {
    if (ctx->request_buf) {
      slab_free(ctx->request_buf);
      ctx->request_buf = NULL;
    }
    if (ctx->response_buf) {
      buffer_release(ctx->response_buf);
      ctx->response_buf = NULL;
    }

    if (ctx->chunk_buf) {
      slab_free(ctx->chunk_buf);
      ctx->chunk_buf = NULL;
    }

    // Clear the context before returning to pool
    memset(ctx, 0, sizeof(connection_ctx_t));

    // Return to pool or free
    if (connection_pool) {
      object_pool_release(connection_pool, ctx);
    } else {
      slab_free(ctx);
    }

    // Decrement active connections
    if (active_connections > 0) {
      active_connections--;
    }
  }

  // Always free the handle itself
  slab_free(handle);
}

static void accept_connection(uv_stream_t *server, int status) {
  if (status < 0) {
    LOGE("[HTTP-pico] Connection error: %s", uv_strerror(status));
    return;
  }

  // Get connection context from pool
  connection_ctx_t *ctx = NULL;
  if (connection_pool) {
    ctx = (connection_ctx_t *)object_pool_get(connection_pool);
  }

  if (!ctx) {
    ctx = slab_alloc(sizeof(connection_ctx_t));
    if (!ctx) {
      LOGE("[HTTP-pico] Failed to allocate connection context");
      return;
    }
  }

  // CRITICAL: Always zero-initialize the context
  memset(ctx, 0, sizeof(connection_ctx_t));

  // Initialize request buffer for picohttpparser
  ctx->request_capacity = PICO_PARTIAL_SIZE;
  ctx->request_buf      = slab_alloc(ctx->request_capacity);
  if (!ctx->request_buf) {
    LOGE("[HTTP-pico] Failed to allocate request buffer");
    slab_free(ctx);
    return;
  }

  ctx->response_buf = buffer_create(MAX_RESPONSE_SIZE);
  if (!ctx->response_buf) {
    LOGE("[HTTP-pico] Failed to allocate response buffer");
    slab_free(ctx->request_buf);
    slab_free(ctx);
    return;
  }

  // Create client socket
  uv_tcp_t *client = slab_alloc(sizeof(uv_tcp_t));
  if (!client) {
    LOGE("[HTTP-pico] Failed to allocate client socket");
    slab_free(ctx->request_buf);
    buffer_release(ctx->response_buf);
    slab_free(ctx);
    return;
  }

  uv_tcp_init(main_loop, client);

  // Enable TCP optimizations for sub-1ms latency
  uv_tcp_nodelay(client, TCP_NODELAY);
  uv_tcp_keepalive(client, TCP_KEEPALIVE, 60);

  ctx->client        = client;
  ctx->start_time_ns = get_time_ns();
  ctx->keep_alive    = 1; // Default to keep-alive
  ctx->request_id    = 1;
  ctx->parse_state   = 0; // Start parsing headers
  client->data       = ctx;

  if (uv_accept(server, (uv_stream_t *)client) == 0) {
    uv_read_start((uv_stream_t *)client, alloc_cb, read_cb);
    active_connections++;
  } else {
    uv_close((uv_handle_t *)client, connection_close_cb);
  }
}
// ═══════════════════════════════════════════════════════════════════════════════
// Performance Statistics Timer
// ═══════════════════════════════════════════════════════════════════════════════
static uv_timer_t stats_timer;

static uv_timer_t lat_timer;
static void init_latency_timer(void) { uv_timer_init(main_loop, &lat_timer); }

// ═══════════════════════════════════════════════════════════════════════════════
// Public API - Initialize the Beast
// ═══════════════════════════════════════════════════════════════════════════════
void http_server_init(App *app, int port) {
  (void)app; // Framework integration handled by router

  // Create object pools
  connection_pool = object_pool_create(CONNECTION_POOL_SIZE, NULL, NULL);
  buffer_pool     = object_pool_create(BUFFER_POOL_SIZE, NULL, NULL);
  write_pool      = object_pool_create(4096, NULL, NULL);

  if (!connection_pool || !buffer_pool) {
    LOGE("Failed to create object pools");
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
  uv_tcp_t *server = slab_alloc(sizeof(uv_tcp_t));
  uv_tcp_init(main_loop, server);

  /* Pre-bind a socket with SO_REUSEPORT, then adopt it in libuv.   */
  int fd = make_reuseport_listener_ipv4("0.0.0.0", port);
  if (fd < 0) {
    LOGE("Failed to create SO_REUSEPORT listener");
    exit(1);
  }

  int open_rc = uv_tcp_open(server, fd);
  if (open_rc != 0) {
    LOGE("uv_tcp_open failed: %s", uv_strerror(open_rc));
    close(fd);
    exit(1);
  }

  // Start listening with large backlog for high-traffic scenarios
  int listen_result = uv_listen((uv_stream_t *)server, 8192, accept_connection);
  if (listen_result != 0) {
    LOGE("Listen failed: %s", uv_strerror(listen_result));
    exit(1);
  }

  load_auth_env_overrides();
  load_ha_http_env_overrides();
  parse_bearer_tokens_from_env();

  parse_auth_skip_prefixes_from_env();

  ha_auto_configure_http();

  // Run the event loop
  uv_run(main_loop, UV_RUN_DEFAULT);

  // Cleanup
  object_pool_destroy(connection_pool);
  object_pool_destroy(buffer_pool);
  object_pool_destroy(write_pool);
  slab_destroy();
}

// ═══════════════════════════════════════════════════════════════════════════════
// Graceful shutdown
// ═══════════════════════════════════════════════════════════════════════════════
void http_server_shutdown(void) {
  LOGI("Shutting down Ayder HTTP Server...");
  uv_timer_stop(&date_timer);
  uv_timer_stop(&lat_timer);
  uv_timer_stop(&stats_timer);
  // Event loop will exit naturally
}
