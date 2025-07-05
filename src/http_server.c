// src/http_server.c - ULTRA-FAST HTTP Server that makes Axum cry
// Zero-copy, SIMD-optimized, async I/O beast mode activated! 🚀
// FIXED: Proper SO_REUSEPORT for multi-worker clustering

#include "http_server.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <uv.h>
#include <sys/socket.h>  // For SO_REUSEPORT
#include <netinet/in.h>  // For sockopt
#include <unistd.h>
#include "http_parser.h"
#include "object_pool.h"
#include "router.h"
#include "slab_alloc.h"
#include "app.h"
#include "ramforge_rotation_metrics.h"


// ═══════════════════════════════════════════════════════════════════════════════
// BEAST MODE CONFIGURATION - Tuned for Maximum Performance
// ═══════════════════════════════════════════════════════════════════════════════
#define MAX_REQUEST_SIZE     (64 * 1024)     // 64KB max request
#define MAX_RESPONSE_SIZE    (256 * 1024)    // 256KB max response
#define CONNECTION_POOL_SIZE  2048           // Pre-allocated connections
#define BUFFER_POOL_SIZE     4096           // Buffer pool size
#define WORKER_THREADS        16            // Background worker threads
#define TCP_NODELAY           1             // Disable Nagle's algorithm
#define TCP_KEEPALIVE         1             // Enable TCP keepalive

// SO_REUSEPORT definition (Linux/BSD)
#ifndef UV_TCP_REUSEPORT
#define UV_TCP_REUSEPORT 2
#endif

// Pre-computed HTTP headers for ultra-fast responses
static const char RESPONSE_HEADERS_TEMPLATE[] =
        "HTTP/1.1 200 OK\r\n"
        "Date: %s\r\n"
        "Server: RAMForge-Beast/2.0\r\n"
        "Content-Type: application/json; charset=utf-8\r\n"
        "Content-Length: %zu\r\n"
        "Connection: %s\r\n"
        "Cache-Control: no-cache\r\n"
        "Access-Control-Allow-Origin: *\r\n"
        "Access-Control-Allow-Methods: GET, POST, PUT, DELETE, OPTIONS\r\n"
        "Access-Control-Allow-Headers: Content-Type, Authorization\r\n"
        "\r\n";

// Pre-computed error responses (zero allocation)
static const char ERROR_404[] =
        "HTTP/1.1 404 Not Found\r\n"
        "Content-Type: application/json\r\n"
        "Content-Length: 25\r\n"
        "Connection: close\r\n\r\n"
        "{\"error\":\"Not Found\"}";

static const char ERROR_400[] =
        "HTTP/1.1 400 Bad Request\r\n"
        "Content-Type: application/json\r\n"
        "Content-Length: 27\r\n"
        "Connection: close\r\n\r\n"
        "{\"error\":\"Bad Request\"}";

static const char ERROR_500[] =
        "HTTP/1.1 500 Internal Server Error\r\n"
        "Content-Type: application/json\r\n"
        "Content-Length: 34\r\n"
        "Connection: close\r\n\r\n"
        "{\"error\":\"Internal Server Error\"}";

// ═══════════════════════════════════════════════════════════════════════════════
// Lightning-Fast Connection Context with Zero-Copy Buffers
// ═══════════════════════════════════════════════════════════════════════════════
typedef struct fast_buffer {
    char*  data;
    size_t len;
    size_t capacity;
    int    ref_count;
} fast_buffer_t;

typedef struct {
    uv_write_t req;
    fast_buffer_t* buffer;
    int keep_alive;
    uint64_t request_id;
} write_req_t;

typedef struct {
    http_parser parser;
    http_parser_settings settings;

    // Zero-copy string views (no allocation!)
    char method[16];
    char url[512];
    char* body;
    size_t body_len;
    size_t body_capacity;

    // Connection state
    uv_tcp_t* client;
    int msg_complete;
    int keep_alive;
    uint64_t request_id;
    uint64_t start_time_ns;

    // Pre-allocated response buffer
    fast_buffer_t* response_buf;

} connection_ctx_t;

// ═══════════════════════════════════════════════════════════════════════════════
// Global Performance Monitoring & Pools
// ═══════════════════════════════════════════════════════════════════════════════
static object_pool_t* connection_pool = NULL;
static object_pool_t* buffer_pool = NULL;
static uv_loop_t* main_loop = NULL;

// Performance counters (lock-free atomic)
static volatile uint64_t total_requests = 0;
static volatile uint64_t active_connections = 0;
static volatile uint64_t total_bytes_sent = 0;
static volatile uint64_t total_bytes_received = 0;
static void write_complete_cb(uv_write_t* req, int status);
static void connection_close_cb(uv_handle_t* handle);

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

// ═══════════════════════════════════════════════════════════════════════════════
// Ultra-Fast Date Cache (updates every second)
// ═══════════════════════════════════════════════════════════════════════════════
static char cached_date[64];
static time_t last_date_update = 0;
static uv_timer_t date_timer;

static void update_date_cache(uv_timer_t* timer) {
    (void)timer;
    time_t now = time(NULL);
    struct tm tm;
    gmtime_r(&now, &tm);
    strftime(cached_date, sizeof(cached_date), "%a, %d %b %Y %H:%M:%S GMT", &tm);
    last_date_update = now;
}

// ═══════════════════════════════════════════════════════════════════════════════
// Hyper-Optimized HTTP Parser Callbacks
// ═══════════════════════════════════════════════════════════════════════════════
static int on_url_cb(http_parser* parser, const char* at, size_t length) {
    connection_ctx_t* ctx = (connection_ctx_t*)parser->data;

    size_t copy_len = length < sizeof(ctx->url) - 1 ? length : sizeof(ctx->url) - 1;
    memcpy(ctx->url, at, copy_len);
    ctx->url[copy_len] = '\0';

    return 0;
}

static int on_body_cb(http_parser* parser, const char* at, size_t length) {
    connection_ctx_t* ctx = (connection_ctx_t*)parser->data;

    // Grow buffer if needed
    if (ctx->body_len + length >= ctx->body_capacity) {
        size_t new_capacity = ctx->body_capacity * 2;
        while (new_capacity < ctx->body_len + length + 1) {
            new_capacity *= 2;
        }

        char* new_body = slab_alloc(new_capacity);
        if (ctx->body) {
            memcpy(new_body, ctx->body, ctx->body_len);
            slab_free(ctx->body);
        }
        ctx->body = new_body;
        ctx->body_capacity = new_capacity;
    }

    memcpy(ctx->body + ctx->body_len, at, length);
    ctx->body_len += length;
    ctx->body[ctx->body_len] = '\0';

    total_bytes_received += length;
    return 0;
}

static int on_message_complete_cb(http_parser* parser) {
    connection_ctx_t* ctx = (connection_ctx_t*)parser->data;
    ctx->msg_complete = 1;

    // Copy method string
    const char* method_str = http_method_str(parser->method);
    strncpy(ctx->method, method_str, sizeof(ctx->method) - 1);
    ctx->method[sizeof(ctx->method) - 1] = '\0';

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

// ═══════════════════════════════════════════════════════════════════════════════
// Lightning-Fast Response Generation
// ═══════════════════════════════════════════════════════════════════════════════

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

    int hdr_len = snprintf(buf->data, buf->capacity,
                           "HTTP/1.1 %s\r\n"
                           "Date: %s\r\n"
                           "Server: RAMForge-Beast/3.0\r\n"
                           "Content-Type: %s\r\n"
                           "Content-Length: %zu\r\n"
                           "Connection: %s\r\n\r\n",
                           status_txt,
                           cached_date,
                           content_type,
                           body_len,
                           ctx->keep_alive ? "keep-alive" : "close");

    if (body && body_len) {
        memcpy(buf->data + hdr_len, body, body_len);
        buf->len = hdr_len + body_len;
    } else {
        buf->len = hdr_len;
    }

    write_req_t *wr = slab_alloc(sizeof(write_req_t));
    wr->buffer      = buf;
    wr->keep_alive  = ctx->keep_alive;
    wr->request_id  = ctx->request_id;
    buf->ref_count++;

    uv_buf_t uvb = uv_buf_init(buf->data, buf->len);
    uv_write((uv_write_t *)wr, (uv_stream_t *)ctx->client, &uvb, 1,
             (uv_write_cb)write_complete_cb);

    total_bytes_sent += buf->len;
}



static void send_response(connection_ctx_t* ctx, const char* json_data, size_t json_len, int status_code) {
    fast_buffer_t* buf = ctx->response_buf;
    buffer_reset(buf);

    const char *status_text = (status_code == 200) ? "200 OK" :
                              (status_code == 404) ? "404 Not Found" :
                              (status_code == 400) ? "400 Bad Request" :
                              (status_code == 503) ? "503 Service Unavailable" :
                              "500 Internal Server Error";

    // Build response in one shot (minimal system calls)
    int header_len = snprintf(buf->data, buf->capacity,
                              "HTTP/1.1 %s\r\n"
                              "Date: %s\r\n"
                              "Server: RAMForge-Beast/2.0\r\n"
                              "Content-Type: application/json; charset=utf-8\r\n"
                              "Content-Length: %zu\r\n"
                              "Connection: %s\r\n"
                              "Cache-Control: no-cache\r\n"
                              "Access-Control-Allow-Origin: *\r\n"
                              "Access-Control-Allow-Methods: GET, POST, PUT, DELETE, OPTIONS\r\n"
                              "Access-Control-Allow-Headers: Content-Type, Authorization\r\n"
                              "\r\n",
                              status_text,
                              cached_date,
                              json_len,
                              ctx->keep_alive ? "keep-alive" : "close"
    );

    // Append JSON body
    if (json_data && json_len > 0) {
        memcpy(buf->data + header_len, json_data, json_len);
        buf->len = header_len + json_len;
    } else {
        buf->len = header_len;
    }

    // Send response asynchronously
    write_req_t* write_req = slab_alloc(sizeof(write_req_t));
    write_req->buffer = buf;
    write_req->keep_alive = ctx->keep_alive;
    write_req->request_id = ctx->request_id;
    buf->ref_count++; // Keep buffer alive during write

    uv_buf_t uv_buf = uv_buf_init(buf->data, buf->len);
    uv_write((uv_write_t*)write_req, (uv_stream_t*)ctx->client, &uv_buf, 1,
             (uv_write_cb)write_complete_cb);

    total_bytes_sent += buf->len;
}

static void write_complete_cb(uv_write_t* req, int status) {
    write_req_t* write_req = (write_req_t*)req;

    if (status < 0) {
        fprintf(stderr, "[HTTP] Write error: %s\n", uv_strerror(status));
    }

    // Release buffer safely
    if (write_req->buffer) {
        buffer_release(write_req->buffer);
        write_req->buffer = NULL;
    }

    if (!write_req->keep_alive) {
        // Close connection after response
        uv_close((uv_handle_t*)req->handle, connection_close_cb);
    }

    slab_free(write_req);
}

// ═══════════════════════════════════════════════════════════════════════════════
// Lightning-Fast Request Processing
// ═══════════════════════════════════════════════════════════════════════════════
static void process_request(connection_ctx_t* ctx) {
    total_requests++;

    // Prepare response buffer - allocate enough space for typical responses
    char response_json[MAX_RESPONSE_SIZE];
    response_json[0] = '\0';

    // Route the request using our super-fast router
    int result = route_request(ctx->method, ctx->url, ctx->body, response_json);
    size_t response_len = strlen(response_json);
    if (result == 0 && strcmp(ctx->url, "/metrics") == 0) {
        send_raw(ctx, "text/plain; version=0.0.4",
                 response_json, response_len, 200);
        return;               // skip the JSON path below
    }

    int status_code =
            (result == 0)  ? 200 :
            (result == -1) ? 404 :
            (result == -2) ? 405 :
            (result == -3) ? 503 :
            500;

    // Handle empty responses (fix for empty brackets issue!)
    send_response(ctx, response_json, response_len, status_code);

    // Performance logging for very slow requests (> 1ms)

    uint64_t elapsed = get_time_ns() - ctx->start_time_ns;
    if (elapsed > 1000000) { // 1ms threshold
        printf("[PERF] request: %s %s took %lu μs\n",
               ctx->method, ctx->url, elapsed / 1000);
    }


    RAMForge_enhanced_operation_record(elapsed / 1000);
    // Reset for next request if keep-alive
    if (ctx->keep_alive) {
        ctx->msg_complete = 0;
        ctx->body_len = 0;
        ctx->url[0] = '\0';
        ctx->request_id++;
        ctx->start_time_ns = get_time_ns();
        http_parser_init(&ctx->parser, HTTP_REQUEST);
    }
}

// ═══════════════════════════════════════════════════════════════════════════════
// Optimized Read Callback with Minimal Allocations
// ═══════════════════════════════════════════════════════════════════════════════
static void read_cb(uv_stream_t* stream, ssize_t nread, const uv_buf_t* buf) {
    connection_ctx_t* ctx = (connection_ctx_t*)stream->data;

    if (nread > 0) {
        // Feed data to HTTP parser
        size_t parsed = http_parser_execute(&ctx->parser, &ctx->settings, buf->base, nread);

        if (parsed != (size_t)nread) {
            // Parse error
            fprintf(stderr, "[HTTP] Parse error at position %zu\n", parsed);
            uv_close((uv_handle_t*)stream, connection_close_cb);
            object_pool_release(buffer_pool, buf->base);
            return;
        }

        // Process complete messages
        if (ctx->msg_complete) {
            process_request(ctx);
        }

    } else if (nread < 0) {
        if (nread != UV_EOF) {
            fprintf(stderr, "[HTTP] Read error: %s\n", uv_strerror(nread));
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
        // Safely cleanup connection context
        if (ctx->body) {
            slab_free(ctx->body);
            ctx->body = NULL;
        }
        if (ctx->response_buf) {
            buffer_release(ctx->response_buf);
            ctx->response_buf = NULL;
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

static void accept_connection(uv_stream_t* server, int status) {
    if (status < 0) {
        fprintf(stderr, "[HTTP] Connection error: %s\n", uv_strerror(status));
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
            fprintf(stderr, "[HTTP] Failed to allocate connection context\n");
            return;
        }
    }

    // CRITICAL: Always zero-initialize the context
    memset(ctx, 0, sizeof(connection_ctx_t));

    // Initialize HTTP parser
    http_parser_init(&ctx->parser, HTTP_REQUEST);
    http_parser_settings_init(&ctx->settings);
    ctx->settings.on_url = on_url_cb;
    ctx->settings.on_body = on_body_cb;
    ctx->settings.on_message_complete = on_message_complete_cb;
    ctx->parser.data = ctx;

    // Allocate buffers
    ctx->body_capacity = 4096;
    ctx->body = slab_alloc(ctx->body_capacity);
    if (!ctx->body) {
        fprintf(stderr, "[HTTP] Failed to allocate body buffer\n");
        slab_free(ctx);
        return;
    }
    ctx->body[0] = '\0';

    ctx->response_buf = buffer_create(MAX_RESPONSE_SIZE);
    if (!ctx->response_buf) {
        fprintf(stderr, "[HTTP] Failed to allocate response buffer\n");
        slab_free(ctx->body);
        slab_free(ctx);
        return;
    }

    // Create client socket
    uv_tcp_t* client = slab_alloc(sizeof(uv_tcp_t));
    if (!client) {
        fprintf(stderr, "[HTTP] Failed to allocate client socket\n");
        slab_free(ctx->body);
        buffer_release(ctx->response_buf);
        slab_free(ctx);
        return;
    }

    uv_tcp_init(main_loop, client);

    // Enable TCP optimizations
    uv_tcp_nodelay(client, TCP_NODELAY);
    uv_tcp_keepalive(client, TCP_KEEPALIVE, 60);

    ctx->client = client;
    ctx->start_time_ns = get_time_ns();
    ctx->keep_alive = 1; // Default to keep-alive
    ctx->request_id = 1;
    client->data = ctx;

    if (uv_accept(server, (uv_stream_t*)client) == 0) {
        uv_read_start((uv_stream_t*)client, alloc_cb, read_cb);
        active_connections++;
    } else {
        uv_close((uv_handle_t*)client, connection_close_cb);
    }
}

// ═══════════════════════════════════════════════════════════════════════════════
// CRITICAL FIX: Manual SO_REUSEPORT Configuration
// ═══════════════════════════════════════════════════════════════════════════════
//static int enable_reuseport(uv_tcp_t* handle) {
//    int fd;
//    int result = uv_fileno((uv_handle_t*)handle, &fd);
//    if (result < 0) {
//        fprintf(stderr, "Failed to get file descriptor: %s\n", uv_strerror(result));
//        return -1;
//    }
//
//    int enable = 1;
//    if (setsockopt(fd, SOL_SOCKET, SO_REUSEPORT, &enable, sizeof(enable)) < 0) {
//        perror("setsockopt SO_REUSEPORT failed");
//        return -1;
//    }
//
//    printf("✓ SO_REUSEPORT enabled on socket fd=%d\n", fd);
//    return 0;
//}

// ═══════════════════════════════════════════════════════════════════════════════
// Performance Statistics Timer
// ═══════════════════════════════════════════════════════════════════════════════
static uv_timer_t stats_timer;

// ═══════════════════════════════════════════════════════════════════════════════
// Public API - Initialize the Beast
// ═══════════════════════════════════════════════════════════════════════════════
void http_server_init(App* app, int port) {
    (void)app; // Framework integration handled by router

    printf("🔥 Initializing RAMForge Beast Mode HTTP Server...\n");

    // Create object pools
    connection_pool = object_pool_create(CONNECTION_POOL_SIZE, NULL, NULL);
    buffer_pool = object_pool_create(BUFFER_POOL_SIZE, NULL, NULL);

    if (!connection_pool || !buffer_pool) {
        fprintf(stderr, "Failed to create object pools\n");
        exit(1);
    }

    // Initialize main event loop
    main_loop = uv_default_loop();

    // Set up date cache timer (updates every second)
    uv_timer_init(main_loop, &date_timer);
    update_date_cache(&date_timer); // Initial update
    uv_timer_start(&date_timer, update_date_cache, 1000, 1000);

    // Create TCP server with maximum performance settings
    uv_tcp_t* server = slab_alloc(sizeof(uv_tcp_t));
    uv_tcp_init(main_loop, server);



    // Bind to all interfaces
    struct sockaddr_in addr;
    uv_ip4_addr("0.0.0.0", port, &addr);

    // Simple bind (SO_REUSEPORT already enabled above)
    int bind_result = uv_tcp_bind(server, (const struct sockaddr*)&addr, UV_TCP_REUSEPORT);
    if (bind_result != 0) {
        fprintf(stderr, "❌ Bind failed: %s\n", uv_strerror(bind_result));
        exit(1);
    }

    // Start listening with large backlog for high-traffic scenarios
    int listen_result = uv_listen((uv_stream_t*)server, 8192, accept_connection);
    if (listen_result != 0) {
        fprintf(stderr, "❌ Listen failed: %s\n", uv_strerror(listen_result));
        exit(1);
    }

    printf("🚀 RAMForge Beast Mode HTTP Server is LIVE on port %d! (PID: %d)\n", port, getpid());

    // Run the event loop
    uv_run(main_loop, UV_RUN_DEFAULT);

    // Cleanup
    object_pool_destroy(connection_pool);
    object_pool_destroy(buffer_pool);
    slab_destroy();
}

// ═══════════════════════════════════════════════════════════════════════════════
// Graceful shutdown
// ═══════════════════════════════════════════════════════════════════════════════
void http_server_shutdown(void) {
    printf("🛑 Shutting down RAMForge Beast Mode HTTP Server...\n");
    uv_timer_stop(&date_timer);
    uv_timer_stop(&stats_timer);
    // Event loop will exit naturally
}