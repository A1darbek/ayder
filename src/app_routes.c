#include <stdlib.h>
#include <string.h>
#include "app.h"
#include "persistence.h"
#include "user.h"
#include "aof_batch.h"
#include "fast_json.h"
#include "router.h"
#include "ramforge_rotation_metrics.h"
#include "shared_storage.h"
#include "zero_pause_rdb.h"
#include "zero_pause_restore.h"
#include "globals.h"

extern App *g_app;

// ═══════════════════════════════════════════════════════════════════════════════
// Optimized Request/Response Handling
// ═══════════════════════════════════════════════════════════════════════════════

typedef struct {
    char* buffer;
    size_t len;
    size_t capacity;
} fast_response_t;

static inline void response_ensure_capacity(fast_response_t* res, size_t needed) {
    if (res->len + needed > res->capacity) {
        while (res->capacity < res->len + needed) {
            res->capacity *= 2;
        }
        res->buffer = realloc(res->buffer, res->capacity);
    }
}

// ═══════════════════════════════════════════════════════════════════════════════
// Lightning-Fast Route Handlers
// ═══════════════════════════════════════════════════════════════════════════════

// POST /users → create or update a user (sub-100μs target)
int create_user_fast(Request *req, Response *res) {
    // Parse JSON using zero-copy parser
    json_value_t* root = json_parse(req->body, strlen(req->body));
    if (!root || root->type != JSON_OBJECT) {
        const char* error = "{\"error\":\"Invalid JSON\"}";
        size_t error_len = strlen(error);
        memcpy(res->buffer, error, error_len);
        res->buffer[error_len] = '\0';
        if (root) json_free(root);
        return -1;
    }

    // Extract fields using fast lookup
    json_value_t* id_field = json_get_field(root, "id");
    json_value_t* name_field = json_get_field(root, "name");

    if (!id_field || !name_field ||
        id_field->type != JSON_INT ||
        name_field->type != JSON_STRING) {
        const char* error = "{\"error\":\"Missing or invalid fields\"}";
        size_t error_len = strlen(error);
        memcpy(res->buffer, error, error_len);
        res->buffer[error_len] = '\0';
        json_free(root);
        return -1;
    }

    // Create user struct
    User u = {0};
    u.id = id_field->as.i;

    // Copy name (safe bounds checking)
    size_t name_len = name_field->as.s.len;
    if (name_len >= sizeof(u.name)) name_len = sizeof(u.name) - 1;
    memcpy(u.name, name_field->as.s.ptr, name_len);
    u.name[name_len] = '\0';

    // AOF-FIRST: Persist to AOF before memory (ensures durability)
    if (AOF_append(u.id, &u, sizeof(u)) < 0) {
        const char *error = "{\"error\":\"Disk full\"}";
        size_t error_len = strlen(error);
        memcpy(res->buffer, error, error_len);
        res->buffer[error_len] = '\0';
        json_free(root);
        return -3;  // disk full -> HTTP 503
    }

    // UPDATED: Store in SHARED storage (visible to all workers)
    if (shared_storage_set(g_shared_storage, u.id, &u, sizeof(u)) < 0) {
        const char *error = "{\"error\":\"Storage full\"}";
        size_t error_len = strlen(error);
        memcpy(res->buffer, error, error_len);
        res->buffer[error_len] = '\0';
        json_free(root);
        return -2;
    }

    // Optional: Also store in local storage for faster local access
    storage_save(g_app->storage, u.id, &u, sizeof(u));

    // Generate response using template (ultra-fast)
    size_t response_len = serialize_user_fast(res->buffer, u.id, u.name);
    res->buffer[response_len] = '\0';

    json_free(root);
    return 0;
}


// GET /users/:id (sub-50μs target)
int get_user_fast(Request *req, Response *res) {
    const char* id_str = req->params[0].value;
    int id = 0;

    // Inline fast atoi (avoid library call overhead)
    const char* p = id_str;
    while (*p >= '0' && *p <= '9') {
        id = id * 10 + (*p - '0');
        p++;
    }

    User u;

    // UPDATED: Check SHARED storage first (authoritative source)
    if (shared_storage_get(g_shared_storage, id, &u, sizeof(u))) {
        // SUCCESS: User found in shared storage

        // Optional: Cache in local storage for faster future access
        storage_save(g_app->storage, id, &u, sizeof(u));

        size_t len = serialize_user_fast(res->buffer, u.id, u.name);
        res->buffer[len] = '\0';
    } else {
        // Fallback: Check local storage (might be stale but better than nothing)
        if (storage_get(g_app->storage, id, &u, sizeof(u))) {
            size_t len = serialize_user_fast(res->buffer, u.id, u.name);
            res->buffer[len] = '\0';
        } else {
            // FAILURE: User not found anywhere
            const char* error = "{\"error\":\"User not found\"}";
            size_t error_len = strlen(error);
            memcpy(res->buffer, error, error_len);
            res->buffer[error_len] = '\0';
        }
    }

    return 0;  // Always return success to HTTP layer
}

// Health check optimized for monitoring tools
int health_fast(Request *req, Response *res) {
    (void)req;

    // Pre-computed response (only 8 bytes!)
    static const char health_response[] = "{\"ok\":1}";
    static const size_t health_len = sizeof(health_response) - 1;

    memcpy(res->buffer, health_response, health_len);
    res->buffer[health_len] = '\0';
}

// Admin compaction with progress tracking
int compact_handler_fast(Request *req, Response *res) {
    (void)req;

    // Start compaction in background
    Persistence_compact();

    // Immediate response (don't wait for completion)
    static const char compact_response[] = "{\"result\":\"compaction_started\",\"async\":true}";
    static const size_t compact_len = sizeof(compact_response) - 1;

    memcpy(res->buffer, compact_response, compact_len);
    res->buffer[compact_len] = '\0';
}

int prometheus_metrics_handler(Request* req, Response* res) {
    (void)req;
    RAMForge_export_prometheus_metrics_buffer(res->buffer, RESPONSE_BUFFER_SIZE);
    return 0;
}

int zp_snapshot_handler(Request *req, Response *res)
{
    (void)req;
    ZeroPauseRDB_snapshot();                 /* fork-less, zero-pause */
    memcpy(res->buffer, "ok\n", 3);
    return 0;
}


// ═══════════════════════════════════════════════════════════════════════════════
// Framework Integration & Route Registration
// ═══════════════════════════════════════════════════════════════════════════════

// Main registration function that cluster.c expects
void register_application_routes(App *app) {
    // Set global app reference for route handlers
    g_app = app;
    // Core CRUD operations using the App's route registration methods
    app->post(app, "/users", create_user_fast);
    app->get(app, "/users/:id", get_user_fast);
//    app->get(app, "/users", list_users_fast);
    // System routes
    app->get(app, "/health", health_fast);
    app->post(app, "/admin/compact", compact_handler_fast);
    app->get(app, "/metrics", prometheus_metrics_handler);
    app->post(app, "/admin/zp_snapshot", zp_snapshot_handler);

    // Zero-pause snapshot and restore
    app->post(app, "/admin/zp_snapshot", zp_snapshot_handler);
    app->post(app, "/admin/zp_restore", zp_restore_handler);              // Async restore
    app->get(app, "/admin/zp_restore/status", zp_restore_status_handler); // Check status
    app->post(app, "/admin/zp_restore/sync", zp_restore_sync_handler);    // Sync restore

}
