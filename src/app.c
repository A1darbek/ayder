// src/app.c
#include <stdlib.h>
#include <stdio.h>
#include "app.h"
#include "router.h"
#include "http_server.h"

/* ─────────────── wrappers around the global router API ──────────────── */
static void router_get_wrap(struct App *self,
                            const char *path,
                            RouteHandler h)
{
    (void)self;                 /* storage not needed here */
    register_route("GET", path, h);
}

static void router_post_wrap(struct App *self,
                             const char *path,
                             RouteHandler h)
{
    (void)self;
    register_route("POST", path, h);
}

/* stub (HTTP server lives in cluster.c) */
static void app_start_http(struct App *self, int port)
{
    http_server_init(self, port);
}

static void app_shutdown_wrap(void)
{
    http_server_shutdown();
    Persistence_shutdown();
}

/* ─────────────────── public factory ─────────────────────────────────── */
App *app_create(Storage *storage)
{
    /* 1) make sure router tables are ready exactly once */
    static int router_ready = 0;
    if (!router_ready) { router_init(); router_ready = 1; }

    /* 2) allocate + wire all v-table pointers */
    App *app   = calloc(1, sizeof *app);
    if (!app)  { perror("calloc"); return NULL; }

    app->storage  = storage;
    app->get      = router_get_wrap;
    app->post     = router_post_wrap;
    app->start    = app_start_http;
    app->shutdown = app_shutdown_wrap;

    return app;
}