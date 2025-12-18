#include "ramforge.h"
#include "app.h"
#include "storage.h"
#include "router.h"
#include "http_server.h"
#include <stdlib.h>

static void _get(App *a, const char *p, RouteHandler h){
    register_route("GET",  p, h);
}
static void _post(App *a, const char *p, RouteHandler h){
    register_route("POST", p, h);
}

static void start(App *app, int port){
    http_server_init(app, port);
}

static void shutdown_hook(void){
    Persistence_shutdown();
}

App *RamForge_create_custom(const char *aof_file_path){
    App *app = malloc(sizeof(*app));
    app->storage  = malloc(sizeof(*app->storage));
    storage_init(app->storage);

    app->get      = _get;
    app->post     = _post;
    app->start    = start;
    app->shutdown = shutdown_hook;
    return app;
}

App *RamForge_create(void){
    return RamForge_create_custom("ramforge.aof");
}