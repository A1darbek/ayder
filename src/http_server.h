#ifndef HTTP_SERVER_H
#define HTTP_SERVER_H

#include "app.h"
#include <uv.h>

/**
 * Initialize & run HTTP server on `port`.
 * Call this *after* Persistence_init() in each worker.
 */
void http_server_init(App *app, int port);
void http_server_shutdown(void);

#endif // HTTP_SERVER_H