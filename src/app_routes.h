#ifndef APP_ROUTES_H
#define APP_ROUTES_H

#include "app.h"

extern App *g_app;

// Developers implement this function to register their business logic and routes.
// It is automatically called by the built-in cluster mode.

void register_application_routes(App *app, int control_plane);

#endif
