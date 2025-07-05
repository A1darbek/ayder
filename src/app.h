// app.h
#ifndef APP_H
#define APP_H

#include "storage.h"
#include "router.h"
#include "persistence.h"   // for Persistence_shutdown()

typedef struct App {
    Storage        *storage;
    void          (*get)     (struct App*, const char*, RouteHandler);
    void          (*post)    (struct App*, const char*, RouteHandler);
    void          (*start)   (struct App*, int port);
    void          (*shutdown)(void);             // call Persistence_shutdown()
} App;

App *app_create(Storage *storage);

#endif // APP_H