// app.h
#ifndef APP_H
#define APP_H

#include "persistence_zp.h"
#include "router.h"
#include "storage.h"
typedef struct App {
  Storage *storage;
  void (*get)(struct App *, const char *, RouteHandler);
  void (*post)(struct App *, const char *, RouteHandler);
  void (*delete)(struct App *, const char *, RouteHandler);
  void (*start)(struct App *, int port);
  void (*shutdown)(void); // call Persistence_shutdown()
} App;

App *app_create(Storage *storage);

#endif // APP_H