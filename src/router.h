// router.h
#ifndef ROUTER_H
#define ROUTER_H

#include "request.h"
#include "response.h"

typedef int (*RouteHandler)(Request*, Response*);

/// Initialize the router (must call once before registering any routes)
void router_init(void);

/// Register a route under a given HTTP method and path pattern.
/// Path segments starting with ':' are named parameters (e.g. "/users/:id").
/// Example: register_route("GET", "/users/:id", get_user_handler);
void register_route(const char* method, const char* path, RouteHandler handler);

/// Match an incoming request and dispatch to the handler, serializing JSON into response_buffer.
/// If no match is found, returns `{"error":"Not found"}`.
int route_request(const char* method,
                   const char* path,
                   const char* body,
                   char* response_buffer);

int route_request_len(const char *method,
                      const char *path,
                      const char *body, size_t body_len,
                      char *response_buffer);

#endif // ROUTER_H
