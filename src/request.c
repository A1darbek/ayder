// request.c
#include "request.h"
#include "slab_alloc.h"
#include <string.h>

Request parse_request_len(const char *body, size_t body_len) {
  Request req;
  memset(&req, 0, sizeof req);
  if (body && body_len) {
    char *copy = slab_alloc(body_len + 1);
    memcpy(copy, body, body_len);
    copy[body_len] = '\0'; // force NUL sentinel
    req.body       = copy;
    req.body_len   = body_len;
  }
  return req;
}

void free_request(Request *req) {
  if (!req)
    return;
  if (req->body) {
    slab_free(req->body);
    req->body = NULL;
  }
  req->body_len    = 0;
  req->param_count = 0;
}
