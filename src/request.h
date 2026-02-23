#ifndef REQUEST_H
#define REQUEST_H

#include <stddef.h>
#include <string.h>

#define MAX_ROUTE_PARAMS 10
#define MAX_PARAM_LEN 64

typedef struct {
  char name[MAX_PARAM_LEN];
  char value[MAX_PARAM_LEN];
} RequestParam;

typedef struct {
  int param_count;
  RequestParam params[MAX_ROUTE_PARAMS];
  char *body;
  size_t body_len;
  const char *query_string;
} Request;

// Parse the raw body into a Request
Request parse_request_len(const char *body, size_t body_len);

// Free any allocated memory inside Request
void free_request(Request *req);

#endif // REQUEST_H
