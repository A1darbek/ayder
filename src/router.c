// router.c
#include "router.h"
#include "request.h"
#include "response.h"
#include <stdbool.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#define MAX_METHOD_LEN 8
#define MAX_PATH_SEGMENTS 64

int route_request_len(const char *method, const char *path, const char *body, size_t body_len, char *response_buffer);

// Supported methods
static const char *methods[] = {"GET", "POST", "PUT", "DELETE", NULL};

/// Trie node representing one path segment (static or parameter).
typedef struct TrieNode {
  char *segment;             // static segment text (NULL if param)
  bool is_param;             // true if this node matches ":param"
  bool is_splat;             // SPLAT: ":param+" means greedy tail capture
  char *param_name;          // name of param (without ':' / '+')
  struct TrieNode *children; // first child (linked list)
  struct TrieNode *sibling;  // next sibling in list
  RouteHandler handler;      // non-NULL if a route ends here
} TrieNode;

/// One root per HTTP method
static TrieNode *method_roots[4];

/// Map method string to index in method_roots
static int method_index(const char *method) {
  for (int i = 0; methods[i]; i++) {
    if (strncmp(method, methods[i], MAX_METHOD_LEN) == 0) {
      return i;
    }
  }
  return -1;
}

/// Allocate a new trie node
static TrieNode *create_node(const char *segment, bool is_param) {
  TrieNode *n = malloc(sizeof(*n));
  n->is_param = is_param;
  n->is_splat = false;
  n->handler  = NULL;
  n->children = NULL;
  n->sibling  = NULL;
  if (is_param) {
    size_t len = strlen(segment);
    // SPLAT: ":param+" -> tail capture
    if (len >= 2 && segment[len - 1] == '+') {
      n->is_splat   = true;
      n->param_name = strndup(segment + 1, len - 2); // drop ':' and trailing '+'
    } else {
      n->param_name = strdup(segment + 1); // drop ':'
    }
    n->segment = NULL;
  } else {
    n->segment    = strdup(segment);
    n->param_name = NULL;
  }
  return n;
}

/// Initialize all method roots to NULL
void router_init(void) {
  for (int i = 0; i < 4; i++) {
    method_roots[i] = NULL;
  }
}

/// Register a route into the corresponding method trie
void register_route(const char *method, const char *path, RouteHandler handler) {
  int mi = method_index(method);
  if (mi < 0)
    return; // unsupported method

  // Duplicate path so strtok_r is safe
  char *p       = strdup(path);
  char *saveptr = NULL, *seg = NULL;

  // parent_ptr always points to the pointer where we should insert/find
  TrieNode **parent_ptr = &method_roots[mi];
  TrieNode *node        = NULL;

  seg = strtok_r(p, "/", &saveptr);
  while (seg) {
    bool is_param   = (seg[0] == ':');
    TrieNode *child = *parent_ptr, *prev = NULL, *match = NULL;

    // Search siblings for matching static or param node
    while (child) {
      if (!child->is_param && !is_param && strcmp(child->segment, seg) == 0) {
        match = child;
        break;
      }
      // both params (either plain or splat) are considered same “class” for merge
      if (child->is_param && is_param) {
        // prefer same splat-ness
        size_t len      = strlen(seg);
        bool want_splat = (len >= 2 && seg[len - 1] == '+');
        if (child->is_splat == want_splat) {
          match = child;
          break;
        }
      }
      prev  = child;
      child = child->sibling;
    }
    if (!match) {
      // Create new node and link it
      match = create_node(seg, is_param);
      if (prev)
        prev->sibling = match;
      else
        *parent_ptr = match;
    }
    // Descend into this node's children
    parent_ptr = &match->children;
    node       = match;
    seg        = strtok_r(NULL, "/", &saveptr);
  }
  free(p);

  // At final node, attach the handler
  if (node) {
    node->handler = handler;
  }
}

int route_request(const char *method, const char *path, const char *body, char *response_buffer) {
  size_t blen = body ? strnlen(body, 1u << 20) : 0; // cap to avoid UMR
  return route_request_len(method, path, body, blen, response_buffer);
}

// helper: join segments [i..j] into dst with '/' and clamp
static void join_segments(char *dst, size_t dst_cap, char *segments[], int i, int j) {
  size_t pos = 0;
  dst[0]     = '\0';
  for (int k = i; k <= j; k++) {
    size_t sl = strlen(segments[k]);
    if (pos && pos < dst_cap - 1)
      dst[pos++] = '/';
    if (pos + sl >= dst_cap) {
      size_t room = (dst_cap > pos) ? dst_cap - pos - 1 : 0;
      if (room) {
        memcpy(dst + pos, segments[k], room);
        pos += room;
      }
      break;
    }
    memcpy(dst + pos, segments[k], sl);
    pos += sl;
  }
  if (dst_cap)
    dst[(pos < dst_cap) ? pos : (dst_cap - 1)] = '\0';
}

/// Match an incoming request path and invoke the handler
int route_request_len(const char *method, const char *path, const char *body, size_t body_len, char *response_buffer) {
  int mi = method_index(method);
  if (mi < 0) {
    sprintf(response_buffer, "{\"error\":\"Unsupported method '%s'\"}", method);
    return -2;
  }

  // Prepare a modifiable copy of path and strip the query part for tokenizing
  const char *qs_in_path = strchr(path, '?');
  char *pcopy            = strdup(path);
  if (!pcopy) {
    sprintf(response_buffer, "{\"error\":\"OOM\"}");
    return -2;
  }
  char *qs_in_pcopy = strchr(pcopy, '?');
  if (qs_in_pcopy)
    *qs_in_pcopy = '\0';

  // Split incoming (query-stripped) path into segments
  char *segments[MAX_PATH_SEGMENTS];
  int seg_count = 0;
  char *saveptr = NULL, *seg = NULL;
  seg = strtok_r(pcopy, "/", &saveptr);
  while (seg && seg_count < MAX_PATH_SEGMENTS) {
    segments[seg_count++] = seg;
    seg                   = strtok_r(NULL, "/", &saveptr);
  }

  // Prepare Request struct
  Request req      = parse_request_len(body, body_len);
  req.param_count  = 0;
  req.query_string = qs_in_path ? (qs_in_path + 1) : NULL; // pointer into caller path

  // Traverse trie
  TrieNode *child_list = method_roots[mi];
  TrieNode *node       = NULL;
  for (int i = 0; i < seg_count; i++) {
    node                  = NULL;
    TrieNode *cur         = child_list;
    TrieNode *param_match = NULL;
    TrieNode *splat_match = NULL;

    // Try to match static first; remember param & splat
    while (cur) {
      if (!cur->is_param && strcmp(cur->segment, segments[i]) == 0) {
        node = cur; // exact static match
        break;
      }
      if (cur->is_param) {
        if (cur->is_splat)
          splat_match = cur;
        else
          param_match = cur;
      }
      cur = cur->sibling;
    }

    if (!node) {
      if (splat_match) {
        /* ── CASE A: splat at end → capture all remaining and finish ── */
        if (!splat_match->children) {
          node = splat_match;
          if (req.param_count < MAX_ROUTE_PARAMS) {
            strncpy(req.params[req.param_count].name, node->param_name, sizeof(req.params[0].name) - 1);
            req.params[req.param_count].name[sizeof(req.params[0].name) - 1] = '\0';
            join_segments(req.params[req.param_count].value, sizeof(req.params[0].value), segments, i, seg_count - 1);
            req.param_count++;
          }
          i          = seg_count; /* consume all */
          child_list = node->children;
          break; /* done */
        } else {
          /* ── CASE B: splat with a static suffix chain (e.g. /meta) ── */
          char *suffix[16];
          int depth   = 0;
          TrieNode *t = splat_match->children;
          while (t && !t->is_param && depth < 16) {
            suffix[depth++] = t->segment;
            t               = t->children;
          }

          if (depth > 0 && i + depth <= seg_count) {
            bool ok = true;
            for (int off = 0; off < depth; off++) {
              if (strcmp(segments[seg_count - depth + off], suffix[off]) != 0) {
                ok = false;
                break;
              }
            }
            if (ok) {
              /* capture i .. (seg_count - depth - 1) into splat param */
              node = splat_match;
              if (req.param_count < MAX_ROUTE_PARAMS) {
                strncpy(req.params[req.param_count].name, node->param_name, sizeof(req.params[0].name) - 1);
                req.params[req.param_count].name[sizeof(req.params[0].name) - 1] = '\0';
                join_segments(req.params[req.param_count].value, sizeof(req.params[0].value), segments, i,
                              seg_count - depth - 1);
                req.param_count++;
              }
              /* Continue matching at the FIRST suffix segment. We set i to
                 "just before" it so the for-loop's i++ lands on it. */
              child_list = node->children;
              i          = (seg_count - depth) - 1;
              continue;
            }
          }

          /* Fallback: suffix didn't match. If the splat node has its own handler
             (e.g. GET /kv/:ns/:key+), treat this as a greedy match. */
          if (splat_match->handler) {
            node = splat_match;
            if (req.param_count < MAX_ROUTE_PARAMS) {
              strncpy(req.params[req.param_count].name, node->param_name, sizeof(req.params[0].name) - 1);
              req.params[req.param_count].name[sizeof(req.params[0].name) - 1] = '\0';
              join_segments(req.params[req.param_count].value, sizeof(req.params[0].value), segments, i, seg_count - 1);
              req.param_count++;
            }
            i          = seg_count;
            child_list = node->children;
            break;
          }
        }
      }

      if (param_match) {
        node = param_match;
        if (req.param_count < MAX_ROUTE_PARAMS) {
          strncpy(req.params[req.param_count].name, node->param_name, sizeof(req.params[0].name) - 1);
          req.params[req.param_count].name[sizeof(req.params[0].name) - 1] = '\0';
          strncpy(req.params[req.param_count].value, segments[i], sizeof(req.params[0].value) - 1);
          req.params[req.param_count].value[sizeof(req.params[0].value) - 1] = '\0';
          req.param_count++;
        }
      } else {
        sprintf(response_buffer, "{\"error\":\"Not found\"}");
        free(pcopy);
        free_request(&req);
        return -1;
      }
    }

    // descend
    child_list = node->children;
  }
  free(pcopy);

  // If we found a node with a handler, call it
  if (node && node->handler) {
    Response res;
    memset(&res, 0, sizeof(res));
    int rc = node->handler(&req, &res);
    free_request(&req);
    size_t n = strnlen(res.buffer, RESPONSE_BUFFER_SIZE); // never scans past res.buffer
    memcpy(response_buffer, res.buffer, n);
    response_buffer[n] = '\0';
    return rc;
  }
  free_request(&req);
  sprintf(response_buffer, "{\"error\":\"Not found\"}");
  return -1;
}

static void free_trie(TrieNode *n) {
  while (n) {
    TrieNode *next = n->sibling;
    if (n->children)
      free_trie(n->children);
    free(n->segment);
    free(n->param_name);
    free(n);
    n = next;
  }
}

void router_shutdown(void) {
  for (int i = 0; i < 4; i++) {
    if (method_roots[i]) {
      free_trie(method_roots[i]);
      method_roots[i] = NULL;
    }
  }
}
