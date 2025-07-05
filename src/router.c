// router.c
#include "router.h"
#include <stdlib.h>
#include <string.h>
#include <stdio.h>
#include <stdbool.h>
#include "request.h"
#include "response.h"

#define MAX_METHOD_LEN 8
#define MAX_PATH_SEGMENTS 64

// Supported methods
static const char *methods[] = {
        "GET",
        "POST",
        "PUT",
        "DELETE",
        NULL
};

/// Trie node representing one path segment (static or parameter).
typedef struct TrieNode {
    char               *segment;     // static segment text (NULL if param)
    bool                is_param;    // true if this node matches ":param"
    char               *param_name;  // name of param (without ':')
    struct TrieNode    *children;    // first child (linked list)
    struct TrieNode    *sibling;     // next sibling in list
    RouteHandler        handler;     // non-NULL if a route ends here
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
    n->is_param   = is_param;
    n->handler    = NULL;
    n->children   = NULL;
    n->sibling    = NULL;
    if (is_param) {
        n->segment    = NULL;
        n->param_name = strdup(segment + 1);  // skip ':'
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
void register_route(const char *method,
                    const char *path,
                    RouteHandler handler)
{
    int mi = method_index(method);
    if (mi < 0) return;  // unsupported method

    // Duplicate path so strtok_r is safe
    char *p = strdup(path);
    char *saveptr = NULL, *seg = NULL;

    // parent_ptr always points to the pointer where we should insert/find
    TrieNode **parent_ptr = &method_roots[mi];
    TrieNode *node = NULL;

    seg = strtok_r(p, "/", &saveptr);
    while (seg) {
        bool is_param = (seg[0] == ':');
        TrieNode *child = *parent_ptr, *prev = NULL, *match = NULL;

        // Search siblings for matching static or param node
        while (child) {
            if (!child->is_param && !is_param
                && strcmp(child->segment, seg) == 0)
            {
                match = child;
                break;
            }
            if (child->is_param && is_param) {
                match = child;
                break;
            }
            prev  = child;
            child = child->sibling;
        }
        if (!match) {
            // Create new node and link it
            match = create_node(seg, is_param);
            if (prev) prev->sibling = match;
            else        *parent_ptr    = match;
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

/// Match an incoming request path and invoke the handler
int route_request(const char *method,
                   const char *path,
                   const char *body,
                   char       *response_buffer)
{
    int mi = method_index(method);
    if (mi < 0) {
        sprintf(response_buffer,
                "{\"error\":\"Unsupported method '%s'\"}",
                method);
        return -2;
    }

    // Split incoming path into segments
    char *segments[MAX_PATH_SEGMENTS];
    int   seg_count = 0;
    char *pcopy = strdup(path), *saveptr = NULL, *seg = NULL;
    seg = strtok_r(pcopy, "/", &saveptr);
    while (seg && seg_count < MAX_PATH_SEGMENTS) {
        segments[seg_count++] = seg;
        seg = strtok_r(NULL, "/", &saveptr);
    }

    // Prepare Request struct
    Request req = parse_request(body);
    req.param_count = 0;

    // Traverse trie
    TrieNode *child_list = method_roots[mi];
    TrieNode *node = NULL;
    for (int i = 0; i < seg_count; i++) {
        node = NULL;
        TrieNode *cur = child_list;
        TrieNode *param_match = NULL;

        // Try to match static first, then param
        while (cur) {
            if (!cur->is_param && strcmp(cur->segment, segments[i]) == 0) {
                node = cur;
                break;
            }
            if (cur->is_param) {
                param_match = cur;
            }
            cur = cur->sibling;
        }
        if (!node) {
            // fallback to parameter if available
            if (param_match) {
                node = param_match;
                // record param name/value
                strncpy(req.params[req.param_count].name,
                        node->param_name,
                        sizeof(req.params[0].name));
                strncpy(req.params[req.param_count].value,
                        segments[i],
                        sizeof(req.params[0].value));
                req.param_count++;
            } else {
                // no match
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
        strcpy(response_buffer, res.buffer);
        return rc;
    }
    free_request(&req);
    sprintf(response_buffer, "{\"error\":\"Not found\"}");
    return -1;
}
