#pragma once
#include <stdint.h>

#if defined(__STDC_VERSION__) && (__STDC_VERSION__ >= 201112L)
#define RF_TLS _Thread_local
#else
#define RF_TLS __thread
#endif

/* Filled by http_server.c right before calling the router */
extern RF_TLS uint64_t rf_http_recv_parse_us;  /* first_byte -> msg_complete */
extern RF_TLS uint64_t rf_http_queue_us;       /* msg_complete -> handler start */