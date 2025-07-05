/* log.h  ─────────────────────────────────────────────────────────── */
#pragma once
#include <stdio.h>

#ifndef LOG_DEFAULT_LEVEL
#   ifdef DEBUG                 /* test / chaos builds */
#       define LOG_DEFAULT_LEVEL 1   /* DEBUG */
#   else                        /* prod / normal make */
#       define LOG_DEFAULT_LEVEL 2   /* INFO  */
#   endif
#endif

enum { LOG_DEBUG = 1, LOG_INFO = 2, LOG_WARN = 3, LOG_ERR = 4 };

static int g_log_level = LOG_DEFAULT_LEVEL;

#define LOG(level, fmt, ...)                                  \
    do { if ((level) >= g_log_level)                          \
            fprintf(stderr, fmt, ##__VA_ARGS__); } while (0)

#define LOGD(fmt, ...) LOG(LOG_DEBUG, fmt, ##__VA_ARGS__)
#define LOGI(fmt, ...) LOG(LOG_INFO,  fmt, ##__VA_ARGS__)
#define LOGW(fmt, ...) LOG(LOG_WARN,  fmt, ##__VA_ARGS__)
#define LOGE(fmt, ...) LOG(LOG_ERR,   fmt, ##__VA_ARGS__)
