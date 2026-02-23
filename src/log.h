/* log.h  ─────────────────────────────────────────────────────────── */
#pragma once
#include <errno.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <unistd.h>

enum {
  LOG_EMERGENCY = 0,
  LOG_ALERT     = 1,
  LOG_CRITICAL  = 2,
  LOG_ERROR     = 3,
  LOG_WARNING   = 4,
  LOG_NOTICE    = 5,
  LOG_INFO      = 6,
  LOG_DEBUG     = 7
};

extern int g_log_level;                             // Default log level, can be overridden by environment variable
extern int g_log_timestamp;                         // Runtime timestamp control, defaults to disabled
extern __thread char g_log_worker_id[8];            // Per-thread worker ID

/* Initialize worker ID for logging (call once per worker) */
static inline void log_set_worker_id(int worker_id) {
  snprintf(g_log_worker_id, sizeof(g_log_worker_id), "W%d", worker_id);
}

/* Initialize log level from environment */
static inline void log_init_from_env(void) {
  const char *log_level_env = getenv("LOG_LEVEL");
  if (log_level_env && *log_level_env) {
    /* Support string levels (case-insensitive) */
    if (strcasecmp(log_level_env, "EMERG") == 0 || strcasecmp(log_level_env, "EMERGENCY") == 0) {
      g_log_level = LOG_EMERGENCY;
    } else if (strcasecmp(log_level_env, "ALERT") == 0) {
      g_log_level = LOG_ALERT;
    } else if (strcasecmp(log_level_env, "CRIT") == 0 || strcasecmp(log_level_env, "CRITICAL") == 0) {
      g_log_level = LOG_CRITICAL;
    } else if (strcasecmp(log_level_env, "ERROR") == 0 || strcasecmp(log_level_env, "ERR") == 0) {
      g_log_level = LOG_ERROR;
    } else if (strcasecmp(log_level_env, "WARN") == 0 || strcasecmp(log_level_env, "WARNING") == 0) {
      g_log_level = LOG_WARNING;
    } else if (strcasecmp(log_level_env, "NOTICE") == 0) {
      g_log_level = LOG_NOTICE;
    } else if (strcasecmp(log_level_env, "INFO") == 0) {
      g_log_level = LOG_INFO;
    } else if (strcasecmp(log_level_env, "DEBUG") == 0 || strcasecmp(log_level_env, "DBG") == 0) {
      g_log_level = LOG_DEBUG;
    } else {
      g_log_level = LOG_INFO;
    }
  }

  /* Check if timestamps should be enabled */
  const char *timestamp_env = getenv("LOG_TIMESTAMP");
  if (timestamp_env && *timestamp_env) {
    if (strcasecmp(timestamp_env, "true") == 0 || strcasecmp(timestamp_env, "1") == 0 ||
        strcasecmp(timestamp_env, "yes") == 0 || strcasecmp(timestamp_env, "on") == 0) {
      g_log_timestamp = 1;
    }
  }
}

#define LOG(level, fmt, ...)                                                                                           \
  do {                                                                                                                 \
    if ((level) <= g_log_level) {                                                                                      \
      const char *level_str = (level == LOG_EMERGENCY)  ? "EME"                                                        \
                              : (level == LOG_ALERT)    ? "ALE"                                                        \
                              : (level == LOG_CRITICAL) ? "CRI"                                                        \
                              : (level == LOG_ERROR)    ? "ERR"                                                        \
                              : (level == LOG_WARNING)  ? "WRN"                                                        \
                              : (level == LOG_NOTICE)   ? "NOT"                                                        \
                              : (level == LOG_INFO)     ? "INF"                                                        \
                                                        : "DBG";                                                           \
      if (g_log_timestamp) {                                                                                           \
        struct timespec tspec;                                                                                            \
        clock_gettime(CLOCK_REALTIME_COARSE, &tspec);                                                                     \
        struct tm tm;                                                                                                  \
        localtime_r(&tspec.tv_sec, &tm);                                                                                  \
        fprintf(stderr, "%04d-%02d-%02d %02d:%02d:%02d.%03ld\t<%s>\t%s%s " fmt "\n", tm.tm_year + 1900, tm.tm_mon + 1, \
                tm.tm_mday, tm.tm_hour, tm.tm_min, tm.tm_sec, tspec.tv_nsec / 1000000, level_str,                         \
                g_log_worker_id[0] ? " " : "", g_log_worker_id[0] ? g_log_worker_id : "", ##__VA_ARGS__);              \
      } else {                                                                                                         \
        fprintf(stderr, "<%s>\t%s%s " fmt "\n", level_str, g_log_worker_id[0] ? " " : "",                              \
                g_log_worker_id[0] ? g_log_worker_id : "", ##__VA_ARGS__);                                             \
      }                                                                                                                \
    }                                                                                                                  \
  } while (0)

#define LOGD(fmt, ...) LOG(LOG_DEBUG, fmt, ##__VA_ARGS__)
#define LOGI(fmt, ...) LOG(LOG_INFO, fmt, ##__VA_ARGS__)
#define LOGN(fmt, ...) LOG(LOG_NOTICE, fmt, ##__VA_ARGS__)
#define LOGW(fmt, ...) LOG(LOG_WARNING, fmt, ##__VA_ARGS__)
#define LOGE(fmt, ...) LOG(LOG_ERROR, fmt, ##__VA_ARGS__)

/* For system errors (replaces perror) */
#define LOGE_ERRNO(msg) LOGE("%s: %s", msg, strerror(errno))
