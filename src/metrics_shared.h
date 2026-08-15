#ifndef RAMFORGE_METRICS_SHARED_H
#define RAMFORGE_METRICS_SHARED_H

#pragma once
#include "ramforge_rotation_metrics.h" /* defines RAMForgeMetrics */
#include <errno.h>
#include <fcntl.h> /* shm_open */
#include <stdatomic.h>
#include <stdio.h>
#include <stdlib.h>
#include <stdint.h>
#include <string.h>   /* memset */
#include <sys/mman.h> /* mmap  */
#include <sys/stat.h>
#include <time.h>
#include <unistd.h> /* getpagesize */
#include <unistd.h>
void init_shared_metrics(void);
extern RAMForgeMetrics *g_metrics_ptr;
#define g_metrics (*g_metrics_ptr) /* keeps old code unchanged */

static inline const char *metrics_shared_name(void) {
  const char *name = getenv("AYDER_METRICS_SHM_NAME");
  if (name && name[0] == '/') return name;

  const char *node_id = getenv("RF_HA_NODE_ID");
  if (node_id && *node_id) {
    static char derived[96];
    size_t out = (size_t)snprintf(derived, sizeof(derived), "/ayder_metrics_");
    for (const unsigned char *p = (const unsigned char *)node_id;
         *p && out + 1 < sizeof(derived); ++p) {
      if ((*p >= 'a' && *p <= 'z') || (*p >= 'A' && *p <= 'Z') ||
          (*p >= '0' && *p <= '9') || *p == '_' || *p == '-') {
        derived[out++] = (char)*p;
      }
    }
    derived[out] = '\0';
    if (out > strlen("/ayder_metrics_")) return derived;
  }
  return "/ayder_metrics";
}

static inline void metrics_init_shared(void) {
  size_t sz = (sizeof(RAMForgeMetrics) + getpagesize() - 1) & ~(getpagesize() - 1); /* page-align */

  const char *shm_name = metrics_shared_name();
  int fd = shm_open(shm_name, O_CREAT | O_EXCL | O_RDWR, 0600);
  if (fd < 0) {
    perror("shm_open(metrics)");
    _exit(1);
  }

  /* Make sure the segment is large enough – harmless if it already is */
  if (ftruncate(fd, (off_t)sz) != 0) {
    perror("ftruncate(shared metrics)");
    _exit(1);
  }

  g_metrics_ptr = mmap(NULL, sz, PROT_READ | PROT_WRITE, MAP_SHARED, fd, 0);
  if (g_metrics_ptr == MAP_FAILED) {
    perror("mmap(shared metrics)");
    _exit(1);
  }
  close(fd);

  /* The parent initializes this mapping before forking workers. A fresh
     process must never inherit stale counters from a previous invocation. */
  memset(g_metrics_ptr, 0, sz);
  atomic_store(&g_metrics_ptr->process_start_time_unix, (uint64_t)time(NULL));
}

/* Optional helper for clean shutdown.
   Call it once when *all* processes are done (e.g. from a wrapper script). */
static inline void metrics_cleanup_shared(void) { shm_unlink(metrics_shared_name()); }
#endif // RAMFORGE_METRICS_SHARED_H
