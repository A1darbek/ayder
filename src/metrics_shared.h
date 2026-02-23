#ifndef RAMFORGE_METRICS_SHARED_H
#define RAMFORGE_METRICS_SHARED_H

#pragma once
#include "ramforge_rotation_metrics.h" /* defines RAMForgeMetrics */
#include <errno.h>
#include <fcntl.h> /* shm_open */
#include <stdatomic.h>
#include <stdint.h>
#include <string.h>   /* memset */
#include <sys/mman.h> /* mmap  */
#include <sys/stat.h>
#include <unistd.h> /* getpagesize */
#include <unistd.h>
void init_shared_metrics(void);
extern RAMForgeMetrics *g_metrics_ptr;
#define g_metrics (*g_metrics_ptr) /* keeps old code unchanged */

#define RAMFORGE_METRICS_SHM "/ramforge_metrics"
static inline void metrics_init_shared(void) {
  size_t sz = (sizeof(RAMForgeMetrics) + getpagesize() - 1) & ~(getpagesize() - 1); /* page-align */

  int fd = shm_open(RAMFORGE_METRICS_SHM, O_CREAT | O_RDWR, 0666);
  if (fd < 0) {
    perror("shm_open(" RAMFORGE_METRICS_SHM ")");
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

  /* Zero only the very first time (detect by a sentinel field) */
  if (atomic_load(&g_metrics_ptr->total_operations) == 0) {
    memset(g_metrics_ptr, 0, sz);
  }
}

/* Optional helper for clean shutdown.
   Call it once when *all* processes are done (e.g. from a wrapper script). */
static inline void metrics_cleanup_shared(void) { shm_unlink(RAMFORGE_METRICS_SHM); }
#endif // RAMFORGE_METRICS_SHARED_H
