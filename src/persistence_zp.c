// persistence_zp.c – zero-pause snapshot + AOF
#include "persistence_zp.h"
#include "aof_batch.h"
#include "log.h"
#include "metrics_shared.h"
#include <fcntl.h>
#include <stdatomic.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/file.h>
#include <unistd.h>

static Storage *g_st;
static int g_aof_enabled = 0;
static char *g_aof_path  = NULL;

static _Atomic int g_persistence_ready = 0;

int Persistence_is_ready(void) {
  return atomic_load(&g_persistence_ready) && AOF_sealed_is_healthy() &&
         atomic_load(&g_metrics.persistence_failures_total) == 0;
}

static int replay_sealed_in_this_worker(const char *aof_path) {
  size_t n = AOF_sealed_replay(aof_path);
  if (n == SIZE_MAX) {
    LOGE("PID %d detected unrecoverable sealed AOF corruption", getpid());
    atomic_fetch_add(&g_metrics.persistence_failures_total, 1);
    atomic_fetch_add(&g_metrics.write_failures, 1);
    return -1;
  }
  if (n > 0) {
    LOGI("PID %d replayed %zu sealed record(s)", getpid(), n);
  }
  /* IMPORTANT: do not truncate here. Multiple workers may still be starting.
     You can truncate later from a single elected process (worker 0 / control-plane). */
  return 0;
}

void Persistence_zp_init(const char *aof_path, /* "./append.aof"       */
                         Storage *st, unsigned aof_flush_ms) {
  g_st          = st;
  /* This initializer is called only when AOF mode is enabled. A zero interval
     is the strongest mode (fsync every write), not the disabled sentinel. */
  g_aof_enabled = 1;
  atomic_store(&g_persistence_ready, 0);

  if (g_aof_path) {
    free(g_aof_path);
  }
  g_aof_path = strdup(aof_path);

  /* Enhanced ring capacity for high-throughput */
  size_t ring_capacity = 1 << 17; /* 128K entries */

  AOF_init(aof_path, ring_capacity, aof_flush_ms);

  /* Load and replay AOF log */
  AOF_load(st);

  if (replay_sealed_in_this_worker(aof_path) != 0) return;

  AOF_live_follow_start();

  atomic_store(&g_persistence_ready, 1);
}

void Persistence_shutdown(void) {

  atomic_store(&g_persistence_ready, 0);

  LOGI("Shutting down persistence systems...");

  if (g_aof_enabled) {
    LOGI("Flushing pending AOF writes...");

    /* Wait for pending writes to complete */
    size_t pending = AOF_pending_writes();
    if (pending > 0) {
      LOGI("Waiting for %zu pending writes...", pending);

      /* Wait with timeout */
      for (int i = 0; i < 50 && AOF_pending_writes() > 0; i++) {
        usleep(100000); /* 100ms */
      }
    }

    /* Force sync critical data */
    AOF_sync();

    /* Shutdown AOF system */
    AOF_live_follow_stop();
    AOF_shutdown();
    LOGI("AOF shutdown complete");
  }

  /* Cleanup */
  if (g_aof_path) {
    free(g_aof_path);
    g_aof_path = NULL;
  }

  LOGI("Persistence shutdown complete");
}
void Persistence_compact(void) {
  LOGI("Starting persistence compaction...");

  if (!g_st) {
    LOGW("No storage instance available for compaction");
    return;
  }

  /* 1. Start non-blocking AOF rewrite */
  if (g_aof_enabled) {
    LOGI("Starting AOF rewrite...");
    AOF_rewrite(g_st); /* non-blocking inside */

    /* Optional: wait for rewrite to complete */
    while (AOF_rewrite_in_progress()) {
      LOGI("AOF rewrite in progress...");
      sleep(1);
    }

    LOGI("AOF rewrite completed");
  }

  LOGI("Persistence compaction complete");
}

void Persistence_segment_rewrite(void) {
  if (!g_aof_enabled || !g_aof_path) {
    LOGW("AOF not enabled for segment rewrite");
    return;
  }

  LOGI("Starting AOF segment rewrite...");

  if (AOF_segment_rewrite_in_progress()) {
    LOGW("Segment rewrite already in progress");
    return;
  }

  int result = AOF_begin_rewrite(g_aof_path);
  if (result == 0) {
    LOGI("AOF segment rewrite started");
  } else {
    LOGE("Failed to start AOF segment rewrite");
  }
}
