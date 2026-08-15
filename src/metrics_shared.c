#include "metrics_shared.h"
#include "log.h"
#include <stdlib.h>

/* real global instance, shared by everything that links this TU */
RAMForgeMetrics *g_metrics_ptr = NULL;

/* optional convenience wrapper so callers don’t have to know mmap details */
void init_shared_metrics(void) {
  if (g_metrics_ptr != NULL)
    return; // Already initialized

  if (!getenv("AYDER_METRICS_SHM_NAME")) {
    char unique_name[64];
    int n = snprintf(unique_name, sizeof(unique_name), "/ayder_metrics_%ld",
                     (long)getpid());
    if (n < 0 || (size_t)n >= sizeof(unique_name) ||
        setenv("AYDER_METRICS_SHM_NAME", unique_name, 1) != 0) {
      LOGE("Failed to create a unique shared-metrics instance name");
      exit(1);
    }
  }

  metrics_init_shared(); /* the inline helper in the header */

  // Verify it worked
  if (g_metrics_ptr == NULL || g_metrics_ptr == MAP_FAILED) {
    LOGE("Failed to initialize shared metrics for test");
    exit(1);
  }
}
