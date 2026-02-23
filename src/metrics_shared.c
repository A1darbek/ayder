#include "metrics_shared.h"
#include "log.h"
#include <stdlib.h>

/* real global instance, shared by everything that links this TU */
RAMForgeMetrics *g_metrics_ptr = NULL;

/* optional convenience wrapper so callers don’t have to know mmap details */
void init_shared_metrics(void) {
  if (g_metrics_ptr != NULL)
    return; // Already initialized

  metrics_init_shared(); /* the inline helper in the header */

  // Verify it worked
  if (g_metrics_ptr == NULL || g_metrics_ptr == MAP_FAILED) {
    LOGE("Failed to initialize shared metrics for test");
    exit(1);
  }
}
