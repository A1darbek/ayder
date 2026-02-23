#include "globals.h"
#include "app.h"

SharedStorage *g_shared_storage = NULL;
/// The one and only definition of g_app
App *g_app                      = NULL;

// Log configuration globals
int g_log_level                   = 6;  // LOG_INFO
int g_log_timestamp               = 0;  // disabled
__thread char g_log_worker_id[8]  = {0};
