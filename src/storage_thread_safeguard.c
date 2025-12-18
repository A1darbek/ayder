#include "storage_thread_safeguard.h"
pthread_rwlock_t g_storage_lock = PTHREAD_RWLOCK_INITIALIZER;
