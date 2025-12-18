#ifndef RAMFORGE_ZERO_PAUSE_RESTORE_H
#define RAMFORGE_ZERO_PAUSE_RESTORE_H

#include "storage.h"
#include "shared_storage.h"
#include "request.h"
#include "response.h"

typedef struct {
    int active;
    int success;
    uint64_t entries_loaded;
    uint64_t restore_generation;
    char error_msg[256];
} ZeroPauseRestoreStatus;

// Function prototypes
int ZeroPauseRDB_restore(const char *rdb_path, Storage *target_storage,
                         SharedStorage *shared_storage);
int ZeroPauseRDB_restore_status(ZeroPauseRestoreStatus *status);
int ZeroPauseRDB_restore_wait(void);

// HTTP handlers
int zp_restore_handler(Request *req, Response *res);
int zp_restore_status_handler(Request *req, Response *res);
int zp_restore_sync_handler(Request *req, Response *res);


#endif //RAMFORGE_ZERO_PAUSE_RESTORE_H
