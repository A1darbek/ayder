#ifndef RAMFORGE_PERSISTENCE_ZP_H
#define RAMFORGE_PERSISTENCE_ZP_H

#include "storage.h"

void Persistence_zp_init(const char *aof_path, /* "./append.aof"       */
                         Storage *st, unsigned aof_flush_ms);

void Persistence_compact(void);
int Persistence_is_ready(void);
/// Flush and stop the batch AOF thread (call on shutdown)
void Persistence_shutdown(void);

#endif // RAMFORGE_PERSISTENCE_ZP_H
