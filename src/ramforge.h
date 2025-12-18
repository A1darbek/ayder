#ifndef RAMFORGE_H
#define RAMFORGE_H

#include "app.h"

/// Create a new App using default "ramforge.aof"
App *RamForge_create(void);

/// Create a new App using a custom AOF path
App *RamForge_create_custom(const char *aof_file_path);

#endif // RAMFORGE_H