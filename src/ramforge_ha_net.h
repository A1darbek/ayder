
#ifndef RAMFORGE_RAMFORGE_HA_NET_H
#define RAMFORGE_RAMFORGE_HA_NET_H

#pragma once
#include "ramforge_ha_config.h"

int  HA_net_start_server(ha_config_t *config, ha_runtime_t *runtime);
void HA_net_stop_server(void);

#endif //RAMFORGE_RAMFORGE_HA_NET_H
