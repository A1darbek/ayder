#ifndef RAMFORGE_RAMFORGE_HA_INTEGRATION_H
#define RAMFORGE_RAMFORGE_HA_INTEGRATION_H

#include <stdint.h>
#include <stddef.h>
#include "ramforge_ha_config.h"

typedef struct {
    int      enabled;
    int      role;
    int      state;
    uint64_t term;
    int      leader_index;
    int      redirect_status;
    int      retry_after;
    uint64_t last_heartbeat_us;
    uint64_t config_epoch;
    int      joint_active;
    int      has_fresh_quorum;
    char     leader_url[128];
} ha_shared_snapshot_t;
_Static_assert(sizeof(ha_shared_snapshot_t) <= 256, "ha_shared_snapshot_t exceeds shared-storage payload budget");

int RAMForge_HA_read_snapshot(ha_shared_snapshot_t *out);
int RAMForge_HA_init(void);
void RAMForge_HA_shutdown(void);
void RAMForge_HA_export_metrics(char *buf, size_t cap);
int RAMForge_HA_is_enabled(void);
int RAMForge_HA_has_fresh_quorum(void);
int RAMForge_HA_is_leader(void);
const char* RAMForge_HA_get_leader_url(void);
int RAMForge_HA_get_redirect_status(void);
int RAMForge_HA_get_retry_after(void);
int RAMForge_HA_is_follower(void);
int RAMForge_HA_replicate_write(uint32_t record_type, const void *data, uint32_t size);
const char* RAMForge_HA_get_local_node_id(void);
const char* RAMForge_HA_get_leader_node_id(void);

/* Dynamic membership API */
int RAMForge_HA_get_membership(ha_membership_view_t *out);
int RAMForge_HA_add_member(const ha_node_config_t *node, int *out_index);
int RAMForge_HA_promote_member(const char *node_id);
int RAMForge_HA_remove_member(const char *node_id);

/* Agent->runtime forwarded membership op handler (worker0 only). */
int RAMForge_HA_handle_membership_forward(const void *payload, uint32_t size);

/* Replay dispatch hook for AOF sealed replay/live tail. Returns 1 if handled. */
int RAMForge_HA_replay_record(uint32_t record_type, const void *data, size_t size);

#endif // RAMFORGE_RAMFORGE_HA_INTEGRATION_H
