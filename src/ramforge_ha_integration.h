#ifndef RAMFORGE_RAMFORGE_HA_INTEGRATION_H
#define RAMFORGE_RAMFORGE_HA_INTEGRATION_H
#include <stdint.h>

typedef struct {
    int      enabled;               // 1 if HA is on in this node
    int      role;                  // HA_ROLE_LEADER / HA_ROLE_FOLLOWER / ...
    int      state;                 // HA_STATE_*
    uint64_t term;
    int      leader_index;          // index in ha_config_t->nodes[]
    int      redirect_status;       // 307/308
    int      retry_after;           // seconds
    uint64_t last_heartbeat_us;
    char     leader_url[1024];      // http://host:port (data plane)
} ha_shared_snapshot_t;

/* Returns 1 if a snapshot is present in shared_storage (even if this
 * process didn't call RAMForge_HA_init). Returns 0 if none. */
int RAMForge_HA_read_snapshot(ha_shared_snapshot_t *out);
int RAMForge_HA_init(void);
void RAMForge_HA_shutdown(void);
void RAMForge_HA_export_metrics(char *buf, size_t cap);
int RAMForge_HA_is_enabled(void);
int RAMForge_HA_is_leader(void);
const char* RAMForge_HA_get_leader_url(void);
int RAMForge_HA_get_redirect_status(void);
int RAMForge_HA_get_retry_after(void);
int RAMForge_HA_is_follower(void);
int RAMForge_HA_replicate_write(uint32_t record_type, const void *data, uint32_t size);
// Get local node ID
const char* RAMForge_HA_get_local_node_id(void);

// Get current leader node ID
const char* RAMForge_HA_get_leader_node_id(void);

#endif //RAMFORGE_RAMFORGE_HA_INTEGRATION_H
