//####################
// /include/cluster/cluster_manager.h
// ####################

#ifndef CLUSTER_MANAGER_H
#define CLUSTER_MANAGER_H

#include "../common/types.h"
#include "../transport/tcp_transport.h"
#include "../protocol/protocol_defs.h"

typedef enum {
    CLUSTER_STATE_INIT,
    CLUSTER_STATE_JOINING,
    CLUSTER_STATE_ACTIVE,
    CLUSTER_STATE_LEAVING,
    CLUSTER_STATE_ISOLATED
} ClusterState;

typedef struct {
    NodeInfo self;
    ClusterState state;

    NodeInfo *members;
    size_t member_count;
    size_t member_capacity;

    char cluster_name[32];
    int heartbeat_interval_ms;
} ClusterManager;

int cluster_init(ClusterManager *cm, NodeRole role,
                 const char *node_id,
                 const char *bind_ip,
                 uint16_t port);

int cluster_join(ClusterManager *cm, const char *seed_ip, uint16_t seed_port);

// CHANGED: Decoupled network transport from logic. 
// Now returns payload buffer and length via pointers.
int cluster_handle_join_req(ClusterManager *cm,
                            const JoinRequestPayload *req,
                            uint8_t **out_payload,
                            size_t *out_len);

int cluster_update_member_status(ClusterManager *cm,
                                 const char *node_id,
                                 NodeStatus status);

void cluster_leave(ClusterManager *cm);

#endif