#ifndef NODES_H
#define NODES_H

#include <stdint.h>

typedef enum {
    ROLE_WORKER,    // Data Plane: Executes tasks, follows Gossip
    ROLE_CONTROL    // Control Plane: Raft participant, manages state
} NodeRole;

typedef struct {
    char node_id[37];
    char ip_address[46];
    uint16_t port;
    NodeRole role;
    uint64_t last_seen; // Timestamp for failure detection
} RemoteNode;

#endif