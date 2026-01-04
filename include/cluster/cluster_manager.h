#ifndef CLUSTER_MANAGER_H
#define CLUSTER_MANAGER_H

#include "../common/types.h"
#include "../transport/tcp_transport.h"

// Defines the operational state of the local node
typedef enum {
    CLUSTER_STATE_INIT,     // Node is initializing
    CLUSTER_STATE_JOINING,  // Handshaking with a seed node
    CLUSTER_STATE_ACTIVE,   // Fully participating in the cluster
    CLUSTER_STATE_LEAVING,  // Gracefully shutting down
    CLUSTER_STATE_ISOLATED  // Network partition detected
} ClusterState;

// The central context for Cluster Membership
typedef struct {
    // Identity
    NodeInfo self;          // My ID, IP, Port, Role (Worker/Control)
    ClusterState state;
    
    // Membership View (The "Roster")
    NodeInfo *members;      // Dynamic array of known peers
    size_t member_count;
    size_t member_capacity;
    
    // Configuration
    char cluster_name[32];  // To prevent accidental cross-cluster joins
    int heartbeat_interval_ms;

    // Leader Info (For Control Plane)
    char current_leader_id[37];
    uint64_t current_term;  // For Raft consensus
} ClusterManager;

/**
 * Initializes the cluster manager.
 * @param role: Are we a Worker or a Control Node?
 * @param bind_ip: The IP we are listening on.
 * @param port: The port we are listening on.
 */
int cluster_init(ClusterManager *cm, NodeRole role, const char *node_id, const char *bind_ip, uint16_t port);

/**
 * Sends a JOIN request to a known seed node.
 * This transitions the state to CLUSTER_STATE_JOINING.
 * @param seed_ip: IP of an existing cluster member.
 * @param seed_port: Port of the seed member.
 */
int cluster_join(ClusterManager *cm, const char *seed_ip, uint16_t seed_port);

/**
 * Marks a member as SUSPECT or DEAD based on missed heartbeats.
 * Returns 1 if the member list was updated, 0 otherwise.
 */
int cluster_update_member_status(ClusterManager *cm, const char *node_id, NodeStatus new_status);

// Handlers for incoming requests
int cluster_handle_join_req(ClusterManager *cm, const JoinRequestPayload *req, TcpConnection *client);

/**
 * Gracefully announces departure to peers and cleans up resources.
 */
void cluster_leave(ClusterManager *cm);

#endif // CLUSTER_MANAGER_H