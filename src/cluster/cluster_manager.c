/* src/cluster/cluster_manager.c */
#include "cluster/cluster_manager.h"
#include "protocol/rpc_protocol.h"
#include "protocol/serializer.h"
#include "common/logger.h"
#include <string.h>
#include <stdlib.h>

// Helper to create the payload
static void create_join_payload(ClusterManager *cm, JoinRequestPayload *payload) {
    memset(payload, 0, sizeof(JoinRequestPayload));
    strncpy(payload->node_id, cm->self.node_id, sizeof(payload->node_id) - 1);
    strncpy(payload->ip_address, cm->self.ip_address, sizeof(payload->ip_address) - 1);
    payload->port = cm->self.port;
    payload->role = (uint16_t)cm->self.role;
}

int cluster_join(ClusterManager *cm, const char *seed_ip, uint16_t seed_port) {
    if (cm->state != CLUSTER_STATE_INIT) {
        LOG_ERR("Cannot join: Node is not in INIT state");
        return -1;
    }

    LOG_INFO("Initiating Cluster Join to Seed %s:%d", seed_ip, seed_port);
    cm->state = CLUSTER_STATE_JOINING;

    // 1. Establish temporary TCP connection to seed
    TcpConnection conn;
    if (tcp_client_connect(seed_ip, seed_port, &conn) != 0) {
        LOG_ERR("Failed to connect to seed node");
        cm->state = CLUSTER_STATE_ISOLATED;
        return -1;
    }

    // 2. Prepare Join Request
    JoinRequestPayload payload;
    create_join_payload(cm, &payload);

    RpcHeader header = {
        .magic = RPC_MAGIC,
        .version = 1,
        .command_type = RPC_CMD_JOIN_REQ,
        .payload_len = sizeof(JoinRequestPayload)
    };
    strncpy(header.origin_id, cm->self.node_id, sizeof(header.origin_id));

    // 3. Serialize and Send
    uint8_t header_buf[sizeof(RpcHeader)];
    serializer_pack_rpc(&header, header_buf);

    if (tcp_send_all(&conn, header_buf, sizeof(header_buf)) != 0 ||
        tcp_send_all(&conn, &payload, sizeof(payload)) != 0) {
        LOG_ERR("Failed to send JOIN_REQ");
        tcp_close(&conn);
        return -1;
    }

    // 4. Wait for Response (Blocking for simplicity in startup phase)
    // In a pure reactor, this would be a separate state callback.
    RpcHeader resp_header;
    if (tcp_recv_all(&conn, header_buf, sizeof(header_buf)) != 0) {
        LOG_ERR("Failed to receive JOIN confirmation");
        tcp_close(&conn);
        return -1;
    }
    serializer_unpack_rpc(header_buf, &resp_header);

    if (resp_header.command_type == RPC_CMD_JOIN_RESP) {
        LOG_INFO("Joined Cluster Successfully!");
        cm->state = CLUSTER_STATE_ACTIVE;
        // Parse payload here to populate cm->members...
    } else {
        LOG_WARN("Received unexpected RPC response: %d", resp_header.command_type);
    }

    tcp_close(&conn);
    return 0;
}

void cluster_leave(ClusterManager *cm) {
    if (cm->state == CLUSTER_STATE_LEAVING || cm->state == CLUSTER_STATE_INIT) return;

    LOG_INFO("Leaving cluster...");
    cm->state = CLUSTER_STATE_LEAVING;

    // Broadcast leave message via Gossip (Best effort)
    GossipPacket leave_pkt = {
        .magic = GOSSIP_MAGIC,
        .type = 3, // SUSPECT/LEAVE type
        .sequence = (uint32_t)cm->current_term // or time
    };
    strncpy(leave_pkt.node_id, cm->self.node_id, sizeof(leave_pkt.node_id));
    
    // Ideally, we would flush this to the UDP socket here
    // But since this function is pure logic, we might just set the flag 
    // and let the next tick handle the transmission if the loop is still running.
}

int cluster_handle_join_req(ClusterManager *cm, const JoinRequestPayload *req, TcpConnection *client) {
    // ... logic provided in previous response ...
}

int cluster_handle_join_req(ClusterManager *cm, const JoinRequestPayload *req, TcpConnection *client) {
    LOG_INFO("Handling JOIN_REQ from %s (%s:%d)", req->node_id, req->ip_address, req->port);

    // 1. Add new node to our member list
    NodeInfo new_node;
    strncpy(new_node.node_id, req->node_id, 37);
    strncpy(new_node.ip_address, req->ip_address, 46);
    new_node.port = req->port;
    new_node.role = (NodeRole)req->role;
    new_node.status = NODE_STATUS_ALIVE;
    
    // (Logic to add to cm->members array omitted for brevity)
    
    // 2. Send Response
    RpcHeader resp = {
        .magic = RPC_MAGIC,
        .version = 1,
        .command_type = RPC_CMD_JOIN_RESP,
        .payload_len = 0 // For now, just ACK
    };
    
    uint8_t buf[sizeof(RpcHeader)];
    serializer_pack_rpc(&resp, buf);
    return tcp_send_all(client, buf, sizeof(buf));
}