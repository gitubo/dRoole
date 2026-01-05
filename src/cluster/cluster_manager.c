//####################
// /src/cluster/cluster_manager.c
// ####################

#include "cluster/cluster_manager.h"
#include "protocol/serializer.h"
#include "protocol/protocol_defs.h"
#include "common/logger.h"
#include "transport/tcp_transport.h"
#include <stdlib.h>
#include <string.h>
#include <time.h>

static void ensure_capacity(ClusterManager *cm) {
    if (cm->member_count < cm->member_capacity) return;
    cm->member_capacity = cm->member_capacity ? cm->member_capacity * 2 : 8;
    cm->members = realloc(cm->members,
                           cm->member_capacity * sizeof(NodeInfo));
}

int cluster_init(ClusterManager *cm, NodeRole role,
                 const char *node_id,
                 const char *bind_ip,
                 uint16_t port) {
    memset(cm, 0, sizeof(*cm));
    strncpy(cm->self.node_id, node_id, 36);
    strncpy(cm->self.ip_address, bind_ip, 45);
    cm->self.tcp_port = port;
    cm->self.udp_port = port;
    cm->self.role = role;
    cm->self.status = NODE_STATUS_ALIVE;
    cm->self.last_updated_ts = time(NULL);
    cm->state = CLUSTER_STATE_INIT;
    return 0;
}

int cluster_update_member_status(ClusterManager *cm,
                                 const char *node_id,
                                 NodeStatus status) {
    for (size_t i = 0; i < cm->member_count; i++) {
        if (strcmp(cm->members[i].node_id, node_id) == 0) {
            cm->members[i].status = status;
            cm->members[i].last_updated_ts = time(NULL);
            return 1;
        }
    }
    return 0;
}

// CHANGED: Pure logic, no socket I/O
int cluster_handle_join_req(ClusterManager *cm,
                            const JoinRequestPayload *req,
                            uint8_t **out_payload,
                            size_t *out_len) {
    ensure_capacity(cm);
    NodeInfo *n = &cm->members[cm->member_count++];
    memset(n, 0, sizeof(*n));
    strncpy(n->node_id, req->node_id, 36);
    strncpy(n->ip_address, req->ip_address, 45);
    n->tcp_port = req->tcp_port;
    n->udp_port = req->udp_port;
    n->role = req->role;
    n->status = NODE_STATUS_ALIVE;
    n->last_updated_ts = time(NULL);

    *out_len = sizeof(JoinResponseHeader) +
               cm->member_count * sizeof(NodeInfo);
    
    JoinResponseHeader hdr = {
        .status = 0,
        .member_count = cm->member_count
    };

    *out_payload = malloc(*out_len);
    if (!*out_payload) return -1;

    memcpy(*out_payload, &hdr, sizeof(hdr));
    memcpy(*out_payload + sizeof(hdr),
           cm->members,
           cm->member_count * sizeof(NodeInfo));
           
    return 0;
}

int cluster_join(ClusterManager *cm,
                 const char *seed_ip,
                 uint16_t seed_port) {
    TcpConnection conn;
    if (tcp_client_connect(seed_ip, seed_port, &conn) != 0)
        return -1;

    JoinRequestPayload req = {0};
    strncpy(req.node_id, cm->self.node_id, 36);
    strncpy(req.ip_address, cm->self.ip_address, 45);
    req.tcp_port = cm->self.tcp_port;
    req.udp_port = cm->self.udp_port;
    req.role = cm->self.role;

    RpcHeader hdr = {
        .magic = RPC_MAGIC,
        .version = 1,
        .command_type = RPC_CMD_JOIN_REQ,
        .payload_len = sizeof(req)
    };
    
    uint8_t hbuf[sizeof(hdr)];
    serializer_pack_rpc(&hdr, hbuf);

    // This is synchronous client code, so blocking send is fine here
    tcp_send_all(&conn, hbuf, sizeof(hbuf));
    tcp_send_all(&conn, &req, sizeof(req));

    serializer_unpack_rpc(hbuf, &hdr);
    tcp_recv_all(&conn, hbuf, sizeof(hbuf));
    serializer_unpack_rpc(hbuf, &hdr);

    if (hdr.payload_len > 0) {
        uint8_t *payload = malloc(hdr.payload_len);
        tcp_recv_all(&conn, payload, hdr.payload_len);

        JoinResponseHeader *resp = (JoinResponseHeader *)payload;
        NodeInfo *nodes = (NodeInfo *)(payload + sizeof(*resp));

        for (uint32_t i = 0; i < resp->member_count; i++) {
            ensure_capacity(cm);
            cm->members[cm->member_count++] = nodes[i];
        }
        free(payload);
    }

    ensure_capacity(cm);
    NodeInfo *seed = &cm->members[cm->member_count++];
    memset(seed, 0, sizeof(NodeInfo));
    strncpy(seed->node_id, "seed-node", 36); // Ideally this comes from handshake, but hardcode for now
    strncpy(seed->ip_address, seed_ip, 45);
    seed->tcp_port = seed_port;
    seed->udp_port = seed_port; 
    seed->status = NODE_STATUS_ALIVE;
    seed->last_updated_ts = time(NULL);

    tcp_close(&conn);
    cm->state = CLUSTER_STATE_ACTIVE;
    LOG_INFO("Joined Cluster Successfully via %s:%d", seed_ip, seed_port);
    return 0;
}

void cluster_leave(ClusterManager *cm) {
    cm->state = CLUSTER_STATE_LEAVING;
    
    RpcHeader hdr = {
        .magic = RPC_MAGIC,
        .version = 1,
        .command_type = RPC_CMD_LEAVE,
        .payload_len = 0
    };
    strncpy(hdr.origin_id, cm->self.node_id, 36);
    memset(hdr.request_id, 0, sizeof(hdr.request_id));
    memcpy(hdr.request_id, "LEAVE-MSG", 9);

    uint8_t hbuf[sizeof(RpcHeader)];
    serializer_pack_rpc(&hdr, hbuf);

    int sent_count = 0;
    for (size_t i = 0; i < cm->member_count && sent_count < 3; i++) {
        if (cm->members[i].status == NODE_STATUS_ALIVE) {
            TcpConnection conn;
            if (tcp_client_connect(cm->members[i].ip_address, 
                                   cm->members[i].tcp_port, &conn) == 0) {
                tcp_send_all(&conn, hbuf, sizeof(hbuf));
                tcp_close(&conn);
                sent_count++;
            }
        }
    }
    LOG_INFO("Cluster leave notification sent to %d peers", sent_count);
}