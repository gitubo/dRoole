#include "cluster/cluster_manager.h"
#include "protocol/serializer.h"
#include "protocol/protocol_defs.h"
#include "common/logger.h"
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

int cluster_handle_join_req(ClusterManager *cm,
                            const JoinRequestPayload *req,
                            TcpConnection *client) {
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

    size_t payload_len =
        sizeof(JoinResponseHeader) +
        cm->member_count * sizeof(NodeInfo);

    JoinResponseHeader hdr = {
        .status = 0,
        .member_count = cm->member_count
    };

    uint8_t *payload = malloc(payload_len);
    memcpy(payload, &hdr, sizeof(hdr));
    memcpy(payload + sizeof(hdr),
           cm->members,
           cm->member_count * sizeof(NodeInfo));

    RpcHeader resp = {
        .magic = RPC_MAGIC,
        .version = 1,
        .command_type = RPC_CMD_JOIN_RESP,
        .payload_len = payload_len
    };

    uint8_t hbuf[sizeof(RpcHeader)];
    serializer_pack_rpc(&resp, hbuf);

    tcp_send_all(client, hbuf, sizeof(hbuf));
    tcp_send_all(client, payload, payload_len);
    free(payload);
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

    tcp_send_all(&conn, hbuf, sizeof(hbuf));
    tcp_send_all(&conn, &req, sizeof(req));

    serializer_unpack_rpc(hbuf, &hdr);
    tcp_recv_all(&conn, hbuf, sizeof(hbuf));
    serializer_unpack_rpc(hbuf, &hdr);

    uint8_t *payload = malloc(hdr.payload_len);
    tcp_recv_all(&conn, payload, hdr.payload_len);

    JoinResponseHeader *resp = (JoinResponseHeader *)payload;
    NodeInfo *nodes = (NodeInfo *)(payload + sizeof(*resp));

    for (uint32_t i = 0; i < resp->member_count; i++) {
        ensure_capacity(cm);
        cm->members[cm->member_count++] = nodes[i];
    }

    free(payload);
    tcp_close(&conn);
    cm->state = CLUSTER_STATE_ACTIVE;
    return 0;
}

void cluster_leave(ClusterManager *cm) {
    cm->state = CLUSTER_STATE_LEAVING;
}
