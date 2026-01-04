#include "../../include/proto/rpc_protocol.h"
#include "../../include/net/tcp_transport.h"
#include "../../include/proto/protocol_defs.h" 
#include <stdlib.h>
#include <stdio.h>

int rpc_send_message(TcpConnection *conn, RpcHeader *header, const void *payload) {
    if (tcp_send_all(conn, header, sizeof(RpcHeader)) != 0) return -1;

    // Fixed: payload_len is now uint64_t in the unified header
    if (header->payload_len > 0 && payload != NULL) {
        if (tcp_send_all(conn, payload, header->payload_len) != 0) return -1;
    }
    return 0;
}

int rpc_recv_message(TcpConnection *conn, RpcHeader *header, void **payload_out) {
    if (tcp_recv_all(conn, header, sizeof(RpcHeader)) != 0) return -1;

    if (header->magic != RPC_MAGIC) { // Use RPC_MAGIC from protocol_defs.h
        fprintf(stderr, "Invalid RPC Magic Number\n");
        return -2;
    }

    if (header->payload_len > 0) {
        *payload_out = malloc(header->payload_len);
        if (tcp_recv_all(conn, *payload_out, header->payload_len) != 0) {
            free(*payload_out);
            return -1;
        }
    } else {
        *payload_out = NULL;
    }

    return 0;
}

/**
 * Heuristic to decide the next hop.
 * Returns a pointer to the NodeInfo of the best candidate, or NULL to handle locally.
 */
NodeInfo* rpc_select_best_node(NodeInfo *members, int count, uint8_t cpu_threshold) {
    NodeInfo *best = NULL;
    uint8_t lowest_load = cpu_threshold;

    for (int i = 0; i < count; i++) {
        if (members[i].status == NODE_STATUS_ALIVE && members[i].cpu_load < lowest_load) {
            lowest_load = members[i].cpu_load;
            best = &members[i];
        }
    }
    return best;
}