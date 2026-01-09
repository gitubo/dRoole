//####################
// /src/cluster/cluster_manager.c - WITH LOGGING
// ####################

#include "cluster/cluster_manager.h"
#include "protocol/serializer.h"
#include "protocol/protocol_defs.h"
#include "common/logger.h"
#include "common/utils.h"
#include "transport/tcp_transport.h"
#include "common/utils.h"
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <arpa/inet.h>
#include <endian.h>
#include <time.h>


static void ensure_capacity(ClusterManager *cm) {
    if (cm->member_count < cm->member_capacity) return;
    
    size_t new_capacity = cm->member_capacity ? cm->member_capacity * 2 : 8;
    NodeInfo *new_members = realloc(cm->members, new_capacity * sizeof(NodeInfo));
    
    if (!new_members) {
        LOG_ERR("Failed to reallocate member array to %zu entries", new_capacity);
        return; // Keep old capacity, will fail later
    }
    
    cm->members = new_members;
    cm->member_capacity = new_capacity;
    LOG_DEBUG("Expanded member capacity to %zu", new_capacity);
}

int cluster_init(ClusterManager *cm, NodeRole role,
                 const char *node_id, const char *bind_ip, uint16_t port) {
    memset(cm, 0, sizeof(*cm));
    strncpy(cm->self.node_id, node_id, 36);
    cm->self.node_id[36] = '\0';
    strncpy(cm->self.ip_address, bind_ip, 45);
    cm->self.ip_address[45] = '\0';
    cm->self.tcp_port = port;
    cm->self.udp_port = port;
    cm->self.role = role;
    cm->self.status = NODE_STATUS_ALIVE;
    
    cm->self.last_updated_ts = get_monotonic_time_ms();
    
    cm->state = CLUSTER_STATE_INIT;
    LOG_INFO("Cluster initialized: node_id=%s, role=%s, port=%d",
             node_id, role == ROLE_CONTROL_NODE ? "CONTROL" : "WORKER", port);
    return 0;
}

int cluster_update_member_status(ClusterManager *cm, const char *node_id, NodeStatus status) {
    for (size_t i = 0; i < cm->member_count; i++) {
        if (strcmp(cm->members[i].node_id, node_id) == 0) {
            NodeStatus old_status = cm->members[i].status;
            cm->members[i].status = status;
            
            // ARCHITECTURAL CHANGE: Record arrival in milliseconds
            cm->members[i].last_updated_ts = get_monotonic_time_ms();
            
            if (old_status != status) {
                const char *status_str[] = {"ALIVE", "SUSPECT", "DEAD"};
                LOG_INFO("Member %s status transition: %s -> %s", 
                         node_id, status_str[old_status], status_str[status]);
            }
            return 1;
        }
    }
    return 0;
}

int cluster_handle_join_req(ClusterManager *cm,
                            const JoinRequestPayload *req,
                            uint8_t **out_payload,
                            size_t *out_len) {

    uint16_t req_tcp_port = ntohs(req->tcp_port);
    uint16_t req_udp_port = ntohs(req->udp_port);
    uint16_t req_role = ntohs(req->role);

    LOG_DEBUG("Handling JOIN_REQ: node_id=%s, ip=%s, tcp_port=%d, udp_port=%d, role=%d",
              req->node_id, req->ip_address, req_tcp_port, req_udp_port, req_role);

    // ✅ ADD: Check for null pointers
    if (!cm || !req || !out_payload || !out_len) {
        LOG_ERR("NULL pointer in cluster_handle_join_req");
        return -1;
    }
    
    LOG_DEBUG("Checking %zu existing members", cm->member_count);

    // 1. Check if node already exists or needs update
    for (size_t i = 0; i < cm->member_count; i++) {
        LOG_DEBUG("  Comparing with member %zu: %s", i, cm->members[i].node_id);
        if (strcmp(cm->members[i].node_id, req->node_id) == 0) {
            LOG_WARN("Node %s already exists in cluster, updating info", req->node_id);
            // Update existing node
            strncpy(cm->members[i].ip_address, req->ip_address, 45);
            cm->members[i].tcp_port = req_tcp_port;
            cm->members[i].udp_port = req_udp_port;
            cm->members[i].role = req_role; 
            
            cm->members[i].status = NODE_STATUS_ALIVE;
            cm->members[i].last_updated_ts = time(NULL);
            goto build_response;
        }
    }
    
    LOG_DEBUG("Node not found, adding as new member");
    
    // 2. Add new node if capacity allows
    LOG_DEBUG("Current capacity: count=%zu, capacity=%zu", cm->member_count, cm->member_capacity);
    ensure_capacity(cm);
    LOG_DEBUG("After ensure_capacity: count=%zu, capacity=%zu", cm->member_count, cm->member_capacity);
    
    if (cm->member_count >= cm->member_capacity) {
        LOG_ERR("Failed to expand member list, cannot add node %s", req->node_id);
        return -1;
    }
    
    LOG_DEBUG("Allocating member slot %zu", cm->member_count);
    NodeInfo *n = &cm->members[cm->member_count++];
    memset(n, 0, sizeof(*n));
    strncpy(n->node_id, req->node_id, 36);
    n->node_id[36] = '\0';
    strncpy(n->ip_address, req->ip_address, 45);
    n->ip_address[45] = '\0';
    n->tcp_port = req_tcp_port;
    n->udp_port = req_udp_port;
    n->role = req_role;
    n->status = NODE_STATUS_ALIVE;
    n->last_updated_ts = time(NULL);
    
    LOG_INFO("Added new member: %s (total members: %zu)", req->node_id, cm->member_count);

build_response:
    LOG_DEBUG("Building response, member_count=%zu", cm->member_count);
    
    // 3. Construct Response
    *out_len = sizeof(JoinResponseHeader) + cm->member_count * sizeof(NodeInfo);
    
    JoinResponseHeader hdr = {
        .status = 0,
        .member_count = htonl(cm->member_count)
    };

    strncpy(hdr.responder_id, cm->self.node_id, sizeof(hdr.responder_id) - 1);
    hdr.responder_id[sizeof(hdr.responder_id) - 1] = '\0';

    LOG_DEBUG("Allocating %zu bytes for response", *out_len);
    *out_payload = malloc(*out_len);
    if (!*out_payload) {
        LOG_ERR("Failed to allocate %zu bytes for JOIN_RESP", *out_len);
        return -1;
    }

    // Copy Header
    memcpy(*out_payload, &hdr, sizeof(hdr));
    
    LOG_DEBUG("Serializing %zu members", cm->member_count);
    
    // Copy Member List
    uint8_t *member_ptr = *out_payload + sizeof(hdr);
    for (size_t i = 0; i < cm->member_count; i++) {
        NodeInfo *src = &cm->members[i];
        NodeInfo *dst = (NodeInfo *)(member_ptr + i * sizeof(NodeInfo));
        
        memcpy(dst->node_id, src->node_id, sizeof(dst->node_id));
        memcpy(dst->ip_address, src->ip_address, sizeof(dst->ip_address));
        dst->tcp_port = htons(src->tcp_port);
        dst->udp_port = htons(src->udp_port);
        dst->role = htons(src->role);
        dst->status = htons(src->status);
        dst->cpu_load = src->cpu_load;
        dst->mem_load = src->mem_load;
        dst->last_updated_ts = htobe64(src->last_updated_ts);
    }
    
    LOG_DEBUG("Built JOIN_RESP: %zu bytes, %zu members, responder=%s", 
              *out_len, cm->member_count, hdr.responder_id);
    return 0;
}

/**
 * ARCHITECTURAL FIX: cluster_join
 * 1. Establishes TCP handshake with Seed.
 * 2. Receives explicit Seed identity (e.g., "seed-01") via responder_id.
 * 3. Sanitizes all member timestamps using local CLOCK_MONOTONIC to 
 * ignore remote clock skew/lack of NTP.
 */
int cluster_join(ClusterManager *cm, const char *seed_ip, uint16_t seed_port) {
    LOG_INFO("Attempting to join cluster via seed %s:%d", seed_ip, seed_port);
    
    TcpConnection conn;
    if (tcp_client_connect(seed_ip, seed_port, &conn) != 0) {
        LOG_ERR("Failed to connect to seed %s:%d", seed_ip, seed_port);
        return -1;
    }
    
    LOG_DEBUG("Connected to seed, sending JOIN_REQ");

    // 1. Prepare Request Payload
    JoinRequestPayload req = {0};
    strncpy(req.node_id, cm->self.node_id, 36);
    strncpy(req.ip_address, cm->self.ip_address, 45);
    req.tcp_port = htons(cm->self.tcp_port);
    req.udp_port = htons(cm->self.udp_port);
    req.role = htons(cm->self.role);

    // 2. Prepare RPC Header
    RpcHeader hdr = {
        .magic = RPC_MAGIC,
        .version = 1,
        .command_type = RPC_CMD_JOIN_REQ,
        .payload_len = sizeof(req)
    };
    strncpy(hdr.origin_id, cm->self.node_id, 36);
    
    uint8_t hbuf[sizeof(hdr)];
    serializer_pack_rpc(&hdr, hbuf);

    // 3. Send Request
    if (tcp_send_all(&conn, hbuf, sizeof(hbuf)) != 0) {
        LOG_ERR("Failed to send JOIN_REQ header");
        tcp_close(&conn);
        return -1;
    }
    
    if (tcp_send_all(&conn, &req, sizeof(req)) != 0) {
        LOG_ERR("Failed to send JOIN_REQ payload");
        tcp_close(&conn);
        return -1;
    }
    
    // 4. Receive Response Header
    if (tcp_recv_all(&conn, hbuf, sizeof(hbuf)) != 0) {
        LOG_ERR("Failed to receive JOIN_RESP header");
        tcp_close(&conn);
        return -1;
    }
    
    serializer_unpack_rpc(hbuf, &hdr);
    if (hdr.magic != RPC_MAGIC || hdr.command_type != RPC_CMD_JOIN_RESP) {
        LOG_ERR("Invalid response from seed: magic=0x%x, cmd=%d", hdr.magic, hdr.command_type);
        tcp_close(&conn);
        return -1;
    }
    
    // 5. Receive and Process Response Payload
    if (hdr.payload_len > 0) {
        uint8_t *payload = malloc(hdr.payload_len);
        if (!payload) {
            LOG_ERR("Out of memory for JOIN_RESP payload");
            tcp_close(&conn);
            return -1;
        }
        
        if (tcp_recv_all(&conn, payload, hdr.payload_len) != 0) {
            LOG_ERR("Failed to receive JOIN_RESP payload");
            free(payload);
            tcp_close(&conn);
            return -1;
        }

        JoinResponseHeader *resp = (JoinResponseHeader *)payload;
        uint32_t member_count = ntohl(resp->member_count);
        
        // [FIX] Capture the Seed's real identity from the header
        char seed_real_id[37];
        strncpy(seed_real_id, resp->responder_id, 36);
        seed_real_id[36] = '\0';
        
        LOG_DEBUG("JOIN_RESP received. Member count: %u, Seed ID: %s", member_count, seed_real_id);

        NodeInfo *nodes = (NodeInfo *)(payload + sizeof(JoinResponseHeader));

        // Get current local monotonic time once for all new members
        uint64_t now_monotonic = get_monotonic_time_ms();

        // 6. Import Members List
        for (uint32_t i = 0; i < member_count; i++) {
            NodeInfo *src = &nodes[i];
            if (strcmp(src->node_id, cm->self.node_id) == 0) continue;
            
            ensure_capacity(cm);
            if (cm->member_count >= cm->member_capacity) break;
            
            NodeInfo *dst = &cm->members[cm->member_count++];
            
            // Map data
            memcpy(dst->node_id, src->node_id, sizeof(dst->node_id));
            memcpy(dst->ip_address, src->ip_address, sizeof(dst->ip_address));
            dst->tcp_port = ntohs(src->tcp_port);
            dst->udp_port = ntohs(src->udp_port);
            dst->role = ntohs(src->role);
            dst->status = ntohs(src->status);
            
            // ARCHITECTURAL FIX: Timestamp Sanitization
            // We ignore src->last_updated_ts (remote time) and use local monotonic time.
            dst->last_updated_ts = now_monotonic;
            
            LOG_DEBUG("  Imported member: %s (Status: %d)", dst->node_id, dst->status);
        }
        
        // 7. Explicitly ensure the Seed is in the list with its real ID
        int seed_found = 0;
        for (size_t i = 0; i < cm->member_count; i++) {
            if (strcmp(cm->members[i].node_id, seed_real_id) == 0) {
                seed_found = 1;
                break;
            }
        }
        
        if (!seed_found) {
            ensure_capacity(cm);
            if (cm->member_count < cm->member_capacity) {
                NodeInfo *seed = &cm->members[cm->member_count++];
                memset(seed, 0, sizeof(NodeInfo));
                
                // Use the confirmed ID from the handshake
                strncpy(seed->node_id, seed_real_id, 36);
                strncpy(seed->ip_address, seed_ip, 45);
                seed->tcp_port = seed_port;
                seed->udp_port = seed_port; 
                seed->status = NODE_STATUS_ALIVE;
                seed->last_updated_ts = now_monotonic;
                
                LOG_INFO("Registered seed node as [%s]", seed->node_id);
            }
        }

        free(payload);
    }

    tcp_close(&conn);
    cm->state = CLUSTER_STATE_ACTIVE;
    LOG_INFO("Successfully joined cluster via %s. Known members: %zu", seed_ip, cm->member_count);
    return 0;
}

void cluster_leave(ClusterManager *cm) {
    LOG_INFO("Initiating graceful cluster leave");
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
    int attempt_count = 0;
    
    for (size_t i = 0; i < cm->member_count && sent_count < 3; i++) {
        if (strcmp(cm->members[i].node_id, cm->self.node_id) == 0) {
            continue;
        }
        if (cm->members[i].status == NODE_STATUS_ALIVE) {
            attempt_count++;
            TcpConnection conn;
            
            LOG_DEBUG("Sending LEAVE to %s at %s:%d", 
                      cm->members[i].node_id,
                      cm->members[i].ip_address, 
                      cm->members[i].tcp_port);
            
            if (tcp_client_connect(cm->members[i].ip_address, 
                                   cm->members[i].tcp_port, &conn) == 0) {
                if (tcp_send_all(&conn, hbuf, sizeof(hbuf)) == 0) {
                    sent_count++;
                    LOG_DEBUG("LEAVE sent to %s", cm->members[i].node_id);
                } else {
                    LOG_WARN("Failed to send LEAVE to %s", cm->members[i].node_id);
                }
                tcp_close(&conn);
            } else {
                LOG_WARN("Failed to connect to %s for LEAVE", cm->members[i].node_id);
            }
        }
    }
    
    LOG_INFO("Cluster leave: sent to %d/%d peers", sent_count, attempt_count);
}