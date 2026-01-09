// ============================================================================
// FILE: src/cluster/raft_rpc_integration.c
// ============================================================================
#include "../../include/cluster/raft.h"
#include "../../include/protocol/protocol_defs.h"
#include "../../include/protocol/rpc_protocol.h"
#include "../../include/transport/tcp_transport.h"
#include "../../include/common/logger.h"
#include "../../include/common/utils.h"
#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <unistd.h>
#include <arpa/inet.h>

// Add endian conversion support
#if defined(__linux__)
    #include <endian.h>
#elif defined(__APPLE__)
    #include <libkern/OSByteOrder.h>
    #define htobe64(x) OSSwapHostToBigInt64(x)
    #define be64toh(x) OSSwapBigToHostInt64(x)
#elif defined(__FreeBSD__) || defined(__NetBSD__)
    #include <sys/endian.h>
#else
    // Fallback for systems without native support
    static inline uint64_t htobe64(uint64_t host_64bits) {
        union { uint64_t u64; uint8_t u8[8]; } src, dest;
        src.u64 = host_64bits;
        dest.u8[0] = src.u8[7];
        dest.u8[1] = src.u8[6];
        dest.u8[2] = src.u8[5];
        dest.u8[3] = src.u8[4];
        dest.u8[4] = src.u8[3];
        dest.u8[5] = src.u8[2];
        dest.u8[6] = src.u8[1];
        dest.u8[7] = src.u8[0];
        return dest.u64;
    }
    #define be64toh(x) htobe64(x)
#endif

// ============================================================================
// RAFT RPC COMMAND TYPES (extend protocol_defs.h)
// ============================================================================

#define RPC_CMD_RAFT_REQUEST_VOTE      0x20
#define RPC_CMD_RAFT_REQUEST_VOTE_RESP 0x21
#define RPC_CMD_RAFT_APPEND_ENTRIES    0x22
#define RPC_CMD_RAFT_APPEND_ENTRIES_RESP 0x23
#define RPC_CMD_RAFT_INSTALL_SNAPSHOT  0x24
#define RPC_CMD_RAFT_INSTALL_SNAPSHOT_RESP 0x25

// ============================================================================
// SERIALIZATION HELPERS
// ============================================================================

/**
 * Convert RaftRequestVoteReq to network byte order
 */
static void pack_request_vote_req(const RaftRequestVoteReq *src, uint8_t *dest) {
    RaftRequestVoteReq *out = (RaftRequestVoteReq *)dest;
    out->term = htobe64(src->term);
    memcpy(out->candidate_id, src->candidate_id, sizeof(out->candidate_id));
    out->last_log_index = htobe64(src->last_log_index);
    out->last_log_term = htobe64(src->last_log_term);
}

static void unpack_request_vote_req(const uint8_t *src, RaftRequestVoteReq *dest) {
    const RaftRequestVoteReq *in = (const RaftRequestVoteReq *)src;
    dest->term = be64toh(in->term);
    memcpy(dest->candidate_id, in->candidate_id, sizeof(dest->candidate_id));
    dest->last_log_index = be64toh(in->last_log_index);
    dest->last_log_term = be64toh(in->last_log_term);
}

static void pack_request_vote_resp(const RaftRequestVoteResp *src, uint8_t *dest) {
    RaftRequestVoteResp *out = (RaftRequestVoteResp *)dest;
    out->term = htobe64(src->term);
    out->vote_granted = src->vote_granted;
}

static void unpack_request_vote_resp(const uint8_t *src, RaftRequestVoteResp *dest) {
    const RaftRequestVoteResp *in = (const RaftRequestVoteResp *)src;
    dest->term = be64toh(in->term);
    dest->vote_granted = in->vote_granted;
}

/**
 * Convert RaftAppendEntriesReq to network byte order
 */
static void pack_append_entries_req(const RaftAppendEntriesReq *src, uint8_t *dest) {
    RaftAppendEntriesReq *out = (RaftAppendEntriesReq *)dest;
    out->term = htobe64(src->term);
    memcpy(out->leader_id, src->leader_id, sizeof(out->leader_id));
    out->prev_log_index = htobe64(src->prev_log_index);
    out->prev_log_term = htobe64(src->prev_log_term);
    out->leader_commit = htobe64(src->leader_commit);
    out->entry_count = htonl(src->entry_count);
}

static void unpack_append_entries_req(const uint8_t *src, RaftAppendEntriesReq *dest) {
    const RaftAppendEntriesReq *in = (const RaftAppendEntriesReq *)src;
    dest->term = be64toh(in->term);
    memcpy(dest->leader_id, in->leader_id, sizeof(dest->leader_id));
    dest->prev_log_index = be64toh(in->prev_log_index);
    dest->prev_log_term = be64toh(in->prev_log_term);
    dest->leader_commit = be64toh(in->leader_commit);
    dest->entry_count = ntohl(in->entry_count);
}

static void pack_append_entries_resp(const RaftAppendEntriesResp *src, uint8_t *dest) {
    RaftAppendEntriesResp *out = (RaftAppendEntriesResp *)dest;
    out->term = htobe64(src->term);
    out->success = src->success;
    out->match_index = htobe64(src->match_index);
}

static void unpack_append_entries_resp(const uint8_t *src, RaftAppendEntriesResp *dest) {
    const RaftAppendEntriesResp *in = (const RaftAppendEntriesResp *)src;
    dest->term = be64toh(in->term);
    dest->success = in->success;
    dest->match_index = be64toh(in->match_index);
}

// ============================================================================
// RPC SENDER (Called by Raft core)
// ============================================================================

typedef struct {
    RaftNode *raft_node;
    // Peer connection cache could go here
} RaftRpcContext;

/**
 * Callback: Send RPC to peer
 * This is called by Raft core when it needs to send RequestVote or AppendEntries
 */
static int raft_send_rpc_callback(void *context, const char *peer_id,
                                  const void *request, size_t req_len) {
    RaftRpcContext *ctx = (RaftRpcContext *)context;
    RaftNode *node = ctx->raft_node;
    
    // Find peer to get connection info
    RaftPeer *peer = NULL;
    for (size_t i = 0; i < node->peer_count; i++) {
        if (strcmp(node->peers[i].node_id, peer_id) == 0) {
            peer = &node->peers[i];
            break;
        }
    }
    
    if (!peer) {
        LOG_ERR("[Raft-RPC] Unknown peer: %s", peer_id);
        return -1;
    }
    
    // Determine RPC type by request size
    uint16_t cmd_type;
    if (req_len == sizeof(RaftRequestVoteReq)) {
        cmd_type = RPC_CMD_RAFT_REQUEST_VOTE;
    } else if (req_len >= sizeof(RaftAppendEntriesReq)) {
        cmd_type = RPC_CMD_RAFT_APPEND_ENTRIES;
    } else {
        LOG_ERR("[Raft-RPC] Unknown request size: %zu", req_len);
        return -1;
    }
    
    // Establish TCP connection
    TcpConnection conn;
    if (tcp_client_connect(peer->ip_address, peer->port, &conn) != 0) {
        LOG_DEBUG("[Raft-RPC] Failed to connect to %s at %s:%d",
                  peer_id, peer->ip_address, peer->port);
        return -1;
    }
    
    // Construct RPC header
    RpcHeader hdr = {
        .magic = RPC_MAGIC,
        .version = 1,
        .command_type = cmd_type,
        .payload_len = req_len
    };
    strncpy(hdr.origin_id, node->node_id, sizeof(hdr.origin_id) - 1);
    snprintf(hdr.request_id, sizeof(hdr.request_id), "raft-%lu", 
             (unsigned long)get_monotonic_time_ms());
    
    // Send request (synchronous for now, could be async)
    if (rpc_send_message(&conn, &hdr, request) != 0) {
        tcp_close(&conn);
        return -1;
    }
    
    // Receive response
    void *response = NULL;
    if (rpc_recv_message(&conn, &hdr, &response) != 0) {
        tcp_close(&conn);
        return -1;
    }
    
    // Process response
    if (cmd_type == RPC_CMD_RAFT_REQUEST_VOTE && 
        hdr.command_type == RPC_CMD_RAFT_REQUEST_VOTE_RESP) {
        RaftRequestVoteResp resp;
        unpack_request_vote_resp((const uint8_t *)response, &resp);
        raft_handle_request_vote_response(node, peer_id, &resp);
    } else if (cmd_type == RPC_CMD_RAFT_APPEND_ENTRIES &&
               hdr.command_type == RPC_CMD_RAFT_APPEND_ENTRIES_RESP) {
        RaftAppendEntriesResp resp;
        unpack_append_entries_resp((const uint8_t *)response, &resp);
        raft_handle_append_entries_response(node, peer_id, &resp);
    }
    
    free(response);
    tcp_close(&conn);
    return 0;
}

/**
 * Initialize Raft RPC context and set callbacks
 */
RaftRpcContext* raft_rpc_init(RaftNode *node) {
    RaftRpcContext *ctx = calloc(1, sizeof(RaftRpcContext));
    if (!ctx) return NULL;
    
    ctx->raft_node = node;
    
    // Set Raft callbacks
    node->callbacks.send_rpc = raft_send_rpc_callback;
    node->callbacks.context = ctx;
    node->callbacks.persist = (int (*)(void*))raft_persist_state;
    
    return ctx;
}

void raft_rpc_destroy(RaftRpcContext *ctx) {
    if (ctx) free(ctx);
}

// ============================================================================
// RPC RECEIVER (Called by existing RPC handler)
// ============================================================================

/**
 * Handler for incoming Raft RPCs
 * This should be called from your existing rpc_handler.c
 */
void raft_handle_incoming_rpc(RaftNode *node, 
                              const RpcHeader *header,
                              const uint8_t *payload) {
    if (!node || !header || !payload) return;
    
    const char *peer_id = header->origin_id;
    
    switch (header->command_type) {
        case RPC_CMD_RAFT_REQUEST_VOTE: {
            RaftRequestVoteReq req;
            unpack_request_vote_req(payload, &req);
            
            RaftRequestVoteResp resp = {0};
            raft_handle_request_vote(node, peer_id, &req, &resp);
            
            // Send response (handled by caller in production)
            LOG_DEBUG("[Raft-RPC] RequestVote from %s: granted=%d",
                      peer_id, resp.vote_granted);
            break;
        }
        
        case RPC_CMD_RAFT_APPEND_ENTRIES: {
            RaftAppendEntriesReq req;
            unpack_append_entries_req(payload, &req);
            
            // Parse log entries (if any)
            RaftLogEntry *entries = NULL;
            if (req.entry_count > 0) {
                // TODO: Deserialize log entries from payload
                // entries = parse_log_entries(payload + sizeof(req), req.entry_count);
            }
            
            RaftAppendEntriesResp resp = {0};
            raft_handle_append_entries(node, peer_id, &req, 
                                      entries, req.entry_count, &resp);
            
            if (entries) free(entries);
            
            LOG_DEBUG("[Raft-RPC] AppendEntries from %s: success=%d",
                      peer_id, resp.success);
            break;
        }
        
        case RPC_CMD_RAFT_INSTALL_SNAPSHOT: {
            // TODO: Implement snapshot handling
            LOG_WARN("[Raft-RPC] InstallSnapshot not yet implemented");
            break;
        }
        
        default:
            LOG_WARN("[Raft-RPC] Unknown Raft command: 0x%x", 
                     header->command_type);
            break;
    }
}

// ============================================================================
// STATE MACHINE CALLBACK
// ============================================================================

/**
 * Example state machine: Key-Value store
 * This is called by Raft when entries are committed
 */
typedef struct {
    char key[64];
    uint8_t value[256];
    size_t value_len;
} KvCommand;

static int kv_apply_callback(void *context, const uint8_t *data, size_t len) {
    (void)context;
    
    if (len < sizeof(KvCommand)) {
        LOG_WARN("[Raft] Invalid command size: %zu", len);
        return -1;
    }
    
    const KvCommand *cmd = (const KvCommand *)data;
    LOG_INFO("[Raft] Applied command: key=%s, value_len=%zu",
             cmd->key, cmd->value_len);
    
    // In production, update your KV store here
    // kv_update_rule(cmd->key, ...);
    
    return 0;
}

// ============================================================================
// INTEGRATION EXAMPLE
// ============================================================================

/**
 * Example: Initialize Raft subsystem in main()
 */
RaftNode* example_raft_init(const char *node_id, 
                            const char **peer_ids,
                            const char **peer_ips,
                            uint16_t *peer_ports,
                            size_t peer_count) {
    // Initialize Raft node
    RaftNode *node = raft_init(node_id, kv_apply_callback, NULL);
    if (!node) return NULL;
    
    // Add peers
    for (size_t i = 0; i < peer_count; i++) {
        raft_add_peer(node, peer_ids[i], peer_ips[i], peer_ports[i]);
    }
    
    // Setup RPC layer
    RaftRpcContext *rpc_ctx = raft_rpc_init(node);
    if (!rpc_ctx) {
        raft_destroy(node);
        return NULL;
    }
    
    // Restore from disk
    raft_restore_state(node);
    
    // Start participating in elections
    raft_start(node);
    
    LOG_INFO("[Raft] Initialized with %zu peers", peer_count);
    return node;
}

/**
 * Example: Add to event loop tick
 */
void example_raft_tick_handler(void *context, int fd, uint32_t events) {
    (void)fd; (void)events;
    RaftNode *node = (RaftNode *)context;
    
    uint64_t expirations;
    read(fd, &expirations, sizeof(expirations));
    
    // Call Raft periodic tick (handles elections, heartbeats, commits)
    raft_tick(node);
}

/**
 * Example: Submit client write request (only on leader)
 */
int example_submit_write(RaftNode *node, const char *key, 
                        const uint8_t *value, size_t value_len) {
    if (!raft_is_leader(node)) {
        LOG_INFO("Not leader, redirect to: %s", raft_get_leader_id(node));
        return -1;
    }
    
    KvCommand cmd;
    strncpy(cmd.key, key, sizeof(cmd.key) - 1);
    memcpy(cmd.value, value, value_len);
    cmd.value_len = value_len;
    
    return raft_submit(node, (const uint8_t *)&cmd, sizeof(cmd));
}

int raft_send_request_vote(RaftNode *raft, NodeInfo *target) {
    // 1. Create the RequestVote message
    RequestVoteRequest req = {
        .term = raft->current_term,
        .candidate_id = raft->node_id,
        .last_log_index = raft->last_log_index,
        .last_log_term = raft->last_log_term
    };

    // 2. Serialize and send via the TCP transport
    // We reuse the existing RPC infrastructure
    return rpc_send_request(target->ip_address, target->tcp_port, RPC_CMD_RAFT_VOTE, &req);
}