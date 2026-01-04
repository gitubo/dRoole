#ifndef PROTOCOL_DEFS_H
#define PROTOCOL_DEFS_H

#include <stdint.h>

#define RPC_MAGIC 0xCAFEBABE
#define GOSSIP_MAGIC 0xDEADC0DE
#define MAX_NODE_ID 37

// Unified RPC Header structure for Control Plane
typedef struct __attribute__((packed)) {
    uint32_t magic;         // 0xCAFEBABE
    uint16_t version;       // Protocol version
    uint16_t command_type;  // Operation type
    uint64_t payload_len;   // Data length following header
    char origin_id[MAX_NODE_ID];
    char request_id[MAX_NODE_ID];
} RpcHeader;

// Unified Gossip Packet for Data Plane
typedef struct __attribute__((packed)) {
    uint32_t magic;         // 0xDEADC0DE
    uint32_t type;          // 1=PING, 2=PONG, 3=SUSPECT, 4=ALIVE
    uint32_t sequence;      // Monotonic sequence number
    char node_id[MAX_NODE_ID];
    // Payload can be appended here (e.g., events)
} GossipPacket;

// Command Types for RPC
#define RPC_CMD_JOIN_REQ    0x01
#define RPC_CMD_JOIN_RESP   0x02
#define RPC_CMD_LEAVE       0x03

// Payload for Join Request
typedef struct __attribute__((packed)) {
    char node_id[37];
    char ip_address[46];
    uint16_t tcp_port;  
    uint16_t udp_port;
    uint16_t role; 
} JoinRequestPayload;

// Payload for Join Response (Simplified Snapshot)
typedef struct __attribute__((packed)) {
    uint32_t status;        // 0=OK, 1=REJECT
    uint32_t member_count;
    // Followed by member_count * NodeInfo
} JoinResponseHeader;

#endif