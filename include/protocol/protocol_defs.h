#ifndef PROTOCOL_DEFS_H
#define PROTOCOL_DEFS_H

#include <stdint.h>

#define RPC_MAGIC    0xCAFEBABE
#define GOSSIP_MAGIC 0xDEADC0DE
#define MAX_NODE_ID  37

typedef struct __attribute__((packed)) {
    uint32_t magic;
    uint16_t version;
    uint16_t command_type;
    uint64_t payload_len;
    char origin_id[MAX_NODE_ID];
    char request_id[MAX_NODE_ID];
} RpcHeader;

typedef struct __attribute__((packed)) {
    uint32_t magic;
    uint32_t type;
    uint32_t sequence;
    char node_id[MAX_NODE_ID];
} GossipPacket;

#define RPC_CMD_JOIN_REQ     0x01
#define RPC_CMD_JOIN_RESP    0x02
#define RPC_CMD_LEAVE        0x03
#define RPC_CMD_SYNC_RULES   0x10

typedef struct __attribute__((packed)) {
    char node_id[37];
    char ip_address[46];
    uint16_t tcp_port;
    uint16_t udp_port;
    uint16_t role;
} JoinRequestPayload;

typedef struct __attribute__((packed)) {
    uint32_t status;
    uint32_t member_count;
} JoinResponseHeader;

#endif
