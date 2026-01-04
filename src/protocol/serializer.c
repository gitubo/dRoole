#include "../../include/protocol/serializer.h"
#include <arpa/inet.h>
#include <string.h>

// Helper macros for 64-bit endian conversion if not available on system
#ifndef htobe64
#if __BYTE_ORDER__ == __ORDER_LITTLE_ENDIAN__
#define htobe64(x) __builtin_bswap64(x)
#define be64toh(x) __builtin_bswap64(x)
#else
#define htobe64(x) (x)
#define be64toh(x) (x)
#endif
#endif

void serializer_pack_rpc(const RpcHeader *src, uint8_t *dest) {
    size_t offset = 0;
    
    // Pack magic (4 bytes)
    uint32_t magic = htonl(src->magic);
    memcpy(dest + offset, &magic, sizeof(uint32_t));
    offset += sizeof(uint32_t);
    
    // Pack version (2 bytes)
    uint16_t version = htons(src->version);
    memcpy(dest + offset, &version, sizeof(uint16_t));
    offset += sizeof(uint16_t);
    
    // Pack command_type (2 bytes)
    uint16_t command_type = htons(src->command_type);
    memcpy(dest + offset, &command_type, sizeof(uint16_t));
    offset += sizeof(uint16_t);
    
    // Pack payload_len (8 bytes)
    uint64_t payload_len = htobe64(src->payload_len);
    memcpy(dest + offset, &payload_len, sizeof(uint64_t));
    offset += sizeof(uint64_t);
    
    // Pack origin_id (37 bytes) - strings are byte arrays, no endian conversion
    memcpy(dest + offset, src->origin_id, MAX_NODE_ID);
    offset += MAX_NODE_ID;
    
    // Pack request_id (37 bytes)
    memcpy(dest + offset, src->request_id, MAX_NODE_ID);
    // offset += MAX_NODE_ID;  // Uncomment if more fields follow
}

void serializer_unpack_rpc(const uint8_t *src, RpcHeader *dest) {
    const RpcHeader *in = (const RpcHeader *)src;
    dest->magic = ntohl(in->magic);
    dest->version = ntohs(in->version);
    dest->command_type = ntohs(in->command_type);
    dest->payload_len = be64toh(in->payload_len);
    memcpy(dest->origin_id, in->origin_id, sizeof(dest->origin_id));
    memcpy(dest->request_id, in->request_id, sizeof(dest->request_id));
}

void serializer_pack_gossip(const GossipPacket *src, uint8_t *dest) {
    GossipPacket *out = (GossipPacket *)dest;
    out->magic = htonl(src->magic);
    out->type = htonl(src->type);
    out->sequence = htonl(src->sequence);
    memcpy(out->node_id, src->node_id, sizeof(out->node_id));
}

void serializer_unpack_gossip(const uint8_t *src, GossipPacket *dest) {
    const GossipPacket *in = (const GossipPacket *)src;
    dest->magic = ntohl(in->magic);
    dest->type = ntohl(in->type);
    dest->sequence = ntohl(in->sequence);
    memcpy(dest->node_id, in->node_id, sizeof(dest->node_id));
}