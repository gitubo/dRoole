#include "../../include/proto/serializer.h"
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
    RpcHeader *out = (RpcHeader *)dest;
    out->magic = htonl(src->magic);
    out->version = htons(src->version);
    out->command_type = htons(src->command_type);
    out->payload_len = htobe64(src->payload_len);
    // Strings are single-byte arrays, no endianness swap needed
    memcpy(out->origin_id, src->origin_id, sizeof(out->origin_id));
    memcpy(out->request_id, src->request_id, sizeof(out->request_id));
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