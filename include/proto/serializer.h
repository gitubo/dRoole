#ifndef SERIALIZER_H
#define SERIALIZER_H

#include "protocol_defs.h"
#include <stdint.h>

// Serializes RpcHeader into network byte order buffer (safe for transport)
void serializer_pack_rpc(const RpcHeader *src, uint8_t *dest);

// Deserializes buffer into Host RpcHeader (safe for application use)
void serializer_unpack_rpc(const uint8_t *src, RpcHeader *dest);

// Serializes GossipPacket into network byte order buffer
void serializer_pack_gossip(const GossipPacket *src, uint8_t *dest);

// Deserializes buffer into Host GossipPacket
void serializer_unpack_gossip(const uint8_t *src, GossipPacket *dest);

#endif