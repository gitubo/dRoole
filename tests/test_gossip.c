#ifndef _DEFAULT_SOURCE
#define _DEFAULT_SOURCE
#endif

#include "../include/net/udp_transport.h"
#include "../include/proto/protocol_defs.h"
#include "../include/proto/serializer.h"
#include <stdio.h>
#include <string.h>
#include <assert.h>
#include <unistd.h> // Correct header for usleep

/**
 * Main test entry for Gossip protocol serialization and UDP non-blocking transport.
 * Verifies that packets can be packed, sent, and received over loopback.
 */
int main() {
    UdpSocket sock;
    char sender_ip[46];
    uint16_t sender_port;
    uint8_t buffer[sizeof(GossipPacket)];

    // Initialize UDP (now non-blocking by default)
    if (udp_init("127.0.0.1", 9090, &sock) < 0) {
        return 1;
    }

    GossipPacket out = { .magic = GOSSIP_MAGIC, .type = 1, .sequence = 1 };
    strncpy(out.node_id, "test-node", MAX_NODE_ID - 1);
    
    serializer_pack_gossip(&out, buffer);

    // Test Loopback Send
    int sent = udp_send(&sock, "127.0.0.1", 9090, buffer, sizeof(GossipPacket));
    assert(sent > 0);

    // Poll for data (Non-blocking alignment)
    int received = 0;
    int attempts = 0;
    while (attempts < 100) {
        received = udp_recv(&sock, buffer, sizeof(buffer), sender_ip, &sender_port);
        if (received > 0) break;
        usleep(1000); // 1ms wait
        attempts++;
    }

    assert(received == sizeof(GossipPacket));
    
    GossipPacket in;
    serializer_unpack_gossip(buffer, &in);
    assert(in.magic == GOSSIP_MAGIC);
    assert(in.sequence == 1);
    assert(strcmp(in.node_id, "test-node") == 0);

    udp_close(&sock);
    printf("[Test] Gossip/UDP alignment passed.\n");
    return 0;
}