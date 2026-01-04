#include "../../include/common/event_loop.h"
#include "../../include/transport/udp_transport.h"
#include "../../include/protocol/serializer.h"
#include "../../include/protocol/protocol_defs.h" 
#include <stdio.h>
#include <unistd.h>

typedef struct {
    UdpSocket *udp_sock;
    const char *target_ip;
    uint16_t target_port;
} GossipDaemon;

void on_gossip_tick(void *context, int fd, uint32_t events) {
    (void)events;
    GossipDaemon *daemon = (GossipDaemon *)context;
    
    uint64_t expirations;
    if (read(fd, &expirations, sizeof(expirations)) < 0) return;

    // Use unified GossipPacket
    GossipPacket pkt = { 
        .magic = GOSSIP_MAGIC, 
        .type = 1, // PING
        .sequence = 101 
    };
    snprintf(pkt.node_id, sizeof(pkt.node_id), "node-01");

    // Buffer must be large enough for the packed struct
    uint8_t buffer[sizeof(GossipPacket)];
    
    // Use new serializer name
    serializer_pack_gossip(&pkt, buffer);

    // Use new UDP interface (udp_send instead of udp_send_to)
    udp_send(daemon->udp_sock, 
             daemon->target_ip, 
             daemon->target_port, 
             buffer, 
             sizeof(GossipPacket) // Use sizeof instead of manual calculation
    );
}