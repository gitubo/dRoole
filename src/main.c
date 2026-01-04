#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <time.h>

#include "core/event_loop.h"
#include "net/tcp_transport.h"
#include "net/udp_transport.h"
#include "proto/serializer.h"
#include "proto/protocol_defs.h"
#include "core/kv_store.h"

typedef struct {
    EventLoop *loop;
    UdpSocket gossip_sock;
    int tcp_server_fd;
    const char *node_id;
} NodeContext;

// TCP Handling (Placeholder for now)
void on_rpc_accept(void *context, int server_fd, uint32_t events) {
    (void)context; (void)events;
    TcpConnection client_conn;
    if (tcp_server_accept(server_fd, &client_conn) == 0) {
        printf("[RPC] New connection from %s:%d\n", client_conn.remote_ip, client_conn.remote_port);
        tcp_close(&client_conn); 
    }
}

// UDP Handling: Updated to use udp_recv and serializer_unpack_gossip
void on_gossip_receive(void *context, int fd, uint32_t events) {
    (void)fd; (void)events;
    NodeContext *ctx = (NodeContext *)context;
    
    uint8_t raw_buf[1024];
    char s_ip[46];
    uint16_t s_port;

    // Use new udp_recv (handles non-blocking internally)
    int bytes = udp_recv(&ctx->gossip_sock, raw_buf, sizeof(raw_buf), s_ip, &s_port);
    
    if (bytes >= (int)sizeof(GossipPacket)) {
        GossipPacket pkt;
        serializer_unpack_gossip(raw_buf, &pkt);
        
        if (pkt.magic == GOSSIP_MAGIC) {
            printf("[Gossip] Received %s from Node: %s at %s:%d\n", 
                   (pkt.type == 1 ? "PING" : "MSG"), pkt.node_id, s_ip, s_port);
        }
    }
}

void on_heartbeat_timer(void *context, int fd, uint32_t events) {
    (void)events;
    NodeContext *ctx = (NodeContext *)context;
    
    uint64_t expirations;
    read(fd, &expirations, sizeof(expirations));

    GossipPacket pkt = { 
        .magic = GOSSIP_MAGIC,
        .type = 1, 
        .sequence = (uint32_t)time(NULL) 
    };
    strncpy(pkt.node_id, ctx->node_id, sizeof(pkt.node_id)-1);

    uint8_t send_buf[sizeof(GossipPacket)];
    serializer_pack_gossip(&pkt, send_buf);

    // Send to self/peer for testing
    udp_send(&ctx->gossip_sock, "127.0.0.1", 9000, send_buf, sizeof(GossipPacket));
    printf("[System] Heartbeat broadcasted.\n");
}

int main(int argc, char *argv[]) {
    if (argc < 3) {
        fprintf(stderr, "Usage: %s <node_id> <port>\n", argv[0]);
        return 1;
    }

    const char *node_id = argv[1];
    uint16_t port = (uint16_t)atoi(argv[2]);

    EventLoop *loop = loop_create(1024);
    NodeContext ctx = { .loop = loop, .node_id = node_id };

    // Setup TCP
    ctx.tcp_server_fd = tcp_server_listen("0.0.0.0", port, 128);
    if (ctx.tcp_server_fd < 0) return 1;

    // Setup UDP (New Interface)
    if (udp_init("0.0.0.0", port, &ctx.gossip_sock) < 0) return 1;

    loop_add_fd(loop, ctx.tcp_server_fd, EVENT_READ, on_rpc_accept, &ctx);
    loop_add_fd(loop, ctx.gossip_sock.sockfd, EVENT_READ, on_gossip_receive, &ctx);
    loop_add_timer(loop, 3000, on_heartbeat_timer, &ctx);

    printf("--- Distri-C Node [%s] Live on Port %d ---\n", node_id, port);
    
    loop_run(loop);
    loop_free(loop);
    return 0;
}