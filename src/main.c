// ####################
// /src/main.c
// ####################

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <time.h>
#include <signal.h>

// Project Headers
#include "common/event_loop.h"
#include "transport/tcp_transport.h"
#include "transport/udp_transport.h"
#include "protocol/serializer.h"
#include "protocol/protocol_defs.h"
#include "cluster/kv_store.h"
#include "common/logger.h"
#include "cluster/cluster_manager.h"

// Global pointer for graceful signal handling
static EventLoop *global_loop = NULL;

typedef struct {
    EventLoop *loop;
    UdpSocket gossip_sock;
    int tcp_server_fd;
    ClusterManager cluster;
} NodeContext;

/**
 * Signal handler to break the event loop and allow cleanup
 */
void handle_shutdown(int sig) {
    (void)sig;
    if (global_loop) {
        LOG_WARN("Shutdown signal received. Closing engine...");
        loop_stop(global_loop);
    }
}

/**
 * TCP Callback: Handles incoming RPC connections
 */
void on_rpc_accept(void *context, int server_fd, uint32_t events) {
    (void)events;
    NodeContext *ctx = (NodeContext *)context;
    TcpConnection client;
    
    // 1. Accept Connection
    if (tcp_server_accept(server_fd, &client) == 0) {
        
        // 2. Read Header (Blocking read for simplicity in this handler)
        // Production Note: This should be added to epoll, but for handshake 
        // short messages, blocking read is often acceptable in minimal implementations.
        uint8_t head_buf[sizeof(RpcHeader)];
        if (tcp_recv_all(&client, head_buf, sizeof(head_buf)) == 0) {
            RpcHeader header;
            serializer_unpack_rpc(head_buf, &header);

            if (header.magic == RPC_MAGIC) {
                if (header.command_type == RPC_CMD_JOIN_REQ) {
                    // Read Payload
                    JoinRequestPayload payload;
                    if (tcp_recv_all(&client, &payload, sizeof(payload)) == 0) {
                        cluster_handle_join_req(&ctx->cluster, &payload, &client);
                    }
                }
            }
        }
        tcp_close(&client);
    }
}

/**
 * UDP Callback: Handles incoming Gossip/Membership packets
 */
void on_gossip_receive(void *context, int fd, uint32_t events) {
    (void)fd; (void)events;
    NodeContext *ctx = (NodeContext *)context;
    
    uint8_t raw_buf[2048];
    char s_ip[46];
    uint16_t s_port;

    int bytes = udp_recv(&ctx->gossip_sock, raw_buf, sizeof(raw_buf), s_ip, &s_port);
    if (bytes >= (int)sizeof(GossipPacket)) {
        GossipPacket pkt;
        serializer_unpack_gossip(raw_buf, &pkt);

        if (pkt.magic == GOSSIP_MAGIC) {
            LOG_INFO("[Gossip] Heartbeat from %s (Seq: %u) via %s:%d", 
                     pkt.node_id, pkt.sequence, s_ip, s_port);
            
            // Update the cluster manager view
            cluster_update_member_status(&ctx->cluster, pkt.node_id, NODE_STATUS_ALIVE);
        } else {
            LOG_WARN("[Gossip] Dropping packet: Invalid Magic from %s", s_ip);
        }
    }
}

/**
 * Timer Callback: Periodic Heartbeat
 */
void on_heartbeat_timer(void *context, int fd, uint32_t events) {
    (void)events;
    NodeContext *ctx = (NodeContext *)context;
    
    uint64_t expirations;
    if (read(fd, &expirations, sizeof(expirations)) < 0) return;

    GossipPacket pkt = { 
        .magic = GOSSIP_MAGIC,
        .type = 1, 
        .sequence = (uint32_t)time(NULL) 
    };
    strncpy(pkt.node_id, ctx->cluster.self.node_id, sizeof(pkt.node_id)-1);

    uint8_t send_buf[sizeof(GossipPacket)];
    serializer_pack_gossip(&pkt, send_buf);

    // Simple broadcast to a known port for local testing
    udp_send(&ctx->gossip_sock, "127.0.0.1", 9000, send_buf, sizeof(GossipPacket));
    
    LOG_INFO("[System] Local heartbeat generated for Cluster: %s", ctx->cluster.cluster_name);
}

int main(int argc, char *argv[]) {
    // 1. Initialize High-Performance Logger
    if (logger_init() != 0) {
        fprintf(stderr, "Critical Error: Could not initialize Async Logger\n");
        return 1;
    }

    // 2. Argument Parsing
    if (argc < 4) {
        LOG_ERR("Usage: %s <node_id> <port> <role: -c|-w> [seed_ip] [seed_port]", argv[0]);
        logger_shutdown();
        return 1;
    }

    const char *node_id = argv[1];
    uint16_t port = (uint16_t)atoi(argv[2]);
    NodeRole role = (strcmp(argv[3], "-c") == 0) ? ROLE_CONTROL_NODE : ROLE_WORKER_NODE;

    // 3. Initialize Event Loop
    EventLoop *loop = loop_create(1024);
    if (!loop) {
        LOG_ERR("Failed to create Reactor Event Loop");
        logger_shutdown();
        return 1;
    }
    global_loop = loop;
    signal(SIGINT, handle_shutdown);
    signal(SIGTERM, handle_shutdown);

    // 4. Initialize Cluster Manager Layer
    NodeContext ctx;
    ctx.loop = loop;
    if (cluster_init(&ctx.cluster, role, node_id, "0.0.0.0", port) != 0) {
        LOG_ERR("Failed to initialize Cluster Manager");
        goto cleanup;
    }
    strncpy(ctx.cluster.cluster_name, "DistriC-Prod-01", 31);

    // 5. Network Setup (TCP & UDP)
    ctx.tcp_server_fd = tcp_server_listen("0.0.0.0", port, 128);
    if (ctx.tcp_server_fd < 0) {
        LOG_ERR("TCP Bind failed on port %d", port);
        goto cleanup;
    }

    if (udp_init("0.0.0.0", port, &ctx.gossip_sock) < 0) {
        LOG_ERR("UDP Bind failed on port %d", port);
        close(ctx.tcp_server_fd);
        goto cleanup;
    }

    // 6. Register Events
    loop_add_fd(loop, ctx.tcp_server_fd, EVENT_READ, on_rpc_accept, &ctx);
    loop_add_fd(loop, ctx.gossip_sock.sockfd, EVENT_READ, on_gossip_receive, &ctx);
    loop_add_timer(loop, 2000, on_heartbeat_timer, &ctx);

    // 7. Handle Cluster Joining
    if (argc >= 6) {
        const char *seed_ip = argv[4];
        uint16_t seed_port = (uint16_t)atoi(argv[5]);
        cluster_join(&ctx.cluster, seed_ip, seed_port);
    }

    LOG_INFO("Distri-C Engine Started | ID: %s | Role: %s | Port: %d", 
             node_id, (role == ROLE_CONTROL_NODE ? "CONTROL" : "WORKER"), port);

    // 8. Run Engine
    loop_run(loop);

    // 9. Cleanup
    LOG_WARN("Stopping transport layers...");
    cluster_leave(&ctx.cluster);
    close(ctx.tcp_server_fd);
    udp_close(&ctx.gossip_sock);

cleanup:
    loop_free(loop);
    LOG_INFO("Finalizing logs. Goodbye.");
    logger_shutdown(); 
    return 0;
}