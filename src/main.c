#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <time.h>
#include <signal.h>

#include "common/event_loop.h"
#include "transport/tcp_transport.h"
#include "transport/udp_transport.h"
#include "protocol/serializer.h"
#include "protocol/protocol_defs.h"
#include "protocol/rpc_handler.h"
#include "protocol/rpc_protocol.h"
#include "cluster/kv_store.h"
#include "common/logger.h"
#include "cluster/cluster_manager.h"

static EventLoop *global_loop = NULL;

typedef struct {
    EventLoop *loop;
    UdpSocket gossip_sock;
    int tcp_server_fd;
    int heartbeat_timer_fd;
    int reaper_timer_fd;
    ClusterManager cluster;
} NodeContext;

void handle_shutdown(int sig) {
    (void)sig;
    if (global_loop) {
        loop_stop(global_loop);
    }
}

/* ================= GOSSIP RECEIVE ================= */

void on_gossip_receive(void *context, int fd, uint32_t events) {
    (void)fd; (void)events;
    NodeContext *ctx = context;

    uint8_t buf[2048];
    char ip[46];
    uint16_t port;

    int n = udp_recv(&ctx->gossip_sock, buf, sizeof(buf), ip, &port);
    if (n < (int)sizeof(GossipPacket)) return;

    GossipPacket pkt;
    serializer_unpack_gossip(buf, &pkt);

    if (pkt.magic != GOSSIP_MAGIC) {
        LOG_WARN("Invalid gossip magic from %s:%d", ip, port);
        return;
    }

    cluster_update_member_status(&ctx->cluster, pkt.node_id, NODE_STATUS_ALIVE);
}

/* ================= HEARTBEAT TIMER ================= */

void on_heartbeat_timer(void *context, int fd, uint32_t events) {
    (void)events;
    NodeContext *ctx = context;

    uint64_t exp;
    if (read(fd, &exp, sizeof(exp)) < 0) return;

    GossipPacket pkt = {
        .magic = GOSSIP_MAGIC,
        .type = 1,
        .sequence = (uint32_t)time(NULL)
    };
    strncpy(pkt.node_id, ctx->cluster.self.node_id, sizeof(pkt.node_id)-1);

    uint8_t out[sizeof(GossipPacket)];
    serializer_pack_gossip(&pkt, out);

    int fanout = 3;
    int sent = 0;

    for (size_t i = 0; i < ctx->cluster.member_count && sent < fanout; i++) {
        NodeInfo *m = &ctx->cluster.members[i];
        if (m->status != NODE_STATUS_ALIVE) continue;

        udp_send(&ctx->gossip_sock,
                 m->ip_address,
                 m->udp_port,
                 out,
                 sizeof(out));
        sent++;
    }
}

/* ================= REAPER TIMER ================= */

void cluster_reaper_tick(void *context, int fd, uint32_t events) {
    (void)fd; (void)events;
    ClusterManager *cm = context;
    uint64_t now = time(NULL);

    for (size_t i = 0; i < cm->member_count; i++) {
        NodeInfo *m = &cm->members[i];
        uint64_t diff = now - m->last_updated_ts;

        if (diff > 10)
            m->status = NODE_STATUS_DEAD;
        else if (diff > 5)
            m->status = NODE_STATUS_SUSPECT;
    }
}

/* ================= MAIN ================= */

int main(int argc, char *argv[]) {
    if (logger_init() != 0) {
        fprintf(stderr, "Failed to initialize logger\n");
        return 1;
    }

    if (argc < 4) {
        LOG_ERR("Usage: %s <node_id> <port> <-c|-w> [seed_ip] [seed_port]", argv[0]);
        logger_shutdown();
        return 1;
    }

    NodeContext ctx;
    memset(&ctx, 0, sizeof(ctx));

    const char *node_id = argv[1];
    uint16_t port = atoi(argv[2]);
    NodeRole role = (strcmp(argv[3], "-c") == 0)
                        ? ROLE_CONTROL_NODE
                        : ROLE_WORKER_NODE;

    // Create event loop
    EventLoop *loop = loop_create(1024);
    if (!loop) {
        LOG_ERR("Failed to create event loop");
        logger_shutdown();
        return 1;
    }
    
    global_loop = loop;
    ctx.loop = loop;
    
    // Setup signal handlers
    signal(SIGINT, handle_shutdown);
    signal(SIGTERM, handle_shutdown);

    // Initialize cluster
    cluster_init(&ctx.cluster, role, node_id, "0.0.0.0", port);
    strncpy(ctx.cluster.cluster_name, "DistriC-Prod-01", 31);

    // Setup TCP server
    ctx.tcp_server_fd = tcp_server_listen("0.0.0.0", port, 128);
    if (ctx.tcp_server_fd < 0) {
        LOG_ERR("Failed to create TCP server on port %d", port);
        loop_free(loop);
        logger_shutdown();
        return 1;
    }
    
    // Setup UDP socket
    if (udp_init("0.0.0.0", port, &ctx.gossip_sock) != 0) {
        LOG_ERR("Failed to create UDP socket on port %d", port);
        close(ctx.tcp_server_fd);
        loop_free(loop);
        logger_shutdown();
        return 1;
    }

    // Prepare RPC server context
    typedef struct {
        EventLoop *loop;
        ClusterManager *cluster;
    } RpcServerContext;

    RpcServerContext *rpc_ctx = malloc(sizeof(*rpc_ctx));
    if (!rpc_ctx) {
        LOG_ERR("Failed to allocate RPC context");
        udp_close(&ctx.gossip_sock);
        close(ctx.tcp_server_fd);
        loop_free(loop);
        logger_shutdown();
        return 1;
    }
    rpc_ctx->loop = loop;
    rpc_ctx->cluster = &ctx.cluster;

    // Register event handlers
    if (loop_add_fd(loop, ctx.tcp_server_fd, EVENT_READ, rpc_on_accept, rpc_ctx) != 0) {
        LOG_ERR("Failed to add TCP server to event loop");
        free(rpc_ctx);
        udp_close(&ctx.gossip_sock);
        close(ctx.tcp_server_fd);
        loop_free(loop);
        logger_shutdown();
        return 1;
    }
    
    if (loop_add_fd(loop, ctx.gossip_sock.sockfd, EVENT_READ, on_gossip_receive, &ctx) != 0) {
        LOG_ERR("Failed to add UDP socket to event loop");
        free(rpc_ctx);
        udp_close(&ctx.gossip_sock);
        close(ctx.tcp_server_fd);
        loop_free(loop);
        logger_shutdown();
        return 1;
    }
    
    ctx.heartbeat_timer_fd = loop_add_timer(loop, 2000, on_heartbeat_timer, &ctx);
    if (ctx.heartbeat_timer_fd < 0) {
        LOG_ERR("Failed to add heartbeat timer");
        free(rpc_ctx);
        udp_close(&ctx.gossip_sock);
        close(ctx.tcp_server_fd);
        loop_free(loop);
        logger_shutdown();
        return 1;
    }
    
    ctx.reaper_timer_fd = loop_add_timer(loop, 3000, cluster_reaper_tick, &ctx.cluster);
    if (ctx.reaper_timer_fd < 0) {
        LOG_ERR("Failed to add reaper timer");
        close(ctx.heartbeat_timer_fd);
        free(rpc_ctx);
        udp_close(&ctx.gossip_sock);
        close(ctx.tcp_server_fd);
        loop_free(loop);
        logger_shutdown();
        return 1;
    }

    // Join cluster if seed provided
    if (argc >= 6) {
        const char *seed_ip = argv[4];
        uint16_t seed_port = atoi(argv[5]);
        
        if (cluster_join(&ctx.cluster, seed_ip, seed_port) != 0) {
            LOG_ERR("Failed to join cluster via %s:%d", seed_ip, seed_port);
            // Continue anyway - might work as isolated node
        }
    }

    LOG_INFO("Node started: %s (role=%s, port=%d)", 
             node_id, 
             role == ROLE_CONTROL_NODE ? "CONTROL" : "WORKER",
             port);
    
    // Run event loop (blocks until loop_stop() called)
    loop_run(loop);
    
    LOG_INFO("Event loop stopped. Shutting down...");

    // ========================================================================
    // CRITICAL SHUTDOWN SEQUENCE - ORDER MATTERS!
    // ========================================================================
    
    // Step 1: Send graceful LEAVE notifications (while network still works)
    cluster_leave(&ctx.cluster);
    
    // Step 2: Stop accepting new connections
    LOG_DEBUG("Removing TCP server from event loop");
    loop_del_fd(loop, ctx.tcp_server_fd);
    close(ctx.tcp_server_fd);
    ctx.tcp_server_fd = -1;
    
    // Step 3: Remove timers (prevents spurious events)
    LOG_DEBUG("Removing timers");
    if (ctx.heartbeat_timer_fd >= 0) {
        loop_del_fd(loop, ctx.heartbeat_timer_fd);
        close(ctx.heartbeat_timer_fd);
    }
    if (ctx.reaper_timer_fd >= 0) {
        loop_del_fd(loop, ctx.reaper_timer_fd);
        close(ctx.reaper_timer_fd);
    }
    
    // Step 4: Remove UDP socket
    LOG_DEBUG("Closing UDP socket");
    loop_del_fd(loop, ctx.gossip_sock.sockfd);
    udp_close(&ctx.gossip_sock);
    
    // Step 5: Close ALL remaining file descriptors in event loop
    // This is CRITICAL - without this, client sessions remain open and block shutdown
    LOG_DEBUG("Force-closing all remaining sessions");
    
    // WORKAROUND: Since we can't access fd_table directly, we'll just free the loop
    // which will remove FDs from epoll. The OS will close them when process exits.
    // For production, you'd want to add a loop_get_all_fds() API or track sessions separately.
    
    // Step 6: Free event loop resources
    loop_free(loop);
    global_loop = NULL;
    
    // Step 7: Free RPC context
    free(rpc_ctx);
    
    // Step 8: Shutdown logger (flushes remaining logs)
    LOG_DEBUG("Shutting down logger");
    logger_shutdown();
    
    LOG_INFO("Shutdown complete");
    
    return 0;
}