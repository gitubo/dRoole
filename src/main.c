#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <time.h>
#include <signal.h>
#include <sys/timerfd.h>
#include <errno.h>

#include "common/event_loop.h"
#include "transport/tcp_transport.h"
#include "transport/udp_transport.h"
#include "protocol/serializer.h"
#include "protocol/protocol_defs.h"
#include "protocol/rpc_handler.h"
#include "protocol/rpc_protocol.h"
#include "cluster/kv_store.h"
#include "common/logger.h"
#include "common/utils.h"
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
        .sequence = (uint32_t)get_monotonic_time_ms() 
    };
    strncpy(pkt.node_id, ctx->cluster.self.node_id, sizeof(pkt.node_id)-1);

    uint8_t out[sizeof(GossipPacket)];
    serializer_pack_gossip(&pkt, out);

    int fanout = 3;
    int sent = 0;
    
    if (ctx->cluster.member_count > 0) {
        size_t start_idx = rand() % ctx->cluster.member_count;
        
        for (size_t i = 0; i < ctx->cluster.member_count && sent < fanout; i++) {
            size_t target_idx = (start_idx + i) % ctx->cluster.member_count;
            NodeInfo *m = &ctx->cluster.members[target_idx];

            // Don't gossip to self or dead nodes
            if (strcmp(m->node_id, ctx->cluster.self.node_id) == 0) continue;
            if (m->status == NODE_STATUS_DEAD) continue;

            udp_send(&ctx->gossip_sock, m->ip_address, m->udp_port, out, sizeof(out));
            sent++;
        }
    }
}

/* ================= REAPER TIMER ================= */

void cluster_reaper_tick(void *context, int fd, uint32_t events) {
    (void)fd; (void)events;
    ClusterManager *cm = context;
    
    uint64_t now = get_monotonic_time_ms();

    for (size_t i = 0; i < cm->member_count; i++) {
        NodeInfo *m = &cm->members[i];
        if (strcmp(m->node_id, cm->self.node_id) == 0) continue;

        // Calculate delta in milliseconds
        uint64_t diff = (now > m->last_updated_ts) ? (now - m->last_updated_ts) : 0;

        // NEW THRESHOLDS:
        // 1500ms -> SUSPECT (Missing ~1-2 heartbeats)
        // 3000ms -> DEAD    (Definitive failure in high-speed cluster)
        if (diff > 3000) {
            if (m->status != NODE_STATUS_DEAD) {
                LOG_WARN("Node %s TIMEOUT: DEAD (No contact for %lu ms)", m->node_id, diff);
                m->status = NODE_STATUS_DEAD;
            }
        } else if (diff > 1500) {
            if (m->status == NODE_STATUS_ALIVE) {
                LOG_INFO("Node %s UNSTABLE: SUSPECT (No contact for %lu ms)", m->node_id, diff);
                m->status = NODE_STATUS_SUSPECT;
            }
        }
    }
}

static void set_timer_ms(int fd, uint64_t ms) {
    struct itimerspec ts;
    ts.it_interval.tv_sec = ms / 1000;
    ts.it_interval.tv_nsec = (ms % 1000) * 1000000ULL;
    ts.it_value.tv_sec = ms / 1000;
    ts.it_value.tv_nsec = (ms % 1000) * 1000000ULL;
    
    if (timerfd_settime(fd, 0, &ts, NULL) == -1) {
        LOG_ERR("Failed to set timerfd time");
    }
}

/* ================= MAIN ================= */


int main(int argc, char *argv[]) {
    // 1. Initialize Logger
    if (logger_init() != 0) {
        fprintf(stderr, "Failed to initialize logger\n");
        return EXIT_FAILURE;
    }

    // 2. Argument Parsing
    if (argc < 4) {
        LOG_ERR("Usage: %s <node_id> <port> <-c|-w> [seed_ip] [seed_port]", argv[0]); 
        logger_shutdown();
        return EXIT_FAILURE;
    }

    NodeContext ctx;
    memset(&ctx, 0, sizeof(ctx));

    const char *node_id = argv[1];
    uint16_t port = (uint16_t)atoi(argv[2]);
    
    // Corrected Enum Names from include/common/types.h
    NodeRole role = (strcmp(argv[3], "-c") == 0) 
                        ? ROLE_CONTROL_NODE 
                        : ROLE_WORKER_NODE; 

    // 3. Initialize Event Loop & Signals
    EventLoop *loop = loop_create(1024);
    if (!loop) {
        LOG_ERR("Failed to create event loop");
        logger_shutdown();
        return EXIT_FAILURE;
    }
    global_loop = loop;
    ctx.loop = loop;

    signal(SIGINT, handle_shutdown); 
    signal(SIGTERM, handle_shutdown);

    // 4. Initialize Cluster and Networking
    cluster_init(&ctx.cluster, role, node_id, "0.0.0.0", port); 
    
    ctx.tcp_server_fd = tcp_server_listen("0.0.0.0", port, 128); 
    if (ctx.tcp_server_fd < 0) {
        LOG_ERR("Failed to create TCP server on port %d", port);
        loop_free(loop);
        logger_shutdown();
        return EXIT_FAILURE;
    }
    
    if (udp_init("0.0.0.0", port, &ctx.gossip_sock) != 0) { 
        LOG_ERR("Failed to create UDP socket on port %d", port);
        close(ctx.tcp_server_fd);
        loop_free(loop);
        logger_shutdown();
        return EXIT_FAILURE;
    }

    // 5. Register Event Handlers
    // RPC Server Acceptor
    if (loop_add_fd(loop, ctx.tcp_server_fd, EVENT_READ, rpc_on_accept, &ctx) != 0) { 
        LOG_ERR("Failed to add TCP server to event loop");
        // Cleanup and exit...
    }
    
    // Gossip Receiver
    if (loop_add_fd(loop, ctx.gossip_sock.sockfd, EVENT_READ, on_gossip_receive, &ctx) != 0) { 
        LOG_ERR("Failed to add UDP socket to event loop");
        // Cleanup and exit...
    }

    // 6. Setup Millisecond Timers
    // Heartbeat: 1000ms
    ctx.heartbeat_timer_fd = loop_add_timer(loop, 1000, on_heartbeat_timer, &ctx); 
    
    // Reaper/Failure Detector: 500ms
    ctx.reaper_timer_fd = loop_add_timer(loop, 500, cluster_reaper_tick, &ctx.cluster); 

    if (ctx.heartbeat_timer_fd < 0 || ctx.reaper_timer_fd < 0) {
        LOG_ERR("Failed to initialize timers");
        // Cleanup and exit...
    }

    // 7. Join Cluster (if seed provided)
    if (argc >= 6) {
        const char *seed_ip = argv[4];
        uint16_t seed_port = (uint16_t)atoi(argv[5]);
        
        if (cluster_join(&ctx.cluster, seed_ip, seed_port) != 0) {
            LOG_WARN("Could not join via seed, starting in isolation");
        }
    }

    LOG_INFO("Node %s started on port %d", node_id, port);

    // 8. Run Loop and Graceful Shutdown
    loop_run(loop); 

    LOG_INFO("Shutdown initiated...");
    cluster_leave(&ctx.cluster); 
    
    // Cleanup sequence in reverse order of initialization
    loop_del_fd(loop, ctx.tcp_server_fd); 
    close(ctx.tcp_server_fd);
    
    loop_del_fd(loop, ctx.gossip_sock.sockfd); 
    udp_close(&ctx.gossip_sock); 
    
    loop_free(loop); 
    logger_shutdown(); 

    return EXIT_SUCCESS;
}