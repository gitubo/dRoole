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
    ClusterManager cluster;
} NodeContext;

void handle_shutdown(int sig) {
    (void)sig;
    if (global_loop) {
        LOG_WARN("Shutdown signal received");
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

    if (pkt.magic != GOSSIP_MAGIC) return;

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
    if (logger_init() != 0) return 1;

    if (argc < 4) {
        LOG_ERR("Usage: %s <node_id> <port> <-c|-w> [seed_ip] [seed_port]", argv[0]);
        return 1;
    }

    NodeContext ctx;
    memset(&ctx, 0, sizeof(ctx));

    const char *node_id = argv[1];
    uint16_t port = atoi(argv[2]);
    NodeRole role = (strcmp(argv[3], "-c") == 0)
                        ? ROLE_CONTROL_NODE
                        : ROLE_WORKER_NODE;

    EventLoop *loop = loop_create(1024);
    global_loop = loop;
    signal(SIGINT, handle_shutdown);
    signal(SIGTERM, handle_shutdown);

    cluster_init(&ctx.cluster, role, node_id, "0.0.0.0", port);
    strncpy(ctx.cluster.cluster_name, "DistriC-Prod-01", 31);

    ctx.tcp_server_fd = tcp_server_listen("0.0.0.0", port, 128);
    udp_init("0.0.0.0", port, &ctx.gossip_sock);

    typedef struct {
        EventLoop *loop;
        ClusterManager *cluster;
    } RpcServerContext;

    RpcServerContext *rpc_ctx = malloc(sizeof(*rpc_ctx));
    rpc_ctx->loop = loop;
    rpc_ctx->cluster = &ctx.cluster;

    loop_add_fd(loop, ctx.tcp_server_fd, EVENT_READ, rpc_on_accept, rpc_ctx);
    loop_add_fd(loop, ctx.gossip_sock.sockfd, EVENT_READ, on_gossip_receive, &ctx);
    loop_add_timer(loop, 2000, on_heartbeat_timer, &ctx);
    loop_add_timer(loop, 3000, cluster_reaper_tick, &ctx.cluster);

    if (argc >= 6) {
        cluster_join(&ctx.cluster, argv[4], atoi(argv[5]));
    }

    LOG_INFO("Node started: %s", node_id);
    loop_run(loop);

    cluster_leave(&ctx.cluster);
    udp_close(&ctx.gossip_sock);
    close(ctx.tcp_server_fd);
    loop_free(loop);
    logger_shutdown();
    return 0;
}
