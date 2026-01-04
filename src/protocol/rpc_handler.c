/*
 * rpc_handler.c
 * Complete RPC implementation with both sync client and async server
 */

#include "../../include/protocol/rpc_protocol.h"
#include "../../include/protocol/serializer.h"
#include "../../include/protocol/protocol_defs.h"
#include "../../include/protocol/rpc_handler.h"
#include "../../include/transport/tcp_transport.h"
#include "../../include/common/event_loop.h"
#include "../../include/common/logger.h"
#include "../../include/common/types.h"
#include "../../include/cluster/cluster_manager.h"
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <errno.h>
#include <sys/socket.h>

// Forward declaration for callbacks
static void rpc_on_client_event(void *context, int fd, uint32_t events); 

#define MAX_PAYLOAD_SIZE (10 * 1024 * 1024)  // 10MB


// ============================================================================
// CLIENT-SIDE FUNCTIONS (Synchronous - for outgoing requests)
// ============================================================================

int rpc_send_message(TcpConnection *conn, RpcHeader *header, const void *payload) {
    if (tcp_send_all(conn, header, sizeof(RpcHeader)) != 0) return -1;

    if (header->payload_len > 0 && payload != NULL) {
        if (tcp_send_all(conn, payload, header->payload_len) != 0) return -1;
    }
    return 0;
}

int rpc_recv_message(TcpConnection *conn, RpcHeader *header, void **payload_out) {
    if (tcp_recv_all(conn, header, sizeof(RpcHeader)) != 0) return -1;

    if (header->magic != RPC_MAGIC) {
        LOG_ERR("Invalid RPC Magic Number: 0x%x", header->magic);
        return -2;
    }

    if (header->payload_len > MAX_PAYLOAD_SIZE) {
        LOG_ERR("Payload too large: %lu bytes", header->payload_len);
        return -2;
    }

    if (header->payload_len > 0) {
        *payload_out = malloc(header->payload_len);
        if (!*payload_out) return -1;
        
        if (tcp_recv_all(conn, *payload_out, header->payload_len) != 0) {
            free(*payload_out);
            *payload_out = NULL;
            return -1;
        }
    } else {
        *payload_out = NULL;
    }

    return 0;
}

NodeInfo* rpc_select_best_node(NodeInfo *members, int count, uint8_t cpu_threshold) {
    NodeInfo *best = NULL;
    uint8_t lowest_load = cpu_threshold;

    for (int i = 0; i < count; i++) {
        if (members[i].status == NODE_STATUS_ALIVE && members[i].cpu_load < lowest_load) {
            lowest_load = members[i].cpu_load;
            best = &members[i];
        }
    }
    return best;
}

// ============================================================================
// SERVER-SIDE ASYNC SESSION MANAGEMENT (for event loop)
// ============================================================================

typedef enum {
    SESSION_READING_HEADER,
    SESSION_READING_PAYLOAD,
    SESSION_PROCESSING,
    SESSION_SENDING_RESPONSE,
    SESSION_DONE
} SessionState;

typedef struct ClientSession {
    TcpConnection conn;
    SessionState state;
    
    uint8_t header_buf[sizeof(RpcHeader)];
    size_t header_bytes_read;
    
    uint8_t *payload_buf;
    size_t payload_bytes_read;
    size_t payload_total;
    
    RpcHeader header;
    
    uint8_t *response_buf;
    size_t response_len;
    size_t response_bytes_sent;
    
    void *app_context;
    EventLoop *loop;
} ClientSession;

// ============================================================================
// INTERNAL HELPERS
// ============================================================================

static ssize_t read_into_buffer(int fd, uint8_t *buf, size_t offset, size_t total) {
    size_t remaining = total - offset;
    if (remaining == 0) return 0;
    
    ssize_t n = recv(fd, buf + offset, remaining, MSG_DONTWAIT);
    
    if (n < 0) {
        if (errno == EAGAIN || errno == EWOULDBLOCK) return 0;
        return -1;
    }
    if (n == 0) return -1;
    
    return n;
}

static ssize_t write_from_buffer(int fd, const uint8_t *buf, size_t offset, size_t total) {
    size_t remaining = total - offset;
    if (remaining == 0) return 0;
    
    ssize_t n = send(fd, buf + offset, remaining, MSG_DONTWAIT);
    
    if (n < 0) {
        if (errno == EAGAIN || errno == EWOULDBLOCK) return 0;
        return -1;
    }
    
    return n;
}

ClientSession* session_create(int client_fd, EventLoop *loop, void *app_context) {
    ClientSession *session = calloc(1, sizeof(ClientSession));
    if (!session) return NULL;
    
    session->conn.sockfd = client_fd;
    session->state = SESSION_READING_HEADER;
    session->loop = loop;
    session->app_context = app_context;
    
    return session;
}

void session_destroy(ClientSession *session) {
    if (!session) return;
    
    if (session->payload_buf) free(session->payload_buf);
    if (session->response_buf) free(session->response_buf);
    
    if (session->loop && session->conn.sockfd >= 0) {
        loop_del_fd(session->loop, session->conn.sockfd);
    }
    
    tcp_close(&session->conn);
    free(session);
}

// ============================================================================
// STATE HANDLERS
// ============================================================================

static int handle_reading_header(ClientSession *session) {
    ssize_t n = read_into_buffer(session->conn.sockfd, session->header_buf,
                                  session->header_bytes_read, sizeof(RpcHeader));
    
    if (n < 0) {
        LOG_ERR("Error reading header from fd %d", session->conn.sockfd);
        return -1;
    }
    if (n == 0) return 0;
    
    session->header_bytes_read += n;
    
    if (session->header_bytes_read == sizeof(RpcHeader)) {
        serializer_unpack_rpc(session->header_buf, &session->header);
        
        if (session->header.magic != RPC_MAGIC) {
            LOG_ERR("Invalid RPC magic: 0x%x", session->header.magic);
            return -1;
        }
        
        if (session->header.payload_len > MAX_PAYLOAD_SIZE) {
            LOG_ERR("Payload too large: %lu bytes", session->header.payload_len);
            return -1;
        }
        
        LOG_DEBUG("Header received: cmd=%d, payload_len=%lu", 
                  session->header.command_type, session->header.payload_len);
        
        if (session->header.payload_len > 0) {
            session->payload_buf = malloc(session->header.payload_len);
            if (!session->payload_buf) return -1;
            session->payload_total = session->header.payload_len;
            session->state = SESSION_READING_PAYLOAD;
        } else {
            session->state = SESSION_PROCESSING;
        }
    }
    
    return 0;
}

static int handle_reading_payload(ClientSession *session) {
    ssize_t n = read_into_buffer(session->conn.sockfd, session->payload_buf,
                                  session->payload_bytes_read, session->payload_total);
    
    if (n < 0) {
        LOG_ERR("Error reading payload from fd %d", session->conn.sockfd);
        return -1;
    }
    if (n == 0) return 0;
    
    session->payload_bytes_read += n;
    
    if (session->payload_bytes_read == session->payload_total) {
        LOG_DEBUG("Payload received: %zu bytes", session->payload_total);
        session->state = SESSION_PROCESSING;
    }
    
    return 0;
}

static int handle_processing(ClientSession *session) {
    LOG_INFO("Processing RPC command: %d from %s", 
             session->header.command_type, session->header.origin_id);
    
    switch (session->header.command_type) {
        case RPC_CMD_JOIN_REQ: {
            if (session->payload_total != sizeof(JoinRequestPayload)) {
                LOG_ERR("Invalid JOIN_REQ payload size");
                return -1;
            }
            
            JoinRequestPayload *req = (JoinRequestPayload *)session->payload_buf;
            LOG_INFO("JOIN_REQ from node %s at %s:%d", 
                     req->node_id, req->ip_address, req->tcp_port);
            
            ClusterManager *cm = (ClusterManager *)session->app_context;
            cluster_handle_join_req(cm, req, &session->conn);

            RpcHeader resp_header = {
                .magic = RPC_MAGIC,
                .version = 1,
                .command_type = RPC_CMD_JOIN_RESP,
                .payload_len = 0
            };
            strncpy(resp_header.origin_id, session->header.origin_id, 
                    sizeof(resp_header.origin_id));
            
            session->response_len = sizeof(RpcHeader);
            session->response_buf = malloc(session->response_len);
            if (!session->response_buf) return -1;
            
            serializer_pack_rpc(&resp_header, session->response_buf);
            session->state = SESSION_SENDING_RESPONSE;
            
            loop_mod_fd(session->loop, session->conn.sockfd, 
                       EVENT_WRITE, rpc_on_client_event, session);
            break;
        }
        
        case RPC_CMD_LEAVE: {
            LOG_INFO("LEAVE request from %s", session->header.origin_id);
            session->state = SESSION_DONE;
            break;
        }
        
        default:
            LOG_WARN("Unknown RPC command: %d", session->header.command_type);
            return -1;
    }
    
    return 0;
}

static int handle_sending_response(ClientSession *session) {
    ssize_t n = write_from_buffer(session->conn.sockfd, session->response_buf,
                                   session->response_bytes_sent, session->response_len);
    
    if (n < 0) {
        LOG_ERR("Error sending response to fd %d", session->conn.sockfd);
        return -1;
    }
    if (n == 0) return 0;
    
    session->response_bytes_sent += n;
    
    if (session->response_bytes_sent == session->response_len) {
        LOG_DEBUG("Response sent: %zu bytes", session->response_len);
        session->state = SESSION_DONE;
    }
    
    return 0;
}

// ============================================================================
// PUBLIC ASYNC API
// ============================================================================

void rpc_on_client_event(void *context, int fd, uint32_t events) {
    ClientSession *session = (ClientSession *)context;
    
    if (events & EVENT_ERROR) {
        LOG_WARN("Error on client fd %d, closing", fd);
        session_destroy(session);
        return;
    }
    
    int result = 0;
    
    switch (session->state) {
        case SESSION_READING_HEADER:
            if (events & EVENT_READ) result = handle_reading_header(session);
            break;
        case SESSION_READING_PAYLOAD:
            if (events & EVENT_READ) result = handle_reading_payload(session);
            break;
        case SESSION_PROCESSING:
            result = handle_processing(session);
            break;
        case SESSION_SENDING_RESPONSE:
            if (events & EVENT_WRITE) result = handle_sending_response(session);
            break;
        case SESSION_DONE:
            result = -1;
            break;
    }
    
    if (result < 0 || session->state == SESSION_DONE) {
        LOG_DEBUG("Session complete for fd %d", fd);
        session_destroy(session);
    }
}

void rpc_on_accept(void *context, int server_fd, uint32_t events) {
    (void)events;
    
    typedef struct {
        EventLoop *loop;
        void *app_context;
    } ServerContext;
    
    ServerContext *srv_ctx = (ServerContext *)context;
    
    TcpConnection client;
    int ret = tcp_server_accept(server_fd, &client);
    
    if (ret == -2) return;  // EAGAIN
    if (ret != 0) {
        LOG_ERR("Failed to accept connection");
        return;
    }
    
    LOG_INFO("Accepted connection from %s:%d (fd=%d)", 
             client.remote_ip, client.remote_port, client.sockfd);
    
    extern int net_set_nonblocking(int fd);
    if (net_set_nonblocking(client.sockfd) < 0) {
        LOG_ERR("Failed to set client socket non-blocking");
        tcp_close(&client);
        return;
    }
    
    ClientSession *session = session_create(client.sockfd, srv_ctx->loop, 
                                           srv_ctx->app_context);
    if (!session) {
        LOG_ERR("Failed to create client session");
        tcp_close(&client);
        return;
    }
    
    session->conn = client;
    
    if (loop_add_fd(srv_ctx->loop, client.sockfd, EVENT_READ, 
                    rpc_on_client_event, session) != 0) {
        LOG_ERR("Failed to register client with event loop");
        session_destroy(session);
        return;
    }
    
    LOG_DEBUG("Client session registered with event loop");
}