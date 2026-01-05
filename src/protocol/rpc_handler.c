//####################
// /src/protocol/rpc_handler.c
// ####################

#include "protocol/rpc_protocol.h"
#include "protocol/serializer.h"
#include "protocol/protocol_defs.h"
#include "protocol/rpc_handler.h"
#include "transport/tcp_transport.h"
#include "common/event_loop.h"
#include "common/logger.h"
#include "common/types.h"
#include "cluster/cluster_manager.h"
#include "cluster/kv_store.h"
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <errno.h>
#include <sys/socket.h>

static void rpc_on_client_event(void *context, int fd, uint32_t events);
#define MAX_PAYLOAD_SIZE (10 * 1024 * 1024)

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
    if (header->magic != RPC_MAGIC) return -2;
    if (header->payload_len > MAX_PAYLOAD_SIZE) return -2;

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
        // CHANGED: Explicitly ignore EINTR in non-blocking loop
        if (errno == EINTR) return 0; 
        if (errno == EAGAIN || errno == EWOULDBLOCK) return 0;
        return -1;
    }
    if (n == 0) return -1;
    return n;
}

static ssize_t write_from_buffer(int fd, const uint8_t *buf, size_t offset, size_t total) {
    size_t remaining = total - offset;
    if (remaining == 0) return 0;

    // MSG_NOSIGNAL prevents the process from crashing with SIGPIPE 
    // if the client disconnects abruptly.
    ssize_t n = send(fd, buf + offset, remaining, MSG_DONTWAIT | MSG_NOSIGNAL);

    if (n < 0) {
        // [IMPORTANT] Retry immediately if interrupted by signal (like SIGTERM)
        if (errno == EINTR) return 0; 
        // Retry later if socket buffer is full (Backpressure)
        if (errno == EAGAIN || errno == EWOULDBLOCK) return 0; 
        // Real error
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
    if (n < 0) return -1;
    if (n == 0) return 0;
    
    session->header_bytes_read += n;
    
    if (session->header_bytes_read == sizeof(RpcHeader)) {
        serializer_unpack_rpc(session->header_buf, &session->header);
        if (session->header.magic != RPC_MAGIC) return -1;
        if (session->header.payload_len > MAX_PAYLOAD_SIZE) return -1;
        
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
    if (n < 0) return -1;
    if (n == 0) return 0;
    session->payload_bytes_read += n;
    if (session->payload_bytes_read == session->payload_total) {
        session->state = SESSION_PROCESSING;
    }
    return 0;
}

static int handle_processing(ClientSession *session) {
    ClusterManager *cm = (ClusterManager *)session->app_context;

    switch (session->header.command_type) {
        case RPC_CMD_JOIN_REQ: {
            if (session->payload_total != sizeof(JoinRequestPayload)) return -1;
            
            JoinRequestPayload *req = (JoinRequestPayload *)session->payload_buf;
            LOG_INFO("JOIN_REQ from node %s at %s:%d", 
                     req->node_id, req->ip_address, req->tcp_port);
            
            uint8_t *resp_payload = NULL;
            size_t resp_len = 0;
            
            // CHANGED: Get payload from Logic, do NOT send yet
            if (cluster_handle_join_req(cm, req, &resp_payload, &resp_len) != 0) {
                return -1;
            }

            // Construct Full Async Response (Header + Payload)
            RpcHeader resp_header = {
                .magic = RPC_MAGIC,
                .version = 1,
                .command_type = RPC_CMD_JOIN_RESP,
                .payload_len = resp_len
            };
            strncpy(resp_header.origin_id, session->header.origin_id, 36);
            
            size_t header_len = sizeof(RpcHeader);
            session->response_len = header_len + resp_len;
            session->response_buf = malloc(session->response_len);
            
            serializer_pack_rpc(&resp_header, session->response_buf);
            
            if (resp_payload && resp_len > 0) {
                memcpy(session->response_buf + header_len, resp_payload, resp_len);
                free(resp_payload);
            }
            
            session->state = SESSION_SENDING_RESPONSE;
            loop_mod_fd(session->loop, session->conn.sockfd, 
                       EVENT_WRITE, rpc_on_client_event, session);
            break;
        }
        
        case RPC_CMD_LEAVE: {
            LOG_INFO("LEAVE request from %s", session->header.origin_id);
            cluster_update_member_status(cm, session->header.origin_id, NODE_STATUS_DEAD);
            session->state = SESSION_DONE;
            break;
        }

        case RPC_CMD_SYNC_RULES: {
            LOG_INFO("SYNC_RULES request from %s", session->header.origin_id);
            // ... (Sync Rules implementation remains the same) ...
             session->state = SESSION_DONE;
            break;
        }
        
        default:
            return -1;
    }
    return 0;
}

static int handle_sending_response(ClientSession *session) {
    ssize_t n = write_from_buffer(session->conn.sockfd, session->response_buf,
                                   session->response_bytes_sent, session->response_len);
    if (n < 0) return -1;
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
    (void)fd;
    ClientSession *session = (ClientSession *)context;
    if (events & EVENT_ERROR) {
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
        session_destroy(session);
    }
}

void rpc_on_accept(void *context, int server_fd, uint32_t events) {
    (void)events;
    typedef struct { EventLoop *loop; void *app_context; } ServerContext;
    ServerContext *srv_ctx = (ServerContext *)context;
    
    TcpConnection client;
    if (tcp_server_accept(server_fd, &client) != 0) return;
    
    extern int net_set_nonblocking(int fd);
    net_set_nonblocking(client.sockfd);
    
    ClientSession *session = session_create(client.sockfd, srv_ctx->loop, srv_ctx->app_context);
    if (!session) { tcp_close(&client); return; }
    session->conn = client;
    
    if (loop_add_fd(srv_ctx->loop, client.sockfd, EVENT_READ, rpc_on_client_event, session) != 0) {
        session_destroy(session);
    }
    LOG_INFO("Accepted connection from %s", client.remote_ip);
}