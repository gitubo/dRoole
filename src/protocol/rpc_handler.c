//####################
// /src/protocol/rpc_handler.c - COMPLETE ENHANCED VERSION
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

// Forward declaration
static void rpc_on_client_event(void *context, int fd, uint32_t events);

// Maximum payload size to prevent memory exhaustion attacks
#define MAX_PAYLOAD_SIZE (10 * 1024 * 1024)

// ============================================================================
// CLIENT-SIDE FUNCTIONS (Synchronous - for outgoing requests)
// ============================================================================

/**
 * Send an RPC message (blocking, used by client code)
 */
int rpc_send_message(TcpConnection *conn, RpcHeader *header, const void *payload) {
    if (tcp_send_all(conn, header, sizeof(RpcHeader)) != 0) {
        LOG_ERR("Failed to send RPC header");
        return -1;
    }
    if (header->payload_len > 0 && payload != NULL) {
        if (tcp_send_all(conn, payload, header->payload_len) != 0) {
            LOG_ERR("Failed to send RPC payload (%zu bytes)", (size_t)header->payload_len);
            return -1;
        }
    }
    LOG_DEBUG("Sent RPC: cmd=%d, payload_len=%zu", header->command_type, (size_t)header->payload_len);
    return 0;
}

/**
 * Receive an RPC message (blocking, used by client code)
 */
int rpc_recv_message(TcpConnection *conn, RpcHeader *header, void **payload_out) {
    if (tcp_recv_all(conn, header, sizeof(RpcHeader)) != 0) {
        LOG_ERR("Failed to receive RPC header");
        return -1;
    }
    if (header->magic != RPC_MAGIC) {
        LOG_ERR("Invalid RPC magic: 0x%x (expected 0x%x)", header->magic, RPC_MAGIC);
        return -2;
    }
    if (header->payload_len > MAX_PAYLOAD_SIZE) {
        LOG_ERR("Payload too large: %zu > %d", (size_t)header->payload_len, MAX_PAYLOAD_SIZE);
        return -2;
    }

    if (header->payload_len > 0) {
        *payload_out = malloc(header->payload_len);
        if (!*payload_out) {
            LOG_ERR("Failed to allocate %zu bytes for payload", (size_t)header->payload_len);
            return -1;
        }
        if (tcp_recv_all(conn, *payload_out, header->payload_len) != 0) {
            LOG_ERR("Failed to receive payload");
            free(*payload_out);
            *payload_out = NULL;
            return -1;
        }
    } else {
        *payload_out = NULL;
    }
    LOG_DEBUG("Received RPC: cmd=%d, payload_len=%zu", header->command_type, (size_t)header->payload_len);
    return 0;
}

// ============================================================================
// SERVER-SIDE ASYNC SESSION MANAGEMENT (for event loop)
// ============================================================================

/**
 * State machine for async RPC processing
 */
typedef enum {
    SESSION_READING_HEADER,     // Reading RpcHeader
    SESSION_READING_PAYLOAD,    // Reading payload bytes
    SESSION_PROCESSING,         // Processing request and preparing response
    SESSION_SENDING_RESPONSE,   // Sending response back to client
    SESSION_DONE                // Finished, ready to cleanup
} SessionState;

/**
 * Represents an active client connection being processed asynchronously
 */
typedef struct ClientSession {
    TcpConnection conn;              // TCP connection details
    SessionState state;              // Current state in the processing pipeline
    
    // Header reading state
    uint8_t header_buf[sizeof(RpcHeader)];
    size_t header_bytes_read;
    
    // Payload reading state
    uint8_t *payload_buf;
    size_t payload_bytes_read;
    size_t payload_total;
    
    // Parsed header
    RpcHeader header;
    
    // Response state
    uint8_t *response_buf;
    size_t response_len;
    size_t response_bytes_sent;
    
    // Context
    void *app_context;               // Pointer to ClusterManager
    EventLoop *loop;                 // Event loop for callbacks
} ClientSession;

// ============================================================================
// INTERNAL HELPER FUNCTIONS
// ============================================================================

/**
 * Convert session state to string for logging
 */
static const char* state_to_string(SessionState state) {
    switch(state) {
        case SESSION_READING_HEADER: return "READING_HEADER";
        case SESSION_READING_PAYLOAD: return "READING_PAYLOAD";
        case SESSION_PROCESSING: return "PROCESSING";
        case SESSION_SENDING_RESPONSE: return "SENDING_RESPONSE";
        case SESSION_DONE: return "DONE";
        default: return "UNKNOWN";
    }
}

/**
 * Non-blocking read into buffer
 * Returns: bytes read (>0), 0 if would block, -1 on error/close
 */
static ssize_t read_into_buffer(int fd, uint8_t *buf, size_t offset, size_t total) {
    size_t remaining = total - offset;
    if (remaining == 0) return 0;
    
    ssize_t n = recv(fd, buf + offset, remaining, MSG_DONTWAIT);
    if (n < 0) {
        if (errno == EINTR) return 0;  // Interrupted by signal, retry later
        if (errno == EAGAIN || errno == EWOULDBLOCK) return 0;  // Would block
        LOG_ERR("recv() error on fd=%d: %s", fd, strerror(errno));
        return -1;
    }
    if (n == 0) {
        LOG_DEBUG("Client closed connection (EOF) on fd=%d", fd);
        return -1;  // Connection closed
    }
    return n;
}

/**
 * Non-blocking write from buffer
 * Returns: bytes written (>0), 0 if would block, -1 on error
 */
static ssize_t write_from_buffer(int fd, const uint8_t *buf, size_t offset, size_t total) {
    size_t remaining = total - offset;
    if (remaining == 0) return 0;

    // MSG_NOSIGNAL prevents SIGPIPE if client disconnects
    ssize_t n = send(fd, buf + offset, remaining, MSG_DONTWAIT | MSG_NOSIGNAL);

    if (n < 0) {
        if (errno == EINTR) return 0;  // Interrupted by signal, retry
        if (errno == EAGAIN || errno == EWOULDBLOCK) return 0;  // Would block
        LOG_ERR("send() error on fd=%d: %s", fd, strerror(errno));
        return -1;
    }
    return n;
}

/**
 * Create a new client session
 */
ClientSession* session_create(int client_fd, EventLoop *loop, void *app_context) {
    ClientSession *session = calloc(1, sizeof(ClientSession));
    if (!session) {
        LOG_ERR("Failed to allocate ClientSession");
        return NULL;
    }
    session->conn.sockfd = client_fd;
    session->state = SESSION_READING_HEADER;
    session->loop = loop;
    session->app_context = app_context;
    LOG_DEBUG("Session created for fd=%d", client_fd);
    return session;
}

/**
 * Destroy a client session and free all resources
 */
void session_destroy(ClientSession *session) {
    if (!session) return;
    
    int fd = session->conn.sockfd;
    LOG_DEBUG("Destroying session for fd=%d (state=%s)", fd, state_to_string(session->state));
    
    // Free allocated buffers
    if (session->payload_buf) free(session->payload_buf);
    if (session->response_buf) free(session->response_buf);
    
    // Remove from event loop and close connection
    if (session->loop && session->conn.sockfd >= 0) {
        loop_del_fd(session->loop, session->conn.sockfd);
    }
    tcp_close(&session->conn);
    
    free(session);
}

// ============================================================================
// STATE HANDLERS - Each function handles one state in the state machine
// ============================================================================

/**
 * Handle reading the RPC header (91 bytes)
 */
static int handle_reading_header(ClientSession *session) {
    ssize_t n = read_into_buffer(session->conn.sockfd, session->header_buf,
                                  session->header_bytes_read, sizeof(RpcHeader));
    if (n < 0) return -1;  // Error or connection closed
    if (n == 0) return 0;   // Would block, try again later
    
    session->header_bytes_read += n;
    LOG_DEBUG("Read %zd bytes of header (%zu/%zu)", n, session->header_bytes_read, sizeof(RpcHeader));
    
    if (session->header_bytes_read == sizeof(RpcHeader)) {
        // Complete header received, deserialize it
        serializer_unpack_rpc(session->header_buf, &session->header);
        
        LOG_DEBUG("Header complete: magic=0x%x, cmd=%d, payload_len=%zu", 
                  session->header.magic, session->header.command_type, 
                  (size_t)session->header.payload_len);
        
        // Validate header
        if (session->header.magic != RPC_MAGIC) {
            LOG_ERR("Invalid magic in header: 0x%x (expected 0x%x)", 
                    session->header.magic, RPC_MAGIC);
            return -1;
        }
        if (session->header.payload_len > MAX_PAYLOAD_SIZE) {
            LOG_ERR("Payload too large: %zu bytes", (size_t)session->header.payload_len);
            return -1;
        }
        
        // Transition to next state
        if (session->header.payload_len > 0) {
            session->payload_buf = malloc(session->header.payload_len);
            if (!session->payload_buf) {
                LOG_ERR("Failed to allocate %zu bytes for payload", 
                        (size_t)session->header.payload_len);
                return -1;
            }
            session->payload_total = session->header.payload_len;
            session->state = SESSION_READING_PAYLOAD;
            LOG_DEBUG("Transitioning to READING_PAYLOAD");
        } else {
            session->state = SESSION_PROCESSING;
            LOG_DEBUG("No payload, transitioning to PROCESSING");
        }
    }
    return 0;
}

/**
 * Handle reading the payload bytes
 */
static int handle_reading_payload(ClientSession *session) {
    ssize_t n = read_into_buffer(session->conn.sockfd, session->payload_buf,
                                  session->payload_bytes_read, session->payload_total);
    if (n < 0) return -1;
    if (n == 0) return 0;
    
    session->payload_bytes_read += n;
    LOG_DEBUG("Read %zd bytes of payload (%zu/%zu)", n, 
              session->payload_bytes_read, session->payload_total);
    
    if (session->payload_bytes_read == session->payload_total) {
        session->state = SESSION_PROCESSING;
        LOG_DEBUG("Payload complete, transitioning to PROCESSING");
    }
    return 0;
}

/**
 * Handle processing the request and preparing the response
 */
static int handle_processing(ClientSession *session) {
    ClusterManager *cm = (ClusterManager *)session->app_context;
    
    LOG_DEBUG("Processing command: %d", session->header.command_type);

    switch (session->header.command_type) {
        case RPC_CMD_JOIN_REQ: {
            // Validate payload size
            if (session->payload_total != sizeof(JoinRequestPayload)) {
                LOG_ERR("JOIN_REQ payload size mismatch: got %zu, expected %zu",
                        session->payload_total, sizeof(JoinRequestPayload));
                return -1;
            }
            
            JoinRequestPayload *req = (JoinRequestPayload *)session->payload_buf;
            LOG_INFO("JOIN_REQ from node %s at %s:%d", 
                     req->node_id, req->ip_address, req->tcp_port);
            
            // Call cluster logic to handle join (returns malloc'd payload)
            uint8_t *resp_payload = NULL;
            size_t resp_len = 0;
            
            if (cluster_handle_join_req(cm, req, &resp_payload, &resp_len) != 0) {
                LOG_ERR("cluster_handle_join_req() failed");
                return -1;
            }
            
            LOG_DEBUG("Prepared JOIN_RESP payload: %zu bytes", resp_len);

            // Construct response header
            RpcHeader resp_header = {
                .magic = RPC_MAGIC,
                .version = 1,
                .command_type = RPC_CMD_JOIN_RESP,
                .payload_len = resp_len
            };
            strncpy(resp_header.origin_id, session->header.origin_id, 36);
            resp_header.origin_id[36] = '\0';
            
            // Allocate buffer for full response (header + payload)
            size_t header_len = sizeof(RpcHeader);
            session->response_len = header_len + resp_len;
            session->response_buf = malloc(session->response_len);
            
            if (!session->response_buf) {
                LOG_ERR("Failed to allocate %zu bytes for response", session->response_len);
                if (resp_payload) free(resp_payload);
                return -1;
            }
            
            // Serialize header into response buffer
            serializer_pack_rpc(&resp_header, session->response_buf);
            
            // Copy payload into response buffer
            if (resp_payload && resp_len > 0) {
                memcpy(session->response_buf + header_len, resp_payload, resp_len);
                free(resp_payload);
            }
            
            session->state = SESSION_SENDING_RESPONSE;
            LOG_DEBUG("Transitioning to SENDING_RESPONSE (%zu bytes total)", session->response_len);
            
            // Switch from monitoring reads to monitoring writes
            if (loop_mod_fd(session->loop, session->conn.sockfd, 
                       EVENT_WRITE, rpc_on_client_event, session) != 0) {
                LOG_ERR("loop_mod_fd() failed for WRITE");
                return -1;
            }
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
            
            // Parse SyncRulesHeader
            if (session->payload_total < sizeof(SyncRulesHeader)) {
                LOG_ERR("SYNC_RULES payload too small");
                return -1;
            }
            
            SyncRulesHeader *sync_hdr = (SyncRulesHeader *)session->payload_buf;
            LOG_DEBUG("Syncing rules for key: %s, count: %d", 
                      sync_hdr->key, sync_hdr->rule_count);
            
            // In a real implementation, we would:
            // 1. Parse the rule list from payload
            // 2. Update the KV store
            // 3. Send acknowledgment
            
            // For now, just acknowledge
            session->state = SESSION_DONE;
            break;
        }
        
        default:
            LOG_ERR("Unknown command type: %d", session->header.command_type);
            return -1;
    }
    return 0;
}

/**
 * Handle sending the response back to the client
 */
static int handle_sending_response(ClientSession *session) {
    ssize_t n = write_from_buffer(session->conn.sockfd, session->response_buf,
                                   session->response_bytes_sent, session->response_len);
    if (n < 0) return -1;
    if (n == 0) return 0;
    
    session->response_bytes_sent += n;
    LOG_DEBUG("Sent %zd bytes of response (%zu/%zu)", n, 
              session->response_bytes_sent, session->response_len);
    
    if (session->response_bytes_sent == session->response_len) {
        LOG_INFO("Response sent successfully: %zu bytes", session->response_len);
        session->state = SESSION_DONE;
    }
    return 0;
}

// ============================================================================
// PUBLIC ASYNC API - Called by event loop
// ============================================================================

/**
 * Event callback for client sessions (handles READ/WRITE/ERROR events)
 */
void rpc_on_client_event(void *context, int fd, uint32_t events) {
    (void)fd;
    ClientSession *session = (ClientSession *)context;
    
    // Handle errors first
    if (events & EVENT_ERROR) {
        LOG_WARN("EVENT_ERROR on fd=%d, destroying session", session->conn.sockfd);
        session_destroy(session);
        return;
    }
    
    LOG_DEBUG("Event on fd=%d: state=%s, events=0x%x", 
              session->conn.sockfd, state_to_string(session->state), events);

    // CHANGED: Loop to handle state transitions that don't require I/O wait (e.g., PROCESSING)
    int result = 0;
    int keep_processing = 1;

    while (keep_processing && result == 0) {
        keep_processing = 0; // Default: exit loop after one pass unless state requires immediate action

        switch (session->state) {
            case SESSION_READING_HEADER:
                if (events & EVENT_READ) {
                    result = handle_reading_header(session);
                    // Optimistic: If header read complete, we could try reading payload immediately,
                    // but for now, we only force loop if we hit PROCESSING.
                    if (session->state == SESSION_PROCESSING) keep_processing = 1;
                    else if (session->state == SESSION_READING_PAYLOAD) {
                         // Optional: could set keep_processing=1 here to try reading payload immediately
                    }
                }
                break;

            case SESSION_READING_PAYLOAD:
                if (events & EVENT_READ) {
                    result = handle_reading_payload(session);
                    // CRITICAL FIX: If payload finished, we moved to PROCESSING.
                    // We MUST loop again to execute processing logic immediately.
                    if (session->state == SESSION_PROCESSING) {
                        keep_processing = 1; 
                    }
                }
                break;

            case SESSION_PROCESSING:
                // Processing logic is CPU-bound and blocking, run immediately
                result = handle_processing(session);
                // After processing, we are in SENDING_RESPONSE or DONE.
                // We usually stop here to wait for the WRITE event (registered in handle_processing).
                if (session->state == SESSION_SENDING_RESPONSE) {
                    keep_processing = 1;
                }
                break;
            
            case SESSION_SENDING_RESPONSE:
                if (events & EVENT_WRITE) {
                    result = handle_sending_response(session);
                }
                break;

            case SESSION_DONE:
                LOG_DEBUG("Session in DONE state, destroying");
                result = -1;
                break;
        }
    }
    
    // If any handler returned error or we're done, cleanup
    if (result < 0) {
        if (result == -1) {
            LOG_DEBUG("Handler returned error or done, destroying session");
        }
        session_destroy(session);
    }
}

/**
 * Accept callback for the server socket
 * Called by event loop when a new connection arrives
 */
void rpc_on_accept(void *context, int server_fd, uint32_t events) {
    (void)events;
    
    // Context contains EventLoop and ClusterManager pointers
    typedef struct { 
        EventLoop *loop; 
        void *app_context; 
    } ServerContext;
    ServerContext *srv_ctx = (ServerContext *)context;
    
    // Accept the connection
    TcpConnection client;
    int accept_result = tcp_server_accept(server_fd, &client);
    
    // Check for EAGAIN (spurious wakeup)
    if (accept_result == -2) {
        LOG_DEBUG("accept() returned EAGAIN (spurious wakeup)");
        return;
    }
    
    if (accept_result != 0) {
        LOG_ERR("tcp_server_accept() failed with code %d", accept_result);
        return;
    }
    
    LOG_INFO("Accepted connection from %s:%d (fd=%d)", 
             client.remote_ip, client.remote_port, client.sockfd);
    
    // Set client socket to non-blocking mode
    extern int net_set_nonblocking(int fd);
    if (net_set_nonblocking(client.sockfd) < 0) {
        LOG_ERR("Failed to set non-blocking mode on fd=%d", client.sockfd);
        tcp_close(&client);
        return;
    }
    
    // Create session for this client
    ClientSession *session = session_create(client.sockfd, srv_ctx->loop, srv_ctx->app_context);
    if (!session) { 
        tcp_close(&client); 
        return; 
    }
    session->conn = client;
    
    // Register session with event loop
    if (loop_add_fd(srv_ctx->loop, client.sockfd, EVENT_READ, rpc_on_client_event, session) != 0) {
        LOG_ERR("loop_add_fd() failed for fd=%d", client.sockfd);
        session_destroy(session);
        return;
    }
    
    LOG_DEBUG("Session initialized for fd=%d", client.sockfd);
}