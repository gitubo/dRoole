#ifndef TCP_TRANSPORT_H
#define TCP_TRANSPORT_H

#include <stdint.h>
#include <stddef.h>

// Handle for a TCP connection
typedef struct {
    int sockfd;
    char remote_ip[46];
    uint16_t remote_port;
} TcpConnection;

// Server: Initialize and Bind
// Returns socket fd on success, -1 on error
int tcp_server_listen(const char *ip, uint16_t port, int backlog);

// Server: Accept a new connection
// Returns 0 on success, -1 on error. Fills *conn with details.
int tcp_server_accept(int server_fd, TcpConnection *conn);

// Client: Connect to a server
// Returns 0 on success, -1 on error. Fills *conn with details.
int tcp_client_connect(const char *ip, uint16_t port, TcpConnection *conn);

// Common: Send exact number of bytes (handles partial writes)
// Returns 0 on success, -1 on failure
int tcp_send_all(TcpConnection *conn, const void *data, size_t len);

// Common: Receive exact number of bytes (handles partial reads)
// Returns 0 on success, -1 on failure/close
int tcp_recv_all(TcpConnection *conn, void *buffer, size_t len);

// Common: Close connection
void tcp_close(TcpConnection *conn);

#endif