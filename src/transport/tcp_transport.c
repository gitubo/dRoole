#include "../../include/transport/tcp_transport.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <fcntl.h>
#include <arpa/inet.h>
#include <sys/socket.h>
#include <errno.h>

/**
 * Internal Helper: Set a file descriptor to non-blocking mode.
 * This is crucial for the Reactor pattern to prevent the event loop from freezing.
 */
int set_nonblocking(int fd) {
    int flags = fcntl(fd, F_GETFL, 0);
    if (flags == -1) return -1;
    return fcntl(fd, F_SETFL, flags | O_NONBLOCK);
}

/**
 * Initialize and Bind a TCP Server Socket.
 * Returns socket fd on success, -1 on error.
 */
int tcp_server_listen(const char *ip, uint16_t port, int backlog) {
    int fd = socket(AF_INET, SOCK_STREAM, 0);
    if (fd < 0) return -1;

    // Allow immediate reuse of the port (avoids "Address already in use" after restart)
    int opt = 1;
    if (setsockopt(fd, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt)) < 0) {
        perror("setsockopt(SO_REUSEADDR) failed");
        close(fd);
        return -1;
    }

    struct sockaddr_in addr;
    memset(&addr, 0, sizeof(addr));
    addr.sin_family = AF_INET;
    addr.sin_port = htons(port);
    
    if (inet_pton(AF_INET, ip, &addr.sin_addr) <= 0) {
        fprintf(stderr, "Invalid IP address: %s\n", ip);
        close(fd);
        return -1;
    }

    if (bind(fd, (struct sockaddr *)&addr, sizeof(addr)) < 0) {
        perror("bind failed");
        close(fd);
        return -1;
    }

    if (listen(fd, backlog) < 0) {
        perror("listen failed");
        close(fd);
        return -1;
    }

    return fd;
}

/**
 * Extended: Listen using Non-Blocking Mode.
 * Use this when initializing the server for the Event Loop.
 */
int tcp_server_listen_nonblock(const char *ip, uint16_t port, int backlog) {
    int fd = tcp_server_listen(ip, port, backlog);
    if (fd >= 0) {
        if (set_nonblocking(fd) < 0) {
            perror("Failed to set non-blocking mode on server socket");
            close(fd);
            return -1;
        }
    }
    return fd;
}

/**
 * Accept a new connection.
 * Fills *conn with details. Returns 0 on success, -1 on error.
 */
int tcp_server_accept(int server_fd, TcpConnection *conn) {
    struct sockaddr_in client_addr;
    socklen_t addr_len = sizeof(client_addr);
    
    // In a non-blocking reactor, accept() might return EAGAIN or EWOULDBLOCK
    // if no connection is actually pending (spurious wake-up).
    int client_fd = accept(server_fd, (struct sockaddr *)&client_addr, &addr_len);
    
    if (client_fd < 0) {
        if (errno == EAGAIN || errno == EWOULDBLOCK) {
            return -2; // Special code indicating "Try again later"
        }
        perror("accept failed");
        return -1;
    }

    // IMPORTANT: The new client socket usually starts as blocking.
    // For the reactor, you typically want to make this non-blocking immediately.
    // We leave it blocking by default here to preserve compatibility with sync code,
    // but the Event Loop callback should call set_nonblocking(client_fd).

    conn->sockfd = client_fd;
    conn->remote_port = ntohs(client_addr.sin_port);
    inet_ntop(AF_INET, &client_addr.sin_addr, conn->remote_ip, sizeof(conn->remote_ip));
    
    return 0;
}

/**
 * Connect to a server.
 * Returns 0 on success, -1 on error.
 */
int tcp_client_connect(const char *ip, uint16_t port, TcpConnection *conn) {
    int fd = socket(AF_INET, SOCK_STREAM, 0);
    if (fd < 0) return -1;

    struct sockaddr_in addr;
    memset(&addr, 0, sizeof(addr));
    addr.sin_family = AF_INET;
    addr.sin_port = htons(port);
    
    if (inet_pton(AF_INET, ip, &addr.sin_addr) <= 0) {
        close(fd);
        return -1;
    }

    if (connect(fd, (struct sockaddr *)&addr, sizeof(addr)) < 0) {
        // Note: In non-blocking mode, connect might return EINPROGRESS.
        // This basic implementation assumes blocking connect.
        close(fd);
        return -1;
    }

    conn->sockfd = fd;
    strncpy(conn->remote_ip, ip, sizeof(conn->remote_ip) - 1);
    conn->remote_ip[sizeof(conn->remote_ip) - 1] = '\0';
    conn->remote_port = port;
    return 0;
}

/**
 * Send exact number of bytes (handles partial writes).
 * Returns 0 on success, -1 on failure.
 */
int tcp_send_all(TcpConnection *conn, const void *data, size_t len) {
    const uint8_t *ptr = data;
    size_t remaining = len;
    
    while (remaining > 0) {
        ssize_t sent = send(conn->sockfd, ptr, remaining, 0);
        if (sent < 0) {
            if (errno == EINTR) continue; // Interrupted by signal, retry
            return -1; // Error
        }
        ptr += sent;
        remaining -= sent;
    }
    return 0;
}

/**
 * Receive exact number of bytes (handles partial reads).
 * Returns 0 on success, -1 on failure/close.
 */
int tcp_recv_all(TcpConnection *conn, void *buffer, size_t len) {
    uint8_t *ptr = buffer;
    size_t remaining = len;
    
    while (remaining > 0) {
        ssize_t received = recv(conn->sockfd, ptr, remaining, 0);
        
        if (received < 0) {
            if (errno == EINTR) continue;
            return -1;
        }
        if (received == 0) {
            return -1; // Connection closed by peer
        }
        
        ptr += received;
        remaining -= received;
    }
    return 0;
}

/**
 * Close connection.
 */
void tcp_close(TcpConnection *conn) {
    if (conn->sockfd >= 0) {
        close(conn->sockfd);
        conn->sockfd = -1;
    }
}