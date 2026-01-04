#include "../include/net/tcp_transport.h"
#include <stdio.h>
#include <string.h>
#include <assert.h>
#include <unistd.h>
#include <sys/wait.h>

// Simple test protocol: Send "PING", expect "PONG"
int main() {
    int port = 9090;
    const char *ip = "127.0.0.1";
    pid_t pid = fork();

    if (pid == 0) {
        // Child Process: TCP Server
        printf("[Server] Starting...\n");
        int s_fd = tcp_server_listen(ip, port, 1);
        assert(s_fd >= 0);

        TcpConnection client;
        assert(tcp_server_accept(s_fd, &client) == 0);
        printf("[Server] Client connected from %s\n", client.remote_ip);

        char buf[5];
        assert(tcp_recv_all(&client, buf, 4) == 0);
        buf[4] = '\0';
        printf("[Server] Received: %s\n", buf);
        assert(strcmp(buf, "PING") == 0);

        assert(tcp_send_all(&client, "PONG", 4) == 0);
        printf("[Server] Sent PONG\n");

        tcp_close(&client);
        close(s_fd);
        printf("[Server] Done.\n");
        return 0;
    } else {
        // Parent Process: TCP Client
        sleep(1); // Give server time to bind
        printf("[Client] Connecting...\n");
        
        TcpConnection conn;
        assert(tcp_client_connect(ip, port, &conn) == 0);

        assert(tcp_send_all(&conn, "PING", 4) == 0);
        printf("[Client] Sent PING\n");

        char buf[5];
        assert(tcp_recv_all(&conn, buf, 4) == 0);
        buf[4] = '\0';
        printf("[Client] Received: %s\n", buf);
        assert(strcmp(buf, "PONG") == 0);

        tcp_close(&conn);
        wait(NULL); // Wait for child
        printf("TCP Test Passed.\n");
    }
    return 0;
}