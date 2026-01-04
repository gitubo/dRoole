#ifndef UDP_TRANSPORT_H
#define UDP_TRANSPORT_H

#include <stdint.h>
#include <stddef.h>

typedef struct {
    int sockfd;
    uint16_t bind_port;
} UdpSocket;

// Initialize UDP socket, bind it, and set to non-blocking mode
int udp_init(const char *ip, uint16_t port, UdpSocket *sock);

// Send data to specific destination (stateless)
int udp_send(UdpSocket *sock, const char *dest_ip, uint16_t dest_port, const void *data, size_t len);

// Receive data with sender info, handles EAGAIN internally
int udp_recv(UdpSocket *sock, void *buf, size_t max_len, char *sender_ip, uint16_t *sender_port);

// Cleanup socket resources
void udp_close(UdpSocket *sock);

#endif