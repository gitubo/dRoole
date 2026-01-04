#include "../../include/net/udp_transport.h"
#include "../../include/net/net_utils.h"
#include <sys/socket.h>
#include <arpa/inet.h>
#include <unistd.h>
#include <string.h>
#include <errno.h>

int udp_init(const char *ip, uint16_t port, UdpSocket *sock) {
    int fd = socket(AF_INET, SOCK_DGRAM, 0);
    if (fd < 0) return -1;

    // Apply Non-Blocking immediately to align with EventLoop architecture
    if (net_set_nonblocking(fd) < 0) {
        close(fd);
        return -1;
    }

    struct sockaddr_in addr;
    memset(&addr, 0, sizeof(addr));
    addr.sin_family = AF_INET;
    addr.sin_port = htons(port);
    if (inet_pton(AF_INET, ip, &addr.sin_addr) <= 0) {
        close(fd);
        return -1;
    }

    if (bind(fd, (struct sockaddr *)&addr, sizeof(addr)) < 0) {
        close(fd);
        return -1;
    }

    sock->sockfd = fd;
    sock->bind_port = port;
    return 0;
}

int udp_send(UdpSocket *sock, const char *dest_ip, uint16_t dest_port, const void *data, size_t len) {
    struct sockaddr_in addr;
    memset(&addr, 0, sizeof(addr));
    addr.sin_family = AF_INET;
    addr.sin_port = htons(dest_port);
    inet_pton(AF_INET, dest_ip, &addr.sin_addr);

    // cast to void* is implicit in C, strict aliasing safe for char* data
    return sendto(sock->sockfd, data, len, 0, (struct sockaddr *)&addr, sizeof(addr));
}

int udp_recv(UdpSocket *sock, void *buf, size_t max_len, char *sender_ip, uint16_t *sender_port) {
    struct sockaddr_in addr;
    socklen_t addr_len = sizeof(addr);

    ssize_t ret = recvfrom(sock->sockfd, buf, max_len, 0, (struct sockaddr *)&addr, &addr_len);

    if (ret < 0) {
        // Return 0 if no data is available (EAGAIN), -1 for actual errors
        if (errno == EAGAIN || errno == EWOULDBLOCK) return 0;
        return -1;
    }

    inet_ntop(AF_INET, &addr.sin_addr, sender_ip, 46);
    *sender_port = ntohs(addr.sin_port);
    return ret;
}

void udp_close(UdpSocket *sock) {
    if (sock->sockfd >= 0) {
        close(sock->sockfd);
        sock->sockfd = -1;
    }
}