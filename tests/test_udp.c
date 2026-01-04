#ifndef _DEFAULT_SOURCE
#define _DEFAULT_SOURCE
#endif

#include "../include/transport/udp_transport.h"
#include <stdio.h>
#include <string.h>
#include <assert.h>
#include <unistd.h>

/**
 * Validates the non-blocking UDP transport alignment.
 * Tests loopback initialization, stateless send, and non-blocking receive.
 */
int main() {
    UdpSocket s_sock;
    const char *ip = "127.0.0.1";
    uint16_t port = 9091;
    const char *msg = "UDP_NONBLOCK_TEST";
    
    // 1. Initialize socket (sets O_NONBLOCK internally)
    assert(udp_init(ip, port, &s_sock) == 0);

    // 2. Test Stateless Send (Send to self)
    int sent = udp_send(&s_sock, ip, port, msg, strlen(msg));
    assert(sent == (int)strlen(msg));

    // 3. Test Non-blocking Receive with Polling
    char rx_buf[64];
    char sender_ip[46];
    uint16_t sender_port;
    int received = 0;
    int retries = 0;

    while (retries < 100) {
        received = udp_recv(&s_sock, rx_buf, sizeof(rx_buf) - 1, sender_ip, &sender_port);
        if (received > 0) break; 
        
        // If received is 0, it means EAGAIN (no data yet)
        usleep(1000); 
        retries++;
    }

    assert(received == (int)strlen(msg));
    rx_buf[received] = '\0';
    assert(strcmp(rx_buf, msg) == 0);
    assert(sender_port == port);

    udp_close(&s_sock);
    printf("[Test] UDP Transport Alignment: PASSED\n");
    
    return 0;
}