#ifndef RPC_HANDLER_H
#define RPC_HANDLER_H

#include "../common/event_loop.h"

/*
 * Accept callback for async RPC server
 * Context must contain:
 *  - EventLoop *
 *  - application context (e.g. ClusterManager *)
 */
void rpc_on_accept(void *context, int server_fd, uint32_t events);

#endif
