#ifndef RPC_PROTOCOL_H
#define RPC_PROTOCOL_H

#include "../structures/types.h"
#include "../net/tcp_transport.h"
#include "protocol_defs.h" // Essential for RpcHeader

int rpc_send_message(TcpConnection *conn, RpcHeader *header, const void *payload);
int rpc_recv_message(TcpConnection *conn, RpcHeader *header, void **payload_out);

#endif