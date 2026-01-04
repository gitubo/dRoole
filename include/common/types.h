#ifndef TYPES_H
#define TYPES_H

#include <stdint.h>
#include <stddef.h>

// 1. Enums
typedef enum {
    ROLE_UNKNOWN = 0,
    ROLE_CONTROL_NODE = 1,
    ROLE_WORKER_NODE = 2
} NodeRole;

typedef enum {
    NODE_STATUS_ALIVE = 0,
    NODE_STATUS_SUSPECT = 1,
    NODE_STATUS_DEAD = 2
} NodeStatus;

// 3. Node Information
typedef struct {
    char node_id[37];
    char ip_address[46];
    uint16_t tcp_port;
    uint16_t udp_port;
    NodeRole role;
    NodeStatus status;
    uint8_t cpu_load;
    uint8_t mem_load;
    uint64_t last_updated_ts;
} NodeInfo;

// 2. Base Structures
typedef struct Rule {
    char *rule_definition;
    size_t rule_definition_len;
    struct Rule *next;
} Rule;

typedef struct {
    char key[64];
    Rule *rule_head;
} RuleConfig;


#endif