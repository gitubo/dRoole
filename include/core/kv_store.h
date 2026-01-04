#ifndef KV_STORE_H
#define KV_STORE_H

#include <stdint.h>
#include <stddef.h>

typedef struct {
    char *key;
    char *value;
} kv_entry_t;

// Basic operations for the Control Plane state machine
int kv_init();
int kv_set(const char *key, const char *value);
char* kv_get(const char *key);
void kv_destroy();

#endif