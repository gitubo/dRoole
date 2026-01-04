#include "../../include/structures/types.h"
#include <stdlib.h>
#include <string.h>

#define MAX_KV_ENTRIES 1024

static RuleConfig *local_store[MAX_KV_ENTRIES];
static int store_count = 0;

/**
 * Adds a rule to the local eventually-consistent store.
 * (This would be called by the Gossip/Config sync layer).
 */
int kv_update_rule(const char *key, Rule *new_rules) {
    for (int i = 0; i < store_count; i++) {
        if (strcmp(local_store[i]->key, key) == 0) {
            local_store[i]->rule_head = new_rules;
            return 0;
        }
    }

    if (store_count < MAX_KV_ENTRIES) {
        RuleConfig *entry = malloc(sizeof(RuleConfig));
        strncpy(entry->key, key, 63);
        entry->rule_head = new_rules;
        local_store[store_count++] = entry;
        return 0;
    }
    return -1;
}

/**
 * Finds rules associated with a message type.
 */
Rule* kv_get_rules(const char *key) {
    for (int i = 0; i < store_count; i++) {
        if (strcmp(local_store[i]->key, key) == 0) {
            return local_store[i]->rule_head;
        }
    }
    return NULL;
}