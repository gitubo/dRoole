#include "cluster/kv_store.h"
#include <stdlib.h>
#include <string.h>

#define MAX_KV_ENTRIES 1024

static RuleConfig *store[MAX_KV_ENTRIES];
static int store_count = 0;

int kv_init(void) {
    memset(store, 0, sizeof(store));
    store_count = 0;
    return 0;
}

int kv_update_rule(const char *key, Rule *rules) {
    for (int i = 0; i < store_count; i++) {
        if (strcmp(store[i]->key, key) == 0) {
            store[i]->rule_head = rules;
            return 0;
        }
    }

    if (store_count >= MAX_KV_ENTRIES)
        return -1;

    RuleConfig *cfg = malloc(sizeof(RuleConfig));
    if (!cfg) return -1;

    memset(cfg, 0, sizeof(*cfg));
    strncpy(cfg->key, key, sizeof(cfg->key) - 1);
    cfg->rule_head = rules;

    store[store_count++] = cfg;
    return 0;
}

Rule *kv_get_rules(const char *key) {
    for (int i = 0; i < store_count; i++) {
        if (strcmp(store[i]->key, key) == 0) {
            return store[i]->rule_head;
        }
    }
    return NULL;
}

void kv_destroy(void) {
    for (int i = 0; i < store_count; i++) {
        free(store[i]);
    }
    store_count = 0;
}
