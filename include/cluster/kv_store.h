#ifndef KV_STORE_H
#define KV_STORE_H

#include <stddef.h>
#include "../common/types.h"

/*
 * Key → RuleConfig store
 * Used by Control nodes to manage rules
 * Used by Worker nodes to apply synced rules
 */

int kv_init(void);

/*
 * Update or insert rules for a given key
 * Ownership of rule list remains with caller
 */
int kv_update_rule(const char *key, Rule *rules);

/*
 * Retrieve rules for a key
 * Returns NULL if key not found
 */
Rule *kv_get_rules(const char *key);

/*
 * Free all stored rules (shutdown only)
 */
void kv_destroy(void);

#endif
