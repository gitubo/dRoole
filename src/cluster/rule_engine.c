#include "../../include/common/types.h"
#include <stdio.h>
#include <string.h>

/**
 * Executes a chain of rules against a payload.
 * For this implementation, we will simulate the execution.
 */
int execute_rules(const char *message_type, const void *payload, size_t payload_len, Rule *rule_chain) {
    (void)payload;
    
    printf("Rule Engine: Processing message type [%s]\n", message_type);
    
    Rule *current = rule_chain;
    int rule_count = 0;

    while (current != NULL) {
        rule_count++;
        // Logic simulation: In a real system, this might be a regex match 
        // or a call to a Lua script/WASM module.
        printf("  -> Applying Rule #%d: %s\n", rule_count, current->rule_definition);
        
        // Example logic: if rule says "REJECT", we stop.
        if (strstr(current->rule_definition, "REJECT") != NULL) {
            printf("  !! Rule Triggered REJECT. Stopping.\n");
            return -1;
        }

        current = current->next;
    }

    printf("Rule Engine: Successfully applied %d rules to %zu bytes.\n", rule_count, payload_len);
    return 0;
}