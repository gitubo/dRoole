#include "../include/cluster/raft.h"
#include "../include/common/logger.h"
#include "../include/common/utils.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <assert.h>
#include <unistd.h>

// ============================================================================
// TEST FRAMEWORK
// ============================================================================

#define TEST(name) static void test_##name()
#define RUN_TEST(name) do { \
    printf("Running: %s\n", #name); \
    test_##name(); \
    printf("  PASS: %s\n\n", #name); \
} while(0)

#define ASSERT(expr) do { \
    if (!(expr)) { \
        fprintf(stderr, "ASSERTION FAILED: %s\n  at %s:%d\n", \
                #expr, __FILE__, __LINE__); \
        exit(1); \
    } \
} while(0)

#define ASSERT_EQ(a, b) ASSERT((a) == (b))
#define ASSERT_NEQ(a, b) ASSERT((a) != (b))
#define ASSERT_TRUE(expr) ASSERT(expr)
#define ASSERT_FALSE(expr) ASSERT(!(expr))
#define ASSERT_STREQ(a, b) ASSERT(strcmp((a), (b)) == 0)

// ============================================================================
// MOCK STATE MACHINE
// ============================================================================

typedef struct {
    int apply_count;
    uint8_t last_data[256];
    size_t last_data_len;
} MockStateMachine;

static int mock_apply(void *context, const uint8_t *data, size_t len) {
    MockStateMachine *sm = (MockStateMachine *)context;
    sm->apply_count++;
    sm->last_data_len = len < 256 ? len : 256;
    memcpy(sm->last_data, data, sm->last_data_len);
    return 0;
}

// ============================================================================
// BASIC INITIALIZATION TESTS
// ============================================================================

TEST(init_and_destroy) {
    MockStateMachine sm = {0};
    RaftNode *node = raft_init("node-1", mock_apply, &sm);
    
    ASSERT_NEQ(node, NULL);
    ASSERT_STREQ(node->node_id, "node-1");
    ASSERT_EQ(node->state, RAFT_STATE_FOLLOWER);
    ASSERT_EQ(node->current_term, 0);
    ASSERT_EQ(node->commit_index, 0);
    ASSERT_EQ(node->last_applied, 0);
    
    raft_destroy(node);
}

TEST(add_remove_peer) {
    MockStateMachine sm = {0};
    RaftNode *node = raft_init("node-1", mock_apply, &sm);
    
    ASSERT_EQ(raft_add_peer(node, "node-2", "127.0.0.1", 9001), 0);
    ASSERT_EQ(node->peer_count, 1);
    ASSERT_STREQ(node->peers[0].node_id, "node-2");
    
    ASSERT_EQ(raft_add_peer(node, "node-3", "127.0.0.1", 9002), 0);
    ASSERT_EQ(node->peer_count, 2);
    
    ASSERT_EQ(raft_remove_peer(node, "node-2"), 0);
    ASSERT_EQ(node->peer_count, 1);
    ASSERT_STREQ(node->peers[0].node_id, "node-3");
    
    raft_destroy(node);
}

// ============================================================================
// LEADER ELECTION TESTS
// ============================================================================

TEST(election_timeout) {
    MockStateMachine sm = {0};
    RaftNode *node = raft_init("node-1", mock_apply, &sm);
    
    raft_add_peer(node, "node-2", "127.0.0.1", 9001);
    raft_add_peer(node, "node-3", "127.0.0.1", 9002);
    
    raft_start(node);
    
    ASSERT_EQ(node->state, RAFT_STATE_FOLLOWER);
    ASSERT_EQ(node->current_term, 0);
    
    // Simulate election timeout
    node->last_heartbeat_ms = get_monotonic_time_ms() - 500;
    raft_tick(node);
    
    // Should transition to CANDIDATE and start election
    ASSERT_EQ(node->state, RAFT_STATE_CANDIDATE);
    ASSERT_EQ(node->current_term, 1);
    ASSERT_EQ(node->votes_received, 1);  // Voted for self
    
    raft_destroy(node);
}

TEST(win_election_with_majority) {
    MockStateMachine sm = {0};
    RaftNode *node = raft_init("node-1", mock_apply, &sm);
    
    raft_add_peer(node, "node-2", "127.0.0.1", 9001);
    raft_add_peer(node, "node-3", "127.0.0.1", 9002);
    
    raft_start(node);
    
    // Force to CANDIDATE state
    node->state = RAFT_STATE_CANDIDATE;
    node->current_term = 1;
    node->votes_received = 1;
    
    // Receive vote from node-2 (now have 2/3 votes)
    RaftRequestVoteResp resp = {
        .term = 1,
        .vote_granted = 1
    };
    raft_handle_request_vote_response(node, "node-2", &resp);
    
    // Should become LEADER with majority
    ASSERT_EQ(node->state, RAFT_STATE_LEADER);
    ASSERT_STREQ(node->leader_id, "node-1");
    
    raft_destroy(node);
}

TEST(reject_vote_for_stale_term) {
    MockStateMachine sm = {0};
    RaftNode *node = raft_init("node-1", mock_apply, &sm);
    node->current_term = 5;
    
    RaftRequestVoteReq req = {
        .term = 3,  // Stale term
        .last_log_index = 0,
        .last_log_term = 0
    };
    strncpy(req.candidate_id, "node-2", sizeof(req.candidate_id));
    
    RaftRequestVoteResp resp = {0};
    raft_handle_request_vote(node, "node-2", &req, &resp);
    
    ASSERT_EQ(resp.vote_granted, 0);
    ASSERT_EQ(resp.term, 5);
    
    raft_destroy(node);
}

TEST(grant_vote_for_up_to_date_log) {
    MockStateMachine sm = {0};
    RaftNode *node = raft_init("node-1", mock_apply, &sm);
    node->current_term = 5;
    node->state = RAFT_STATE_LEADER;
    
    // Our log: [term=1, idx=1]
    raft_submit(node, (uint8_t*)"test", 4);
    node->log[0].term = 1;
    node->log[0].index = 1;
    
    node->state = RAFT_STATE_FOLLOWER;
    
    RaftRequestVoteReq req = {
        .term = 6,
        .last_log_index = 2,
        .last_log_term = 3  // Same or higher term
    };
    strncpy(req.candidate_id, "node-2", sizeof(req.candidate_id));
    
    RaftRequestVoteResp resp = {0};
    raft_handle_request_vote(node, "node-2", &req, &resp);
    
    ASSERT_EQ(resp.vote_granted, 1);
    ASSERT_STREQ(node->voted_for, "node-2");
    
    raft_destroy(node);
}

// ============================================================================
// LOG REPLICATION TESTS
// ============================================================================

TEST(leader_submit_entry) {
    MockStateMachine sm = {0};
    RaftNode *node = raft_init("node-1", mock_apply, &sm);
    node->state = RAFT_STATE_LEADER;
    node->current_term = 1;
    
    uint8_t data[] = "hello world";
    ASSERT_EQ(raft_submit(node, data, sizeof(data)), 0);
    
    ASSERT_EQ(node->log_count, 1);
    ASSERT_EQ(node->log[0].term, 1);
    ASSERT_EQ(node->log[0].index, 1);
    ASSERT_EQ(node->log[0].data_len, sizeof(data));
    ASSERT_EQ(memcmp(node->log[0].data, data, sizeof(data)), 0);
    
    raft_destroy(node);
}

TEST(follower_append_matching_entry) {
    MockStateMachine sm = {0};
    RaftNode *node = raft_init("node-1", mock_apply, &sm);
    node->current_term = 2;
    
    RaftAppendEntriesReq req = {
        .term = 2,
        .prev_log_index = 0,
        .prev_log_term = 0,
        .leader_commit = 0,
        .entry_count = 1
    };
    strncpy(req.leader_id, "node-2", sizeof(req.leader_id));
    
    RaftLogEntry entry = {
        .term = 2,
        .index = 1,
        .entry_type = 0,
        .data = (uint8_t*)"test",
        .data_len = 4
    };
    
    RaftAppendEntriesResp resp = {0};
    raft_handle_append_entries(node, "node-2", &req, &entry, 1, &resp);
    
    ASSERT_EQ(resp.success, 1);
    ASSERT_EQ(node->log_count, 1);
    ASSERT_EQ(node->log[0].index, 1);
    
    raft_destroy(node);
}

TEST(follower_reject_mismatched_prev_log) {
    MockStateMachine sm = {0};
    RaftNode *node = raft_init("node-1", mock_apply, &sm);
    node->current_term = 2;
    node->state = RAFT_STATE_LEADER;
    
    // Add entry at index 1, term 1
    raft_submit(node, (uint8_t*)"first", 5);
    node->log[0].term = 1;
    node->log[0].index = 1;
    
    node->state = RAFT_STATE_FOLLOWER;
    
    // Try to append with prev_log_term=2 (mismatch)
    RaftAppendEntriesReq req = {
        .term = 2,
        .prev_log_index = 1,
        .prev_log_term = 2,  // Wrong! Our term is 1
        .leader_commit = 0,
        .entry_count = 0
    };
    strncpy(req.leader_id, "node-2", sizeof(req.leader_id));
    
    RaftAppendEntriesResp resp = {0};
    raft_handle_append_entries(node, "node-2", &req, NULL, 0, &resp);
    
    ASSERT_EQ(resp.success, 0);
    
    raft_destroy(node);
}

TEST(commit_index_advancement) {
    MockStateMachine sm = {0};
    RaftNode *node = raft_init("node-1", mock_apply, &sm);
    node->state = RAFT_STATE_LEADER;
    node->current_term = 1;
    
    // Add 3 peers
    raft_add_peer(node, "node-2", "127.0.0.1", 9001);
    raft_add_peer(node, "node-3", "127.0.0.1", 9002);
    raft_add_peer(node, "node-4", "127.0.0.1", 9003);
    
    // Submit entry
    raft_submit(node, (uint8_t*)"cmd", 3);
    ASSERT_EQ(node->log[0].index, 1);
    ASSERT_EQ(node->commit_index, 0);  // Not committed yet
    
    // Simulate successful replication to 2 followers (majority)
    node->peers[0].match_index = 1;
    node->peers[1].match_index = 1;
    // node->peers[2].match_index = 0; (still replicating)
    
    // Tick should advance commit_index
    raft_tick(node);
    
    ASSERT_EQ(node->commit_index, 1);
    ASSERT_EQ(node->last_applied, 1);
    ASSERT_EQ(sm.apply_count, 1);
    
    raft_destroy(node);
}

// ============================================================================
// PERSISTENCE TESTS
// ============================================================================

TEST(persist_and_restore_state) {
    MockStateMachine sm = {0};
    
    // Create node and add some state
    RaftNode *node1 = raft_init("test-persist", mock_apply, &sm);
    node1->current_term = 5;
    node1->state = RAFT_STATE_LEADER;
    strncpy(node1->voted_for, "node-2", sizeof(node1->voted_for));
    
    // Add log entries
    raft_submit(node1, (uint8_t*)"entry1", 6);
    node1->log[0].term = 3;
    node1->log[0].index = 1;
    
    raft_submit(node1, (uint8_t*)"entry2", 6);
    node1->log[1].term = 4;
    node1->log[1].index = 2;
    
    // Persist
    ASSERT_EQ(raft_persist_state(node1), 0);
    raft_destroy(node1);
    
    // Restore into new node
    RaftNode *node2 = raft_init("test-persist", mock_apply, &sm);
    ASSERT_EQ(raft_restore_state(node2), 0);
    
    // Verify restored state
    ASSERT_EQ(node2->current_term, 5);
    ASSERT_STREQ(node2->voted_for, "node-2");
    ASSERT_EQ(node2->log_count, 2);
    ASSERT_EQ(node2->log[0].index, 1);
    ASSERT_EQ(node2->log[0].term, 3);
    ASSERT_EQ(node2->log[1].index, 2);
    ASSERT_EQ(node2->log[1].term, 4);
    
    raft_destroy(node2);
    
    // Cleanup
    remove("./raft_data/test-persist.wal");
}

TEST(snapshot_creation_and_load) {
    MockStateMachine sm = {0};
    RaftNode *node = raft_init("test-snap", mock_apply, &sm);
    node->state = RAFT_STATE_LEADER;
    
    // Add some entries
    for (int i = 0; i < 10; i++) {
        char buf[32];
        snprintf(buf, sizeof(buf), "entry-%d", i);
        raft_submit(node, (uint8_t*)buf, strlen(buf));
        node->log[i].term = 1;
        node->log[i].index = i + 1;
    }
    
    ASSERT_EQ(node->log_count, 10);
    
    // Create snapshot at index 5
    uint8_t snapshot_data[] = "snapshot-state";
    ASSERT_EQ(raft_save_snapshot(node, 5, 1, snapshot_data, sizeof(snapshot_data)), 0);
    
    // Verify log was truncated
    ASSERT_EQ(node->log_count, 5);  // Entries 6-10 remain
    ASSERT_EQ(node->log[0].index, 6);
    ASSERT_EQ(node->last_snapshot_index, 5);
    
    // Load snapshot into new node
    RaftNode *node2 = raft_init("test-snap", mock_apply, &sm);
    uint64_t snap_index, snap_term;
    uint8_t *loaded_data;
    size_t loaded_len;
    
    ASSERT_EQ(raft_load_snapshot(node2, &snap_index, &snap_term, 
                                 &loaded_data, &loaded_len), 0);
    
    ASSERT_EQ(snap_index, 5);
    ASSERT_EQ(snap_term, 1);
    ASSERT_EQ(loaded_len, sizeof(snapshot_data));
    ASSERT_EQ(memcmp(loaded_data, snapshot_data, loaded_len), 0);
    
    free(loaded_data);
    raft_destroy(node);
    raft_destroy(node2);
    
    remove("./raft_data/test-snap.snap");
}

// ============================================================================
// EDGE CASE TESTS
// ============================================================================

TEST(step_down_on_higher_term) {
    MockStateMachine sm = {0};
    RaftNode *node = raft_init("node-1", mock_apply, &sm);
    node->state = RAFT_STATE_LEADER;
    node->current_term = 5;
    
    // Receive AppendEntries from higher term
    RaftAppendEntriesReq req = {
        .term = 7,
        .prev_log_index = 0,
        .prev_log_term = 0,
        .leader_commit = 0,
        .entry_count = 0
    };
    strncpy(req.leader_id, "node-2", sizeof(req.leader_id));
    
    RaftAppendEntriesResp resp = {0};
    raft_handle_append_entries(node, "node-2", &req, NULL, 0, &resp);
    
    // Should step down to FOLLOWER
    ASSERT_EQ(node->state, RAFT_STATE_FOLLOWER);
    ASSERT_EQ(node->current_term, 7);
    ASSERT_STREQ(node->leader_id, "node-2");
    
    raft_destroy(node);
}

TEST(reject_entries_from_stale_term) {
    MockStateMachine sm = {0};
    RaftNode *node = raft_init("node-1", mock_apply, &sm);
    node->current_term = 10;
    
    RaftAppendEntriesReq req = {
        .term = 5,  // Stale
        .prev_log_index = 0,
        .prev_log_term = 0,
        .leader_commit = 0,
        .entry_count = 0
    };
    
    RaftAppendEntriesResp resp = {0};
    raft_handle_append_entries(node, "node-2", &req, NULL, 0, &resp);
    
    ASSERT_EQ(resp.success, 0);
    ASSERT_EQ(resp.term, 10);
    
    raft_destroy(node);
}

// ============================================================================
// MAIN TEST RUNNER
// ============================================================================

int main(void) {
    logger_init();
    
    printf("=================================\n");
    printf("  RAFT CONSENSUS TEST SUITE\n");
    printf("=================================\n\n");
    
    RUN_TEST(init_and_destroy);
    RUN_TEST(add_remove_peer);
    
    printf("--- Election Tests ---\n");
    RUN_TEST(election_timeout);
    RUN_TEST(win_election_with_majority);
    RUN_TEST(reject_vote_for_stale_term);
    RUN_TEST(grant_vote_for_up_to_date_log);
    
    printf("\n--- Log Replication Tests ---\n");
    RUN_TEST(leader_submit_entry);
    RUN_TEST(follower_append_matching_entry);
    RUN_TEST(follower_reject_mismatched_prev_log);
    RUN_TEST(commit_index_advancement);
    
    printf("\n--- Persistence Tests ---\n");
    RUN_TEST(persist_and_restore_state);
    RUN_TEST(snapshot_creation_and_load);
    
    printf("\n--- Edge Case Tests ---\n");
    RUN_TEST(step_down_on_higher_term);
    RUN_TEST(reject_entries_from_stale_term);
    
    printf("\n=================================\n");
    printf("  ALL TESTS PASSED ✓\n");
    printf("=================================\n");
    
    logger_shutdown();
    return 0;
}