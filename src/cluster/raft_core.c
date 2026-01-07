#include "../../include/cluster/raft.h"
#include "../../include/common/logger.h"
#include "../../include/common/utils.h"
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <assert.h>

// ============================================================================
// INTERNAL HELPERS
// ============================================================================

static uint64_t random_election_timeout() {
    return RAFT_ELECTION_TIMEOUT_MIN_MS + 
           (rand() % (RAFT_ELECTION_TIMEOUT_MAX_MS - RAFT_ELECTION_TIMEOUT_MIN_MS));
}

static RaftPeer* find_peer(RaftNode *node, const char *peer_id) {
    for (size_t i = 0; i < node->peer_count; i++) {
        if (strcmp(node->peers[i].node_id, peer_id) == 0) {
            return &node->peers[i];
        }
    }
    return NULL;
}

static void ensure_log_capacity(RaftNode *node) {
    if (node->log_count >= node->log_capacity) {
        size_t new_cap = node->log_capacity ? node->log_capacity * 2 : 16;
        RaftLogEntry *new_log = realloc(node->log, new_cap * sizeof(RaftLogEntry));
        if (!new_log) {
            LOG_ERR("Failed to expand Raft log to %zu entries", new_cap);
            return;
        }
        node->log = new_log;
        node->log_capacity = new_cap;
    }
}

static uint64_t last_log_index(const RaftNode *node) {
    if (node->log_count == 0) {
        return node->last_snapshot_index;
    }
    return node->log[node->log_count - 1].index;
}

static uint64_t last_log_term(const RaftNode *node) {
    if (node->log_count == 0) {
        return node->last_snapshot_term;
    }
    return node->log[node->log_count - 1].term;
}

static RaftLogEntry* get_log_entry(RaftNode *node, uint64_t index) {
    if (index <= node->last_snapshot_index) return NULL;
    
    uint64_t array_idx = index - node->last_snapshot_index - 1;
    if (array_idx >= node->log_count) return NULL;
    
    return &node->log[array_idx];
}

static uint64_t term_at_index(RaftNode *node, uint64_t index) {
    if (index == 0) return 0;
    if (index == node->last_snapshot_index) return node->last_snapshot_term;
    
    RaftLogEntry *entry = get_log_entry(node, index);
    return entry ? entry->term : 0;
}

static void persist_state(RaftNode *node) {
    if (node->callbacks.persist) {
        node->callbacks.persist(node->callbacks.context);
    }
}

// ============================================================================
// STATE TRANSITIONS
// ============================================================================

static void become_follower(RaftNode *node, uint64_t term) {
    LOG_INFO("[Raft] %s -> FOLLOWER (term %lu)", 
             raft_state_string(node->state), term);
    
    node->state = RAFT_STATE_FOLLOWER;
    node->current_term = term;
    memset(node->voted_for, 0, sizeof(node->voted_for));
    memset(node->leader_id, 0, sizeof(node->leader_id));
    node->votes_received = 0;
    
    persist_state(node);
}

static void become_candidate(RaftNode *node) {
    LOG_INFO("[Raft] FOLLOWER -> CANDIDATE (term %lu -> %lu)", 
             node->current_term, node->current_term + 1);
    
    node->state = RAFT_STATE_CANDIDATE;
    node->current_term++;
    strncpy(node->voted_for, node->node_id, sizeof(node->voted_for) - 1);
    node->votes_received = 1;  // Vote for self
    node->last_election_ms = get_monotonic_time_ms();
    node->election_timeout_ms = random_election_timeout();
    node->elections_started++;
    
    memset(node->leader_id, 0, sizeof(node->leader_id));
    
    // Reset vote tracking
    for (size_t i = 0; i < node->peer_count; i++) {
        node->peers[i].vote_granted = false;
    }
    
    persist_state(node);
    
    // Send RequestVote RPCs to all peers
    RaftRequestVoteReq req = {
        .term = node->current_term,
        .last_log_index = last_log_index(node),
        .last_log_term = last_log_term(node)
    };
    strncpy(req.candidate_id, node->node_id, sizeof(req.candidate_id) - 1);
    
    for (size_t i = 0; i < node->peer_count; i++) {
        LOG_DEBUG("[Raft] Requesting vote from %s", node->peers[i].node_id);
        if (node->callbacks.send_rpc) {
            node->callbacks.send_rpc(node->callbacks.context,
                                    node->peers[i].node_id,
                                    &req, sizeof(req));
        }
    }
}

static void become_leader(RaftNode *node) {
    LOG_INFO("[Raft] CANDIDATE -> LEADER (term %lu, votes: %u/%zu)",
             node->current_term, node->votes_received, node->peer_count + 1);
    
    node->state = RAFT_STATE_LEADER;
    strncpy(node->leader_id, node->node_id, sizeof(node->leader_id) - 1);
    
    // Initialize leader state
    uint64_t next_idx = last_log_index(node) + 1;
    for (size_t i = 0; i < node->peer_count; i++) {
        node->peers[i].next_index = next_idx;
        node->peers[i].match_index = 0;
        node->peers[i].inflight_rpcs = 0;
    }
    
    // Append no-op entry to commit entries from previous terms
    uint8_t noop = 0;
    raft_submit(node, &noop, 1);
    
    // Send immediate heartbeat
    node->last_heartbeat_ms = 0;
}

// ============================================================================
// PUBLIC API IMPLEMENTATION
// ============================================================================

RaftNode* raft_init(const char *node_id,
                    int (*apply_fn)(void*, const uint8_t*, size_t),
                    void *context) {
    RaftNode *node = calloc(1, sizeof(RaftNode));
    if (!node) return NULL;
    
    strncpy(node->node_id, node_id, sizeof(node->node_id) - 1);
    node->state = RAFT_STATE_FOLLOWER;
    node->current_term = 0;
    node->commit_index = 0;
    node->last_applied = 0;
    node->election_timeout_ms = random_election_timeout();
    
    node->callbacks.apply = apply_fn;
    node->callbacks.context = context;
    
    LOG_INFO("[Raft] Initialized node %s", node_id);
    return node;
}

void raft_destroy(RaftNode *node) {
    if (!node) return;
    
    // Free log entries
    for (size_t i = 0; i < node->log_count; i++) {
        if (node->log[i].data) {
            free(node->log[i].data);
        }
    }
    free(node->log);
    
    // Free peers
    free(node->peers);
    
    // Free snapshot
    if (node->snapshot_data) {
        free(node->snapshot_data);
    }
    
    free(node);
}

int raft_add_peer(RaftNode *node, const char *peer_id, 
                  const char *ip, uint16_t port) {
    if (find_peer(node, peer_id)) {
        LOG_WARN("[Raft] Peer %s already exists", peer_id);
        return -1;
    }
    
    if (node->peer_count >= node->peer_capacity) {
        size_t new_cap = node->peer_capacity ? node->peer_capacity * 2 : 8;
        RaftPeer *new_peers = realloc(node->peers, new_cap * sizeof(RaftPeer));
        if (!new_peers) return -1;
        node->peers = new_peers;
        node->peer_capacity = new_cap;
    }
    
    RaftPeer *peer = &node->peers[node->peer_count++];
    memset(peer, 0, sizeof(RaftPeer));
    strncpy(peer->node_id, peer_id, sizeof(peer->node_id) - 1);
    strncpy(peer->ip_address, ip, sizeof(peer->ip_address) - 1);
    peer->port = port;
    
    LOG_INFO("[Raft] Added peer %s (%s:%d)", peer_id, ip, port);
    return 0;
}

int raft_remove_peer(RaftNode *node, const char *peer_id) {
    for (size_t i = 0; i < node->peer_count; i++) {
        if (strcmp(node->peers[i].node_id, peer_id) == 0) {
            // Shift remaining peers
            memmove(&node->peers[i], &node->peers[i + 1],
                   (node->peer_count - i - 1) * sizeof(RaftPeer));
            node->peer_count--;
            LOG_INFO("[Raft] Removed peer %s", peer_id);
            return 0;
        }
    }
    return -1;
}

int raft_start(RaftNode *node) {
    node->last_heartbeat_ms = get_monotonic_time_ms();
    LOG_INFO("[Raft] Node %s started (peers: %zu)", 
             node->node_id, node->peer_count);
    return 0;
}

int raft_submit(RaftNode *node, const uint8_t *data, size_t len) {
    if (node->state != RAFT_STATE_LEADER) {
        LOG_DEBUG("[Raft] Reject submit: not leader");
        return -1;
    }
    
    ensure_log_capacity(node);
    if (node->log_count >= node->log_capacity) {
        LOG_ERR("[Raft] Log capacity exhausted");
        return -2;
    }
    
    uint64_t new_index = last_log_index(node) + 1;
    
    RaftLogEntry *entry = &node->log[node->log_count++];
    entry->term = node->current_term;
    entry->index = new_index;
    entry->data = malloc(len);
    if (!entry->data) {
        node->log_count--;
        return -2;
    }
    memcpy(entry->data, data, len);
    entry->data_len = len;
    entry->entry_type = 0;  // Command
    
    LOG_DEBUG("[Raft] Appended entry: index=%lu, term=%lu, len=%zu",
              new_index, node->current_term, len);
    
    persist_state(node);
    return 0;
}

// ============================================================================
// PERIODIC TICK
// ============================================================================

static void apply_committed_entries(RaftNode *node) {
    while (node->last_applied < node->commit_index) {
        node->last_applied++;
        
        RaftLogEntry *entry = get_log_entry(node, node->last_applied);
        if (!entry) {
            LOG_WARN("[Raft] Cannot find entry at index %lu", node->last_applied);
            break;
        }
        
        LOG_DEBUG("[Raft] Applying entry: index=%lu, term=%lu",
                  entry->index, entry->term);
        
        if (node->callbacks.apply) {
            node->callbacks.apply(node->callbacks.context,
                                entry->data, entry->data_len);
        }
    }
}

static void leader_tick(RaftNode *node) {
    uint64_t now = get_monotonic_time_ms();
    
    // Send heartbeats periodically
    if (now - node->last_heartbeat_ms >= RAFT_HEARTBEAT_INTERVAL_MS) {
        node->last_heartbeat_ms = now;
        node->heartbeats_sent++;
        
        for (size_t i = 0; i < node->peer_count; i++) {
            RaftPeer *peer = &node->peers[i];
            
            // Prepare AppendEntries RPC
            RaftAppendEntriesReq req = {
                .term = node->current_term,
                .prev_log_index = peer->next_index - 1,
                .prev_log_term = term_at_index(node, peer->next_index - 1),
                .leader_commit = node->commit_index,
                .entry_count = 0  // Heartbeat only for now
            };
            strncpy(req.leader_id, node->node_id, sizeof(req.leader_id) - 1);
            
            // TODO: Attach log entries if peer->next_index < last_log_index()
            
            if (node->callbacks.send_rpc) {
                node->callbacks.send_rpc(node->callbacks.context,
                                        peer->node_id, &req, sizeof(req));
            }
        }
    }
    
    // Update commit index based on match_index
    for (uint64_t n = node->commit_index + 1; n <= last_log_index(node); n++) {
        if (term_at_index(node, n) != node->current_term) continue;
        
        uint32_t replicas = 1;  // Count self
        for (size_t i = 0; i < node->peer_count; i++) {
            if (node->peers[i].match_index >= n) {
                replicas++;
            }
        }
        
        // Majority?
        if (replicas > (node->peer_count + 1) / 2) {
            LOG_DEBUG("[Raft] Commit index: %lu -> %lu (replicas: %u/%zu)",
                      node->commit_index, n, replicas, node->peer_count + 1);
            node->commit_index = n;
        }
    }
}

static void follower_candidate_tick(RaftNode *node) {
    uint64_t now = get_monotonic_time_ms();
    
    if (now - node->last_heartbeat_ms >= node->election_timeout_ms) {
        LOG_INFO("[Raft] Election timeout (no heartbeat for %lu ms)",
                 now - node->last_heartbeat_ms);
        become_candidate(node);
    }
}

void raft_tick(RaftNode *node) {
    if (node->state == RAFT_STATE_LEADER) {
        leader_tick(node);
    } else {
        follower_candidate_tick(node);
    }
    
    apply_committed_entries(node);
}

// ============================================================================
// RPC HANDLERS
// ============================================================================

void raft_handle_request_vote(RaftNode *node, const char *peer_id,
                              const RaftRequestVoteReq *req,
                              RaftRequestVoteResp *resp) {
    resp->term = node->current_term;
    resp->vote_granted = 0;
    
    // Step down if we see higher term
    if (req->term > node->current_term) {
        become_follower(node, req->term);
    }
    
    // Reject if term is stale
    if (req->term < node->current_term) {
        LOG_DEBUG("[Raft] Reject vote for %s: stale term %lu < %lu",
                  peer_id, req->term, node->current_term);
        return;
    }
    
    // Check if already voted
    bool already_voted = (strlen(node->voted_for) > 0) &&
                         (strcmp(node->voted_for, req->candidate_id) != 0);
    if (already_voted) {
        LOG_DEBUG("[Raft] Reject vote for %s: already voted for %s",
                  peer_id, node->voted_for);
        return;
    }
    
    // Check log up-to-date (§5.4.1)
    uint64_t my_last_idx = last_log_index(node);
    uint64_t my_last_term = last_log_term(node);
    
    bool log_ok = (req->last_log_term > my_last_term) ||
                  (req->last_log_term == my_last_term &&
                   req->last_log_index >= my_last_idx);
    
    if (!log_ok) {
        LOG_DEBUG("[Raft] Reject vote for %s: log not up-to-date", peer_id);
        return;
    }
    
    // Grant vote
    strncpy(node->voted_for, req->candidate_id, sizeof(node->voted_for) - 1);
    persist_state(node);
    
    resp->vote_granted = 1;
    node->last_heartbeat_ms = get_monotonic_time_ms();  // Reset election timer
    
    LOG_INFO("[Raft] Granted vote to %s for term %lu", peer_id, req->term);
}

void raft_handle_append_entries(RaftNode *node, const char *peer_id,
                                const RaftAppendEntriesReq *req,
                                const RaftLogEntry *entries,
                                uint32_t entry_count,
                                RaftAppendEntriesResp *resp) {
    resp->term = node->current_term;
    resp->success = 0;
    resp->match_index = 0;
    
    // Step down if we see higher term
    if (req->term > node->current_term) {
        become_follower(node, req->term);
    }
    
    // Reject if term is stale
    if (req->term < node->current_term) {
        LOG_DEBUG("[Raft] Reject AppendEntries: stale term %lu < %lu",
                  req->term, node->current_term);
        return;
    }
    
    // Valid leader for this term
    node->last_heartbeat_ms = get_monotonic_time_ms();
    node->heartbeats_received++;
    strncpy(node->leader_id, req->leader_id, sizeof(node->leader_id) - 1);
    
    if (node->state != RAFT_STATE_FOLLOWER) {
        become_follower(node, req->term);
    }
    
    // Check log matching (§5.3)
    if (req->prev_log_index > 0) {
        uint64_t prev_term = term_at_index(node, req->prev_log_index);
        if (prev_term != req->prev_log_term) {
            LOG_DEBUG("[Raft] Reject AppendEntries: log mismatch at %lu",
                      req->prev_log_index);
            resp->match_index = node->last_snapshot_index;
            return;
        }
    }
    
    // Append entries (§5.3)
    for (uint32_t i = 0; i < entry_count; i++) {
        const RaftLogEntry *new_entry = &entries[i];
        
        // Check if entry conflicts with existing entry
        RaftLogEntry *existing = get_log_entry(node, new_entry->index);
        if (existing && existing->term != new_entry->term) {
            // Delete conflicting entry and all following
            uint64_t del_from = new_entry->index - node->last_snapshot_index - 1;
            for (size_t j = del_from; j < node->log_count; j++) {
                if (node->log[j].data) free(node->log[j].data);
            }
            node->log_count = del_from;
            LOG_DEBUG("[Raft] Truncated log from index %lu", new_entry->index);
        }
        
        // Append if not already present
        if (!get_log_entry(node, new_entry->index)) {
            ensure_log_capacity(node);
            RaftLogEntry *entry = &node->log[node->log_count++];
            entry->term = new_entry->term;
            entry->index = new_entry->index;
            entry->entry_type = new_entry->entry_type;
            entry->data_len = new_entry->data_len;
            entry->data = malloc(new_entry->data_len);
            memcpy(entry->data, new_entry->data, new_entry->data_len);
            
            LOG_DEBUG("[Raft] Appended log entry: index=%lu, term=%lu",
                      entry->index, entry->term);
        }
    }
    
    persist_state(node);
    
    // Update commit index
    if (req->leader_commit > node->commit_index) {
        uint64_t new_commit = req->leader_commit;
        if (last_log_index(node) < new_commit) {
            new_commit = last_log_index(node);
        }
        node->commit_index = new_commit;
        LOG_DEBUG("[Raft] Updated commit index to %lu", node->commit_index);
    }
    
    resp->success = 1;
    resp->match_index = last_log_index(node);
}

void raft_handle_request_vote_response(RaftNode *node, const char *peer_id,
                                       const RaftRequestVoteResp *resp) {
    // Step down if we see higher term
    if (resp->term > node->current_term) {
        become_follower(node, resp->term);
        return;
    }
    
    // Ignore stale responses
    if (node->state != RAFT_STATE_CANDIDATE || resp->term < node->current_term) {
        return;
    }
    
    RaftPeer *peer = find_peer(node, peer_id);
    if (!peer) return;
    
    if (resp->vote_granted && !peer->vote_granted) {
        peer->vote_granted = true;
        node->votes_received++;
        
        LOG_DEBUG("[Raft] Received vote from %s (%u/%zu)",
                  peer_id, node->votes_received, node->peer_count + 1);
        
        // Check if won election
        if (node->votes_received > (node->peer_count + 1) / 2) {
            become_leader(node);
        }
    }
}

void raft_handle_append_entries_response(RaftNode *node, const char *peer_id,
                                         const RaftAppendEntriesResp *resp) {
    // Step down if we see higher term
    if (resp->term > node->current_term) {
        become_follower(node, resp->term);
        return;
    }
    
    // Ignore if not leader or stale term
    if (node->state != RAFT_STATE_LEADER || resp->term < node->current_term) {
        return;
    }
    
    RaftPeer *peer = find_peer(node, peer_id);
    if (!peer) return;
    
    if (resp->success) {
        peer->match_index = resp->match_index;
        peer->next_index = resp->match_index + 1;
        LOG_DEBUG("[Raft] Updated %s: match=%lu, next=%lu",
                  peer_id, peer->match_index, peer->next_index);
    } else {
        // Log conflict, decrement next_index
        if (peer->next_index > 1) {
            peer->next_index--;
            LOG_DEBUG("[Raft] Log mismatch with %s, retry with next=%lu",
                      peer_id, peer->next_index);
        }
    }
}

void raft_handle_install_snapshot(RaftNode *node, const char *peer_id,
                                  const RaftInstallSnapshotReq *req,
                                  const uint8_t *snapshot_data,
                                  RaftInstallSnapshotResp *resp) {
    (void)peer_id;
    (void)req;
    (void)snapshot_data;
    resp->term = node->current_term;
    // TODO: Implement snapshot installation
}

// ============================================================================
// QUERY API
// ============================================================================

bool raft_is_leader(const RaftNode *node) {
    return node->state == RAFT_STATE_LEADER;
}

const char* raft_get_leader_id(const RaftNode *node) {
    return node->leader_id;
}

uint64_t raft_get_term(const RaftNode *node) {
    return node->current_term;
}

uint64_t raft_get_commit_index(const RaftNode *node) {
    return node->commit_index;
}

const char* raft_state_string(RaftState state) {
    switch (state) {
        case RAFT_STATE_FOLLOWER: return "FOLLOWER";
        case RAFT_STATE_CANDIDATE: return "CANDIDATE";
        case RAFT_STATE_LEADER: return "LEADER";
        default: return "UNKNOWN";
    }
}