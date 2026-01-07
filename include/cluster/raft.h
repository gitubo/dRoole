#ifndef RAFT_H
#define RAFT_H

#include <stdint.h>
#include <stddef.h>
#include <stdbool.h>

// ============================================================================
// RAFT CONSTANTS
// ============================================================================

#define RAFT_ELECTION_TIMEOUT_MIN_MS 150
#define RAFT_ELECTION_TIMEOUT_MAX_MS 300
#define RAFT_HEARTBEAT_INTERVAL_MS 50
#define RAFT_MAX_LOG_ENTRIES_PER_RPC 100
#define RAFT_SNAPSHOT_THRESHOLD 1000  // Entries before snapshot

// ============================================================================
// RAFT NODE STATES
// ============================================================================

typedef enum {
    RAFT_STATE_FOLLOWER = 0,
    RAFT_STATE_CANDIDATE = 1,
    RAFT_STATE_LEADER = 2
} RaftState;

// ============================================================================
// LOG ENTRY
// ============================================================================

typedef struct {
    uint64_t term;           // Term when entry was received by leader
    uint64_t index;          // Position in log (1-indexed, 0 = invalid)
    uint8_t *data;           // Command payload
    size_t data_len;         // Payload size in bytes
    uint8_t entry_type;      // 0=Command, 1=Configuration, 2=NoOp
} RaftLogEntry;

// ============================================================================
// PEER STATE (Leader's view of followers)
// ============================================================================

typedef struct {
    char node_id[37];        // UUID of peer
    char ip_address[46];     // IP for RPC
    uint16_t port;           // TCP port
    
    // Volatile state on leaders (reinitialized after election)
    uint64_t next_index;     // Index of next log entry to send
    uint64_t match_index;    // Highest log entry known to be replicated
    
    // Metrics
    uint64_t last_contact_ms;    // Last successful RPC timestamp
    uint32_t inflight_rpcs;      // Number of pending AppendEntries
    bool vote_granted;           // Used during elections
} RaftPeer;

// ============================================================================
// RAFT CORE STATE
// ============================================================================

typedef struct RaftNode {
    // Identity
    char node_id[37];
    
    // Persistent state (must be saved to disk before responding to RPCs)
    uint64_t current_term;       // Latest term server has seen
    char voted_for[37];          // CandidateId that received vote in current term
    RaftLogEntry *log;           // Log entries (dynamic array)
    size_t log_count;            // Number of entries in log
    size_t log_capacity;         // Allocated capacity
    
    // Volatile state on all servers
    RaftState state;             // Current role
    uint64_t commit_index;       // Highest log entry known to be committed
    uint64_t last_applied;       // Highest log entry applied to state machine
    
    // Volatile state on leaders
    RaftPeer *peers;             // Array of peer states
    size_t peer_count;
    size_t peer_capacity;
    
    // Timing
    uint64_t election_timeout_ms;    // Randomized election timeout
    uint64_t last_heartbeat_ms;      // Last time we heard from leader
    uint64_t last_election_ms;       // Last time we started election
    
    // Leadership
    char leader_id[37];          // Current leader (empty if unknown)
    uint32_t votes_received;     // Votes in current election
    
    // Snapshot state
    uint64_t last_snapshot_index;    // Index of last snapshot
    uint64_t last_snapshot_term;     // Term of last snapshot
    uint8_t *snapshot_data;          // Snapshot payload
    size_t snapshot_size;
    
    // Statistics
    uint64_t elections_started;
    uint64_t heartbeats_sent;
    uint64_t heartbeats_received;
    
    // Callbacks
    struct {
        // Apply committed log entry to state machine
        int (*apply)(void *context, const uint8_t *data, size_t len);
        
        // Save snapshot to disk
        int (*save_snapshot)(void *context, uint64_t index, uint64_t term,
                           const uint8_t *data, size_t len);
        
        // Load snapshot from disk
        int (*load_snapshot)(void *context, uint64_t *index, uint64_t *term,
                           uint8_t **data, size_t *len);
        
        // Persist state to disk (term, votedFor, log)
        int (*persist)(void *context);
        
        // Send RPC to peer
        int (*send_rpc)(void *context, const char *peer_id, 
                       const void *request, size_t req_len);
        
        void *context;  // User data for callbacks
    } callbacks;
    
} RaftNode;

// ============================================================================
// RPC MESSAGE STRUCTURES
// ============================================================================

// RequestVote RPC
typedef struct __attribute__((packed)) {
    uint64_t term;               // Candidate's term
    char candidate_id[37];       // Candidate requesting vote
    uint64_t last_log_index;     // Index of candidate's last log entry
    uint64_t last_log_term;      // Term of candidate's last log entry
} RaftRequestVoteReq;

typedef struct __attribute__((packed)) {
    uint64_t term;               // Current term, for candidate to update itself
    uint8_t vote_granted;        // True if candidate received vote
} RaftRequestVoteResp;

// AppendEntries RPC (heartbeat and log replication)
typedef struct __attribute__((packed)) {
    uint64_t term;               // Leader's term
    char leader_id[37];          // So follower can redirect clients
    uint64_t prev_log_index;     // Index of log entry immediately preceding new ones
    uint64_t prev_log_term;      // Term of prev_log_index entry
    uint64_t leader_commit;      // Leader's commit_index
    uint32_t entry_count;        // Number of log entries (0 for heartbeat)
    // Followed by entry_count log entries
} RaftAppendEntriesReq;

typedef struct __attribute__((packed)) {
    uint64_t term;               // Current term, for leader to update itself
    uint8_t success;             // True if follower had matching prev_log
    uint64_t match_index;        // Highest index known to match (optimization)
} RaftAppendEntriesResp;

// InstallSnapshot RPC (for catching up lagging followers)
typedef struct __attribute__((packed)) {
    uint64_t term;               // Leader's term
    char leader_id[37];
    uint64_t last_included_index;  // Snapshot replaces all entries up to this
    uint64_t last_included_term;   // Term of last_included_index
    uint32_t data_len;             // Snapshot size
    // Followed by snapshot data
} RaftInstallSnapshotReq;

typedef struct __attribute__((packed)) {
    uint64_t term;               // Current term
} RaftInstallSnapshotResp;

// ============================================================================
// PUBLIC API
// ============================================================================

/**
 * Initialize Raft node
 * @param node_id: Unique identifier for this node
 * @param apply_fn: Callback to apply committed log entries
 * @param context: User data passed to callbacks
 * @return Initialized RaftNode or NULL on failure
 */
RaftNode* raft_init(const char *node_id,
                    int (*apply_fn)(void*, const uint8_t*, size_t),
                    void *context);

/**
 * Destroy Raft node and free all resources
 */
void raft_destroy(RaftNode *node);

/**
 * Add peer to cluster (must be called before starting)
 */
int raft_add_peer(RaftNode *node, const char *peer_id, 
                  const char *ip, uint16_t port);

/**
 * Remove peer from cluster
 */
int raft_remove_peer(RaftNode *node, const char *peer_id);

/**
 * Start Raft node (begins participating in elections)
 */
int raft_start(RaftNode *node);

/**
 * Submit client request (only succeeds on leader)
 * @return 0 if accepted, -1 if not leader, -2 if queue full
 */
int raft_submit(RaftNode *node, const uint8_t *data, size_t len);

/**
 * Periodic tick (call every 10-50ms from event loop)
 * Handles timeouts, heartbeats, and applies committed entries
 */
void raft_tick(RaftNode *node);

/**
 * Process incoming RequestVote RPC
 */
void raft_handle_request_vote(RaftNode *node, const char *peer_id,
                              const RaftRequestVoteReq *req,
                              RaftRequestVoteResp *resp);

/**
 * Process incoming AppendEntries RPC
 */
void raft_handle_append_entries(RaftNode *node, const char *peer_id,
                                const RaftAppendEntriesReq *req,
                                const RaftLogEntry *entries,
                                uint32_t entry_count,
                                RaftAppendEntriesResp *resp);

/**
 * Process incoming InstallSnapshot RPC
 */
void raft_handle_install_snapshot(RaftNode *node, const char *peer_id,
                                  const RaftInstallSnapshotReq *req,
                                  const uint8_t *snapshot_data,
                                  RaftInstallSnapshotResp *resp);

/**
 * Process RequestVote response from peer
 */
void raft_handle_request_vote_response(RaftNode *node, const char *peer_id,
                                       const RaftRequestVoteResp *resp);

/**
 * Process AppendEntries response from peer
 */
void raft_handle_append_entries_response(RaftNode *node, const char *peer_id,
                                         const RaftAppendEntriesResp *resp);

/**
 * Trigger snapshot creation (call when log grows too large)
 */
int raft_create_snapshot(RaftNode *node, const uint8_t *data, size_t len);

/**
 * Restore from snapshot (call on startup)
 */
int raft_restore_snapshot(RaftNode *node);

/**
 * Persistence functions
 */
int raft_persist_state(RaftNode *node);
int raft_restore_state(RaftNode *node);
int raft_save_snapshot(RaftNode *node, uint64_t index, uint64_t term,
                      const uint8_t *data, size_t len);
int raft_load_snapshot(RaftNode *node, uint64_t *index, uint64_t *term,
                      uint8_t **data, size_t *len);

/**
 * Query API
 */
bool raft_is_leader(const RaftNode *node);
const char* raft_get_leader_id(const RaftNode *node);
uint64_t raft_get_term(const RaftNode *node);
uint64_t raft_get_commit_index(const RaftNode *node);
const char* raft_state_string(RaftState state);

#endif