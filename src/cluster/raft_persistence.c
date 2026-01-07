#include "../../include/cluster/raft.h"
#include "../../include/common/logger.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <fcntl.h>
#include <sys/stat.h>
#include <errno.h>
#include <arpa/inet.h>

// Add endian conversion support
#if defined(__linux__)
    #include <endian.h>
#elif defined(__APPLE__)
    #include <libkern/OSByteOrder.h>
    #define htobe64(x) OSSwapHostToBigInt64(x)
    #define be64toh(x) OSSwapBigToHostInt64(x)
#elif defined(__FreeBSD__) || defined(__NetBSD__)
    #include <sys/endian.h>
#else
    // Fallback for systems without native support
    #include <stdint.h>
    static inline uint64_t htobe64(uint64_t host_64bits) {
        union { uint64_t u64; uint8_t u8[8]; } src, dest;
        src.u64 = host_64bits;
        dest.u8[0] = src.u8[7];
        dest.u8[1] = src.u8[6];
        dest.u8[2] = src.u8[5];
        dest.u8[3] = src.u8[4];
        dest.u8[4] = src.u8[3];
        dest.u8[5] = src.u8[2];
        dest.u8[6] = src.u8[1];
        dest.u8[7] = src.u8[0];
        return dest.u64;
    }
    #define be64toh(x) htobe64(x)
#endif

// ============================================================================
// WRITE-AHEAD LOG (WAL) FORMAT
// ============================================================================

/*
 * File Structure:
 * [Header: 64 bytes]
 * [Entry 1: variable]
 * [Entry 2: variable]
 * ...
 * 
 * Header Format:
 * - magic: 4 bytes (0x52414654 = "RAFT")
 * - version: 4 bytes
 * - current_term: 8 bytes
 * - voted_for: 37 bytes (UUID + null terminator)
 * - reserved: 11 bytes
 * 
 * Log Entry Format:
 * - entry_magic: 4 bytes (0xE0E0E0E0)
 * - term: 8 bytes
 * - index: 8 bytes
 * - entry_type: 1 byte
 * - data_len: 4 bytes
 * - data: variable
 * - crc32: 4 bytes (of entire entry including magic)
 */

#define WAL_MAGIC 0x52414654  // "RAFT" in hex
#define WAL_VERSION 1
#define ENTRY_MAGIC 0xE0E0E0E0
#define WAL_HEADER_SIZE 64

typedef struct __attribute__((packed)) {
    uint32_t magic;
    uint32_t version;
    uint64_t current_term;
    char voted_for[37];
    uint8_t reserved[11];
} WalHeader;

typedef struct __attribute__((packed)) {
    uint32_t entry_magic;
    uint64_t term;
    uint64_t index;
    uint8_t entry_type;
    uint32_t data_len;
} WalEntryHeader;

// ============================================================================
// CRC32 CHECKSUM
// ============================================================================

static const uint32_t crc32_table[256] = {
    0x00000000, 0x77073096, 0xee0e612c, 0x990951ba, 0x076dc419, 0x706af48f,
    0xe963a535, 0x9e6495a3, 0x0edb8832, 0x79dcb8a4, 0xe0d5e91e, 0x97d2d988,
    0x09b64c2b, 0x7eb17cbd, 0xe7b82d07, 0x90bf1d91, 0x1db71064, 0x6ab020f2,
    0xf3b97148, 0x84be41de, 0x1adad47d, 0x6ddde4eb, 0xf4d4b551, 0x83d385c7,
    0x136c9856, 0x646ba8c0, 0xfd62f97a, 0x8a65c9ec, 0x14015c4f, 0x63066cd9,
    0xfa0f3d63, 0x8d080df5, 0x3b6e20c8, 0x4c69105e, 0xd56041e4, 0xa2677172,
    0x3c03e4d1, 0x4b04d447, 0xd20d85fd, 0xa50ab56b, 0x35b5a8fa, 0x42b2986c,
    0xdbbbc9d6, 0xacbcf940, 0x32d86ce3, 0x45df5c75, 0xdcd60dcf, 0xabd13d59,
    0x26d930ac, 0x51de003a, 0xc8d75180, 0xbfd06116, 0x21b4f4b5, 0x56b3c423,
    0xcfba9599, 0xb8bda50f, 0x2802b89e, 0x5f058808, 0xc60cd9b2, 0xb10be924,
    0x2f6f7c87, 0x58684c11, 0xc1611dab, 0xb6662d3d, 0x76dc4190, 0x01db7106,
    0x98d220bc, 0xefd5102a, 0x71b18589, 0x06b6b51f, 0x9fbfe4a5, 0xe8b8d433,
    0x7807c9a2, 0x0f00f934, 0x9609a88e, 0xe10e9818, 0x7f6a0dbb, 0x086d3d2d,
    0x91646c97, 0xe6635c01, 0x6b6b51f4, 0x1c6c6162, 0x856530d8, 0xf262004e,
    0x6c0695ed, 0x1b01a57b, 0x8208f4c1, 0xf50fc457, 0x65b0d9c6, 0x12b7e950,
    0x8bbeb8ea, 0xfcb9887c, 0x62dd1ddf, 0x15da2d49, 0x8cd37cf3, 0xfbd44c65,
    0x4db26158, 0x3ab551ce, 0xa3bc0074, 0xd4bb30e2, 0x4adfa541, 0x3dd895d7,
    0xa4d1c46d, 0xd3d6f4fb, 0x4369e96a, 0x346ed9fc, 0xad678846, 0xda60b8d0,
    0x44042d73, 0x33031de5, 0xaa0a4c5f, 0xdd0d7cc9, 0x5005713c, 0x270241aa,
    0xbe0b1010, 0xc90c2086, 0x5768b525, 0x206f85b3, 0xb966d409, 0xce61e49f,
    0x5edef90e, 0x29d9c998, 0xb0d09822, 0xc7d7a8b4, 0x59b33d17, 0x2eb40d81,
    0xb7bd5c3b, 0xc0ba6cad, 0xedb88320, 0x9abfb3b6, 0x03b6e20c, 0x74b1d29a,
    0xead54739, 0x9dd277af, 0x04db2615, 0x73dc1683, 0xe3630b12, 0x94643b84,
    0x0d6d6a3e, 0x7a6a5aa8, 0xe40ecf0b, 0x9309ff9d, 0x0a00ae27, 0x7d079eb1,
    0xf00f9344, 0x8708a3d2, 0x1e01f268, 0x6906c2fe, 0xf762575d, 0x806567cb,
    0x196c3671, 0x6e6b06e7, 0xfed41b76, 0x89d32be0, 0x10da7a5a, 0x67dd4acc,
    0xf9b9df6f, 0x8ebeeff9, 0x17b7be43, 0x60b08ed5, 0xd6d6a3e8, 0xa1d1937e,
    0x38d8c2c4, 0x4fdff252, 0xd1bb67f1, 0xa6bc5767, 0x3fb506dd, 0x48b2364b,
    0xd80d2bda, 0xaf0a1b4c, 0x36034af6, 0x41047a60, 0xdf60efc3, 0xa867df55,
    0x316e8eef, 0x4669be79, 0xcb61b38c, 0xbc66831a, 0x256fd2a0, 0x5268e236,
    0xcc0c7795, 0xbb0b4703, 0x220216b9, 0x5505262f, 0xc5ba3bbe, 0xb2bd0b28,
    0x2bb45a92, 0x5cb36a04, 0xc2d7ffa7, 0xb5d0cf31, 0x2cd99e8b, 0x5bdeae1d,
    0x9b64c2b0, 0xec63f226, 0x756aa39c, 0x026d930a, 0x9c0906a9, 0xeb0e363f,
    0x72076785, 0x05005713, 0x95bf4a82, 0xe2b87a14, 0x7bb12bae, 0x0cb61b38,
    0x92d28e9b, 0xe5d5be0d, 0x7cdcefb7, 0x0bdbdf21, 0x86d3d2d4, 0xf1d4e242,
    0x68ddb3f8, 0x1fda836e, 0x81be16cd, 0xf6b9265b, 0x6fb077e1, 0x18b74777,
    0x88085ae6, 0xff0f6a70, 0x66063bca, 0x11010b5c, 0x8f659eff, 0xf862ae69,
    0x616bffd3, 0x166ccf45, 0xa00ae278, 0xd70dd2ee, 0x4e048354, 0x3903b3c2,
    0xa7672661, 0xd06016f7, 0x4969474d, 0x3e6e77db, 0xaed16a4a, 0xd9d65adc,
    0x40df0b66, 0x37d83bf0, 0xa9bcae53, 0xdebb9ec5, 0x47b2cf7f, 0x30b5ffe9,
    0xbdbdf21c, 0xcabac28a, 0x53b39330, 0x24b4a3a6, 0xbad03605, 0xcdd70693,
    0x54de5729, 0x23d967bf, 0xb3667a2e, 0xc4614ab8, 0x5d681b02, 0x2a6f2b94,
    0xb40bbe37, 0xc30c8ea1, 0x5a05df1b, 0x2d02ef8d
};

static uint32_t crc32(const uint8_t *data, size_t len) {
    uint32_t crc = 0xFFFFFFFF;
    for (size_t i = 0; i < len; i++) {
        crc = crc32_table[(crc ^ data[i]) & 0xFF] ^ (crc >> 8);
    }
    return crc ^ 0xFFFFFFFF;
}

// ============================================================================
// FILE OPERATIONS
// ============================================================================

static int ensure_directory(const char *path) {
    struct stat st;
    if (stat(path, &st) != 0) {
        if (mkdir(path, 0755) != 0 && errno != EEXIST) {
            LOG_ERR("Failed to create directory %s: %s", path, strerror(errno));
            return -1;
        }
    }
    return 0;
}

static char* wal_path(const char *node_id) {
    static char path[256];
    snprintf(path, sizeof(path), "./raft_data/%s.wal", node_id);
    return path;
}

static char* snapshot_path(const char *node_id) {
    static char path[256];
    snprintf(path, sizeof(path), "./raft_data/%s.snap", node_id);
    return path;
}

// ============================================================================
// PUBLIC PERSISTENCE API
// ============================================================================

int raft_persist_state(RaftNode *node) {
    if (!node) return -1;
    
    ensure_directory("./raft_data");
    
    const char *path = wal_path(node->node_id);
    int fd = open(path, O_WRONLY | O_CREAT | O_TRUNC, 0644);
    if (fd < 0) {
        LOG_ERR("Failed to open WAL file %s: %s", path, strerror(errno));
        return -1;
    }
    
    // Write header
    WalHeader header = {
        .magic = htonl(WAL_MAGIC),
        .version = htonl(WAL_VERSION),
        .current_term = htobe64(node->current_term)
    };
    strncpy(header.voted_for, node->voted_for, sizeof(header.voted_for) - 1);
    
    if (write(fd, &header, sizeof(header)) != sizeof(header)) {
        LOG_ERR("Failed to write WAL header");
        close(fd);
        return -1;
    }
    
    // Write log entries
    for (size_t i = 0; i < node->log_count; i++) {
        RaftLogEntry *entry = &node->log[i];
        
        WalEntryHeader ent_hdr = {
            .entry_magic = htonl(ENTRY_MAGIC),
            .term = htobe64(entry->term),
            .index = htobe64(entry->index),
            .entry_type = entry->entry_type,
            .data_len = htonl(entry->data_len)
        };
        
        // Calculate CRC over header + data
        size_t total_size = sizeof(ent_hdr) + entry->data_len;
        uint8_t *buf = malloc(total_size);
        if (!buf) {
            close(fd);
            return -1;
        }
        
        memcpy(buf, &ent_hdr, sizeof(ent_hdr));
        memcpy(buf + sizeof(ent_hdr), entry->data, entry->data_len);
        
        uint32_t checksum = htonl(crc32(buf, total_size));
        
        // Write: header + data + checksum
        if (write(fd, buf, total_size) != (ssize_t)total_size ||
            write(fd, &checksum, sizeof(checksum)) != sizeof(checksum)) {
            LOG_ERR("Failed to write log entry %lu", entry->index);
            free(buf);
            close(fd);
            return -1;
        }
        
        free(buf);
    }
    
    fsync(fd);
    close(fd);
    
    LOG_DEBUG("[Raft] Persisted state: term=%lu, log_count=%zu",
              node->current_term, node->log_count);
    return 0;
}

int raft_restore_state(RaftNode *node) {
    if (!node) return -1;
    
    const char *path = wal_path(node->node_id);
    int fd = open(path, O_RDONLY);
    if (fd < 0) {
        if (errno == ENOENT) {
            LOG_INFO("[Raft] No existing WAL found, starting fresh");
            return 0;  // No state to restore
        }
        LOG_ERR("Failed to open WAL file %s: %s", path, strerror(errno));
        return -1;
    }
    
    // Read header
    WalHeader header;
    if (read(fd, &header, sizeof(header)) != sizeof(header)) {
        LOG_ERR("Failed to read WAL header");
        close(fd);
        return -1;
    }
    
    if (ntohl(header.magic) != WAL_MAGIC) {
        LOG_ERR("Invalid WAL magic: 0x%x", ntohl(header.magic));
        close(fd);
        return -1;
    }
    
    node->current_term = be64toh(header.current_term);
    strncpy(node->voted_for, header.voted_for, sizeof(node->voted_for) - 1);
    
    // Read log entries
    while (1) {
        WalEntryHeader ent_hdr;
        ssize_t n = read(fd, &ent_hdr, sizeof(ent_hdr));
        
        if (n == 0) break;  // EOF
        if (n != sizeof(ent_hdr)) {
            LOG_ERR("Incomplete entry header");
            break;
        }
        
        if (ntohl(ent_hdr.entry_magic) != ENTRY_MAGIC) {
            LOG_ERR("Invalid entry magic: 0x%x", ntohl(ent_hdr.entry_magic));
            break;
        }
        
        uint64_t term = be64toh(ent_hdr.term);
        uint64_t index = be64toh(ent_hdr.index);
        uint32_t data_len = ntohl(ent_hdr.data_len);
        
        // Read data
        uint8_t *data = malloc(data_len);
        if (!data || read(fd, data, data_len) != (ssize_t)data_len) {
            LOG_ERR("Failed to read entry data");
            free(data);
            break;
        }
        
        // Read and verify checksum
        uint32_t stored_crc, computed_crc;
        if (read(fd, &stored_crc, sizeof(stored_crc)) != sizeof(stored_crc)) {
            LOG_ERR("Failed to read checksum");
            free(data);
            break;
        }
        
        size_t total_size = sizeof(ent_hdr) + data_len;
        uint8_t *buf = malloc(total_size);
        memcpy(buf, &ent_hdr, sizeof(ent_hdr));
        memcpy(buf + sizeof(ent_hdr), data, data_len);
        computed_crc = crc32(buf, total_size);
        free(buf);
        
        if (ntohl(stored_crc) != computed_crc) {
            LOG_ERR("Checksum mismatch for entry %lu", index);
            free(data);
            break;
        }
        
        // Append to log
        if (node->log_count >= node->log_capacity) {
            size_t new_cap = node->log_capacity ? node->log_capacity * 2 : 16;
            RaftLogEntry *new_log = realloc(node->log, new_cap * sizeof(RaftLogEntry));
            if (!new_log) {
                free(data);
                break;
            }
            node->log = new_log;
            node->log_capacity = new_cap;
        }
        
        RaftLogEntry *entry = &node->log[node->log_count++];
        entry->term = term;
        entry->index = index;
        entry->entry_type = ent_hdr.entry_type;
        entry->data = data;
        entry->data_len = data_len;
    }
    
    close(fd);
    
    LOG_INFO("[Raft] Restored state: term=%lu, log_count=%zu",
             node->current_term, node->log_count);
    return 0;
}

int raft_save_snapshot(RaftNode *node, uint64_t index, uint64_t term,
                      const uint8_t *data, size_t len) {
    if (!node) return -1;
    
    ensure_directory("./raft_data");
    
    const char *path = snapshot_path(node->node_id);
    int fd = open(path, O_WRONLY | O_CREAT | O_TRUNC, 0644);
    if (fd < 0) {
        LOG_ERR("Failed to open snapshot file: %s", strerror(errno));
        return -1;
    }
    
    // Write header
    struct __attribute__((packed)) {
        uint32_t magic;
        uint64_t last_index;
        uint64_t last_term;
        uint32_t data_len;
    } snap_hdr = {
        .magic = htonl(0x534E4150),  // "SNAP"
        .last_index = htobe64(index),
        .last_term = htobe64(term),
        .data_len = htonl(len)
    };
    
    if (write(fd, &snap_hdr, sizeof(snap_hdr)) != sizeof(snap_hdr) ||
        write(fd, data, len) != (ssize_t)len) {
        LOG_ERR("Failed to write snapshot");
        close(fd);
        return -1;
    }
    
    fsync(fd);
    close(fd);
    
    // Update node state
    node->last_snapshot_index = index;
    node->last_snapshot_term = term;
    
    // Truncate log (keep only entries after snapshot)
    size_t new_count = 0;
    for (size_t i = 0; i < node->log_count; i++) {
        if (node->log[i].index > index) {
            if (new_count != i) {
                node->log[new_count] = node->log[i];
            }
            new_count++;
        } else {
            // Free old entries
            if (node->log[i].data) free(node->log[i].data);
        }
    }
    node->log_count = new_count;
    
    LOG_INFO("[Raft] Saved snapshot: index=%lu, term=%lu, size=%zu bytes",
             index, term, len);
    return 0;
}

int raft_load_snapshot(RaftNode *node, uint64_t *index, uint64_t *term,
                      uint8_t **data, size_t *len) {
    if (!node) return -1;
    
    const char *path = snapshot_path(node->node_id);
    int fd = open(path, O_RDONLY);
    if (fd < 0) {
        if (errno == ENOENT) return 0;  // No snapshot
        LOG_ERR("Failed to open snapshot: %s", strerror(errno));
        return -1;
    }
    
    struct __attribute__((packed)) {
        uint32_t magic;
        uint64_t last_index;
        uint64_t last_term;
        uint32_t data_len;
    } snap_hdr;
    
    if (read(fd, &snap_hdr, sizeof(snap_hdr)) != sizeof(snap_hdr)) {
        close(fd);
        return -1;
    }
    
    if (ntohl(snap_hdr.magic) != 0x534E4150) {
        LOG_ERR("Invalid snapshot magic");
        close(fd);
        return -1;
    }
    
    *index = be64toh(snap_hdr.last_index);
    *term = be64toh(snap_hdr.last_term);
    *len = ntohl(snap_hdr.data_len);
    
    *data = malloc(*len);
    if (!*data || read(fd, *data, *len) != (ssize_t)*len) {
        free(*data);
        close(fd);
        return -1;
    }
    
    close(fd);
    
    LOG_INFO("[Raft] Loaded snapshot: index=%lu, term=%lu, size=%zu",
             *index, *term, *len);
    return 0;
}