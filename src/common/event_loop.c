#include "../../include/common/event_loop.h"
#include <sys/epoll.h>
#include <stdlib.h>
#include <unistd.h>
#include <stdio.h>
#include <errno.h>
#include <sys/timerfd.h>
#include <time.h>
#include <string.h>

// Hash table size for file descriptor tracking (should be power of 2)
#define FD_HASH_SIZE 256
#define FD_HASH_MASK (FD_HASH_SIZE - 1)

// Wrapper structure to hold the user callback and context
typedef struct FileContext {
    int fd;
    event_callback_t callback;
    void *context;
    struct FileContext *next;  // For hash table chaining
} FileContext;

struct EventLoop {
    int epoll_fd;
    int running;
    struct epoll_event *events;
    int max_events;
    
    // Hash table for tracking all registered file descriptors
    FileContext *fd_table[FD_HASH_SIZE];
};

// ============================================================================
// INTERNAL HELPER FUNCTIONS
// ============================================================================

/**
 * Simple hash function for file descriptors
 */
static inline unsigned int fd_hash(int fd) {
    return ((unsigned int)fd * 2654435761U) & FD_HASH_MASK;
}

/**
 * Find FileContext in hash table by file descriptor
 * Returns pointer to FileContext, or NULL if not found
 */
static FileContext* find_file_context(EventLoop *loop, int fd) {
    unsigned int hash = fd_hash(fd);
    FileContext *fc = loop->fd_table[hash];
    
    while (fc != NULL) {
        if (fc->fd == fd) {
            return fc;
        }
        fc = fc->next;
    }
    
    return NULL;
}

/**
 * Insert FileContext into hash table
 * Returns 0 on success, -1 if already exists
 */
static int insert_file_context(EventLoop *loop, FileContext *fc) {
    // Check if fd already exists
    if (find_file_context(loop, fc->fd) != NULL) {
        return -1;
    }
    
    unsigned int hash = fd_hash(fc->fd);
    
    // Insert at head of chain
    fc->next = loop->fd_table[hash];
    loop->fd_table[hash] = fc;
    
    return 0;
}

/**
 * Remove FileContext from hash table
 * Returns removed FileContext, or NULL if not found
 */
static FileContext* remove_file_context(EventLoop *loop, int fd) {
    unsigned int hash = fd_hash(fd);
    FileContext **pp = &loop->fd_table[hash];
    
    while (*pp != NULL) {
        FileContext *fc = *pp;
        if (fc->fd == fd) {
            *pp = fc->next;  // Unlink from chain
            fc->next = NULL;
            return fc;
        }
        pp = &(*pp)->next;
    }
    
    return NULL;
}

// ============================================================================
// PUBLIC API IMPLEMENTATION
// ============================================================================

EventLoop* loop_create(int max_events) {
    EventLoop *loop = calloc(1, sizeof(EventLoop));
    if (!loop) return NULL;

    loop->epoll_fd = epoll_create1(0);
    if (loop->epoll_fd == -1) {
        free(loop);
        return NULL;
    }

    loop->events = calloc(max_events, sizeof(struct epoll_event));
    if (!loop->events) {
        close(loop->epoll_fd);
        free(loop);
        return NULL;
    }
    
    loop->max_events = max_events;
    loop->running = 0;
    
    // Initialize hash table (calloc already zeroed it)
    
    return loop;
}

/**
 * Free the event loop and all associated resources
 * This properly cleans up all FileContext structures that were allocated
 */
void loop_free(EventLoop *loop) {
    if (!loop) return;
    
    // 1. Free all FileContext structures in hash table
    for (int i = 0; i < FD_HASH_SIZE; i++) {
        FileContext *fc = loop->fd_table[i];
        while (fc != NULL) {
            FileContext *next = fc->next;
            
            // Remove from epoll before freeing
            // Note: We don't close the fd - that's the caller's responsibility
            epoll_ctl(loop->epoll_fd, EPOLL_CTL_DEL, fc->fd, NULL);
            
            free(fc);
            fc = next;
        }
        loop->fd_table[i] = NULL;
    }
    
    // 2. Close epoll file descriptor
    if (loop->epoll_fd >= 0) {
        close(loop->epoll_fd);
        loop->epoll_fd = -1;
    }
    
    // 3. Free event array
    if (loop->events) {
        free(loop->events);
        loop->events = NULL;
    }
    
    // 4. Free the loop structure itself
    free(loop);
}

void loop_stop(EventLoop *loop) {
    if (loop) loop->running = 0;
}

int loop_add_timer(EventLoop *loop, uint32_t interval_ms, event_callback_t cb, void *context) {
    if (!loop || !cb) return -1;
    
    int tfd = timerfd_create(CLOCK_MONOTONIC, TFD_NONBLOCK);
    if (tfd == -1) return -1;

    struct itimerspec ts;
    ts.it_interval.tv_sec = interval_ms / 1000;
    ts.it_interval.tv_nsec = (interval_ms % 1000) * 1000000;
    ts.it_value = ts.it_interval;

    if (timerfd_settime(tfd, 0, &ts, NULL) == -1) {
        close(tfd);
        return -1;
    }

    int result = loop_add_fd(loop, tfd, EVENT_READ, cb, context);
    if (result != 0) {
        close(tfd);
        return -1;
    }
    
    return tfd;
}

/**
 * Add a file descriptor to the event loop
 */
int loop_add_fd(EventLoop *loop, int fd, uint32_t events, event_callback_t cb, void *context) {
    if (!loop || fd < 0 || !cb) {
        errno = EINVAL;
        return -1;
    }
    
    // Allocate FileContext
    FileContext *fc = malloc(sizeof(FileContext));
    if (!fc) {
        return -1;
    }

    fc->fd = fd;
    fc->callback = cb;
    fc->context = context;
    fc->next = NULL;

    // Insert into hash table (checks for duplicates)
    if (insert_file_context(loop, fc) != 0) {
        free(fc);
        errno = EEXIST;
        return -1;
    }

    // Configure epoll event
    struct epoll_event ev = {0};
    ev.data.ptr = fc;
    
    if (events & EVENT_READ) ev.events |= EPOLLIN;
    if (events & EVENT_WRITE) ev.events |= EPOLLOUT;

    // Add to epoll
    if (epoll_ctl(loop->epoll_fd, EPOLL_CTL_ADD, fd, &ev) == -1) {
        // Rollback: remove from hash table
        remove_file_context(loop, fd);
        free(fc);
        return -1;
    }
    
    return 0;
}

/**
 * Modify an existing file descriptor's events and/or callback
 * This is more efficient than del+add when you need to change monitoring
 */
int loop_mod_fd(EventLoop *loop, int fd, uint32_t events, event_callback_t cb, void *context) {
    if (!loop || fd < 0 || !cb) {
        errno = EINVAL;
        return -1;
    }
    
    // Find existing FileContext
    FileContext *fc = find_file_context(loop, fd);
    if (!fc) {
        errno = ENOENT;
        return -1;
    }
    
    // Update callback and context (can change these without epoll_ctl)
    fc->callback = cb;
    fc->context = context;
    
    // Configure new epoll event
    struct epoll_event ev = {0};
    ev.data.ptr = fc;
    
    if (events & EVENT_READ) ev.events |= EPOLLIN;
    if (events & EVENT_WRITE) ev.events |= EPOLLOUT;
    
    // Modify in epoll
    if (epoll_ctl(loop->epoll_fd, EPOLL_CTL_MOD, fd, &ev) == -1) {
        return -1;
    }
    
    return 0;
}

/**
 * Remove a file descriptor from the event loop
 * Properly cleans up the associated FileContext
 * NOTE: This does NOT close the file descriptor - caller must do that
 */
int loop_del_fd(EventLoop *loop, int fd) {
    if (!loop || fd < 0) {
        errno = EINVAL;
        return -1;
    }
    
    // Remove from hash table
    FileContext *fc = remove_file_context(loop, fd);
    if (!fc) {
        errno = ENOENT;
        return -1;
    }
    
    // Remove from epoll
    // Note: In older kernels, the event pointer was required even for DEL
    // Modern kernels ignore it, but we pass NULL for compatibility
    if (epoll_ctl(loop->epoll_fd, EPOLL_CTL_DEL, fd, NULL) == -1) {
        // This is a critical error - the fd is removed from our tracking
        // but still registered with epoll. Log and continue.
        fprintf(stderr, "Warning: epoll_ctl DEL failed for fd %d: %s\n", 
                fd, strerror(errno));
    }
    
    // Free the FileContext
    free(fc);
    
    return 0;
}

/**
 * Main event loop - runs until stopped
 */
void loop_run(EventLoop *loop) {
    if (!loop) return;
    
    loop->running = 1;
    
    while (loop->running) {
        int n = epoll_wait(loop->epoll_fd, loop->events, loop->max_events, -1);
        
        if (n == -1) {
            if (errno == EINTR) {
                // Interrupted by signal, continue
                continue;
            }
            // Other errors are fatal
            fprintf(stderr, "epoll_wait error: %s\n", strerror(errno));
            break;
        }

        for (int i = 0; i < n; i++) {
            FileContext *fc = (FileContext *)loop->events[i].data.ptr;
            
            // Sanity check - should never be NULL
            if (!fc || !fc->callback) {
                fprintf(stderr, "Warning: NULL FileContext or callback in event loop\n");
                continue;
            }
            
            // Translate epoll events to our event types
            uint32_t triggered_events = 0;
            
            if (loop->events[i].events & EPOLLIN) 
                triggered_events |= EVENT_READ;
            if (loop->events[i].events & EPOLLOUT) 
                triggered_events |= EVENT_WRITE;
            if (loop->events[i].events & (EPOLLERR | EPOLLHUP)) 
                triggered_events |= EVENT_ERROR;
            
            // Invoke user callback
            // Note: Callback might call loop_del_fd on this fd, which is safe
            // because we're iterating over a snapshot of events
            fc->callback(fc->context, fc->fd, triggered_events);
        }
    }
}