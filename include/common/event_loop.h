#ifndef EVENT_LOOP_H
#define EVENT_LOOP_H

#include <stdint.h>

typedef enum {
    EVENT_READ  = 1 << 0,  // File descriptor ready for reading
    EVENT_WRITE = 1 << 1,  // File descriptor ready for writing
    EVENT_ERROR = 1 << 2   // Error occurred on file descriptor
} EventType;

/**
 * Callback function signature for event notifications
 * 
 * @param context: User data passed during registration
 * @param fd: The file descriptor triggering the event
 * @param events: Bitmask of EventType flags
 * 
 * Note: Callback may call loop_del_fd() on the triggering fd safely.
 *       Callback should NOT block - use non-blocking I/O operations.
 */
typedef void (*event_callback_t)(void *context, int fd, uint32_t events);

typedef struct EventLoop EventLoop;

// ============================================================================
// LIFECYCLE MANAGEMENT
// ============================================================================

/**
 * Create a new event loop
 * 
 * @param max_events: Maximum number of events to process per epoll_wait
 * @return EventLoop pointer on success, NULL on failure
 * 
 * Note: Typical value is 128-1024 depending on expected concurrency
 */
EventLoop* loop_create(int max_events);

/**
 * Free the event loop and all associated resources
 * 
 * This function:
 * - Removes all registered file descriptors from epoll
 * - Frees all internal FileContext structures
 * - Closes the epoll file descriptor
 * - Frees the EventLoop structure
 * 
 * Note: This does NOT close the registered file descriptors themselves.
 *       Caller is responsible for closing fds before or after calling this.
 * 
 * @param loop: EventLoop to free (NULL-safe)
 */
void loop_free(EventLoop *loop);

/**
 * Run the event loop (blocking)
 * 
 * Processes events until loop_stop() is called or an error occurs.
 * This function blocks until the loop is stopped.
 * 
 * @param loop: EventLoop to run
 */
void loop_run(EventLoop *loop);

/**
 * Stop the event loop
 * 
 * Signals the loop to stop processing events. The loop will exit
 * after processing current events.
 * 
 * Thread-safe: Can be called from signal handlers or other threads.
 * 
 * @param loop: EventLoop to stop (NULL-safe)
 */
void loop_stop(EventLoop *loop);

// ============================================================================
// FILE DESCRIPTOR REGISTRATION
// ============================================================================

/**
 * Add a file descriptor to the event loop
 * 
 * @param loop: EventLoop instance
 * @param fd: File descriptor to monitor (must be >= 0)
 * @param events: Bitmask of EventType flags to monitor
 * @param cb: Callback function (must not be NULL)
 * @param context: User data passed to callback (can be NULL)
 * @return 0 on success, -1 on failure (sets errno)
 * 
 * Errors:
 * - EINVAL: Invalid parameters
 * - EEXIST: File descriptor already registered
 * - ENOMEM: Memory allocation failure
 * - Other: epoll_ctl errors
 * 
 * Example:
 *   loop_add_fd(loop, sockfd, EVENT_READ, on_socket_read, &my_context);
 */
int loop_add_fd(EventLoop *loop, int fd, uint32_t events, 
                event_callback_t cb, void *context);

/**
 * Modify an existing file descriptor's events and/or callback
 * 
 * More efficient than loop_del_fd + loop_add_fd when changing monitoring.
 * 
 * @param loop: EventLoop instance
 * @param fd: File descriptor to modify (must be already registered)
 * @param events: New bitmask of EventType flags
 * @param cb: New callback function (must not be NULL)
 * @param context: New user data (can be NULL)
 * @return 0 on success, -1 on failure (sets errno)
 * 
 * Errors:
 * - EINVAL: Invalid parameters
 * - ENOENT: File descriptor not registered
 * - Other: epoll_ctl errors
 * 
 * Example:
 *   // Change from read-only to read-write monitoring
 *   loop_mod_fd(loop, sockfd, EVENT_READ | EVENT_WRITE, 
 *               on_socket_readwrite, &my_context);
 */
int loop_mod_fd(EventLoop *loop, int fd, uint32_t events, 
                event_callback_t cb, void *context);

/**
 * Remove a file descriptor from the event loop
 * 
 * Unregisters the fd from epoll and frees internal tracking structures.
 * 
 * @param loop: EventLoop instance
 * @param fd: File descriptor to remove
 * @return 0 on success, -1 on failure (sets errno)
 * 
 * Errors:
 * - EINVAL: Invalid parameters
 * - ENOENT: File descriptor not registered
 * 
 * IMPORTANT: This does NOT close the file descriptor. Caller must close it.
 * 
 * Safe to call from within a callback for the same fd.
 * 
 * Example:
 *   loop_del_fd(loop, sockfd);
 *   close(sockfd);  // Caller's responsibility
 */
int loop_del_fd(EventLoop *loop, int fd);

// ============================================================================
// TIMER UTILITIES
// ============================================================================

/**
 * Register a periodic timer
 * 
 * Creates a timerfd and registers it with the event loop.
 * 
 * @param loop: EventLoop instance
 * @param interval_ms: Interval in milliseconds (must be > 0)
 * @param cb: Callback function invoked on each timeout
 * @param context: User data passed to callback
 * @return timer fd on success, -1 on failure
 * 
 * Note: Caller should store the returned fd to remove it later with loop_del_fd
 * 
 * Example:
 *   int timer_fd = loop_add_timer(loop, 1000, on_every_second, NULL);
 *   // Later: loop_del_fd(loop, timer_fd); close(timer_fd);
 */
int loop_add_timer(EventLoop *loop, uint32_t interval_ms, 
                   event_callback_t cb, void *context);

#endif // EVENT_LOOP_H