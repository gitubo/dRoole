#ifndef EVENT_LOOP_H
#define EVENT_LOOP_H

#include <stdint.h>

typedef enum {
    EVENT_READ  = 1 << 0,
    EVENT_WRITE = 1 << 1,
    EVENT_ERROR = 1 << 2
} EventType;

// Callback function signature
// context: User data passed during registration
// fd: The file descriptor triggering the event
// events: Bitmask of EventType
typedef void (*event_callback_t)(void *context, int fd, uint32_t events);

typedef struct EventLoop EventLoop;

// Lifecycle
EventLoop* loop_create(int max_events);
void loop_free(EventLoop *loop);
void loop_run(EventLoop *loop);
void loop_stop(EventLoop *loop);

// Registration
int loop_add_fd(EventLoop *loop, int fd, uint32_t events, event_callback_t cb, void *context);
int loop_mod_fd(EventLoop *loop, int fd, uint32_t events, event_callback_t cb, void *context);
int loop_del_fd(EventLoop *loop, int fd);

/**
 * Register a periodic timer.
 * @param interval_ms: How often to fire (in milliseconds).
 * @param cb: Function to call on timeout.
 * @return timer_fd on success, -1 on failure.
 */
int loop_add_timer(EventLoop *loop, uint32_t interval_ms, event_callback_t cb, void *context);

#endif