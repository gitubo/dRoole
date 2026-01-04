#include "../../include/core/event_loop.h"
#include <sys/epoll.h>
#include <stdlib.h>
#include <unistd.h>
#include <stdio.h>
#include <errno.h>
#include <sys/timerfd.h>
#include <time.h>

struct EventLoop {
    int epoll_fd;
    int running;
    struct epoll_event *events;
    int max_events;
};

// Wrapper structure to hold the user callback and context
typedef struct {
    int fd;
    event_callback_t callback;
    void *context;
} FileContext;

EventLoop* loop_create(int max_events) {
    EventLoop *loop = malloc(sizeof(EventLoop));
    if (!loop) return NULL;

    loop->epoll_fd = epoll_create1(0);
    if (loop->epoll_fd == -1) {
        free(loop);
        return NULL;
    }

    loop->events = calloc(max_events, sizeof(struct epoll_event));
    loop->max_events = max_events;
    loop->running = 0;
    return loop;
}

// Missing function added here to fix linker error
void loop_free(EventLoop *loop) {
    if (!loop) return;
    
    if (loop->epoll_fd >= 0) {
        close(loop->epoll_fd);
    }
    
    if (loop->events) {
        free(loop->events);
    }
    
    free(loop);
}

void loop_stop(EventLoop *loop) {
    if (loop) loop->running = 0;
}

int loop_add_timer(EventLoop *loop, uint32_t interval_ms, event_callback_t cb, void *context) {
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

    return loop_add_fd(loop, tfd, EVENT_READ, cb, context);
}

int loop_add_fd(EventLoop *loop, int fd, uint32_t events, event_callback_t cb, void *context) {
    FileContext *fc = malloc(sizeof(FileContext));
    if (!fc) return -1;

    fc->fd = fd;
    fc->callback = cb;
    fc->context = context;

    struct epoll_event ev = {0};
    ev.data.ptr = fc;
    
    if (events & EVENT_READ) ev.events |= EPOLLIN;
    if (events & EVENT_WRITE) ev.events |= EPOLLOUT;

    if (epoll_ctl(loop->epoll_fd, EPOLL_CTL_ADD, fd, &ev) == -1) {
        free(fc);
        return -1;
    }
    return 0;
}

void loop_run(EventLoop *loop) {
    loop->running = 1;
    while (loop->running) {
        int n = epoll_wait(loop->epoll_fd, loop->events, loop->max_events, -1);
        if (n == -1) {
            if (errno == EINTR) continue;
            break; 
        }

        for (int i = 0; i < n; i++) {
            FileContext *fc = (FileContext *)loop->events[i].data.ptr;
            uint32_t triggered_events = 0;
            
            if (loop->events[i].events & EPOLLIN) triggered_events |= EVENT_READ;
            if (loop->events[i].events & EPOLLOUT) triggered_events |= EVENT_WRITE;
            if (loop->events[i].events & EPOLLERR) triggered_events |= EVENT_ERROR;

            if (fc && fc->callback) {
                fc->callback(fc->context, fc->fd, triggered_events);
            }
        }
    }
}