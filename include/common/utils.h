#ifndef UTILS_H
#define UTILS_H

#include <stdint.h>
#include <time.h>

/**
 * Returns monotonic time in milliseconds.
 * Critical for high-precision heartbeats and reapers.
 */
static inline uint64_t get_monotonic_time_ms() {
    struct timespec ts;
    if (clock_gettime(CLOCK_MONOTONIC, &ts) == -1) {
        return 0;
    }
    return (uint64_t)ts.tv_sec * 1000ULL + (uint64_t)ts.tv_nsec / 1000000ULL;
}

#endif