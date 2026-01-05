//####################
// /src/common/logger.c
// ####################

#include "common/logger.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <pthread.h>
#include <stdatomic.h>
#include <unistd.h>
#include <stdarg.h>
#include <stdint.h>

#define LOG_BUFFER_SIZE 4096
#define LOG_MSG_LEN 128

typedef struct {
    uint64_t timestamp;
    LogLevel level;
    char message[LOG_MSG_LEN];
} LogEntry;

static LogEntry *ring_buffer;
static atomic_size_t write_idx = 0;
static atomic_size_t read_idx = 0;
static int running = 0;
static pthread_t logger_thread;

static uint64_t get_time_ms() {
    struct timespec ts;
    clock_gettime(CLOCK_REALTIME, &ts);
    return (ts.tv_sec * 1000) + (ts.tv_nsec / 1000000);
}

void *logger_worker(void *arg) {
    (void)arg;
    char filename[64];
    snprintf(filename, sizeof(filename), "distric_node_%d.log", getpid());
    
    FILE *fp_file = fopen(filename, "w");
    // Keep stdout for the integration test script
    FILE *fp_out = stdout; 

    while (running || atomic_load(&read_idx) != atomic_load(&write_idx)) {
        size_t r = atomic_load(&read_idx);
        size_t w = atomic_load(&write_idx);

        if (r == w) {
            // [FIX] Check running flag inside the idle check
            if (!running && r == w) break; 
            usleep(5000);
            continue;
        }

        while (r != w) {
            size_t mask_idx = r & (LOG_BUFFER_SIZE - 1);
            LogEntry *entry = &ring_buffer[mask_idx];
            const char *level_str = (entry->level == LOG_ERROR) ? "ERR" : 
                                    (entry->level == LOG_WARN) ? "WRN" : "INF";
            
            // Write to file (if open)
            if (fp_file) fprintf(fp_file, "[%lu] [%s] %s\n", entry->timestamp, level_str, entry->message);
            // Write to stdout (for test script)
            fprintf(fp_out, "[%lu] [%s] %s\n", entry->timestamp, level_str, entry->message);
            
            r++;
        }
        
        if (fp_file) fflush(fp_file);
        fflush(fp_out);
        atomic_store(&read_idx, r);
    }
    
    if (fp_file) fclose(fp_file);
    return NULL;
}

void log_msg(LogLevel level, const char *fmt, ...) {
    size_t w = atomic_load_explicit(&write_idx, memory_order_relaxed);
    size_t r = atomic_load_explicit(&read_idx, memory_order_acquire);
    
    if (w - r >= LOG_BUFFER_SIZE) return;
    
    size_t mask_idx = w & (LOG_BUFFER_SIZE - 1);
    LogEntry *entry = &ring_buffer[mask_idx];
    
    entry->timestamp = get_time_ms();
    entry->level = level;
    va_list args;
    va_start(args, fmt);
    vsnprintf(entry->message, LOG_MSG_LEN, fmt, args);
    va_end(args);

    atomic_thread_fence(memory_order_release);
    atomic_store_explicit(&write_idx, w + 1, memory_order_release);
}

int logger_init() {
    ring_buffer = malloc(sizeof(LogEntry) * LOG_BUFFER_SIZE);
    if (!ring_buffer) return -1;

    running = 1;
    if (pthread_create(&logger_thread, NULL, logger_worker, NULL) != 0) {
        free(ring_buffer);
        return -1;
    }
    return 0;
}

void logger_shutdown() {
    running = 0;
    pthread_join(logger_thread, NULL);
    free(ring_buffer);
}