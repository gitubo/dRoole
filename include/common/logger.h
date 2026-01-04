#ifndef LOGGER_H
#define LOGGER_H

#include <stdint.h>

typedef enum { 
    LOG_DEBUG, 
    LOG_INFO, 
    LOG_WARN, 
    LOG_ERROR 
} LogLevel;

/**
 * Initializes the background logging thread and allocates the Ring Buffer.
 * Must be called before the Event Loop starts.
 * Returns 0 on success, -1 on allocation/thread failure.
 */
int logger_init();

/**
 * Shuts down the backend thread and flushes remaining logs to disk.
 * Must be called after the Event Loop stops.
 */
void logger_shutdown();

/**
 * The main non-blocking logging function.
 * @param level: Severity of the log.
 * @param fmt: Format string (printf-style).
 * @param ...: Variadic arguments.
 */
void log_msg(LogLevel level, const char *fmt, ...);

// Convenience Macros to replace printf/fprintf calls
#define LOG_DEBUG(fmt, ...)  log_msg(LOG_DEBUG,  fmt, ##__VA_ARGS__)
#define LOG_INFO(fmt, ...)   log_msg(LOG_INFO,  fmt, ##__VA_ARGS__)
#define LOG_WARN(fmt, ...)   log_msg(LOG_WARN,  fmt, ##__VA_ARGS__)
#define LOG_ERR(fmt, ...)    log_msg(LOG_ERROR, fmt, ##__VA_ARGS__)

#endif // LOGGER_H