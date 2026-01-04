#include "../../include/transport/net_utils.h"
#include <fcntl.h>
#include <unistd.h>

int net_set_nonblocking(int fd) {
    int flags = fcntl(fd, F_GETFL, 0);
    if (flags == -1) return -1;
    return fcntl(fd, F_SETFL, flags | O_NONBLOCK);
}