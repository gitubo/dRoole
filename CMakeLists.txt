cmake_minimum_required(VERSION 3.10)
project(DistriC C)

set(CMAKE_C_STANDARD 11)
set(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} -Wall -Wextra -g -pthread")

include_directories(include)

# Transport Layer (TCP/UDP)
file(GLOB SRC_TRANSPORT "src/transport/*.c")

# Protocol Layer (Serializer/RPC)
file(GLOB SRC_PROTO "src/protocol/*.c")

# Cluster/Engine Layer
file(GLOB SRC_CLUSTER "src/cluster/*.c")

# Common Layer
file(GLOB SRC_COMMON "src/common/*.c")

# Build Main Server
add_executable(server src/main.c ${SRC_TRANSPORT} ${SRC_PROTO} ${SRC_CLUSTER} ${SRC_COMMON})
target_link_libraries(server pthread)

# Tests
enable_testing()
add_executable(test_udp tests/test_udp.c ${SRC_TRANSPORT} ${SRC_PROTO} ${SRC_CLUSTER} ${SRC_COMMON})
add_test(NAME UDP_Test COMMAND test_udp)