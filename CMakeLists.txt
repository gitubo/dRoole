cmake_minimum_required(VERSION 3.10)
project(DistriC C)

set(CMAKE_C_STANDARD 11)
set(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} -Wall -Wextra -Wpedantic -g -pthread")
set(CMAKE_C_FLAGS_RELEASE "${CMAKE_C_FLAGS_RELEASE} -O3 -DNDEBUG")
set(CMAKE_C_FLAGS_DEBUG "${CMAKE_C_FLAGS_DEBUG} -O0 -DDEBUG -fsanitize=address,undefined")

include_directories(include)

# ============================================================================
# SOURCE FILE GROUPS
# ============================================================================

# Transport Layer (TCP/UDP)
file(GLOB SRC_TRANSPORT "src/transport/*.c")

# Protocol Layer (Serializer/RPC)
file(GLOB SRC_PROTO "src/protocol/*.c")

# Cluster/Engine Layer (excluding Raft files)
file(GLOB SRC_CLUSTER "src/cluster/*.c")
list(FILTER SRC_CLUSTER EXCLUDE REGEX ".*raft.*")

# Raft Consensus (NEW)
set(SRC_RAFT
    src/cluster/raft_core.c
    src/cluster/raft_persistence.c
    src/cluster/raft_rpc_integration.c
)

# Common Layer
file(GLOB SRC_COMMON "src/common/*.c")

# ============================================================================
# MAIN SERVER BINARY
# ============================================================================

add_executable(server 
    src/main.c 
    ${SRC_TRANSPORT} 
    ${SRC_PROTO} 
    ${SRC_CLUSTER}
    ${SRC_RAFT}
    ${SRC_COMMON}
)

target_link_libraries(server pthread)

# Optional: Install target
install(TARGETS server DESTINATION bin)

# ============================================================================
# TESTS
# ============================================================================

enable_testing()

# UDP Transport Test
add_executable(test_udp 
    tests/test_udp.c 
    ${SRC_TRANSPORT} 
    ${SRC_PROTO} 
    ${SRC_CLUSTER} 
    ${SRC_COMMON}
)
add_test(NAME UDP_Test COMMAND test_udp)

# Raft Consensus Test (NEW)
add_executable(test_raft 
    tests/test_raft.c 
    ${SRC_RAFT}
    ${SRC_TRANSPORT} 
    ${SRC_PROTO}     
    ${SRC_CLUSTER} 
    ${SRC_COMMON}
)
target_link_libraries(test_raft pthread)
add_test(NAME Raft_Test COMMAND test_raft)

# ============================================================================
# CODE COVERAGE (Optional)
# ============================================================================

option(ENABLE_COVERAGE "Enable code coverage" OFF)

if(ENABLE_COVERAGE)
    set(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} --coverage -fprofile-arcs -ftest-coverage")
    set(CMAKE_EXE_LINKER_FLAGS "${CMAKE_EXE_LINKER_FLAGS} --coverage")
endif()

# ============================================================================
# SANITIZERS (Debug builds)
# ============================================================================

if(CMAKE_BUILD_TYPE STREQUAL "Debug")
    message(STATUS "Building with AddressSanitizer and UBSan enabled")
endif()

# ============================================================================
# STATIC ANALYSIS (Optional)
# ============================================================================

option(ENABLE_CPPCHECK "Enable cppcheck static analysis" OFF)

if(ENABLE_CPPCHECK)
    find_program(CPPCHECK cppcheck)
    if(CPPCHECK)
        set(CMAKE_C_CPPCHECK 
            "${CPPCHECK}"
            "--enable=warning,style,performance,portability"
            "--inline-suppr"
            "--suppress=missingIncludeSystem"
        )
    endif()
endif()

# ============================================================================
# BENCHMARKS (Optional)
# ============================================================================

option(BUILD_BENCHMARKS "Build performance benchmarks" OFF)

if(BUILD_BENCHMARKS)
    add_executable(bench_raft 
        benchmarks/bench_raft.c 
        ${SRC_RAFT} 
        ${SRC_COMMON}
    )
    target_link_libraries(bench_raft pthread)
endif()

# ============================================================================
# DOCUMENTATION (Doxygen)
# ============================================================================

find_package(Doxygen)
if(DOXYGEN_FOUND)
    set(DOXYGEN_IN ${CMAKE_CURRENT_SOURCE_DIR}/docs/Doxyfile.in)
    set(DOXYGEN_OUT ${CMAKE_CURRENT_BINARY_DIR}/Doxyfile)
    
    configure_file(${DOXYGEN_IN} ${DOXYGEN_OUT} @ONLY)
    
    add_custom_target(docs
        COMMAND ${DOXYGEN_EXECUTABLE} ${DOXYGEN_OUT}
        WORKING_DIRECTORY ${CMAKE_CURRENT_BINARY_DIR}
        COMMENT "Generating API documentation with Doxygen"
        VERBATIM
    )
endif()

# ============================================================================
# PACKAGING (CPack)
# ============================================================================

set(CPACK_PACKAGE_NAME "DistriC")
set(CPACK_PACKAGE_VERSION "1.0.0")
set(CPACK_PACKAGE_DESCRIPTION_SUMMARY "Distributed consensus system with Raft")
set(CPACK_PACKAGE_CONTACT "your-email@example.com")
set(CPACK_GENERATOR "TGZ;DEB")

include(CPack)

# ============================================================================
# PRINT CONFIGURATION SUMMARY
# ============================================================================

message(STATUS "")
message(STATUS "===== DistriC Build Configuration =====")
message(STATUS "Build Type:        ${CMAKE_BUILD_TYPE}")
message(STATUS "C Compiler:        ${CMAKE_C_COMPILER}")
message(STATUS "C Flags:           ${CMAKE_C_FLAGS}")
message(STATUS "Source Files:")
message(STATUS "  Transport:       ${SRC_TRANSPORT}")
message(STATUS "  Protocol:        ${SRC_PROTO}")
message(STATUS "  Cluster:         ${SRC_CLUSTER}")
message(STATUS "  Raft:            ${SRC_RAFT}")
message(STATUS "  Common:          ${SRC_COMMON}")
message(STATUS "Options:")
message(STATUS "  Coverage:        ${ENABLE_COVERAGE}")
message(STATUS "  Cppcheck:        ${ENABLE_CPPCHECK}")
message(STATUS "  Benchmarks:      ${BUILD_BENCHMARKS}")
message(STATUS "=======================================")
message(STATUS "")