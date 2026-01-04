cmake_minimum_required(VERSION 3.10)
project(DistriC C)

# Use C11 standard
set(CMAKE_C_STANDARD 11)
set(CMAKE_C_STANDARD_REQUIRED ON)

# Compiler flags
add_definitions(-D_GNU_SOURCE)
set(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} -Wall -Wextra -g")

# Include directories
include_directories(include)

# 1. Source Files Configuration
file(GLOB_RECURSE LIB_SOURCES 
    "src/net/*.c" 
    "src/proto/*.c" 
    "src/consensus/*.c" 
    "src/core/*.c"
)

# 2. Build Libraries
# Build Static Library (.a)
add_library(cluster_static STATIC ${LIB_SOURCES})
set_target_properties(cluster_static PROPERTIES OUTPUT_NAME "cluster")

# Build Shared Library (.so / .dll)
add_library(cluster_shared SHARED ${LIB_SOURCES})
set_target_properties(cluster_shared PROPERTIES OUTPUT_NAME "cluster")

# 3. Build Main Server Binary
add_executable(server src/main.c)
target_link_libraries(server cluster_static)

# 4. Unit Tests Configuration
enable_testing()

# Find all test files in the tests/ directory
file(GLOB TEST_SOURCES "tests/test_*.c")

foreach(test_source ${TEST_SOURCES})
    # Get the filename without extension (e.g., test_tcp)
    get_filename_component(test_name ${test_source} NAME_WE)
    
    # Create the executable for the test
    add_executable(${test_name} ${test_source})
    
    # Link against the static library
    target_link_libraries(${test_name} cluster_static)
    
    # Register the test with ctest
    add_test(NAME ${test_name} COMMAND ${test_name})
endforeach()