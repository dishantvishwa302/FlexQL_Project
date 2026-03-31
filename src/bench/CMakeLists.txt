cmake_minimum_required(VERSION 3.10)

# FlexQL Benchmark Target
add_executable(flexql_benchmark
    benchmark.cpp
)

target_link_libraries(flexql_benchmark PRIVATE flexql_lib)
target_include_directories(flexql_benchmark PRIVATE ${CMAKE_SOURCE_DIR}/include)

# C++17 standard
set_target_properties(flexql_benchmark PROPERTIES
    CXX_STANDARD 17
    CXX_STANDARD_REQUIRED YES
    CXX_EXTENSIONS NO
)

# Set output directory
set_target_properties(flexql_benchmark PROPERTIES
    RUNTIME_OUTPUT_DIRECTORY "${CMAKE_BINARY_DIR}/bin"
)
