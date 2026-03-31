# FlexQL Database Driver - Makefile
CXX = g++
CXXFLAGS = -std=c++17 -Wall -Wextra -O2  -fPIC -I./include

SRC_DIR = src
BIN_DIR = bin
BUILD_DIR = build

# Get all source files
SRCS = $(shell find $(SRC_DIR) -name "*.cpp" -type f)
OBJS = $(SRCS:$(SRC_DIR)/%.cpp=$(BUILD_DIR)/%.o)

SERVER_BIN = $(BIN_DIR)/flexql_server
CLIENT_BIN = $(BIN_DIR)/flexql_client
# BENCH_BIN = $(BIN_DIR)/flexql_benchmark
# SYSTEM_BENCH_BIN = $(BIN_DIR)/system_benchmark
# BENCH1_BIN = $(BIN_DIR)/flexql_benchmark1
BENCHMARK_FLEXQL_BIN = $(BIN_DIR)/benchmark_flexql
ALL_BINS = $(SERVER_BIN) $(CLIENT_BIN) $(BENCH_BIN) $(SYSTEM_BENCH_BIN) $(BENCH1_BIN) $(BENCHMARK_FLEXQL_BIN)
ALL_BINS := $(SERVER_BIN) $(CLIENT_BIN)  $(BENCHMARK_FLEXQL_BIN)
.PHONY: all server client benchmark clean help run_server run_client bench system_bench bench1

all: $(ALL_BINS)

server: $(SERVER_BIN)

client: $(CLIENT_BIN)

# benchmark: $(BENCH_BIN)

# bench: $(BENCH_BIN)

# system_bench: $(SYSTEM_BENCH_BIN)

# bench1: $(BENCH1_BIN)

benchmark_flexql: $(BENCHMARK_FLEXQL_BIN)

# Link server binary
$(SERVER_BIN): $(filter-out $(BUILD_DIR)/client/%.o $(BUILD_DIR)/bench/%.o, $(OBJS)) $(BUILD_DIR)/server/main.o | $(BIN_DIR)
	@echo "Linking server..."
	$(CXX) $(CXXFLAGS) -pthread -o $@ $^
	@echo "Server compiled: $@"

# Link client binary
$(CLIENT_BIN): $(filter-out $(BUILD_DIR)/server/%.o $(BUILD_DIR)/bench/%.o, $(OBJS)) $(BUILD_DIR)/client/main.o | $(BIN_DIR)
	@echo "Linking client..."
	$(CXX) $(CXXFLAGS) -pthread -o $@ $^
	@echo "Client compiled: $@"

# Non-bench shared objects (excludes all bench/*.o, client/*.o, server/*.o)
SHARED_OBJS = $(filter-out $(BUILD_DIR)/server/%.o $(BUILD_DIR)/client/%.o $(BUILD_DIR)/bench/%.o, $(OBJS))

# # Link benchmark binary (latency tool - standalone, no server)
# $(BENCH_BIN): $(SHARED_OBJS) $(BUILD_DIR)/bench/benchmark.o | $(BIN_DIR)
# 	@echo "Linking benchmark..."
# 	$(CXX) $(CXXFLAGS) -pthread -o $@ $^
# 	@echo "Benchmark compiled: $@"

# # Link system benchmark binary (connects to running server)
# $(SYSTEM_BENCH_BIN): $(BUILD_DIR)/bench/system_benchmark.o | $(BIN_DIR)
# 	@echo "Linking system benchmark..."
# 	$(CXX) $(CXXFLAGS) -pthread -o $@ $^
# 	@echo "System benchmark compiled: $@"

# # Link benchmark1 binary (C API benchmark)
# $(BENCH1_BIN): $(SHARED_OBJS) $(BUILD_DIR)/bench/benchmark1.o | $(BIN_DIR)
# 	@echo "Linking benchmark1..."
# 	$(CXX) $(CXXFLAGS) -pthread -o $@ $^
# 	@echo "Benchmark1 compiled: $@"

# $(BENCHMARK_FLEXQL_BIN): $(SHARED_OBJS) $(BUILD_DIR)/bench/benchmark_flexql.o | $(BIN_DIR)
# 	@echo "Linking benchmark_flexql..."
# 	$(CXX) $(CXXFLAGS) -pthread benchmark_flexql.cpp $(SHARED_OBJS) -o $@
# 	@echo "benchmark_flexql compiled: $@"


# Link benchmark_flexql binary
$(BENCHMARK_FLEXQL_BIN): $(SHARED_OBJS) benchmark_flexql.cpp | $(BIN_DIR)
	@echo "Linking benchmark_flexql..."
	$(CXX) $(CXXFLAGS) -pthread benchmark_flexql.cpp $(SHARED_OBJS) -o $@
	@echo "benchmark_flexql compiled: $@"

# Compile object files
$(BUILD_DIR)/%.o: $(SRC_DIR)/%.cpp
	@mkdir -p $(dir $@)
	@echo "Compiling $<..."
	$(CXX) $(CXXFLAGS) -pthread -c $< -o $@

# Create output directories
$(BIN_DIR) $(BUILD_DIR):
	@mkdir -p $@

clean:
	@echo "Cleaning..."
	@rm -rf $(BUILD_DIR) $(BIN_DIR)
	@echo "Clean complete"

help:
	@echo "FlexQL Build System"
	@echo "  make all          - Build server, client, and all benchmarks"
	@echo "  make server       - Build server only"
	@echo "  make client       - Build client only"
	@echo "  make benchmark    - Build latency benchmark tool"
	@echo "  make system_bench - Build system/server-client benchmark"
	@echo "  make bench1       - Build benchmark1 (C API benchmark)"
	@echo "  make clean        - Remove build artifacts"
	@echo "  make help         - Show this message"
