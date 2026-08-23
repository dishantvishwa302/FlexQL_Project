# FlexQL Database Driver - Makefile
CXX = g++
CXXFLAGS = -std=c++17 -Wall -Wextra -O2 -fPIC -I./include
LDFLAGS = -pthread

SRC_DIR = src
BIN_DIR = bin
BUILD_DIR = build

SRCS = $(shell find $(SRC_DIR) -name "*.cpp" -type f)
OBJS = $(SRCS:$(SRC_DIR)/%.cpp=$(BUILD_DIR)/%.o)

SERVER_BIN = $(BIN_DIR)/flexql_server
CLIENT_BIN = $(BIN_DIR)/flexql_client
BENCHMARK_BIN = $(BIN_DIR)/benchmark_flexql

SHARED_OBJS = $(filter-out $(BUILD_DIR)/server/%.o $(BUILD_DIR)/client/%.o $(BUILD_DIR)/network/server.o, $(OBJS))

.PHONY: all server client benchmark clean help

all: $(SERVER_BIN) $(CLIENT_BIN) $(BENCHMARK_BIN)

server: $(SERVER_BIN)
client: $(CLIENT_BIN)
benchmark: $(BENCHMARK_BIN)

$(SERVER_BIN): $(filter-out $(BUILD_DIR)/client/%.o, $(OBJS)) | $(BIN_DIR)
	@echo "Linking server..."
	$(CXX) $(CXXFLAGS) $(LDFLAGS) -o $@ $^

$(CLIENT_BIN): $(SHARED_OBJS) $(BUILD_DIR)/client/main.o | $(BIN_DIR)
	@echo "Linking client..."
	$(CXX) $(CXXFLAGS) $(LDFLAGS) -o $@ $^

$(BENCHMARK_BIN): $(SHARED_OBJS) benchmark_flexql.cpp | $(BIN_DIR)
	@echo "Linking benchmark..."
	$(CXX) $(CXXFLAGS) $(LDFLAGS) benchmark_flexql.cpp $(SHARED_OBJS) -o $@

$(BUILD_DIR)/%.o: $(SRC_DIR)/%.cpp
	@mkdir -p $(dir $@)
	$(CXX) $(CXXFLAGS) -pthread -c $< -o $@

$(BIN_DIR) $(BUILD_DIR):
	@mkdir -p $@

clean:
	rm -rf $(BUILD_DIR) $(BIN_DIR)

help:
	@echo "make          - build server, client, benchmark"
	@echo "make server   - server only"
	@echo "make client   - client only"
	@echo "make clean    - remove build artifacts"
