#include "../../include/network/server.h"
#include "../../include/query/executor.h"
#include <iostream>
#include <thread>
#include <chrono>
#include <vector>
#include <sys/socket.h>
#include <sys/types.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <cstring>
#include <unistd.h>

namespace flexql {

Server::Server(int port, int max_connections)
    : port(port), max_connections(max_connections), server_socket(-1), running(false),
      database(std::make_shared<Database>()),
      cache(std::make_shared<LRUCache>(4096)),           // 4096 cached queries
      executor(std::make_shared<QueryExecutor>(database, cache)) {}  // pass cache to executor

Server::~Server() {
    stop();
}

bool Server::start() {
    running = true;

    // Create socket
    server_socket = socket(AF_INET, SOCK_STREAM, 0);
    if (server_socket < 0) {
        std::cerr << "Error: Could not create socket" << std::endl;
        return false;
    }

    // Allow port reuse
    int reuse = 1;
    if (setsockopt(server_socket, SOL_SOCKET, SO_REUSEADDR, &reuse, sizeof(reuse)) < 0) {
        std::cerr << "Error: setsockopt failed" << std::endl;
        return false;
    }

    // Bind
    struct sockaddr_in addr;
    memset(&addr, 0, sizeof(addr));
    addr.sin_family = AF_INET;
    addr.sin_addr.s_addr = htonl(INADDR_ANY);
    addr.sin_port = htons(port);

    if (bind(server_socket, (struct sockaddr*)&addr, sizeof(addr)) < 0) {
        std::cerr << "Error: Bind failed on port " << port << std::endl;
        return false;
    }

    // Listen
    if (listen(server_socket, max_connections) < 0) {
        std::cerr << "Error: Listen failed" << std::endl;
        return false;
    }

    std::cout << "✓ FlexQL Server listening on port " << port << std::endl;

    // Background thread: accept new connections
    std::thread accept_thread([this]() { acceptConnections(); });
    accept_thread.detach();

    // Background thread: clean up expired rows every 30 seconds
    std::thread ttl_thread([this]() {
        while (running) {
            std::this_thread::sleep_for(std::chrono::seconds(30));
            if (!running) break;
            try {
                database->cleanupExpiredRows();
            } catch (...) {}
        }
    });
    ttl_thread.detach();

    return true;
}

void Server::stop() {
    running = false;
    if (server_socket >= 0) {
        close(server_socket);
        server_socket = -1;
    }
}

void Server::acceptConnections() {
    struct sockaddr_in client_addr;
    socklen_t addr_len = sizeof(client_addr);

    while (running) {
        int client_socket = accept(server_socket, (struct sockaddr*)&client_addr, &addr_len);
        if (client_socket < 0) break;

        std::thread client_thread([this, client_socket]() {
            handleClient(client_socket);
            close(client_socket);
        });
        client_thread.detach();
    }
}

void Server::handleClient(int client_socket) {
    char buffer[65536]; // Larger buffer for bulk inserts

    std::cout << "✓ Client connected (socket: " << client_socket << ")" << std::endl;

    std::string query_buffer;

    while (running) {
        memset(buffer, 0, sizeof(buffer));
        int n = read(client_socket, buffer, sizeof(buffer) - 1);

        if (n <= 0) break;

        buffer[n] = '\0';
        query_buffer += std::string(buffer, n);

        size_t pos;
        while ((pos = query_buffer.find("\n<EOF>\n")) != std::string::npos) {
            std::string query = query_buffer.substr(0, pos);
            query_buffer = query_buffer.substr(pos + 7);

            // Trim trailing whitespace/newlines
            while (!query.empty() && (query.back() == '\n' || query.back() == '\r' || query.back() == ' '))
                query.pop_back();

            if (query.empty()) continue;

            // Execute query
            auto result = executor->execute(query);

            // Format and send response
            std::string response = formatResponse(result);
            ssize_t sent = write(client_socket, response.c_str(), response.length());
            if (sent < 0) goto connection_close;
        }
    }
connection_close:
    std::cout << "✓ Client disconnected (socket: " << client_socket << ")" << std::endl;
}

std::string Server::formatResponse(const QueryResult& result) {
    std::string response;

    if (!result.success) {
        response = "ERROR: " + result.error_message + "\n";
    } else {
        response = "OK\n";
        response += "TIME: " + std::to_string(result.stats.execution_time_ms) + "ms\n";
        response += "ROWS_SCANNED: " + std::to_string(result.stats.rows_scanned) + "\n";
        response += "ROWS_RETURNED: " + std::to_string(result.stats.rows_returned) + "\n";
        if (result.stats.cache_hit) response += "CACHE: HIT\n";

        if (!result.rows.empty()) {
            response += "---\n";
            for (const auto& row : result.rows) {
                for (size_t i = 0; i < row.values.size(); i++) {
                    response += row.values[i];
                    if (i < row.values.size() - 1) response += " | ";
                }
                response += "\n";
            }
        }
    }

    response += "<EOF>\n";
    return response;
}

} // namespace flexql
