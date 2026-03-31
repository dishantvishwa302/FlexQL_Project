#include "../../include/flexql.h"
#include <cstring>
#include <cstdlib>
#include <string>
#include <vector>
#include <sstream>
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <unistd.h>

struct FlexQL {
    int socket_fd;
    char host[256];
    int port;
};

int flexql_open(const char *host, int port, FlexQL **db) {
    if (!host || !db) return FLEXQL_ERROR;

    FlexQL *database = (FlexQL *)malloc(sizeof(FlexQL));
    if (!database) return FLEXQL_ERROR;

    database->socket_fd = socket(AF_INET, SOCK_STREAM, 0);
    if (database->socket_fd < 0) {
        free(database);
        return FLEXQL_ERROR;
    }

    struct sockaddr_in server_addr;
    memset(&server_addr, 0, sizeof(server_addr));
    server_addr.sin_family = AF_INET;
    server_addr.sin_port = htons(port);

    if (inet_pton(AF_INET, host, &server_addr.sin_addr) <= 0) {
        close(database->socket_fd);
        free(database);
        return FLEXQL_ERROR;
    }

    if (connect(database->socket_fd, (struct sockaddr*)&server_addr, sizeof(server_addr)) < 0) {
        close(database->socket_fd);
        free(database);
        return FLEXQL_ERROR;
    }

    strncpy(database->host, host, 255);
    database->host[255] = '\0';
    database->port = port;
    *db = database;
    return FLEXQL_OK;
}

int flexql_close(FlexQL *db) {
    if (!db) return FLEXQL_ERROR;
    if (db->socket_fd >= 0) {
        close(db->socket_fd);
    }
    free(db);
    return FLEXQL_OK;
}

// Helper: receive complete response from server (reads until connection closes or delimiter)
static std::string recv_response(int fd) {
    std::string response;
    char buf[4096];
    while (true) {
        int n = recv(fd, buf, sizeof(buf) - 1, 0);
        if (n <= 0) break;
        buf[n] = '\0';
        response += std::string(buf, n);
        
        if (response.find("<EOF>\n") != std::string::npos) {
            // Remove the marker before returning
            response.erase(response.length() - 6);
            break;
        }
    }
    return response;
}

// Helper: split string by delimiter
static std::vector<std::string> split(const std::string& s, const std::string& delim) {
    std::vector<std::string> parts;
    size_t start = 0, pos;
    while ((pos = s.find(delim, start)) != std::string::npos) {
        parts.push_back(s.substr(start, pos - start));
        start = pos + delim.size();
    }
    parts.push_back(s.substr(start));
    return parts;
}

int flexql_exec(FlexQL *db, const char *sql,
    int (*callback)(void*, int, char**, char**),
    void *arg, char **errmsg) {

    if (!db || !sql) return FLEXQL_ERROR;

    // Send query
    std::string query_to_send = std::string(sql) + "\n<EOF>\n";
    if (send(db->socket_fd, query_to_send.c_str(), query_to_send.length(), 0) < 0) {
        if (errmsg) *errmsg = strdup("Send failed");
        return FLEXQL_ERROR;
    }

    // Receive complete response
    std::string response = recv_response(db->socket_fd);
    if (response.empty()) {
        if (errmsg) *errmsg = strdup("No response from server");
        return FLEXQL_ERROR;
    }

    // Parse response wire format:
    // OK\nTIME: Xms\nROWS_SCANNED: N\nROWS_RETURNED: M\n---\ncol1 | col2 | ...\n...
    // or ERROR: message\n

    if (response.length() >= 5 && response.substr(0, 5) == "ERROR") {
        if (errmsg) {
            std::string msg = response.substr(7); // skip "ERROR: "
            // Remove trailing newline
            while (!msg.empty() && (msg.back() == '\n' || msg.back() == '\r'))
                msg.pop_back();
            *errmsg = strdup(msg.c_str());
        }
        return FLEXQL_ERROR;
    }

    // Find the row data section (after "---\n")
    size_t sep_pos = response.find("---\n");
    if (sep_pos == std::string::npos || !callback) {
        // No rows or no callback needed
        return FLEXQL_OK;
    }

    // Extract the rows section
    std::string rows_section = response.substr(sep_pos + 4);

    // Parse each row line: "val1 | val2 | val3\n"
    // We need column names too — extract from ROWS_RETURNED onwards
    // The server currently does not send column names in a dedicated line.
    // We'll use positional column names col0, col1, ... as placeholders.
    // (Full column names require a schema query — adequately handled by the REPL)
    std::vector<std::string> row_lines = split(rows_section, "\n");

    for (const auto& line : row_lines) {
        if (line.empty()) continue;

        // Split on " | "
        std::vector<std::string> cells = split(line, " | ");
        int col_count = (int)cells.size();

        // Build C-string arrays for callback
        std::vector<char*> values(col_count);
        std::vector<char*> col_names(col_count);
        std::vector<std::string> name_bufs(col_count);

        for (int i = 0; i < col_count; i++) {
            values[i] = const_cast<char*>(cells[i].c_str());
            name_bufs[i] = "col" + std::to_string(i);
            col_names[i] = const_cast<char*>(name_bufs[i].c_str());
        }

        int ret = callback(arg, col_count, values.data(), col_names.data());
        if (ret != 0) {
            // Callback requested abort
            break;
        }
    }

    return FLEXQL_OK;
}

void flexql_free(void *ptr) {
    free(ptr);
}
