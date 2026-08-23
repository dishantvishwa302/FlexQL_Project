#include "../../include/flexql.h"
#include <cstring>
#include <cstdlib>
#include <string>
#include <vector>
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

    const char *ip = host;
    if (strcmp(host, "localhost") == 0) ip = "127.0.0.1";

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

    if (inet_pton(AF_INET, ip, &server_addr.sin_addr) <= 0) {
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

static std::string recv_response(int fd) {
    std::string response;
    char buf[4096];
    while (true) {
        int n = recv(fd, buf, sizeof(buf) - 1, 0);
        if (n <= 0) break;
        response.append(buf, static_cast<size_t>(n));
        size_t eof_pos = response.find("<EOF>\n");
        if (eof_pos != std::string::npos) {
            response.erase(eof_pos);
            break;
        }
    }
    return response;
}

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

    std::string query_to_send = std::string(sql) + "\n<EOF>\n";
    if (send(db->socket_fd, query_to_send.c_str(), query_to_send.length(), 0) < 0) {
        if (errmsg) *errmsg = strdup("Send failed");
        return FLEXQL_ERROR;
    }

    std::string response = recv_response(db->socket_fd);
    if (response.empty()) {
        if (errmsg) *errmsg = strdup("No response from server");
        return FLEXQL_ERROR;
    }

    if (response.compare(0, 5, "ERROR") == 0) {
        if (errmsg) {
            std::string msg = response.size() > 7 ? response.substr(7) : response;
            while (!msg.empty() && (msg.back() == '\n' || msg.back() == '\r')) msg.pop_back();
            *errmsg = strdup(msg.c_str());
        }
        return FLEXQL_ERROR;
    }

    if (!callback) return FLEXQL_OK;

    std::vector<std::string> col_name_bufs;
    size_t col_line = response.find("COLUMNS: ");
    if (col_line != std::string::npos) {
        size_t eol = response.find('\n', col_line);
        std::string names = response.substr(col_line + 9, eol - col_line - 9);
        col_name_bufs = split(names, " | ");
    }

    size_t sep_pos = response.find("---\n");
    if (sep_pos == std::string::npos) return FLEXQL_OK;

    std::string rows_section = response.substr(sep_pos + 4);
    std::vector<std::string> row_lines = split(rows_section, "\n");

    for (const auto& line : row_lines) {
        if (line.empty()) continue;

        std::vector<std::string> cells = split(line, " | ");
        int col_count = static_cast<int>(cells.size());

        if (col_name_bufs.empty()) {
            for (int i = 0; i < col_count; i++) {
                col_name_bufs.push_back("col" + std::to_string(i));
            }
        }

        std::vector<char*> values(col_count);
        std::vector<char*> names(col_count);
        for (int i = 0; i < col_count; i++) {
            values[i] = const_cast<char*>(cells[i].c_str());
            names[i] = const_cast<char*>(col_name_bufs[static_cast<size_t>(i) < col_name_bufs.size()
                                                           ? static_cast<size_t>(i)
                                                           : 0].c_str());
        }

        if (callback(arg, col_count, values.data(), names.data()) != 0) break;
    }

    return FLEXQL_OK;
}

void flexql_free(void *ptr) {
    free(ptr);
}
