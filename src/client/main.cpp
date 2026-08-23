#include "../../include/flexql.h"
#include <iostream>
#include <string>
#include <chrono>
#include <cstdlib>
#include <cctype>

namespace {

struct PrintState {
    bool header_printed = false;
    int rows = 0;
};

int print_row(void* arg, int col_count, char** values, char** names) {
    auto* state = static_cast<PrintState*>(arg);
    if (!state->header_printed) {
        for (int i = 0; i < col_count; i++) {
            if (i) std::cout << " | ";
            std::cout << (names[i] ? names[i] : "");
        }
        std::cout << "\n─────────────────────────────────────────────────────────────\n";
        state->header_printed = true;
    }
    for (int i = 0; i < col_count; i++) {
        if (i) std::cout << " | ";
        std::cout << (values[i] ? values[i] : "NULL");
    }
    std::cout << "\n";
    state->rows++;
    return 0;
}

void trim(std::string& s) {
    size_t start = s.find_first_not_of(" \t\r\n");
    if (start == std::string::npos) {
        s.clear();
        return;
    }
    size_t end = s.find_last_not_of(" \t\r\n");
    s = s.substr(start, end - start + 1);
}

void displayHeader() {
    std::cout << "\n╔════════════════════════════════════════════════════════════╗\n";
    std::cout << "║           FlexQL Client v1.0                              ║\n";
    std::cout << "╚════════════════════════════════════════════════════════════╝\n\n";
}

void displayHelp() {
    std::cout <<
        "Commands (SQL subset from the assignment + interview extras)\n"
        "\n"
        "  CREATE TABLE name (col TYPE [PRIMARY KEY], ...)\n"
        "  INSERT INTO name VALUES (v1, v2, ...), (...)\n"
        "  SELECT col, ... FROM name [WHERE col op val] [ORDER BY col [ASC|DESC]] [LIMIT n]\n"
        "  SELECT * FROM a INNER JOIN b ON a.col = b.col [WHERE ...] [LIMIT n]\n"
        "  DELETE FROM name [WHERE col op val]\n"
        "  INSERT ... WITH TTL seconds     -- row expires after N seconds\n"
        "\n"
        "  Types: INT, DECIMAL, VARCHAR, DATETIME\n"
        "  help / exit / clear\n"
        "\n"
        "Example:\n"
        "  CREATE TABLE users (id INT PRIMARY KEY, name VARCHAR, age INT);\n"
        "  INSERT INTO users VALUES (1, 'Alice', 22), (2, 'Bob', 30);\n"
        "  SELECT * FROM users WHERE id = 1;\n"
        "  SELECT * FROM users;   -- run twice to see the LRU cache\n\n";
}

} // namespace

int main(int argc, char** argv) {
    std::string host = "127.0.0.1";
    int port = 9000;

    if (argc > 1) host = argv[1];
    if (argc > 2) {
        try {
            port = std::stoi(argv[2]);
        } catch (...) {
            std::cerr << "Usage: " << argv[0] << " [host] [port]\n";
            return 1;
        }
    }

    displayHeader();
    std::cout << "Connecting to " << host << ":" << port << "...\n";

    FlexQL* db = nullptr;
    if (flexql_open(host.c_str(), port, &db) != FLEXQL_OK) {
        std::cerr << "Could not connect. Start the server first: ./bin/flexql_server\n";
        return 1;
    }

    std::cout << "Connected. REPL uses flexql_open / flexql_exec / flexql_close.\n\n";
    displayHelp();

    std::string input;
    while (true) {
        std::cout << "FlexQL> ";
        if (!std::getline(std::cin, input)) break;
        trim(input);
        if (input.empty()) continue;

        if (input == "exit" || input == "quit") {
            flexql_close(db);
            std::cout << "Goodbye.\n";
            return 0;
        }
        if (input == "help") {
            displayHelp();
            continue;
        }
        if (input == "clear") {
            int clear_rc = std::system("clear");
            (void)clear_rc;
            displayHeader();
            continue;
        }

        PrintState state;
        char* errmsg = nullptr;
        auto t0 = std::chrono::high_resolution_clock::now();
        int rc = flexql_exec(db, input.c_str(), print_row, &state, &errmsg);
        auto t1 = std::chrono::high_resolution_clock::now();
        long long ms = std::chrono::duration_cast<std::chrono::milliseconds>(t1 - t0).count();

        if (rc != FLEXQL_OK) {
            std::cout << "ERROR: " << (errmsg ? errmsg : "unknown") << "\n\n";
            if (errmsg) flexql_free(errmsg);
            continue;
        }

        if (state.rows == 0) {
            std::cout << "OK";
        } else {
            std::cout << state.rows << " row(s)";
        }
        std::cout << "  (" << ms << " ms)\n\n";
    }

    flexql_close(db);
    return 0;
}
